//! 代理注册与公网监听（服务端侧）。
//!
//! 注册成功后按类型分发：TCP 起独立 accept 循环；UDP 起 UDP 监听 + 会话表
//! （见 [`crate::udp`]）；HTTP/HTTPS 不占独立端口，只登记域名走共享 vhost 监听
//! （见 [`crate::vhost`]）。TCP 每来一个用户连接即分配 work_id、登记待处理项、
//! 向客户端发 `ReqWorkConn`（见 DESIGN §8.2）。

use std::sync::Arc;

use rfrp_common::config::ServerConfig;
use rfrp_common::constants::*;
use rfrp_common::protocol::msg::*;
use rfrp_common::util::accept::AcceptRetry;
use rfrp_common::util::bridge::bridge;
use rfrp_common::util::counting::{CountingStream, ExtraCounters};
use rfrp_common::util::stream::{AsyncStream, BoxedStream, PrependStream};
use rfrp_common::util::tcp::configure_tcp_stream;
use tokio::io::{AsyncRead, AsyncWriteExt, ReadBuf};
use tokio::net::TcpListener;
use tokio::time::Duration;

use crate::control::{ProxyEntry, Session};
use crate::state::{PendingWork, ServerState};
use rfrp_common::util::control::{send_with_timeout, try_send};

/// 端口是否在 `allow_ports` 允许范围内（fail-closed：配置解析失败视为不允许）。
fn port_allowed(config: &ServerConfig, port: u16) -> bool {
    config.proxy.is_port_allowed(port).unwrap_or_else(|e| {
        tracing::error!(error = %e, "invalid allow_ports config; rejecting proxy registration");
        false
    })
}

/// 注册代理。
///
/// 所有类型共用同一套前置校验与登记流程，只有"句柄从哪来"不同：
/// - TCP：在 `remote_port` 上起独立监听；
/// - UDP：在 `remote_port` 上起 UDP 监听 + 会话表；
/// - HTTP/HTTPS：不绑定端口，走共享 vhost 监听，仅登记域名。
pub async fn register_proxy(
    np: &NewProxy,
    session: &Arc<Session>,
    state: &Arc<ServerState>,
    config: &ServerConfig,
) -> std::result::Result<(), ProxyError> {
    // 会话内代理数上限：认证客户端也不得无限占用端口/内存。
    if session.proxies.lock().len() >= MAX_PROXIES_PER_SESSION {
        tracing::warn!(proxy = %np.proxy_name, "proxy limit per session reached");
        return Err(ProxyError::TooManyProxies);
    }
    // 同名代理一律拒绝，避免静默覆盖旧条目。
    if session.proxies.lock().contains_key(&np.proxy_name) {
        return Err(ProxyError::NameExists);
    }

    // 域名统一小写归一化：vhost 路由按小写 Host 查表（见 vhost::route_and_dispatch），
    // 若按原样登记，配置中含大写字母的域名将永远无法命中；大小写不同的同名域名
    // 也会绕过冲突检测。归一化后路由与冲突判定使用同一表示。
    let domains: Vec<String> = np
        .custom_domains
        .clone()
        .unwrap_or_default()
        .into_iter()
        .map(|d| d.to_lowercase())
        .collect();
    if matches!(np.r#type, ProxyType::Http | ProxyType::Https) && domains.is_empty() {
        return Err(ProxyError::InvalidField);
    }
    // 域名全局唯一（DESIGN §6.6）：所有类型统一校验，避免 TCP/UDP 代理抢占域名后，
    // 同名 vhost 请求被路由到类型不匹配的代理（用户只会看到 404）。
    // 冲突细节（域名/占用者）只写服务端日志，不回显给对端。
    for d in &domains {
        if let Some((_, owner)) = state.session_for_domain(d) {
            tracing::warn!(
                proxy = %np.proxy_name, domain = %d, owner = %owner,
                "vhost domain conflict, registration rejected"
            );
            return Err(ProxyError::DomainConflict);
        }
    }

    let kind = np.r#type;
    let handle = match kind {
        // vhost 代理不绑定独立端口，仅登记域名与元信息（共享 vhost 监听在 Server 启动时创建）。
        ProxyType::Http | ProxyType::Https => tokio::spawn(async {}),
        ProxyType::Udp => {
            let remote_port = np.remote_port.ok_or(ProxyError::InvalidField)?;
            if !port_allowed(config, remote_port) {
                tracing::warn!(
                    proxy = %np.proxy_name, remote_port,
                    allow_ports = %config.proxy.allow_ports,
                    "udp remote_port not allowed by [proxy].allow_ports"
                );
                return Err(ProxyError::PortNotAllowed);
            }
            crate::udp::register_udp_proxy(
                np.proxy_name.clone(),
                remote_port,
                session,
                state,
                &config.server.bind_addr,
                config.server.udp_session_timeout(),
            )
            .await?
        }
        ProxyType::Tcp => {
            let remote_port = np.remote_port.ok_or(ProxyError::InvalidField)?;
            if !port_allowed(config, remote_port) {
                tracing::warn!(
                    proxy = %np.proxy_name, remote_port,
                    allow_ports = %config.proxy.allow_ports,
                    "tcp remote_port not allowed by [proxy].allow_ports"
                );
                return Err(ProxyError::PortNotAllowed);
            }
            let listener =
                match TcpListener::bind((config.server.bind_addr.as_str(), remote_port)).await {
                    Ok(l) => l,
                    Err(e) => {
                        // 具体原因只写服务端日志（DESIGN §8.5）：端口占用可重试，
                        // 权限不足等归为不可重试的内部错误。
                        tracing::warn!(
                            proxy = %np.proxy_name, remote_port, error = %e,
                            "failed to bind proxy port"
                        );
                        return Err(if e.kind() == std::io::ErrorKind::AddrInUse {
                            ProxyError::PortOccupied
                        } else {
                            ProxyError::Internal
                        });
                    }
                };
            let proxy_name = np.proxy_name.clone();
            let session = session.clone();
            let state_loop = state.clone();
            tokio::spawn(async move {
                proxy_accept_loop(listener, proxy_name, session, state_loop).await;
            })
        }
    };

    // 登记：vhost 域名映射 + 会话内条目 + 全局归属索引。
    if !domains.is_empty() {
        {
            let mut map = session.proxy_domains.lock();
            for d in &domains {
                map.insert(d.clone(), np.proxy_name.clone());
            }
        }
        // 全局索引在会话锁之外写入，避免 `proxy_domains → domain_index` 嵌套。
        for d in &domains {
            state.index_domain(d, &session.run_id, &np.proxy_name);
        }
    }
    session.proxies.lock().insert(
        np.proxy_name.clone(),
        ProxyEntry {
            handle,
            kind,
            remote_port: np.remote_port,
            custom_domains: domains,
        },
    );
    state.index_proxy(&np.proxy_name, &session.run_id);
    match kind {
        ProxyType::Udp => {
            tracing::info!(proxy = %np.proxy_name, remote_port = ?np.remote_port, "proxy registered (udp)")
        }
        ProxyType::Tcp => {
            tracing::info!(proxy = %np.proxy_name, remote_port = ?np.remote_port, "proxy registered (tcp)")
        }
        other => tracing::info!(proxy = %np.proxy_name, typ = ?other, "proxy registered (vhost)"),
    }
    Ok(())
}

async fn proxy_accept_loop(
    listener: TcpListener,
    proxy_name: String,
    session: Arc<Session>,
    state: Arc<ServerState>,
) {
    // accept 出错（EMFILE、握手期 reset 等）不得结束循环：该监听的生命周期绑定在
    // 控制会话上，自行退出会造成"端口没监听、进程却一切正常"的静默故障。
    let mut retry = AcceptRetry::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                match accepted {
                    Ok((user, peer)) => {
                        retry.record_ok();
                        if let Err(e) = configure_tcp_stream(&user) {
                            tracing::warn!(%proxy_name, %peer, error = %e, "failed to configure user TCP stream");
                        }
                        tracing::debug!(%proxy_name, %peer, "user connected");
                        dispatch_user_connection(proxy_name.clone(), user, session.clone(), state.clone());
                    }
                    Err(e) => {
                        let backoff = retry.record_err();
                        if retry.should_log() {
                            tracing::warn!(
                                %proxy_name,
                                consecutive = retry.consecutive(),
                                error = %e,
                                "proxy listener accept error; retrying"
                            );
                        }
                        tokio::time::sleep(backoff).await;
                    }
                }
            }
            _ = state.shutdown.cancelled() => {
                tracing::info!(%proxy_name, "shutdown requested, closing proxy listener");
                break;
            }
        }
    }
}

/// 从池中取出一个仍然存活的预热工作连接（跳过已被对端关闭的死连接）。
///
/// 本地服务常对空闲连接做超时踢除（sshd/RDP 均如此），池中预连接因此可能已死；
/// 直接使用会让用户连接立即被重置（表现为 Connection reset by peer）。
fn pop_live_pooled(session: &Session, proxy_name: &str) -> Option<BoxedStream> {
    loop {
        let work = {
            let mut pools = session.pools.lock();
            pools.get_mut(proxy_name).and_then(|v| v.pop())
        }?;
        match probe_alive(work) {
            Ok(work) => return Some(work),
            Err(()) => {
                tracing::debug!(%proxy_name, "discarded dead pooled work connection");
                continue;
            }
        }
    }
}

/// 非阻塞探活：无数据可读视为存活；EOF/错误视为已死；意外数据回灌后使用。
///
/// 使用 noop waker 做单次 poll，不注册唤醒（池取出路径本就在同步上下文中）。
fn probe_alive(mut work: BoxedStream) -> std::result::Result<BoxedStream, ()> {
    let mut one = [0u8; 1];
    let mut buf = ReadBuf::new(&mut one);
    let waker = futures::task::noop_waker();
    let mut cx = std::task::Context::from_waker(&waker);
    match std::pin::Pin::new(&mut work).poll_read(&mut cx, &mut buf) {
        std::task::Poll::Pending => Ok(work),
        std::task::Poll::Ready(Ok(())) => {
            if buf.filled().is_empty() {
                Err(()) // EOF：对端已关闭
            } else {
                // 预连接不应有数据；出现则回灌，保证不丢字节
                let data = buf.filled().to_vec();
                Ok(Box::new(PrependStream::new(data, work)))
            }
        }
        std::task::Poll::Ready(Err(_)) => Err(()),
    }
}

/// 统一处理一条用户连接：优先命中预热池，否则登记 pending 并按需请求工作连接。
/// 桥接与 ReqWorkConn 均放入独立任务，避免阻塞 accept 循环。
pub(crate) fn dispatch_user_connection<S>(
    proxy_name: String,
    user: S,
    session: Arc<Session>,
    state: Arc<ServerState>,
) where
    S: AsyncStream + 'static,
{
    // 并发连接数兜底（防 DoS）：原子地检查并递增，避免并发尖峰突破上限。
    let max = state.max_active.load(std::sync::atomic::Ordering::Relaxed);
    let accepted = state
        .metrics
        .active_connections
        .fetch_update(
            std::sync::atomic::Ordering::Relaxed,
            std::sync::atomic::Ordering::Relaxed,
            |cur| {
                if cur >= max {
                    None
                } else {
                    Some(cur + 1)
                }
            },
        )
        .is_ok();
    if !accepted {
        tracing::warn!(%proxy_name, "too many active connections, rejecting");
        return;
    }

    // 统计连接与流量。active 已在上面原子递增；CountingStream drop 时递减。
    let metrics = state.metrics.clone();
    metrics
        .total_connections
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    // 每代理统计（Dashboard/每代理指标）。
    let stats = state.proxy_stats_for(&proxy_name);
    stats
        .connections_total
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let user: BoxedStream = Box::new(
        CountingStream::new(
            user,
            metrics.bytes_up.clone(),
            metrics.bytes_down.clone(),
            metrics.active_connections.clone(),
        )
        .with_extra(ExtraCounters {
            read: stats.bytes_up.clone(),
            write: stats.bytes_down.clone(),
            active: stats.active_connections.clone(),
        }),
    );

    // 优先命中预热池（§8.2）。池中连接可能已被本地服务（sshd/RDP 等）
    // 在空闲时关闭，出池前先探活，避免用户连接被死连接立即重置。
    let pooled = pop_live_pooled(&session, &proxy_name);
    if let Some(work) = pooled {
        tracing::debug!(%proxy_name, "user connected; pool hit, bridging");
        let pname = proxy_name.clone();
        tokio::spawn(async move {
            let _ = bridge(user, work).await;
            tracing::debug!(proxy = %pname, "pooled work bridge finished");
        });
        // 立即请求补充预热连接（无需等待本次用户断开）；通道满时跳过本次补充。
        try_send(
            &session.tx,
            Message::ReqWorkConn(ReqWorkConn {
                proxy_name,
                work_id: WORK_ID_POOL_RESERVED,
            }),
        );
        return;
    }

    let work_id = state.next_work_id();
    tracing::debug!(%proxy_name, work_id, "user connected (on-demand)");
    state.pending.lock().insert(
        work_id,
        PendingWork {
            proxy_name: proxy_name.clone(),
            session_id: session.session_id.clone(),
            user: Some(user),
            created: std::time::Instant::now(),
        },
    );

    let tx = session.tx.clone();
    let state2 = state.clone();
    let interval = state.pending_request_interval();
    tokio::spawn(async move {
        // 超时兜底由 Server::run 里的单周期扫描任务负责（见 sweep_expired_pending）。
        // 这里在超时窗口内**周期性重发** ReqWorkConn：工作连接建立失败（客户端本地服务
        // 瞬时不可用、建连抖动）时，单次请求会让用户连接白等超时被关闭。
        let deadline = std::time::Instant::now() + Duration::from_secs(WORK_CONN_TIMEOUT_RFRPS);
        loop {
            if !send_with_timeout(
                &tx,
                Message::ReqWorkConn(ReqWorkConn {
                    proxy_name: proxy_name.clone(),
                    work_id,
                }),
            )
            .await
            {
                // 控制连接已断或通道拥堵，清理待处理项（避免任务堆积）。
                state2.pending.lock().remove(&work_id);
                return;
            }
            let now = std::time::Instant::now();
            if now >= deadline {
                return; // 交给扫描任务关闭用户连接
            }
            tokio::time::sleep(interval.min(deadline - now)).await;
            // 已被工作连接消费（或已被清理）→ 结束。
            if !state2.pending.lock().contains_key(&work_id) {
                return;
            }
        }
    });
}

/// 单次扫描：移除超过 `timeout` 仍未配对的待处理项，关闭其用户连接，返回清理条数。
///
/// 用**一个**周期任务扫描整张表，替代"每个用户连接派生一个 sleep 任务"——后者在
/// 高连接速率下会同时存在大量睡眠任务，调度与内存开销随连接数增长（见 DESIGN §8.5）。
pub(crate) async fn sweep_expired_pending(state: &Arc<ServerState>, timeout: Duration) -> usize {
    let now = std::time::Instant::now();
    let expired: Vec<BoxedStream> = {
        let mut pending = state.pending.lock();
        let ids: Vec<u64> = pending
            .iter()
            .filter(|(_, p)| now.duration_since(p.created) >= timeout)
            .map(|(id, _)| *id)
            .collect();
        ids.into_iter()
            .filter_map(|id| pending.remove(&id))
            .filter_map(|pw| pw.user)
            .collect()
    };
    let n = expired.len();
    for mut user in expired {
        let _ = user.shutdown().await;
    }
    n
}

#[cfg(test)]
mod tests;
