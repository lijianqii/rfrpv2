//! 代理公网监听（服务端侧，当前仅 TCP；UDP/HTTP/HTTPS 在 M4 扩展）。
//!
//! 注册成功后为每个 `remote_port` 起一个 accept 循环：每来一个用户连接，
//! 分配 work_id、登记待处理项、向客户端发 `ReqWorkConn`（见 DESIGN §8.2）。

use std::sync::Arc;

use rfrp_common::config::ServerConfig;
use rfrp_common::protocol::msg::*;
use rfrp_common::util::bridge::bridge;
use rfrp_common::util::counting::CountingStream;
use rfrp_common::util::stream::BoxedStream;
use rfrp_common::util::tcp::configure_tcp_stream;
use rfrp_common::{constants::*, error::Result};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::time::{sleep, Duration};

use crate::control::{ProxyEntry, Session};
use crate::state::{PendingWork, ServerState};
use crate::vhost::find_proxy_by_domain;
use rfrp_common::util::control::{send_with_timeout, try_send};

/// 注册代理：TCP 在 `remote_port` 起监听；HTTP 走共享 vhost 监听，仅登记域名。
pub async fn register_proxy(
    np: &NewProxy,
    session: &Arc<Session>,
    state: &Arc<ServerState>,
    config: &ServerConfig,
) -> Result<()> {
    if matches!(np.r#type, ProxyType::Http | ProxyType::Https) {
        // vhost 代理：不绑定独立端口，仅登记域名与元信息（共享 vhost 监听已在 Server 启动）。
        let domains = np.custom_domains.as_ref().ok_or_else(|| {
            rfrp_common::Error::Config("http/https proxy requires custom_domains".into())
        })?;
        // 域名全局唯一：与其他代理冲突则拒绝（DESIGN §6.6）。
        for d in domains {
            if let Some((_, owner)) = find_proxy_by_domain(state, d) {
                return Err(rfrp_common::Error::Config(format!(
                    "domain conflict: {d} owned by {owner}"
                )));
            }
        }
        let mut map = session.proxy_domains.lock().unwrap();
        for d in domains {
            map.insert(d.clone(), np.proxy_name.clone());
        }
        let handle = tokio::spawn(async {});
        session.proxies.lock().unwrap().insert(
            np.proxy_name.clone(),
            ProxyEntry {
                handle,
                kind: np.r#type,
            },
        );
        state.index_proxy(&np.proxy_name, &session.run_id);
        tracing::info!(proxy = %np.proxy_name, typ = ?np.r#type, "proxy registered (vhost)");
        return Ok(());
    }
    if np.r#type == ProxyType::Udp {
        let remote_port = np
            .remote_port
            .ok_or_else(|| rfrp_common::Error::Config("udp proxy requires remote_port".into()))?;
        if !config.proxy.is_port_allowed(remote_port)? {
            return Err(rfrp_common::Error::Config("port not allowed".into()));
        }
        {
            let proxies = session.proxies.lock().unwrap();
            if proxies.contains_key(&np.proxy_name) {
                return Err(rfrp_common::Error::Config("proxy_name exists".into()));
            }
        }
        let handle = crate::udp::register_udp_proxy(
            np.proxy_name.clone(),
            remote_port,
            session,
            state,
            &config.server.bind_addr,
        )
        .await?;
        session.proxies.lock().unwrap().insert(
            np.proxy_name.clone(),
            ProxyEntry {
                handle,
                kind: ProxyType::Udp,
            },
        );
        state.index_proxy(&np.proxy_name, &session.run_id);
        tracing::info!(proxy = %np.proxy_name, remote_port, "proxy registered (udp)");
        return Ok(());
    }
    if np.r#type != ProxyType::Tcp {
        return Err(rfrp_common::Error::Config("unsupported proxy type".into()));
    }
    let remote_port = np
        .remote_port
        .ok_or_else(|| rfrp_common::Error::Config("tcp proxy requires remote_port".into()))?;
    if !config.proxy.is_port_allowed(remote_port)? {
        return Err(rfrp_common::Error::Config("port not allowed".into()));
    }
    {
        let proxies = session.proxies.lock().unwrap();
        if proxies.contains_key(&np.proxy_name) {
            return Err(rfrp_common::Error::Config("proxy_name exists".into()));
        }
    }

    let listener = TcpListener::bind((config.server.bind_addr.as_str(), remote_port)).await;
    let listener = match listener {
        Ok(l) => l,
        // 端口占用/权限问题不回显具体原因（见 DESIGN §8.5）。
        Err(_) => return Err(rfrp_common::Error::Config("internal error".into())),
    };

    let proxy_name = np.proxy_name.clone();
    let session = session.clone();
    let session_for_insert = session.clone();
    let state_loop = state.clone();
    let handle = tokio::spawn(async move {
        proxy_accept_loop(listener, proxy_name, session, state_loop).await;
    });
    session_for_insert.proxies.lock().unwrap().insert(
        np.proxy_name.clone(),
        ProxyEntry {
            handle,
            kind: np.r#type,
        },
    );
    if let Some(domains) = &np.custom_domains {
        let mut map = session_for_insert.proxy_domains.lock().unwrap();
        for d in domains {
            map.insert(d.clone(), np.proxy_name.clone());
        }
    }
    state.index_proxy(&np.proxy_name, &session_for_insert.run_id);
    tracing::info!(proxy = %np.proxy_name, remote_port = ?np.remote_port, "proxy registered (tcp)");
    Ok(())
}

async fn proxy_accept_loop(
    listener: TcpListener,
    proxy_name: String,
    session: Arc<Session>,
    state: Arc<ServerState>,
) {
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                match accepted {
                    Ok((user, peer)) => {
                        if let Err(e) = configure_tcp_stream(&user) {
                            tracing::warn!(%proxy_name, %peer, error = %e, "failed to configure user TCP stream");
                        }
                        tracing::debug!(%proxy_name, %peer, "user connected");
                        dispatch_user_connection(proxy_name.clone(), Box::new(user), session.clone(), state.clone());
                    }
                    Err(e) => {
                        tracing::warn!("proxy listener accept error: {e}");
                        break;
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

/// 统一处理一条用户连接：优先命中预热池，否则登记 pending 并按需请求工作连接。
/// 桥接与 ReqWorkConn 均放入独立任务，避免阻塞 accept 循环。
pub(crate) fn dispatch_user_connection(
    proxy_name: String,
    user: BoxedStream,
    session: Arc<Session>,
    state: Arc<ServerState>,
) {
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

    // 统计连接与流量（M5）。active 已在上面原子递增；CountingStream drop 时递减。
    let metrics = state.metrics.clone();
    metrics
        .total_connections
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let user = Box::new(CountingStream::new(
        user,
        metrics.bytes_up.clone(),
        metrics.bytes_down.clone(),
        metrics.active_connections.clone(),
    ));

    // 优先命中预热池（§8.2）。
    let pooled = {
        let mut pools = session.pools.lock().unwrap();
        pools.get_mut(&proxy_name).and_then(|v| v.pop())
    };
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
    state.pending.lock().unwrap().insert(
        work_id,
        PendingWork {
            proxy_name: proxy_name.clone(),
            session_id: session.session_id.clone(),
            user: Some(user),
        },
    );

    let tx = session.tx.clone();
    let state2 = state.clone();
    tokio::spawn(async move {
        if !send_with_timeout(
            &tx,
            Message::ReqWorkConn(ReqWorkConn {
                proxy_name,
                work_id,
            }),
        )
        .await
        {
            // 控制连接已断或通道拥堵，清理待处理项（避免任务堆积）。
            state2.pending.lock().unwrap().remove(&work_id);
            return;
        }
        // 超时兜底：用户连接长时间等不到工作连接则关闭（见 DESIGN §8.5）。
        spawn_pending_timeout(work_id, state2);
    });
}

/// 超时清理：WORK_CONN_TIMEOUT_RFRPS 后仍未消费则关闭用户连接。
fn spawn_pending_timeout(work_id: u64, state: Arc<ServerState>) {
    tokio::spawn(async move {
        sleep(Duration::from_secs(WORK_CONN_TIMEOUT_RFRPS)).await;
        let user = {
            let mut p = state.pending.lock().unwrap();
            p.remove(&work_id).and_then(|pw| pw.user)
        };
        if let Some(mut u) = user {
            let _ = u.shutdown().await;
        }
    });
}

#[cfg(test)]
mod tests;
