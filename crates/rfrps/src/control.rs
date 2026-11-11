//! 控制连接处理（服务端侧）。
//!
//! 收到 Login 后建立 Session，进入控制循环：处理 NewProxy（注册公网监听）、
//! Heartbeat/HeartbeatResp（保活）、Close。`ReqWorkConn` 由监听任务经 `session.tx` 发往写任务。
//! 收发通过 split 后的 `FramedRead`/`FramedWrite` 并发（见 DESIGN §6.1）。
//!
//! 心跳（§8.3）：本端周期性发送 `Heartbeat`，若 `HEARTBEAT_TIMEOUT` 内未收到对端
//! `HeartbeatResp`，经 `Notify` 通知控制循环断开并清理 Session。
//!
//! 会话停止信号用 `CancellationToken`（见 [`Session::stop`]）：`Notify::notify_waiters()`
//! 不存储许可，任务未恰好在等待时会丢信号；`CancellationToken::cancelled()` 每次 poll
//! 都重新检查标志位，不会漏掉取消。

use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use rfrp_common::auth::verify_token;
use rfrp_common::config::ServerConfig;
use rfrp_common::constants::{
    negotiate_protocol_version, MAX_CUSTOM_DOMAINS, MAX_DOMAIN_LEN, MAX_PROXY_NAME_LEN,
    MAX_RUN_ID_LEN, MAX_TOKEN_LEN, PROTOCOL_VERSIONS_SUPPORTED,
};
use rfrp_common::error::Result;
use rfrp_common::protocol::frame::{Frame, FrameCodec, FramedRead, FramedWrite};
use rfrp_common::protocol::msg::*;
use rfrp_common::util::control::{graceful_close, send_with_timeout, try_send};
use rfrp_common::util::now_ms;
use tokio::io::split;
use tokio::sync::mpsc;
use tokio::sync::Notify;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;

use crate::listener;
use crate::state::ServerState;

mod session;
#[cfg(test)]
pub(crate) use session::{test_entry, test_entry_with, test_session, test_session_with_token};
pub use session::{ProxyEntry, Session};

/// 拒绝登录并下发失败 LoginResp（`error=None` 表示鉴权失败，不回显原因，§10.2）。
pub(crate) async fn reject_login<S>(stream: S, error: Option<&str>) -> Result<()>
where
    S: tokio::io::AsyncWrite + Unpin,
{
    let mut w = FramedWrite::new(stream, FrameCodec);
    // 拒绝响应也要有超时：对端不读时不得挂住任务（单帧通常可入缓冲，此处仅兜底）。
    let _ = tokio::time::timeout(
        Duration::from_secs(rfrp_common::util::control::CONTROL_SEND_TIMEOUT),
        w.send(
            Message::LoginResp(LoginResp {
                ok: false,
                error: error.map(String::from),
                session_id: None,
                work_conn_tls: None,
                work_conn_token: None,
                protocol_version: None,
            })
            .to_frame()?,
        ),
    )
    .await;
    Ok(())
}

/// 控制连接主循环。`S` 为任意双向流（生产用 `TcpStream`，测试用 `DuplexStream`）。
pub async fn handle_control_login<S>(
    login_frame: Frame,
    stream: S,
    peer_ip: std::net::IpAddr,
    state: Arc<ServerState>,
    config: ServerConfig,
    heartbeat_interval: Duration,
    heartbeat_timeout: Duration,
) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    // 同一 IP 的“检查限速 → 校验 token → 记录失败/清除”必须串行；
    // 否则并发尝试可以同时通过检查，再一起记录失败，突破失败次数上限。
    let login_guard = state.login_lock(peer_ip).lock().await;

    // 登录失败限速（防 token 穷举）：超限直接拒绝，且不区分失败原因。
    if !state.login_allowed(peer_ip) {
        drop(login_guard);
        // 限速是临时状态，必须回一个可重试错误；不能用 `error=None`（客户端将其
        // 视为鉴权失败并永久退出，导致合法客户端被同 IP 的其他失败尝试连累）。
        tracing::warn!(%peer_ip, "login rate limited; rejecting attempt");
        reject_login(stream, Some("rate limited")).await?;
        return Ok(());
    }

    let login = Message::from_frame(&login_frame)?;
    let (run_id, version, token) = match login {
        Message::Login(l) => (l.run_id, l.version, l.token),
        _ => {
            return Err(rfrp_common::Error::Protocol(
                "first frame must be Login".into(),
            ))
        }
    };
    // 协议版本协商（DESIGN §6.4）：在支持区间 `[PROTOCOL_MIN_VERSION, PROTOCOL_VERSION]`
    // 内选定版本；区间外直接拒绝（不建立会话，客户端据此判定致命、不重连）。
    let negotiated_version = match negotiate_protocol_version(version) {
        Some(v) => v,
        None => {
            drop(login_guard);
            tracing::warn!(version, "login rejected: protocol version mismatch");
            reject_login(stream, Some(&version_mismatch_detail(version))).await?;
            return Ok(());
        }
    };
    // 字段上限校验（DESIGN §6.2.3）：run_id 必须是 UUID，token 长度受限。
    if uuid::Uuid::parse_str(&run_id).is_err()
        || run_id.len() > MAX_RUN_ID_LEN
        || token.len() > MAX_TOKEN_LEN
    {
        drop(login_guard);
        tracing::warn!(run_id = %run_id, "login rejected: invalid fields");
        reject_login(stream, None).await?;
        return Ok(());
    }

    // token 鉴权。鉴权失败不回显具体原因（DESIGN §10.2），客户端将 `ok=false + error=None` 视为致命鉴权失败。
    if !verify_token(&config.server.token, &token) {
        state.record_login_failure(peer_ip);
        drop(login_guard);
        tracing::warn!(run_id = %run_id, %peer_ip, "login rejected: token mismatch");
        reject_login(stream, None).await?;
        return Ok(());
    }
    state.clear_login_failures(peer_ip);
    drop(login_guard);

    let session_id = uuid::Uuid::new_v4().to_string();
    // 工作连接鉴权令牌：per-session 随机值，不写日志、不经 Dashboard 暴露。
    let work_conn_token = uuid::Uuid::new_v4().to_string();
    let (tx, mut rx) = mpsc::channel::<Message>(256);
    let session = Arc::new(Session {
        run_id,
        session_id: session_id.clone(),
        peer_ip,
        work_conn_token: work_conn_token.clone(),
        tx: tx.clone(),
        proxies: Mutex::new(HashMap::new()),
        proxy_domains: Mutex::new(HashMap::new()),
        stop: CancellationToken::new(),
        pools: Mutex::new(HashMap::new()),
        rtt_ms: std::sync::atomic::AtomicU64::new(0),
    });

    // 并发会话上限（全局 + 每来源 IP）：同一 run_id 的重连替换不计新增。
    // 检查与插入在同一把锁内完成，避免并发登录同时通过检查后一起插入。
    let slot = {
        let mut reg = state.sessions.lock();
        if reg.contains_key(&session.run_id) {
            Ok(reg.insert(session.run_id.clone(), session.clone()))
        } else if reg.len() >= state.max_sessions.load(Ordering::Relaxed) {
            Err("global limit reached")
        } else if reg.values().filter(|s| s.peer_ip == peer_ip).count()
            >= state.max_sessions_per_ip.load(Ordering::Relaxed)
        {
            Err("per-ip limit reached")
        } else {
            reg.insert(session.run_id.clone(), session.clone());
            Ok(None)
        }
    };
    let old = match slot {
        Ok(old) => old,
        Err(reason) => {
            tracing::warn!(%peer_ip, run_id = %session.run_id, %reason, "login rejected: session limit reached");
            reject_login(stream, Some("too many sessions")).await?;
            return Ok(());
        }
    };

    // 重连去重（§8.3）：同一 run_id 的旧会话先清理再接受新登录。
    // 注意：cleanup 在锁外执行，避免 sessions→proxy_index 锁序嵌套。
    if let Some(old) = old {
        cleanup(&old, &state);
    }

    let (read_half, write_half) = split(stream);
    let mut reader = FramedRead::new(read_half, FrameCodec);
    let mut writer = FramedWrite::new(write_half, FrameCodec);

    // 写任务：消费出站控制消息；服务端退出或会话被替换时尽量发送 TLS close_notify。
    let shutdown_writer = state.shutdown.clone();
    let stop_writer = session.stop.clone();
    // 回给客户端的帧用**协商结果**编码（DESIGN §6.4）：客户端的解码器只接受
    // 自己声明的版本区间，用更高版本编码会让低版本客户端直接拒收。
    let frame_version = negotiated_version;
    let mut writer_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                msg = rx.recv() => {
                    let Some(msg) = msg else {
                        // 通道关闭且处于退出流程：补发 Close 再 close_notify。
                        if shutdown_writer.is_cancelled() {
                            graceful_close(&mut writer, "server shutdown", frame_version).await;
                        }
                        break;
                    };
                    match msg.to_frame_with_version(frame_version) {
                        Ok(frame) => {
                            match tokio::time::timeout(
                                std::time::Duration::from_secs(rfrp_common::util::control::CONTROL_SEND_TIMEOUT),
                                writer.send(frame),
                            ).await {
                                Ok(Ok(())) => {}
                                Ok(Err(e)) => {
                                    tracing::warn!("control write error: {e}");
                                    break;
                                }
                                Err(_) => {
                                    tracing::warn!("control write timeout, breaking");
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!("control encode error: {e}");
                            break;
                        }
                    }
                }
                _ = shutdown_writer.cancelled() => {
                    // 服务端退出：先发 Close 帧再 TLS close_notify（DESIGN §6.2.2）。
                    graceful_close(&mut writer, "server shutdown", frame_version).await;
                    break;
                }
                _ = stop_writer.cancelled() => {
                    // 会话被停止（正常断开 / 被重连替换）：尽量发 Close 帧再 close_notify。
                    // 关闭原因按服务端退出令牌区分，便于对端排障。
                    let reason = if shutdown_writer.is_cancelled() {
                        "server shutdown"
                    } else {
                        "session closed"
                    };
                    graceful_close(&mut writer, reason, frame_version).await;
                    break;
                }
            }
        }
    });

    // 登录响应：下发服务端 work_conn_tls 偏好（DESIGN §6.5）。
    if !send_with_timeout(
        &tx,
        Message::LoginResp(LoginResp {
            ok: true,
            error: None,
            session_id: Some(session_id.clone()),
            work_conn_tls: Some(config.server.work_conn_tls),
            work_conn_token: Some(work_conn_token),
            protocol_version: Some(negotiated_version),
        }),
    )
    .await
    {
        tracing::warn!(session = %session_id, "login resp send failed/timeout");
        // 会话已登记：必须注销并清理，否则会留下幽灵会话（Dashboard 可见、run_id 被占）。
        unregister_session(&state, &session);
        cleanup(&session, &state);
        return Ok(());
    }
    tracing::info!(session = %session_id, "control connection established");

    // 心跳（§8.3）：周期性发送 Heartbeat，等待对端在 HEARTBEAT_TIMEOUT 内回应；
    // 超时未收到 HeartbeatResp 则通知断开。采用“每次发送后等待回应”的 ping/pong
    // 语义，避免“心跳间隔 > 超时阈值”时误判断连（客户端仅在收到 Heartbeat 时回应）。
    let disconnect = Arc::new(Notify::new());
    let pong = Arc::new(Notify::new());
    // 记录对端回传的最近心跳 ts：以"本轮是否被回应"判定超时，
    // 避免 Notify 许可残留（无等待者时 notify_one 会存许可）导致漏检一轮。
    let pong_ts = Arc::new(AtomicU64::new(0));
    // 最近一次发出的心跳 ts：HeartbeatResp 的 ts 不得超前于它，否则恶意/异常
    // 客户端可用 u64::MAX 让后续每轮心跳都“立即通过”，永久绕过超时检测。
    let last_sent_ts = Arc::new(AtomicU64::new(0));
    let hb_tx = tx.clone();
    let hb_pong = pong.clone();
    let hb_pong_ts = pong_ts.clone();
    let hb_last_sent = last_sent_ts.clone();
    let hb_disconnect = disconnect.clone();
    let session_id_hb = session_id.clone();
    let heartbeat_task = tokio::spawn(async move {
        let mut iv = interval(heartbeat_interval);
        iv.tick().await; // 消耗首次立即 tick，避免一建立就连发
        loop {
            iv.tick().await;
            let ts = now_ms();
            // 先切换到本轮 ts，再清零上一轮的 pong 标记：即使本轮发送前收到
            // 迟到的旧响应，也不会被误认为本轮已回应；同时避免墙钟回拨后
            // 旧的 pong_ts 大于新 ts 导致心跳永久“立即通过”。
            hb_last_sent.store(ts, Ordering::Relaxed);
            hb_pong_ts.store(0, Ordering::Relaxed);
            tracing::debug!(session = %session_id_hb, ts, "heartbeat sent");
            // 发送失败（出站通道已关闭，或写任务阻塞超过 CONTROL_SEND_TIMEOUT 导致通道满）
            // 说明控制写路径已不可用：此时等不到 pong，必须直接判定连接失效。
            // 旧实现此处 `continue` 跳过本轮，会让看门狗在写侧已死时永不触发——
            // 半开连接下 reader 也不会返回，表现为服务端不清理、客户端不重连。
            if !send_with_timeout(&hb_tx, Message::Heartbeat(Heartbeat { ts })).await {
                tracing::warn!(
                    session = %session_id_hb,
                    "heartbeat send failed; control connection considered dead"
                );
                hb_disconnect.notify_one();
                break;
            }
            // 等待对端心跳回应；超时则判定断连（§8.3）。
            // 等待本轮心跳被回应（pong_ts 必须推进到本轮 ts）。
            let deadline = tokio::time::Instant::now() + heartbeat_timeout;
            let mut timed_out = false;
            loop {
                if hb_pong_ts.load(Ordering::Relaxed) >= ts {
                    break;
                }
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero()
                    || tokio::time::timeout(remaining, hb_pong.notified())
                        .await
                        .is_err()
                {
                    timed_out = true;
                    break;
                }
            }
            if timed_out {
                tracing::warn!(session = %session_id_hb, "heartbeat timeout, disconnecting");
                hb_disconnect.notify_one();
                break;
            }
        }
    });

    loop {
        tokio::select! {
            frame = reader.next() => {
                match frame {
                    Some(Ok(f)) => {
                        // 解码失败不能让函数经 `?` 提前返回：那会跳过会话注销与 cleanup，
                        // 留下幽灵会话、占用的端口与泄漏的任务。改为结束循环走统一清理。
                        let msg = match Message::from_frame(&f) {
                            Ok(m) => m,
                            Err(e) => {
                                tracing::warn!(
                                    session = %session_id, error = %e,
                                    "control frame decode error; closing session"
                                );
                                break;
                            }
                        };
                        match msg {
                            Message::NewProxy(np) => {
                                tracing::info!(proxy = %np.proxy_name, typ = ?np.r#type, "received NewProxy");
                                if let Some(err) = new_proxy_invalid(&np) {
                                    tracing::warn!(proxy = %np.proxy_name, error = err, "NewProxy rejected");
                                    if !send_with_timeout(&tx, Message::NewProxyResp(NewProxyResp {
                                        proxy_name: np.proxy_name,
                                        ok: false,
                                        error: Some(err.into()),
                                    }))
                                    .await
                                    {
                                        tracing::warn!(session = %session_id, "newproxy resp send failed/timeout");
                                        break;
                                    }
                                    continue;
                                }
                                let result = listener::register_proxy(&np, &session, &state, &config).await;
                                let (ok, error) = match result {
                                    Ok(()) => (true, None),
                                    Err(e) => {
                                        // 失败详情（端口占用、权限等）已由 register_proxy
                                        // 记入服务端日志；对端只收到稳定错误码（§6.6）。
                                        tracing::warn!(
                                            session = %session_id, proxy = %np.proxy_name,
                                            code = e.as_str(), "proxy registration failed"
                                        );
                                        (false, Some(e.as_str().to_string()))
                                    }
                                };
                                if !send_with_timeout(&tx, Message::NewProxyResp(NewProxyResp {
                                    proxy_name: np.proxy_name,
                                    ok,
                                    error,
                                }))
                                .await
                                {
                                    tracing::warn!(session = %session_id, "newproxy resp send failed/timeout");
                                    break;
                                }
                            }
                            Message::Heartbeat(h) => {
                                tracing::debug!(session = %session_id, ts = h.ts, "heartbeat received");
                                try_send(&tx, Message::HeartbeatResp(HeartbeatResp { ts: h.ts }));
                            }
                            Message::HeartbeatResp(h) => {
                                let sent = last_sent_ts.load(Ordering::Relaxed);
                                if sent == 0 || h.ts > sent {
                                    // 未来时间戳（或未请求的响应）不得推进 pong_ts。
                                    tracing::debug!(
                                        session = %session_id, ts = h.ts, sent,
                                        "ignoring heartbeat response with invalid ts"
                                    );
                                } else {
                                    // 回传的 ts 即本端发出时间 → RTT = now - ts（§8.3）。
                                    let rtt = now_ms().saturating_sub(h.ts);
                                    // 逐会话记录：多会话时单一全局值语义不明（见 Session::rtt_ms）。
                                    session.rtt_ms.store(rtt, Ordering::Relaxed);
                                    pong_ts.store(h.ts, Ordering::Relaxed);
                                    tracing::debug!(session = %session_id, rtt_ms = rtt, "heartbeat response received");
                                    // 通知心跳任务已收到对端回应（§8.3 ping/pong）。
                                    pong.notify_one();
                                }
                            }
                            Message::Close(c) => {
                                tracing::info!(session = %session_id, reason = ?c.reason, "control connection closed by client");
                                break;
                            }
                            _ => {}
                        }
                    }
                    Some(Err(e)) => { tracing::warn!("control frame error: {e}"); break; }
                    None => {
                        tracing::info!(session = %session_id, "control connection closed by peer (EOF)");
                        break;
                    }
                }
            }
            _ = disconnect.notified() => {
                tracing::info!(session = %session_id, "control connection closed by heartbeat timeout");
                break;
            }
            _ = session.stop.cancelled() => {
                tracing::info!(session = %session_id, "control connection stopped (replaced or shutdown)");
                break;
            }
            _ = state.shutdown.cancelled() => {
                tracing::info!(session = %session_id, "control connection stopped by shutdown");
                break;
            }
        }
    }

    heartbeat_task.abort();
    // 先注销会话并清理资源，**再**等写任务收尾：
    //
    // 1) 立即释放公网端口与代理监听（用户连接不必多等 1 秒才能连上）；
    // 2) `cleanup` 会取消 `session.stop`，写任务随即被唤醒退出——否则写任务
    //    只能等下面的 1 秒硬超时被强杀。
    //
    // 旧实现把这两步放在写任务 await 之后：`Session` 结构里持有一份 `tx` 克隆，
    // `drop(tx)` 并不会关闭通道，写任务的 `rx.recv()` 永远挂起 → 每次控制连接
    // 拆解都要多花 1 秒（端口被占用的窗口期也相应拉长）。
    unregister_session(&state, &session);
    cleanup(&session, &state);
    // 给写任务机会刷出 Close 帧，超时再强杀。
    drop(tx);
    let done = tokio::time::timeout(std::time::Duration::from_secs(1), &mut writer_task).await;
    if done.is_err() {
        writer_task.abort();
    }
    Ok(())
}

/// 从会话注册表移除自身（仅当仍是当前条目，避免误删重连后的新会话）。
fn unregister_session(state: &ServerState, session: &Session) {
    let mut reg = state.sessions.lock();
    if let Some(s) = reg.get(&session.run_id) {
        if s.session_id == session.session_id {
            reg.remove(&session.run_id);
        }
    }
}

/// 版本不匹配时的错误描述。
///
/// 前缀固定为 `version mismatch`（客户端据此判定致命错误并停止重连），
/// 后面附带客户端声明版本与服务端支持列表，便于直接排障。
fn version_mismatch_detail(client_version: u8) -> String {
    let supported = PROTOCOL_VERSIONS_SUPPORTED
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(",");
    format!("version mismatch: client={client_version}, server supports [{supported}]")
}

/// NewProxy 字段校验（DESIGN §6.2.3），返回错误标识或 None。
fn new_proxy_invalid(np: &NewProxy) -> Option<&'static str> {
    if np.proxy_name.is_empty()
        || np.proxy_name.len() > MAX_PROXY_NAME_LEN
        || np.proxy_name.contains('/')
        || np.proxy_name.chars().any(|c| c.is_whitespace())
    {
        return Some("invalid field");
    }
    if matches!(np.r#type, ProxyType::Tcp | ProxyType::Udp) && np.remote_port == Some(0) {
        return Some("invalid field");
    }
    if let Some(doms) = &np.custom_domains {
        if doms.len() > MAX_CUSTOM_DOMAINS {
            return Some("invalid field");
        }
        for d in doms {
            if d.is_empty() || d.len() > MAX_DOMAIN_LEN || !rfrp_common::config::is_valid_domain(d)
            {
                return Some("invalid field");
            }
        }
    }
    None
}

/// 断开时中止所有代理监听任务，并清理本会话的待处理工作连接。
fn cleanup(session: &Session, state: &ServerState) {
    // 取消会话停止信号：唤醒控制循环与写任务，使其尽快退出。
    // `CancellationToken` 的取消是**粘性**的，即使此刻没有等待者，
    // 之后的 `cancelled()` 也会立即返回就绪（`Notify` 不具备该语义）。
    session.stop.cancel();
    // 一次加锁收集需要清理的代理名（全部 + UDP）与域名，随后在锁外操作全局索引。
    let (proxy_names, udp_names, domain_names) = {
        let proxies = session.proxies.lock();
        let proxy_names: Vec<String> = proxies.keys().cloned().collect();
        let udp_names: Vec<String> = proxies
            .iter()
            .filter(|(_, e)| e.kind == ProxyType::Udp)
            .map(|(n, _)| n.clone())
            .collect();
        let domain_names: Vec<String> = session.proxy_domains.lock().keys().cloned().collect();
        (proxy_names, udp_names, domain_names)
    };
    state.remove_proxy_stats(&proxy_names, &session.session_id);
    state.unindex_proxies(proxy_names, session);
    state.unindex_domains(domain_names, session);
    for (_, entry) in session.proxies.lock().drain() {
        entry.handle.abort();
    }
    let mut udp = state.udp.lock();
    for n in udp_names {
        // 仅移除本会话持有的 UDP 运行态：同一 run_id 的新会话可能已注册同名
        // 代理（旧 cleanup 不能按名误删/误停新条目）。
        if udp
            .get(&n)
            .is_some_and(|p| p.owner_session_id == session.session_id)
        {
            // 取消会话级停止信号：结束仍在途的 UDP 工作连接，释放 UDP 端口。
            if let Some(p) = udp.remove(&n) {
                p.stop.cancel();
            }
        }
    }
    session.proxy_domains.lock().clear();
    // 关闭并丢弃所有预热的工作连接池（§8.2）。
    for (_, mut v) in session.pools.lock().drain() {
        for _s in v.drain(..) {
            // 丢弃 TcpStream 即关闭连接。
        }
    }
    state
        .pending
        .lock()
        .retain(|_, p| p.session_id != session.session_id);
}

#[cfg(test)]
mod tests;
