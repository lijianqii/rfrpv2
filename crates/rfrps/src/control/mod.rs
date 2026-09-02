//! 控制连接处理（服务端侧）。
//!
//! 收到 Login 后建立 Session，进入控制循环：处理 NewProxy（注册公网监听）、
//! Heartbeat/HeartbeatResp（保活）、Close。`ReqWorkConn` 由监听任务经 `session.tx` 发往写任务。
//! 收发通过 split 后的 `FramedRead`/`FramedWrite` 并发（见 DESIGN §6.1）。
//!
//! 心跳（§8.3）：本端周期性发送 `Heartbeat`，若 `HEARTBEAT_TIMEOUT` 内未收到对端
//! `HeartbeatResp`，经 `Notify` 通知控制循环断开并清理 Session。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use rfrp_common::auth::verify_token;
use rfrp_common::config::ServerConfig;
use rfrp_common::constants::{
    MAX_CUSTOM_DOMAINS, MAX_DOMAIN_LEN, MAX_PROXY_NAME_LEN, MAX_RUN_ID_LEN, MAX_TOKEN_LEN,
    PROTOCOL_VERSION,
};
use rfrp_common::error::Result;
use rfrp_common::protocol::frame::{Frame, FrameCodec, FramedRead, FramedWrite};
use rfrp_common::protocol::msg::*;
use rfrp_common::util::control::graceful_close;
use rfrp_common::util::now_ms;
use tokio::io::split;
use tokio::sync::mpsc;
use tokio::sync::Notify;
use tokio::time::interval;

use crate::listener;
use crate::server::ServerState;

mod session;
pub use session::{ProxyEntry, Session};

/// 控制连接主循环。`S` 为任意双向流（生产用 `TcpStream`，测试用 `DuplexStream`）。
pub async fn handle_control_login<S>(
    login_frame: Frame,
    stream: S,
    state: Arc<ServerState>,
    config: ServerConfig,
    heartbeat_interval: Duration,
    heartbeat_timeout: Duration,
) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let login = Message::from_frame(&login_frame)?;
    let (run_id, version, token) = match login {
        Message::Login(l) => (l.run_id, l.version, l.token),
        _ => {
            return Err(rfrp_common::Error::Protocol(
                "first frame must be Login".into(),
            ))
        }
    };
    // 协议版本校验：不匹配直接拒绝，且不建立会话（客户端据此判定致命、不重连，§6.6）。
    if version != PROTOCOL_VERSION {
        tracing::warn!(version, "login rejected: protocol version mismatch");
        let mut w = FramedWrite::new(stream, FrameCodec);
        let _ = w
            .send(
                Message::LoginResp(LoginResp {
                    ok: false,
                    error: Some("version mismatch".into()),
                    session_id: None,
                    work_conn_tls: None,
                })
                .to_frame()?,
            )
            .await;
        return Ok(());
    }
    // 字段上限校验（DESIGN §6.2.3）：run_id 必须是 UUID，token 长度受限。
    if uuid::Uuid::parse_str(&run_id).is_err()
        || run_id.len() > MAX_RUN_ID_LEN
        || token.len() > MAX_TOKEN_LEN
    {
        tracing::warn!(run_id = %run_id, "login rejected: invalid fields");
        let mut w = FramedWrite::new(stream, FrameCodec);
        let _ = w
            .send(
                Message::LoginResp(LoginResp {
                    ok: false,
                    error: None,
                    session_id: None,
                    work_conn_tls: None,
                })
                .to_frame()?,
            )
            .await;
        return Ok(());
    }

    // M3：token 鉴权。鉴权失败不回显具体原因（DESIGN §10.2），客户端将 `ok=false + error=None` 视为致命鉴权失败。
    if !verify_token(&config.server.token, &token) {
        tracing::warn!(run_id = %run_id, "login rejected: token mismatch");
        let mut w = FramedWrite::new(stream, FrameCodec);
        let _ = w
            .send(
                Message::LoginResp(LoginResp {
                    ok: false,
                    error: None,
                    session_id: None,
                    work_conn_tls: None,
                })
                .to_frame()?,
            )
            .await;
        return Ok(());
    }

    let session_id = uuid::Uuid::new_v4().to_string();
    let (tx, mut rx) = mpsc::channel::<Message>(64);
    let session = Arc::new(Session {
        run_id,
        session_id: session_id.clone(),
        tx: tx.clone(),
        proxies: Mutex::new(HashMap::new()),
        proxy_domains: Mutex::new(HashMap::new()),
        stop: Arc::new(Notify::new()),
        pools: Mutex::new(HashMap::new()),
    });

    // 重连去重（§8.3）：同一 run_id 的旧会话先清理再接受新登录。
    {
        let mut reg = state.sessions.lock().unwrap();
        if let Some(old) = reg.get(&session.run_id) {
            cleanup(old, &state);
            reg.remove(&session.run_id);
        }
        reg.insert(session.run_id.clone(), session.clone());
    }

    let (read_half, write_half) = split(stream);
    let mut reader = FramedRead::new(read_half, FrameCodec);
    let mut writer = FramedWrite::new(write_half, FrameCodec);

    // 写任务：消费出站控制消息；服务端退出或会话被替换时尽量发送 TLS close_notify。
    let shutdown_writer = state.shutdown.clone();
    let stop_writer = session.stop.clone();
    let mut writer_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                msg = rx.recv() => {
                    let Some(msg) = msg else {
                        // 通道关闭且处于退出流程：补发 Close 再 close_notify。
                        if shutdown_writer.is_cancelled() {
                            graceful_close(&mut writer, "server shutdown").await;
                        }
                        break;
                    };
                    match msg.to_frame() {
                        Ok(frame) => {
                            if let Err(e) = writer.send(frame).await {
                                tracing::warn!("control write error: {e}");
                                break;
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
                    graceful_close(&mut writer, "server shutdown").await;
                    break;
                }
                _ = stop_writer.notified() => {
                    graceful_close(&mut writer, "session replaced").await;
                    break;
                }
            }
        }
    });

    // 登录响应：下发服务端 work_conn_tls 偏好（DESIGN §6.5）。
    tx.send(Message::LoginResp(LoginResp {
        ok: true,
        error: None,
        session_id: Some(session_id.clone()),
        work_conn_tls: Some(config.server.work_conn_tls),
    }))
    .await
    .ok();
    tracing::info!(session = %session_id, "control connection established");

    // 心跳（§8.3）：周期性发送 Heartbeat，等待对端在 HEARTBEAT_TIMEOUT 内回应；
    // 超时未收到 HeartbeatResp 则通知断开。采用“每次发送后等待回应”的 ping/pong
    // 语义，避免“心跳间隔 > 超时阈值”时误判断连（客户端仅在收到 Heartbeat 时回应）。
    let disconnect = Arc::new(Notify::new());
    let pong = Arc::new(Notify::new());
    let hb_tx = tx.clone();
    let hb_pong = pong.clone();
    let hb_disconnect = disconnect.clone();
    let session_id_hb = session_id.clone();
    let heartbeat_task = tokio::spawn(async move {
        let mut iv = interval(heartbeat_interval);
        iv.tick().await; // 消耗首次立即 tick，避免一建立就连发
        loop {
            iv.tick().await;
            let ts = now_ms();
            tracing::debug!(session = %session_id_hb, ts, "heartbeat sent");
            if hb_tx
                .send(Message::Heartbeat(Heartbeat { ts }))
                .await
                .is_err()
            {
                break;
            }
            // 等待对端心跳回应；超时则判定断连（§8.3）。
            if tokio::time::timeout(heartbeat_timeout, hb_pong.notified())
                .await
                .is_err()
            {
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
                        let msg = Message::from_frame(&f)?;
                        match msg {
                            Message::NewProxy(np) => {
                                tracing::info!(proxy = %np.proxy_name, typ = ?np.r#type, "received NewProxy");
                                if let Some(err) = new_proxy_invalid(&np) {
                                    tracing::warn!(proxy = %np.proxy_name, error = err, "NewProxy rejected");
                                    tx.send(Message::NewProxyResp(NewProxyResp {
                                        proxy_name: np.proxy_name,
                                        ok: false,
                                        error: Some(err.into()),
                                    }))
                                    .await
                                    .ok();
                                    continue;
                                }
                                let result = listener::register_proxy(&np, &session, &state, &config).await;
                                let (ok, error) = match result {
                                    Ok(()) => (true, None),
                                    Err(e) => (false, Some(e.to_string())),
                                };
                                tx.send(Message::NewProxyResp(NewProxyResp {
                                    proxy_name: np.proxy_name,
                                    ok,
                                    error,
                                }))
                                .await
                                .ok();
                            }
                            Message::Heartbeat(h) => {
                                tracing::debug!(session = %session_id, ts = h.ts, "heartbeat received");
                                tx.send(Message::HeartbeatResp(HeartbeatResp { ts: h.ts })).await.ok();
                            }
                            Message::HeartbeatResp(_) => {
                                tracing::debug!(session = %session_id, "heartbeat response received");
                                // 通知心跳任务已收到对端回应（§8.3 ping/pong）。
                                pong.notify_one();
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
            _ = session.stop.notified() => {
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
    // 给写任务机会刷出 Close 帧，超时再强杀。
    drop(tx);
    let done = tokio::time::timeout(std::time::Duration::from_secs(1), &mut writer_task).await;
    if done.is_err() {
        writer_task.abort();
    }
    // 从会话注册表移除自身（仅当仍是当前条目，避免误删重连后的新会话）。
    {
        let mut reg = state.sessions.lock().unwrap();
        if let Some(s) = reg.get(&session.run_id) {
            if s.session_id == session.session_id {
                reg.remove(&session.run_id);
            }
        }
    }
    cleanup(&session, &state);
    Ok(())
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
    if let Some(doms) = &np.custom_domains {
        if doms.len() > MAX_CUSTOM_DOMAINS {
            return Some("invalid field");
        }
        for d in doms {
            if d.is_empty() || d.len() > MAX_DOMAIN_LEN {
                return Some("invalid field");
            }
        }
    }
    None
}

/// 断开时中止所有代理监听任务，并清理本会话的待处理工作连接。
fn cleanup(session: &Session, state: &ServerState) {
    session.stop.notify_waiters();
    // 先收集 UDP 代理名，用于清理全局注册表。
    let udp_names: Vec<String> = session
        .proxies
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, e)| e.kind == ProxyType::Udp)
        .map(|(n, _)| n.clone())
        .collect();
    for (_, entry) in session.proxies.lock().unwrap().drain() {
        entry.handle.abort();
    }
    let mut udp = state.udp.lock().unwrap();
    for n in udp_names {
        udp.remove(&n);
    }
    session.proxy_domains.lock().unwrap().clear();
    // 关闭并丢弃所有预热的工作连接池（§8.2）。
    for (_, mut v) in session.pools.lock().unwrap().drain() {
        for _s in v.drain(..) {
            // 丢弃 TcpStream 即关闭连接。
        }
    }
    state
        .pending
        .lock()
        .unwrap()
        .retain(|_, p| p.session_id != session.session_id);
}

#[cfg(test)]
mod tests;
