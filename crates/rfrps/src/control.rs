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
use rfrp_common::util::stream::BoxedStream;
use tokio::io::split;
use tokio::sync::mpsc;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::interval;

use crate::listener;
use crate::server::ServerState;

/// 一个已注册代理的元信息。
pub struct ProxyEntry {
    /// 代理监听任务句柄（断开时清理）。
    pub handle: JoinHandle<()>,
    /// 代理类型（TCP/UDP/HTTP/HTTPS）。
    pub kind: rfrp_common::protocol::msg::ProxyType,
}

/// 一个 rfrpc 与服务端之间的控制连接会话。
pub struct Session {
    pub run_id: String,
    pub session_id: String,
    /// 出站控制消息通道（监听任务发 ReqWorkConn，本任务转交写任务）。
    pub tx: mpsc::Sender<Message>,
    /// 已注册代理（proxy_name -> 监听任务句柄 + 类型）。
    pub proxies: Mutex<HashMap<String, ProxyEntry>>,
    /// vhost 域名 -> proxy_name（HTTP/HTTPS 路由用）。
    pub proxy_domains: Mutex<HashMap<String, String>>,
    /// 断开 / 重连通知（§8.3）：清理旧会话或正常断开时唤醒控制循环退出。
    pub stop: Arc<Notify>,
    /// 预热工作连接池（proxy_name -> 空闲服务端侧工作流），按 §8.2 命中用户连接。
    /// 使用类型擦除以同时支持明文与 TLS 工作连接。
    pub pools: Mutex<HashMap<String, Vec<BoxedStream>>>,
}

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
mod tests {
    use super::*;
    use rfrp_common::constants::PROTOCOL_VERSION;
    use std::time::Duration;
    use tokio::io::duplex;

    async fn send_msg<W: tokio::io::AsyncWrite + Unpin>(
        w: &mut FramedWrite<W, FrameCodec>,
        m: Message,
    ) {
        w.send(m.to_frame().unwrap()).await.unwrap();
    }

    async fn recv_msg<R: tokio::io::AsyncRead + Unpin>(
        r: &mut FramedRead<R, FrameCodec>,
    ) -> Message {
        Message::from_frame(&r.next().await.unwrap().unwrap()).unwrap()
    }

    fn login_frame(run_id: &str) -> Frame {
        // 用确定性 UUID（由 run_id 字节派生）保证同名 run_id 稳定，同时满足服务端 UUID 校验。
        let mut b = [0u8; 16];
        for (i, byte) in run_id.as_bytes().iter().take(16).enumerate() {
            b[i] = *byte;
        }
        let rid = uuid::Uuid::from_bytes(b).to_string();
        Message::Login(Login {
            run_id: rid,
            token: String::new(),
            version: PROTOCOL_VERSION,
        })
        .to_frame()
        .unwrap()
    }

    #[tokio::test]
    async fn cleanup_clears_proxy_domains() {
        let state = ServerState::new();
        let (tx, _rx) = mpsc::channel::<Message>(8);
        let session = Arc::new(Session {
            run_id: "r".into(),
            session_id: "s".into(),
            tx,
            proxies: Mutex::new(HashMap::new()),
            proxy_domains: Mutex::new(HashMap::new()),
            stop: Arc::new(Notify::new()),
            pools: Mutex::new(HashMap::new()),
        });
        session
            .proxy_domains
            .lock()
            .unwrap()
            .insert("dev.example.com".into(), "web".into());
        session.proxies.lock().unwrap().insert(
            "udp".into(),
            ProxyEntry {
                handle: tokio::spawn(async {}),
                kind: ProxyType::Udp,
            },
        );
        let socket = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
        state.udp.lock().unwrap().insert(
            "udp".into(),
            Arc::new(crate::udp::UdpProxy {
                socket,
                sessions: Mutex::new(HashMap::new()),
                pending_by_id: Mutex::new(HashMap::new()),
                pending_client: Mutex::new(HashMap::new()),
                metrics: Arc::new(crate::metrics::Metrics::new()),
                session_timeout: std::time::Duration::from_secs(60),
                pending_timeout: std::time::Duration::from_secs(10),
            }),
        );
        state
            .sessions
            .lock()
            .unwrap()
            .insert("r".into(), session.clone());

        cleanup(&session, &state);
        assert!(
            session.proxy_domains.lock().unwrap().is_empty(),
            "cleanup must clear vhost domain mappings"
        );
        assert!(
            state.udp.lock().unwrap().is_empty(),
            "cleanup must remove udp proxy registry"
        );
    }

    #[tokio::test]
    async fn newproxy_heartbeat_close_flow() {
        let (server_end, client_end) = duplex(8192);
        let state = ServerState::new();
        let config = ServerConfig::default();

        let task = tokio::spawn(handle_control_login(
            login_frame("r1"),
            server_end,
            state,
            config,
            Duration::from_secs(30),
            Duration::from_secs(10),
        ));

        let (cr, cw) = split(client_end);
        let mut cr = FramedRead::new(cr, FrameCodec);
        let mut cw = FramedWrite::new(cw, FrameCodec);

        // 登录响应
        let resp = recv_msg(&mut cr).await;
        assert!(matches!(resp, Message::LoginResp(_)));
        // NewProxy -> NewProxyResp(ok)
        send_msg(
            &mut cw,
            Message::NewProxy(NewProxy {
                proxy_name: "ssh".into(),
                r#type: ProxyType::Tcp,
                remote_port: Some(6000),
                custom_domains: None,
            }),
        )
        .await;
        let np = recv_msg(&mut cr).await;
        match np {
            Message::NewProxyResp(r) => assert!(r.ok),
            _ => panic!("expected NewProxyResp, got {np:?}"),
        }
        // Heartbeat -> HeartbeatResp (echo ts)
        send_msg(&mut cw, Message::Heartbeat(Heartbeat { ts: 42 })).await;
        match recv_msg(&mut cr).await {
            Message::HeartbeatResp(h) => assert_eq!(h.ts, 42),
            _ => panic!("expected HeartbeatResp"),
        }
        // Close -> 控制循环结束
        send_msg(&mut cw, Message::Close(Close { reason: None })).await;
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn heartbeat_timeout_disconnects() {
        let (server_end, client_end) = duplex(8192);
        let state = ServerState::new();
        let config = ServerConfig::default();

        // 极小间隔/超时；客户端不回应 Heartbeat，应触发超时断开。
        let task = tokio::spawn(handle_control_login(
            login_frame("r2"),
            server_end,
            state,
            config,
            Duration::from_millis(30),
            Duration::from_millis(60),
        ));

        // 客户端读取 LoginResp 后保持连接空闲（不回应心跳）。
        let (cr, _cw) = split(client_end);
        let mut cr = FramedRead::new(cr, FrameCodec);
        let _ = recv_msg(&mut cr).await;

        // 控制任务应在数秒内因心跳超时而结束。
        let res = tokio::time::timeout(Duration::from_secs(3), task).await;
        assert!(res.is_ok(), "control task must end on heartbeat timeout");
        res.unwrap().unwrap().unwrap();
    }

    #[tokio::test]
    async fn heartbeat_keeps_alive_when_client_responds() {
        // 客户端正常回应心跳时，服务端不应误判超时断开（§8.3 心跳修复）。
        // 此前“间隔(30s) > 超时(10s)”会导致首个心跳周期即误杀控制连接，
        // 进而触发重连去重、回收代理监听与在途工作连接。
        let (server_end, client_end) = duplex(8192);
        let state = ServerState::new();
        let config = ServerConfig::default();
        let task = tokio::spawn(handle_control_login(
            login_frame("hbAlive"),
            server_end,
            state,
            config,
            Duration::from_millis(30), // interval
            Duration::from_millis(60), // timeout
        ));
        let (cr, cw) = split(client_end);
        let mut cr = FramedRead::new(cr, FrameCodec);
        let mut cw = FramedWrite::new(cw, FrameCodec);
        // 消费 LoginResp，并对每个 Heartbeat 回应 HeartbeatResp。
        let _ = recv_msg(&mut cr).await;
        let responder = tokio::spawn(async move {
            while let Message::Heartbeat(h) = recv_msg(&mut cr).await {
                send_msg(&mut cw, Message::HeartbeatResp(HeartbeatResp { ts: h.ts })).await;
            }
        });
        // 等待超过数个心跳周期，控制任务应仍存活（未被误杀）。
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            !task.is_finished(),
            "control task must stay alive while client responds to heartbeats"
        );
        task.abort();
        responder.abort();
    }

    #[tokio::test]
    async fn non_login_first_frame_errors() {
        let (server_end, _client_end) = duplex(8192);
        let state = ServerState::new();
        let config = ServerConfig::default();
        let bad = Message::Heartbeat(Heartbeat { ts: 1 }).to_frame().unwrap();
        let res = handle_control_login(
            bad,
            server_end,
            state,
            config,
            Duration::from_secs(30),
            Duration::from_secs(10),
        )
        .await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn reconnect_same_run_id_replaces_old_session() {
        let state = ServerState::new();
        let config = ServerConfig::default();

        // 首个同名 run_id 会话登录。
        let (s1, c1) = duplex(8192);
        let t1 = tokio::spawn(handle_control_login(
            login_frame("rdup"),
            s1,
            state.clone(),
            config.clone(),
            Duration::from_secs(30),
            Duration::from_secs(10),
        ));
        let (cr1, _cw1) = split(c1);
        let mut cr1 = FramedRead::new(cr1, FrameCodec);
        let _ = recv_msg(&mut cr1).await; // 消费 LoginResp，避免缓冲满

        // 同名 run_id 的第二个会话登录 -> 应触发旧会话去重清理（§8.3）。
        let (s2, c2) = duplex(8192);
        let t2 = tokio::spawn(handle_control_login(
            login_frame("rdup"),
            s2,
            state.clone(),
            config.clone(),
            Duration::from_secs(30),
            Duration::from_secs(10),
        ));
        let (cr2, _cw2) = split(c2);
        let mut cr2 = FramedRead::new(cr2, FrameCodec);
        let resp2 = recv_msg(&mut cr2).await;
        assert!(
            matches!(resp2, Message::LoginResp(_)),
            "second login must succeed"
        );

        // 旧会话应被 stop 通知退出（去重清理）。
        let r = tokio::time::timeout(Duration::from_secs(3), t1).await;
        assert!(
            r.is_ok(),
            "old control loop must stop on reconnect with same run_id"
        );
        r.unwrap().unwrap().unwrap();

        // 新会话仍存活。
        assert!(!t2.is_finished());
        t2.abort();
    }

    #[tokio::test]
    async fn login_version_mismatch_rejected() {
        // 协议版本不匹配：服务端回 LoginResp{ok=false, "version mismatch"} 并直接结束（§6.6）。
        let (server_end, client_end) = duplex(8192);
        let state = ServerState::new();
        let config = ServerConfig::default();
        let bad = Message::Login(Login {
            run_id: "x".into(),
            token: String::new(),
            version: PROTOCOL_VERSION + 1,
        })
        .to_frame()
        .unwrap();
        let task = tokio::spawn(handle_control_login(
            bad,
            server_end,
            state,
            config,
            Duration::from_secs(30),
            Duration::from_secs(10),
        ));
        let (cr, _cw) = split(client_end);
        let mut cr = FramedRead::new(cr, FrameCodec);
        match recv_msg(&mut cr).await {
            Message::LoginResp(r) => {
                assert!(!r.ok);
                assert_eq!(r.error.as_deref(), Some("version mismatch"));
            }
            other => panic!("expected LoginResp, got {other:?}"),
        }
        // 控制任务应直接结束（未建立会话）。
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn auth_failure_rejects_without_echo() {
        // M3：token 错误时返回 LoginResp{ok=false, error=None}，不建立会话（DESIGN §10.2）。
        let (server_end, client_end) = duplex(8192);
        let state = ServerState::new();
        let mut config = ServerConfig::default();
        config.server.token = "secret".into();

        let bad = Message::Login(Login {
            run_id: "rAuth".into(),
            token: "wrong".into(),
            version: PROTOCOL_VERSION,
        })
        .to_frame()
        .unwrap();
        let task = tokio::spawn(handle_control_login(
            bad,
            server_end,
            state.clone(),
            config,
            Duration::from_secs(30),
            Duration::from_secs(10),
        ));
        let (cr, _cw) = split(client_end);
        let mut cr = FramedRead::new(cr, FrameCodec);
        match recv_msg(&mut cr).await {
            Message::LoginResp(r) => {
                assert!(!r.ok);
                assert!(r.error.is_none(), "auth failure must not echo reason");
                assert!(r.session_id.is_none());
            }
            other => panic!("expected LoginResp, got {other:?}"),
        }
        task.await.unwrap().unwrap();
        assert!(state.sessions.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn control_loop_exits_on_shutdown() {
        // 退出令牌被取消时，控制循环应立即退出（§14.4）。
        let (server_end, client_end) = duplex(8192);
        let state = ServerState::new();
        let config = ServerConfig::default();
        let shutdown = state.shutdown.clone();
        let task = tokio::spawn(handle_control_login(
            login_frame("rsd"),
            server_end,
            state,
            config,
            Duration::from_secs(30),
            Duration::from_secs(10),
        ));
        // 消费 LoginResp，确认已登录且未退出。
        let (cr, _cw) = split(client_end);
        let mut cr = FramedRead::new(cr, FrameCodec);
        let _ = recv_msg(&mut cr).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!task.is_finished());
        shutdown.cancel();
        // 服务端退出应发送 Close 帧（DESIGN §6.2.2）。
        let msg = tokio::time::timeout(Duration::from_secs(2), recv_msg(&mut cr))
            .await
            .expect("server should send Close before shutdown");
        assert!(matches!(msg, Message::Close(_)));
        let r = tokio::time::timeout(Duration::from_secs(3), task).await;
        assert!(r.is_ok(), "control loop must exit on shutdown token");
        r.unwrap().unwrap().unwrap();
    }

    #[tokio::test]
    async fn login_ok_returns_session_id_and_registers() {
        // 正常登录：LoginResp{ok=true, session_id=Some}，且会话写入 registry（§8.1）。
        let (server_end, client_end) = duplex(8192);
        let state = ServerState::new();
        let config = ServerConfig::default();
        let task = tokio::spawn(handle_control_login(
            login_frame("rOk"),
            server_end,
            state.clone(),
            config,
            Duration::from_secs(30),
            Duration::from_secs(10),
        ));
        let (cr, cw) = split(client_end);
        let mut cr = FramedRead::new(cr, FrameCodec);
        let mut cw = FramedWrite::new(cw, FrameCodec);
        match recv_msg(&mut cr).await {
            Message::LoginResp(r) => {
                assert!(r.ok);
                assert!(r.session_id.is_some());
            }
            other => panic!("expected LoginResp, got {other:?}"),
        }
        assert_eq!(state.sessions.lock().unwrap().len(), 1);
        send_msg(&mut cw, Message::Close(Close { reason: None })).await;
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn newproxy_rejection_routed_to_resp() {
        // NewProxy 被拒（端口不在允许范围）应经控制循环回 NewProxyResp{ok=false}（§8.1）。
        let (server_end, client_end) = duplex(8192);
        let state = ServerState::new();
        let mut config = ServerConfig::default();
        config.proxy.allow_ports = "5000-5001".into();
        let task = tokio::spawn(handle_control_login(
            login_frame("rRej"),
            server_end,
            state,
            config,
            Duration::from_secs(30),
            Duration::from_secs(10),
        ));
        let (cr, cw) = split(client_end);
        let mut cr = FramedRead::new(cr, FrameCodec);
        let mut cw = FramedWrite::new(cw, FrameCodec);
        assert!(matches!(recv_msg(&mut cr).await, Message::LoginResp(_)));
        send_msg(
            &mut cw,
            Message::NewProxy(NewProxy {
                proxy_name: "p".into(),
                r#type: ProxyType::Tcp,
                remote_port: Some(18080),
                custom_domains: None,
            }),
        )
        .await;
        match recv_msg(&mut cr).await {
            Message::NewProxyResp(r) => {
                assert_eq!(r.proxy_name, "p");
                assert!(!r.ok);
                assert!(r.error.is_some());
            }
            other => panic!("expected NewProxyResp, got {other:?}"),
        }
        send_msg(&mut cw, Message::Close(Close { reason: None })).await;
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn unknown_control_msg_ignored_keeps_loop_alive() {
        // 控制循环不处理 StartWorkConn：忽略后循环仍存活（§8.2 控制/工作连接分离）。
        let (server_end, client_end) = duplex(8192);
        let state = ServerState::new();
        let config = ServerConfig::default();
        let task = tokio::spawn(handle_control_login(
            login_frame("rIgn"),
            server_end,
            state,
            config,
            Duration::from_secs(30),
            Duration::from_secs(10),
        ));
        let (cr, cw) = split(client_end);
        let mut cr = FramedRead::new(cr, FrameCodec);
        let mut cw = FramedWrite::new(cw, FrameCodec);
        assert!(matches!(recv_msg(&mut cr).await, Message::LoginResp(_)));
        send_msg(
            &mut cw,
            Message::StartWorkConn(StartWorkConn {
                proxy_name: "p".into(),
                work_id: 5,
            }),
        )
        .await;
        // 随后 Heartbeat 仍应得到 HeartbeatResp，证明循环未退出。
        send_msg(&mut cw, Message::Heartbeat(Heartbeat { ts: 1 })).await;
        match recv_msg(&mut cr).await {
            Message::HeartbeatResp(h) => assert_eq!(h.ts, 1),
            other => panic!("expected HeartbeatResp, got {other:?}"),
        }
        send_msg(&mut cw, Message::Close(Close { reason: None })).await;
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn malformed_frame_breaks_control_loop() {
        // 版本不符的帧导致解码错误：控制循环应断开（Ok 返回，§6.1）。
        let (server_end, client_end) = duplex(8192);
        let state = ServerState::new();
        let config = ServerConfig::default();
        let task = tokio::spawn(handle_control_login(
            login_frame("rBad"),
            server_end,
            state,
            config,
            Duration::from_secs(30),
            Duration::from_secs(10),
        ));
        let (cr, cw) = split(client_end);
        let mut cr = FramedRead::new(cr, FrameCodec);
        let mut cw = FramedWrite::new(cw, FrameCodec);
        assert!(matches!(recv_msg(&mut cr).await, Message::LoginResp(_)));
        cw.send(Frame::new(0x02, MSG_HEARTBEAT, b"{}".to_vec()))
            .await
            .unwrap();
        let r = tokio::time::timeout(Duration::from_secs(3), task).await;
        assert!(r.is_ok(), "control loop must break on malformed frame");
        r.unwrap().unwrap().unwrap();
    }

    #[tokio::test]
    async fn eof_closes_control_loop() {
        // 客户端两端关闭 -> 服务端读到 EOF -> 控制循环断开（Ok 返回）。
        let (server_end, client_end) = duplex(8192);
        let state = ServerState::new();
        let config = ServerConfig::default();
        let task = tokio::spawn(handle_control_login(
            login_frame("rEof"),
            server_end,
            state,
            config,
            Duration::from_secs(30),
            Duration::from_secs(10),
        ));
        let (cr, cw) = split(client_end);
        let mut cr = FramedRead::new(cr, FrameCodec);
        let cw = FramedWrite::new(cw, FrameCodec);
        assert!(matches!(recv_msg(&mut cr).await, Message::LoginResp(_)));
        // 关闭客户端两端（模拟断开）-> 服务端读到 EOF -> 控制循环断开。
        drop(cr);
        drop(cw);
        let r = tokio::time::timeout(Duration::from_secs(3), task).await;
        assert!(r.is_ok(), "control loop must break on EOF");
        r.unwrap().unwrap().unwrap();
    }

    #[tokio::test]
    async fn login_invalid_run_id_rejected() {
        let (server_end, client_end) = duplex(8192);
        let state = ServerState::new();
        let config = ServerConfig::default();
        let bad = Message::Login(Login {
            run_id: "not-a-uuid".into(),
            token: String::new(),
            version: PROTOCOL_VERSION,
        })
        .to_frame()
        .unwrap();
        let task = tokio::spawn(handle_control_login(
            bad,
            server_end,
            state,
            config,
            Duration::from_secs(30),
            Duration::from_secs(10),
        ));
        let (cr, _cw) = split(client_end);
        let mut cr = FramedRead::new(cr, FrameCodec);
        match recv_msg(&mut cr).await {
            Message::LoginResp(r) => {
                assert!(!r.ok);
                assert!(r.error.is_none());
            }
            other => panic!("expected LoginResp, got {other:?}"),
        }
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn login_oversize_token_rejected() {
        let (server_end, client_end) = duplex(8192);
        let state = ServerState::new();
        let config = ServerConfig::default();
        let bad = Message::Login(Login {
            run_id: uuid::Uuid::new_v4().to_string(),
            token: "x".repeat(MAX_TOKEN_LEN + 1),
            version: PROTOCOL_VERSION,
        })
        .to_frame()
        .unwrap();
        let task = tokio::spawn(handle_control_login(
            bad,
            server_end,
            state,
            config,
            Duration::from_secs(30),
            Duration::from_secs(10),
        ));
        let (cr, _cw) = split(client_end);
        let mut cr = FramedRead::new(cr, FrameCodec);
        match recv_msg(&mut cr).await {
            Message::LoginResp(r) => assert!(!r.ok),
            other => panic!("expected LoginResp, got {other:?}"),
        }
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn newproxy_invalid_name_rejected() {
        let (server_end, client_end) = duplex(8192);
        let state = ServerState::new();
        let config = ServerConfig::default();
        let login = Message::Login(Login {
            run_id: uuid::Uuid::new_v4().to_string(),
            token: String::new(),
            version: PROTOCOL_VERSION,
        })
        .to_frame()
        .unwrap();
        let task = tokio::spawn(handle_control_login(
            login,
            server_end,
            state,
            config,
            Duration::from_secs(30),
            Duration::from_secs(10),
        ));
        let (cr, cw) = split(client_end);
        let mut cr = FramedRead::new(cr, FrameCodec);
        let mut cw = FramedWrite::new(cw, FrameCodec);
        assert!(matches!(recv_msg(&mut cr).await, Message::LoginResp(_)));
        send_msg(
            &mut cw,
            Message::NewProxy(NewProxy {
                proxy_name: "bad/name".into(),
                r#type: ProxyType::Tcp,
                remote_port: Some(6000),
                custom_domains: None,
            }),
        )
        .await;
        match recv_msg(&mut cr).await {
            Message::NewProxyResp(r) => {
                assert!(!r.ok);
                assert_eq!(r.error.as_deref(), Some("invalid field"));
            }
            other => panic!("expected NewProxyResp, got {other:?}"),
        }
        send_msg(&mut cw, Message::Close(Close { reason: None })).await;
        task.await.unwrap().unwrap();
    }
}
