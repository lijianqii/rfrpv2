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
use std::time::{Duration, Instant};

use futures::{SinkExt, StreamExt};
use rfrp_common::config::ServerConfig;
use rfrp_common::error::Result;
use rfrp_common::protocol::frame::{Frame, FrameCodec, FramedRead, FramedWrite};
use rfrp_common::protocol::msg::*;
use rfrp_common::util::now_ms;
use tokio::io::split;
use tokio::sync::mpsc;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::interval;

use crate::listener;
use crate::server::ServerState;

/// 一个 rfrpc 与服务端之间的控制连接会话。
pub struct Session {
    pub run_id: String,
    pub session_id: String,
    /// 出站控制消息通道（监听任务发 ReqWorkConn，本任务转交写任务）。
    pub tx: mpsc::Sender<Message>,
    /// 已注册代理的监听任务句柄（用于断开时清理）。
    pub proxies: Mutex<HashMap<String, JoinHandle<()>>>,
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
    let run_id = match login {
        Message::Login(l) => l.run_id,
        _ => {
            return Err(rfrp_common::Error::Protocol(
                "first frame must be Login".into(),
            ))
        }
    };
    // M1：token 校验跳过（见 DESIGN §12 M1）。run_id 仅用于日志关联。

    let session_id = uuid::Uuid::new_v4().to_string();
    let (tx, mut rx) = mpsc::channel::<Message>(64);
    let session = Arc::new(Session {
        run_id,
        session_id: session_id.clone(),
        tx: tx.clone(),
        proxies: Mutex::new(HashMap::new()),
    });

    let (read_half, write_half) = split(stream);
    let mut reader = FramedRead::new(read_half, FrameCodec);
    let mut writer = FramedWrite::new(write_half, FrameCodec);

    // 写任务：消费出站控制消息。
    let writer_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
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
    });

    // 登录响应（M1：work_conn_tls=false，无 TLS）。
    tx.send(Message::LoginResp(LoginResp {
        ok: true,
        error: None,
        session_id: Some(session_id.clone()),
        work_conn_tls: Some(false),
    }))
    .await
    .ok();
    tracing::info!(session = %session_id, "control connection established");

    // 心跳：周期性发送 Heartbeat，超时未收到 Resp 则通知断开（§8.3）。
    let last_resp = Arc::new(Mutex::new(Instant::now()));
    let disconnect = Arc::new(Notify::new());
    let hb_tx = tx.clone();
    let hb_last = last_resp.clone();
    let hb_disconnect = disconnect.clone();
    let session_id_hb = session_id.clone();
    let heartbeat_task = tokio::spawn(async move {
        let mut iv = interval(heartbeat_interval);
        iv.tick().await; // 消耗首次立即 tick，避免一建立就连发
        loop {
            iv.tick().await;
            if hb_last.lock().unwrap().elapsed() > heartbeat_timeout {
                tracing::warn!(session = %session_id_hb, "heartbeat timeout, disconnecting");
                hb_disconnect.notify_one();
                break;
            }
            if hb_tx
                .send(Message::Heartbeat(Heartbeat { ts: now_ms() }))
                .await
                .is_err()
            {
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
                                *last_resp.lock().unwrap() = Instant::now();
                                tx.send(Message::HeartbeatResp(HeartbeatResp { ts: h.ts })).await.ok();
                            }
                            Message::HeartbeatResp(_) => {
                                *last_resp.lock().unwrap() = Instant::now();
                            }
                            Message::Close(_) => break,
                            _ => {}
                        }
                    }
                    Some(Err(e)) => { tracing::warn!("control frame error: {e}"); break; }
                    None => break,
                }
            }
            _ = disconnect.notified() => {
                tracing::info!(session = %session_id, "control connection closed by heartbeat timeout");
                break;
            }
        }
    }

    heartbeat_task.abort();
    writer_task.abort();
    cleanup(&session, &state);
    Ok(())
}

/// 断开时中止所有代理监听任务，并清理本会话的待处理工作连接。
fn cleanup(session: &Session, state: &ServerState) {
    for (_, h) in session.proxies.lock().unwrap().drain() {
        h.abort();
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
        Message::Login(Login {
            run_id: run_id.into(),
            token: String::new(),
            version: PROTOCOL_VERSION,
        })
        .to_frame()
        .unwrap()
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
}
