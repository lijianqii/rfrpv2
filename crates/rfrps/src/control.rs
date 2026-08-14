//! 控制连接处理（服务端侧）。
//!
//! 收到 Login 后建立 Session，进入控制循环：处理 NewProxy（注册公网监听）、
//! Heartbeat、Close。`ReqWorkConn` 由监听任务经 `session.tx` 发往写任务。
//! 收发通过 split 后的 `FramedRead`/`FramedWrite` 并发（见 DESIGN §6.1）。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use futures::{SinkExt, StreamExt};
use rfrp_common::config::ServerConfig;
use rfrp_common::error::Result;
use rfrp_common::protocol::frame::{Frame, FrameCodec, FramedRead, FramedWrite};
use rfrp_common::protocol::msg::*;
use tokio::io::split;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

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

pub async fn handle_control_login(
    login_frame: Frame,
    stream: TcpStream,
    state: Arc<ServerState>,
    config: ServerConfig,
) -> Result<()> {
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

    loop {
        match reader.next().await {
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
                        tx.send(Message::HeartbeatResp(HeartbeatResp { ts: h.ts }))
                            .await
                            .ok();
                    }
                    Message::Close(_) => break,
                    _ => {}
                }
            }
            Some(Err(e)) => {
                tracing::warn!("control frame error: {e}");
                break;
            }
            None => break,
        }
    }

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
