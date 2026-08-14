//! 控制连接处理（客户端侧）。
//!
//! 发送 Login 后进入控制循环：处理 NewProxyResp（回传注册结果）、
//! ReqWorkConn（派生工作连接任务）、Heartbeat。读写经 split 后并发。

use std::sync::Arc;

use futures::{SinkExt, StreamExt};
use rfrp_common::config::ClientConfig;
use rfrp_common::constants::PROTOCOL_VERSION;
use rfrp_common::error::Result;
use rfrp_common::protocol::frame::{FrameCodec, FramedRead, FramedWrite};
use rfrp_common::protocol::msg::*;
use tokio::io::split;
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use crate::client::ClientState;
use crate::workconn;

pub async fn control_loop(
    stream: TcpStream,
    mut rx: mpsc::Receiver<Message>,
    state: Arc<ClientState>,
    config: ClientConfig,
) -> Result<()> {
    let (read_half, write_half) = split(stream);
    let mut reader = FramedRead::new(read_half, FrameCodec);
    let mut writer = FramedWrite::new(write_half, FrameCodec);

    // 写任务：消费出站控制消息。
    let (out_tx, mut out_rx) = mpsc::channel::<Message>(64);
    let writer_task = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
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

    // 登录（M1：token 不校验，可空）。
    out_tx
        .send(Message::Login(Login {
            run_id: uuid::Uuid::new_v4().to_string(),
            token: config.client.token.clone(),
            version: PROTOCOL_VERSION,
        }))
        .await
        .ok();

    loop {
        tokio::select! {
            frame = reader.next() => {
                match frame {
                    Some(Ok(f)) => {
                        let msg = Message::from_frame(&f)?;
                        match msg {
                            Message::NewProxyResp(r) => {
                                if let Some(tx) = state.resps.lock().unwrap().remove(&r.proxy_name) {
                                    let _ = tx.send(r);
                                }
                            }
                            Message::ReqWorkConn(r) => {
                                let st = state.clone();
                                let cfg = config.clone();
                                tokio::spawn(async move {
                                    let _ = workconn::handle_work_conn(r, st, cfg).await;
                                });
                            }
                            Message::Heartbeat(h) => {
                                out_tx.send(Message::HeartbeatResp(HeartbeatResp { ts: h.ts })).await.ok();
                            }
                            Message::Close(_) => break,
                            _ => {}
                        }
                    }
                    Some(Err(e)) => { tracing::warn!("control frame error: {e}"); break; }
                    None => break,
                }
            }
            out = rx.recv() => {
                match out {
                    Some(m) => { out_tx.send(m).await.ok(); }
                    None => break,
                }
            }
        }
    }

    writer_task.abort();
    Ok(())
}
