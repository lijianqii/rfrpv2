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
use rfrp_common::util::control::{graceful_close, send_with_timeout, try_send};
use tokio::io::split;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::client::ClientState;
use crate::workconn;

pub async fn control_loop<S>(
    stream: S,
    mut rx: mpsc::Receiver<Message>,
    state: Arc<ClientState>,
    config: ClientConfig,
    shutdown: CancellationToken,
) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (read_half, write_half) = split(stream);
    let mut reader = FramedRead::new(read_half, FrameCodec);
    let mut writer = FramedWrite::new(write_half, FrameCodec);

    // 写任务：消费出站控制消息；收到退出信号时尽量发送 TLS close_notify。
    let (out_tx, mut out_rx) = mpsc::channel::<Message>(256);
    let shutdown_writer = shutdown.clone();
    let mut writer_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                msg = out_rx.recv() => {
                    let Some(msg) = msg else {
                        // 通道关闭且处于退出流程：补发 Close 再 close_notify。
                        if shutdown_writer.is_cancelled() {
                            graceful_close(&mut writer, "client shutdown").await;
                        }
                        break;
                    };
                    match msg.to_frame() {
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
                    // 优雅退出：先发 Close 帧再 TLS close_notify（DESIGN §6.2.2）。
                    graceful_close(&mut writer, "client shutdown").await;
                    break;
                }
            }
        }
    });

    // 登录（token 由服务端在 M3 校验；run_id 来自状态以便重连复用，§6.6 / §8.3）。
    if !send_with_timeout(
        &out_tx,
        Message::Login(Login {
            run_id: state.run_id.clone(),
            token: config.client.token.clone(),
            version: PROTOCOL_VERSION,
        }),
    )
    .await
    {
        tracing::warn!("login send failed/timeout");
        return Ok(());
    }

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
                                tokio::spawn(async move {
                                    let _ = workconn::handle_work_conn(r, st).await;
                                });
                            }
                            Message::Heartbeat(h) => {
                                tracing::debug!(ts = h.ts, "heartbeat received; responding");
                                try_send(&out_tx, Message::HeartbeatResp(HeartbeatResp { ts: h.ts }));
                            }
                            Message::LoginResp(r) => {
                                // 路由到连接阶段，供 run() 区分致命 / 可恢复失败（§8.1）。
                                if let Some(tx) = state.login_tx.lock().unwrap().take() {
                                    let _ = tx.send(r);
                                }
                            }
                            Message::Close(c) => {
                                tracing::info!(reason = ?c.reason, "control connection closed by server");
                                break;
                            }
                            _ => {}
                        }
                    }
                    Some(Err(e)) => { tracing::warn!("control frame error: {e}"); break; }
                    None => {
                        tracing::info!("control connection closed by peer (EOF)");
                        break;
                    }
                }
            }
            out = rx.recv() => {
                match out {
                    Some(m) => {
                        // 心跳响应可丢弃（非关键）；其余（含 NewProxy 注册）带超时发送，
                        // 避免写通道拥堵时静默丢弃关键消息。
                        if matches!(m, Message::HeartbeatResp(_)) {
                            try_send(&out_tx, m);
                        } else if !send_with_timeout(&out_tx, m).await {
                            tracing::warn!("control outbound send failed/timeout");
                            break;
                        }
                    }
                    None => {
                        tracing::info!("control outbound channel closed");
                        break;
                    }
                }
            }
            _ = shutdown.cancelled() => {
                tracing::info!("shutdown requested, exiting control loop");
                break;
            }
        }
    }

    // 给写任务机会刷出 Close 帧，超时再强杀。
    drop(out_tx);
    let done = tokio::time::timeout(std::time::Duration::from_secs(1), &mut writer_task).await;
    if done.is_err() {
        writer_task.abort();
    }
    Ok(())
}

#[cfg(test)]
mod tests;
