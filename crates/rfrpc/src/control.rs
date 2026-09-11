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
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use rfrp_common::util::now_ms;
use tokio::io::split;
use tokio::sync::{mpsc, Notify};
use tokio::time::interval;
use tokio_util::sync::CancellationToken;

use crate::client::ClientState;
use crate::workconn;

pub async fn control_loop<S>(
    stream: S,
    mut rx: mpsc::Receiver<Message>,
    state: Arc<ClientState>,
    config: ClientConfig,
    shutdown: CancellationToken,
    heartbeat_interval: Duration,
    heartbeat_timeout: Duration,
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

    // 心跳看门狗（与服务端对称，§8.3）：周期发送 Heartbeat 并等待 HeartbeatResp，
    // 超时判定控制连接已失效。半开连接（对端进程挂起、链路静默中断，无 FIN/RST）下
    // reader 永远不会返回；若无此看门狗，客户端会永久卡住、无法重连
    // （Windows 上 keepalive 未启用时尤为明显）。
    let disconnect = Arc::new(Notify::new());
    let pong = Arc::new(Notify::new());
    // 记录对端回传的最近心跳 ts：以"本轮是否被回应"判定超时，避免 Notify 许可残留漏检。
    let pong_ts = Arc::new(AtomicU64::new(0));
    let hb_tx = out_tx.clone();
    let hb_pong = pong.clone();
    let hb_pong_ts = pong_ts.clone();
    let hb_disconnect = disconnect.clone();
    let heartbeat_task = tokio::spawn(async move {
        let mut iv = interval(heartbeat_interval);
        iv.tick().await; // 消耗首次立即 tick，避免一建立就连发
        loop {
            iv.tick().await;
            let ts = now_ms();
            if !try_send(&hb_tx, Message::Heartbeat(Heartbeat { ts })) {
                // 写通道满：跳过本轮，避免本地拥塞误判断连。
                continue;
            }
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
                tracing::warn!("heartbeat timeout, control connection considered dead");
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
                            Message::HeartbeatResp(h) => {
                                // 回传的 ts 即本端发出时间 → RTT = now - ts（§8.3）。
                                let rtt = now_ms().saturating_sub(h.ts);
                                state.metrics.set_rtt_ms(rtt);
                                pong_ts.store(h.ts, Ordering::Relaxed);
                                tracing::debug!(rtt_ms = rtt, "heartbeat response received");
                                // 通知心跳任务已收到对端回应（§8.3 ping/pong）。
                                pong.notify_one();
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
            _ = disconnect.notified() => {
                tracing::warn!("control connection dead (heartbeat timeout), reconnecting");
                break;
            }
            _ = shutdown.cancelled() => {
                tracing::info!("shutdown requested, exiting control loop");
                break;
            }
        }
    }

    // 先停心跳任务并等其结束（释放 out_tx 克隆），再关写通道，
    // 否则写任务收不到通道关闭信号、只能靠下面的超时强杀。
    heartbeat_task.abort();
    let _ = heartbeat_task.await;
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
