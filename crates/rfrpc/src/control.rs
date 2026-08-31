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
use tokio::io::{split, AsyncWriteExt};
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
    let (out_tx, mut out_rx) = mpsc::channel::<Message>(64);
    let shutdown_writer = shutdown.clone();
    let mut writer_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                msg = out_rx.recv() => {
                    let Some(msg) = msg else {
                        // 通道关闭且处于退出流程：补发 Close 再 close_notify。
                        if shutdown_writer.is_cancelled() {
                            if let Ok(frame) = Message::Close(Close {
                                reason: Some("client shutdown".into()),
                            })
                            .to_frame()
                            {
                                let _ = writer.send(frame).await;
                            }
                            let _ = writer.get_mut().shutdown().await;
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
                    // 优雅退出：先发 Close 帧再 TLS close_notify（DESIGN §6.2.2）。
                    if let Ok(frame) = Message::Close(Close {
                        reason: Some("client shutdown".into()),
                    })
                    .to_frame()
                    {
                        let _ = writer.send(frame).await;
                    }
                    let _ = writer.get_mut().shutdown().await;
                    break;
                }
            }
        }
    });

    // 登录（token 由服务端在 M3 校验；run_id 来自状态以便重连复用，§6.6 / §8.3）。
    out_tx
        .send(Message::Login(Login {
            run_id: state.run_id.clone(),
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
                                tokio::spawn(async move {
                                    let _ = workconn::handle_work_conn(r, st).await;
                                });
                            }
                            Message::Heartbeat(h) => {
                                tracing::debug!(ts = h.ts, "heartbeat received; responding");
                                out_tx.send(Message::HeartbeatResp(HeartbeatResp { ts: h.ts })).await.ok();
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
                    Some(m) => { out_tx.send(m).await.ok(); }
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
mod tests {
    use super::*;
    use rfrp_common::protocol::msg::{
        Close, Heartbeat, LoginResp, Message, NewProxyResp, ReqWorkConn,
    };
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::sync::Mutex;
    use std::time::Duration;
    use tokio::io::{duplex, AsyncRead, AsyncWrite};
    use tokio::sync::{mpsc, oneshot};
    use tokio_util::sync::CancellationToken;

    async fn send_msg<W: AsyncWrite + Unpin>(w: &mut FramedWrite<W, FrameCodec>, m: Message) {
        w.send(m.to_frame().unwrap()).await.unwrap();
    }

    async fn recv_msg<R: AsyncRead + Unpin>(r: &mut FramedRead<R, FrameCodec>) -> Message {
        Message::from_frame(&r.next().await.unwrap().unwrap()).unwrap()
    }

    fn client_state_with_resp(name: &str) -> (Arc<ClientState>, oneshot::Receiver<NewProxyResp>) {
        let state = Arc::new(ClientState {
            server_addr: "127.0.0.1:7000".parse::<SocketAddr>().unwrap(),
            run_id: String::new(),
            proxies: vec![],
            resps: Mutex::new(HashMap::new()),
            login_tx: Mutex::new(None),
            tls: None,
            work_conn_tls: Mutex::new(false),
        });
        let (otx, orx) = oneshot::channel();
        state.resps.lock().unwrap().insert(name.into(), otx);
        (state, orx)
    }

    fn default_state() -> Arc<ClientState> {
        Arc::new(ClientState {
            server_addr: "127.0.0.1:7000".parse::<SocketAddr>().unwrap(),
            run_id: String::new(),
            proxies: vec![],
            resps: Mutex::new(HashMap::new()),
            login_tx: Mutex::new(None),
            tls: None,
            work_conn_tls: Mutex::new(false),
        })
    }

    #[tokio::test]
    async fn newproxy_resp_routed_to_oneshot() {
        let (client_end, server_end) = duplex(8192);
        let (state, orx) = client_state_with_resp("ssh");
        let (_tx, rx) = mpsc::channel::<Message>(64);
        let config = ClientConfig::default();
        let task = tokio::spawn(control_loop(
            client_end,
            rx,
            state,
            config,
            CancellationToken::new(),
        ));

        let (sr, sw) = split(server_end);
        let mut sr = FramedRead::new(sr, FrameCodec);
        let mut sw = FramedWrite::new(sw, FrameCodec);

        assert!(matches!(recv_msg(&mut sr).await, Message::Login(_)));
        send_msg(
            &mut sw,
            Message::NewProxyResp(NewProxyResp {
                proxy_name: "ssh".into(),
                ok: true,
                error: None,
            }),
        )
        .await;
        let resp = tokio::time::timeout(Duration::from_secs(2), orx)
            .await
            .unwrap()
            .unwrap();
        assert!(resp.ok);
        send_msg(&mut sw, Message::Close(Close { reason: None })).await;
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn heartbeat_responds() {
        let (client_end, server_end) = duplex(8192);
        let (_tx, rx) = mpsc::channel::<Message>(64);
        let config = ClientConfig::default();
        let task = tokio::spawn(control_loop(
            client_end,
            rx,
            default_state(),
            config,
            CancellationToken::new(),
        ));

        let (sr, sw) = split(server_end);
        let mut sr = FramedRead::new(sr, FrameCodec);
        let mut sw = FramedWrite::new(sw, FrameCodec);
        let _ = recv_msg(&mut sr).await; // Login
        send_msg(&mut sw, Message::Heartbeat(Heartbeat { ts: 7 })).await;
        match recv_msg(&mut sr).await {
            Message::HeartbeatResp(h) => assert_eq!(h.ts, 7),
            other => panic!("expected HeartbeatResp, got {other:?}"),
        }
        send_msg(&mut sw, Message::Close(Close { reason: None })).await;
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn reqworkconn_keeps_loop_alive() {
        let (client_end, server_end) = duplex(8192);
        let (_tx, rx) = mpsc::channel::<Message>(64);
        let config = ClientConfig::default();
        let task = tokio::spawn(control_loop(
            client_end,
            rx,
            default_state(),
            config,
            CancellationToken::new(),
        ));

        let (sr, sw) = split(server_end);
        let mut sr = FramedRead::new(sr, FrameCodec);
        let mut sw = FramedWrite::new(sw, FrameCodec);
        let _ = recv_msg(&mut sr).await; // Login
        send_msg(
            &mut sw,
            Message::ReqWorkConn(ReqWorkConn {
                proxy_name: "ssh".into(),
                work_id: 1,
            }),
        )
        .await;
        send_msg(&mut sw, Message::Heartbeat(Heartbeat { ts: 9 })).await;
        match recv_msg(&mut sr).await {
            Message::HeartbeatResp(h) => assert_eq!(h.ts, 9),
            other => panic!("expected HeartbeatResp, got {other:?}"),
        }
        send_msg(&mut sw, Message::Close(Close { reason: None })).await;
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn close_exits() {
        let (client_end, server_end) = duplex(8192);
        let (_tx, rx) = mpsc::channel::<Message>(64);
        let config = ClientConfig::default();
        let task = tokio::spawn(control_loop(
            client_end,
            rx,
            default_state(),
            config,
            CancellationToken::new(),
        ));

        let (sr, sw) = split(server_end);
        let mut sr = FramedRead::new(sr, FrameCodec);
        let mut sw = FramedWrite::new(sw, FrameCodec);
        let _ = recv_msg(&mut sr).await; // Login
        send_msg(&mut sw, Message::Close(Close { reason: None })).await;
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn control_loop_exits_on_shutdown() {
        // 退出令牌被取消时，控制循环应立即退出（§14.4）。
        let (client_end, server_end) = duplex(8192);
        let (_tx, rx) = mpsc::channel::<Message>(64);
        let config = ClientConfig::default();
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(control_loop(
            client_end,
            rx,
            default_state(),
            config,
            shutdown.clone(),
        ));
        // 未取消前应持续存活。
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!task.is_finished());
        shutdown.cancel();
        // 优雅退出应发送 Close 帧（DESIGN §6.2.2）。
        let (sr, _sw) = split(server_end);
        let mut sr = FramedRead::new(sr, FrameCodec);
        // 先消费 Login 帧，再取消并读取 Close 帧。
        let _ = recv_msg(&mut sr).await;
        let msg = tokio::time::timeout(Duration::from_secs(2), recv_msg(&mut sr))
            .await
            .expect("client should send Close before shutdown");
        assert!(matches!(msg, Message::Close(_)));
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn login_resp_routed_to_state() {
        // 校验 LoginResp 被路由到 state.login_tx，供 run() 区分致命/可恢复失败（§8.1）。
        let (client_end, server_end) = duplex(8192);
        let (_tx, rx) = mpsc::channel::<Message>(64);
        let config = ClientConfig::default();
        let state = Arc::new(ClientState {
            server_addr: "127.0.0.1:7000".parse::<SocketAddr>().unwrap(),
            run_id: "rid".into(),
            proxies: vec![],
            resps: Mutex::new(HashMap::new()),
            login_tx: Mutex::new(None),
            tls: None,
            work_conn_tls: Mutex::new(false),
        });
        let (lotx, lorx) = oneshot::channel();
        state.login_tx.lock().unwrap().replace(lotx);

        let task = tokio::spawn(control_loop(
            client_end,
            rx,
            state,
            config,
            CancellationToken::new(),
        ));

        let (sr, sw) = split(server_end);
        let mut sr = FramedRead::new(sr, FrameCodec);
        let mut sw = FramedWrite::new(sw, FrameCodec);
        let _ = recv_msg(&mut sr).await; // 客户端发出的 Login
        send_msg(
            &mut sw,
            Message::LoginResp(LoginResp {
                ok: false,
                error: Some("auth failed".into()),
                session_id: None,
                work_conn_tls: None,
            }),
        )
        .await;

        let resp = tokio::time::timeout(Duration::from_secs(2), lorx)
            .await
            .unwrap()
            .unwrap();
        assert!(!resp.ok);
        assert_eq!(resp.error.as_deref(), Some("auth failed"));
        task.abort();
    }

    #[tokio::test]
    async fn newproxy_resp_err_routed_to_oneshot() {
        // NewProxyResp{ok=false} 也应路由到注册时的 oneshot（§8.1 失败路径）。
        let (client_end, server_end) = duplex(8192);
        let (state, orx) = client_state_with_resp("ssh");
        let (_tx, rx) = mpsc::channel::<Message>(64);
        let config = ClientConfig::default();
        let task = tokio::spawn(control_loop(
            client_end,
            rx,
            state,
            config,
            CancellationToken::new(),
        ));
        let (sr, sw) = split(server_end);
        let mut sr = FramedRead::new(sr, FrameCodec);
        let mut sw = FramedWrite::new(sw, FrameCodec);
        assert!(matches!(recv_msg(&mut sr).await, Message::Login(_)));
        send_msg(
            &mut sw,
            Message::NewProxyResp(NewProxyResp {
                proxy_name: "ssh".into(),
                ok: false,
                error: Some("port occupied".into()),
            }),
        )
        .await;
        let resp = tokio::time::timeout(Duration::from_secs(2), orx)
            .await
            .unwrap()
            .unwrap();
        assert!(!resp.ok);
        assert_eq!(resp.error.as_deref(), Some("port occupied"));
        send_msg(&mut sw, Message::Close(Close { reason: None })).await;
        task.await.unwrap().unwrap();
    }
}
