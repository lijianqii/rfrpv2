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
use tokio::sync::mpsc;

use crate::client::ClientState;
use crate::workconn;

pub async fn control_loop<S>(
    stream: S,
    mut rx: mpsc::Receiver<Message>,
    state: Arc<ClientState>,
    config: ClientConfig,
) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
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

    // 登录（M1：token 不校验，可空；run_id 来自状态以便重连复用，§6.6 / §8.3）。
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
                                let cfg = config.clone();
                                tokio::spawn(async move {
                                    let _ = workconn::handle_work_conn(r, st, cfg).await;
                                });
                            }
                            Message::Heartbeat(h) => {
                                out_tx.send(Message::HeartbeatResp(HeartbeatResp { ts: h.ts })).await.ok();
                            }
                            Message::LoginResp(r) => {
                                // 路由到连接阶段，供 run() 区分致命 / 可恢复失败（§8.1）。
                                if let Some(tx) = state.login_tx.lock().unwrap().take() {
                                    let _ = tx.send(r);
                                }
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
        })
    }

    #[tokio::test]
    async fn newproxy_resp_routed_to_oneshot() {
        let (client_end, server_end) = duplex(8192);
        let (state, orx) = client_state_with_resp("ssh");
        let (_tx, rx) = mpsc::channel::<Message>(64);
        let config = ClientConfig::default();
        let task = tokio::spawn(control_loop(client_end, rx, state, config));

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
        let task = tokio::spawn(control_loop(client_end, rx, default_state(), config));

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
        let task = tokio::spawn(control_loop(client_end, rx, default_state(), config));

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
        let task = tokio::spawn(control_loop(client_end, rx, default_state(), config));

        let (sr, sw) = split(server_end);
        let mut sr = FramedRead::new(sr, FrameCodec);
        let mut sw = FramedWrite::new(sw, FrameCodec);
        let _ = recv_msg(&mut sr).await; // Login
        send_msg(&mut sw, Message::Close(Close { reason: None })).await;
        task.await.unwrap().unwrap();
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
        });
        let (lotx, lorx) = oneshot::channel();
        state.login_tx.lock().unwrap().replace(lotx);

        let task = tokio::spawn(control_loop(client_end, rx, state, config));

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
}
