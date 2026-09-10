//! 客户端控制连接单元测试。

use super::*;
use rfrp_common::protocol::msg::{Close, Heartbeat, LoginResp, Message, NewProxyResp, ReqWorkConn};
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
        proxies: HashMap::new(),
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
        proxies: HashMap::new(),
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
        proxies: HashMap::new(),
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

#[tokio::test]
async fn unknown_control_msg_ignored_keeps_loop_alive() {
    // 客户端收到不认识的 StartWorkConn 等控制消息应忽略并保持循环存活。
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
        Message::StartWorkConn(StartWorkConn {
            proxy_name: "p".into(),
            work_id: 7,
        }),
    )
    .await;
    // 随后 Heartbeat 仍应得到回应，证明循环未退出。
    send_msg(&mut sw, Message::Heartbeat(Heartbeat { ts: 3 })).await;
    match recv_msg(&mut sr).await {
        Message::HeartbeatResp(h) => assert_eq!(h.ts, 3),
        other => panic!("expected HeartbeatResp, got {other:?}"),
    }
    send_msg(&mut sw, Message::Close(Close { reason: None })).await;
    task.await.unwrap().unwrap();
}
