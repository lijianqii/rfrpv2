//! 控制连接断开后，服务端会话资源（公网端口）应被**立即**释放。
//!
//! 回归背景：旧实现把会话清理放在写任务 await **之后**，而写任务因 `Session`
//! 结构里持有一份 `tx` 克隆（`drop(tx)` 并不关闭通道）无法通过通道关闭退出，
//! 只能等 1 秒硬超时被 abort —— 公网端口因此多占用约 1 秒。客户端若以不同
//! `run_id` 重连，这段时间会撞上 `port occupied` 并进入退避重试。

mod common;

use std::time::{Duration, Instant};

use rfrp_common::constants::PROTOCOL_VERSION;
use rfrp_common::protocol::msg::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// 写一帧（帧头 + JSON payload）。
async fn write_msg(s: &mut TcpStream, m: &Message) {
    let f = m.to_frame().expect("encode frame");
    let mut buf = Vec::with_capacity(6 + f.payload.len());
    buf.push(f.version);
    buf.push(f.msg_type);
    buf.extend_from_slice(&(f.payload.len() as u32).to_be_bytes());
    buf.extend_from_slice(&f.payload);
    s.write_all(&buf).await.expect("write frame");
    s.flush().await.expect("flush");
}

/// 读一帧的 payload。
async fn read_payload(s: &mut TcpStream) -> Vec<u8> {
    let mut hdr = [0u8; 6];
    s.read_exact(&mut hdr).await.expect("read header");
    let len = u32::from_be_bytes([hdr[2], hdr[3], hdr[4], hdr[5]]) as usize;
    let mut payload = vec![0u8; len];
    s.read_exact(&mut payload).await.expect("read payload");
    payload
}

/// 控制连接断开后，公网端口必须在 500ms 内被释放。
#[tokio::test]
async fn control_disconnect_releases_public_port_promptly() {
    let mut cfg = common::base_server_config();
    cfg.server.token = "tok".into();
    let (server_task, addr) = common::start_server(cfg).await;
    let proxy_port = common::free_port();

    let mut s = TcpStream::connect(addr)
        .await
        .expect("connect to control port");

    // 登录
    write_msg(
        &mut s,
        &Message::Login(Login {
            run_id: uuid::Uuid::new_v4().to_string(),
            token: "tok".into(),
            version: PROTOCOL_VERSION,
        }),
    )
    .await;
    let resp: LoginResp =
        serde_json::from_slice(&read_payload(&mut s).await).expect("parse LoginResp");
    assert!(resp.ok, "login must succeed: {resp:?}");

    // 注册一个 TCP 代理，占用公网端口
    write_msg(
        &mut s,
        &Message::NewProxy(NewProxy {
            proxy_name: "p1".into(),
            r#type: ProxyType::Tcp,
            remote_port: Some(proxy_port),
            custom_domains: None,
        }),
    )
    .await;
    let npr: NewProxyResp =
        serde_json::from_slice(&read_payload(&mut s).await).expect("parse NewProxyResp");
    assert!(npr.ok, "proxy registration must succeed: {npr:?}");

    // 端口已被代理监听占用。
    TcpStream::connect(("127.0.0.1", proxy_port))
        .await
        .expect("proxy port must be bound");

    // 断开控制连接（模拟客户端异常掉线）。
    let t0 = Instant::now();
    drop(s);

    // 公网端口应被立即释放。
    let mut released_after: Option<Duration> = None;
    for _ in 0..200 {
        if tokio::net::TcpListener::bind(("127.0.0.1", proxy_port))
            .await
            .is_ok()
        {
            released_after = Some(t0.elapsed());
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert!(
        released_after.is_some(),
        "proxy port must be released after the control connection drops"
    );
    assert!(
        released_after.unwrap() < Duration::from_millis(500),
        "proxy port released after {:?}; must be well below the 1s writer timeout \
         (regression: session cleanup ran after the writer task await)",
        released_after
    );

    server_task.abort();
}
