//! M4：UDP 代理集成测试（DESIGN §8.6）。

mod common;

use std::time::Duration;

use common::*;
use rfrp_common::config::ClientProxy;
use rfrp_common::protocol::msg::ProxyType;
use tokio::net::UdpSocket;

/// 本地 UDP echo 服务。
async fn spawn_udp_echo() -> u16 {
    let s = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let port = s.local_addr().unwrap().port();
    tokio::spawn(async move {
        let mut buf = vec![0u8; 65507];
        while let Ok((n, peer)) = s.recv_from(&mut buf).await {
            let _ = s.send_to(&buf[..n], peer).await;
        }
    });
    port
}

fn init_logging() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_writer(std::io::stderr)
        .try_init();
}

fn udp_proxy(local_port: u16, remote_port: u16) -> ClientProxy {
    ClientProxy {
        name: "udp".into(),
        r#type: ProxyType::Udp,
        local_ip: "127.0.0.1".into(),
        local_port,
        remote_port: Some(remote_port),
        custom_domains: None,
        pool_size: 0,
    }
}

/// 通过代理发送一个 UDP 包并等待回声。
async fn udp_echo(server_addr: std::net::SocketAddr, remote_port: u16, data: &[u8]) -> bool {
    let s = match UdpSocket::bind("127.0.0.1:0").await {
        Ok(s) => s,
        Err(_) => return false,
    };
    if s.send_to(data, (server_addr.ip(), remote_port))
        .await
        .is_err()
    {
        return false;
    }
    let mut buf = vec![0u8; data.len()];
    match tokio::time::timeout(Duration::from_secs(2), s.recv_from(&mut buf)).await {
        Ok(Ok((n, _))) => n == data.len() && buf[..n] == *data,
        _ => false,
    }
}

async fn wait_udp_ready(server_addr: std::net::SocketAddr, remote_port: u16) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !udp_echo(server_addr, remote_port, b"ping").await {
        if tokio::time::Instant::now() >= deadline {
            panic!("udp proxy did not become ready");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn udp_proxy_roundtrip() {
    init_logging();
    let echo_port = spawn_udp_echo().await;
    let (srv, addr) = start_server(server_config(0)).await;
    let remote = free_port();
    let cli = start_client(client_config(
        addr,
        vec![udp_proxy(echo_port, remote)],
        None,
    ))
    .await;
    wait_udp_ready(addr, remote).await;

    // 小包
    assert!(
        udp_echo(addr, remote, b"hello udp").await,
        "small packet echo failed"
    );
    // 大包（1400 字节，接近常见 MTU）
    let big = vec![0xABu8; 1400];
    assert!(
        udp_echo(addr, remote, &big).await,
        "large packet echo failed"
    );
    // 多轮
    for i in 0..5 {
        assert!(udp_echo(addr, remote, format!("pkt-{i}").as_bytes()).await);
    }

    srv.abort();
    cli.abort();
}

#[tokio::test]
async fn udp_proxy_multiple_clients() {
    init_logging();
    let echo_port = spawn_udp_echo().await;
    let (srv, addr) = start_server(server_config(0)).await;
    let remote = free_port();
    let cli = start_client(client_config(
        addr,
        vec![udp_proxy(echo_port, remote)],
        None,
    ))
    .await;
    wait_udp_ready(addr, remote).await;

    // 两个不同源端口的客户端各自建立会话并收到回声。
    let a = tokio::spawn(async move { udp_echo(addr, remote, b"from-a").await });
    let b = tokio::spawn(async move { udp_echo(addr, remote, b"from-b").await });
    assert!(a.await.unwrap(), "client A echo failed");
    assert!(b.await.unwrap(), "client B echo failed");

    srv.abort();
    cli.abort();
}
