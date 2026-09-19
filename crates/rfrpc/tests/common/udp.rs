//! UDP 代理测试辅助（见 [`mod@super`] 的说明）。
#![allow(dead_code)]

use std::net::SocketAddr;
use std::time::Duration;

use rfrp_common::config::ClientProxy;
use rfrp_common::protocol::msg::ProxyType;
use tokio::net::UdpSocket;

/// 本地 UDP echo 服务（模拟 RDP 的 UDP 传输 / 通用 UDP 后端）。
pub async fn spawn_udp_echo() -> u16 {
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

/// 构造一个 UDP 代理配置条目。
pub fn udp_proxy(name: &str, local_port: u16, remote_port: u16) -> ClientProxy {
    ClientProxy {
        name: name.into(),
        r#type: ProxyType::Udp,
        local_ip: "127.0.0.1".into(),
        local_port,
        remote_port: Some(remote_port),
        custom_domains: None,
        pool_size: 0,
    }
}

/// 通过 UDP 代理发一个数据报并等待回声；超时/内容不匹配返回 false。
pub async fn udp_echo(addr: SocketAddr, remote_port: u16, data: &[u8]) -> bool {
    let s = match UdpSocket::bind("127.0.0.1:0").await {
        Ok(s) => s,
        Err(_) => return false,
    };
    if s.send_to(data, (addr.ip(), remote_port)).await.is_err() {
        return false;
    }
    let mut buf = vec![0u8; data.len()];
    match tokio::time::timeout(Duration::from_secs(2), s.recv_from(&mut buf)).await {
        Ok(Ok((n, _))) => n == data.len() && buf[..n] == *data,
        _ => false,
    }
}

/// 轮询直到 UDP 代理可用（首个数据报会触发按需工作连接）。
pub async fn wait_udp_ready(server_addr: SocketAddr, remote_port: u16) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !udp_echo(server_addr, remote_port, b"ready").await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "udp proxy did not become ready"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
