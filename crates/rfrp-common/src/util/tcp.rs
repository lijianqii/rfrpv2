//! TCP 连接参数统一配置（NODELAY / keepalive）。
//!
//! RDP 等交互式场景对延迟敏感：禁用 Nagle 可避免小包合并等待；
//! keepalive 用于长连接断线感知。
//!
//! 注意：Windows 下通过 socket2 设置 TCP keepalive 时，若只设置空闲时间而不设置
//! 探测间隔，可能导致空闲连接在约 30s 后被系统主动断开。因此在 Windows 上暂不
//! 启用 keepalive（只保留 TCP_NODELAY），避免空闲 SSH 等连接周期性掉线。

use std::time::Duration;

use socket2::SockRef;
use tokio::net::TcpStream;

use crate::constants::{TCP_KEEPALIVE_INTERVAL, TCP_KEEPALIVE_PROBE_INTERVAL};

/// 对 TCP 流启用 `TCP_NODELAY` 与 keepalive。
pub fn configure_tcp_stream(stream: &TcpStream) -> std::io::Result<()> {
    let sock = SockRef::from(stream);
    sock.set_nodelay(true)?;
    set_tcp_keepalive(&sock)?;
    Ok(())
}

#[cfg(not(windows))]
fn set_tcp_keepalive(sock: &SockRef) -> std::io::Result<()> {
    use socket2::TcpKeepalive;
    sock.set_tcp_keepalive(
        &TcpKeepalive::new()
            .with_time(Duration::from_secs(TCP_KEEPALIVE_INTERVAL))
            .with_interval(Duration::from_secs(TCP_KEEPALIVE_PROBE_INTERVAL)),
    )
}

#[cfg(windows)]
fn set_tcp_keepalive(_sock: &SockRef) -> std::io::Result<()> {
    // Windows 下 socket2 的 keepalive 设置曾导致空闲连接约 30s 后被主动断开
    // （空闲 SSH 客户端周期性掉线），暂不启用；TCP_NODELAY 已足够降低交互延迟。
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn configure_sets_nodelay() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();

        configure_tcp_stream(&client).unwrap();
        assert!(client.nodelay().unwrap());

        configure_tcp_stream(&server).unwrap();
        assert!(server.nodelay().unwrap());
    }
}
