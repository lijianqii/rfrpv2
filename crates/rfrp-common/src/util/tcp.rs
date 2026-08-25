//! TCP 连接参数统一配置（NODELAY / keepalive）。
//!
//! RDP 等交互式场景对延迟敏感：禁用 Nagle 可避免小包合并等待；
//! keepalive 用于长连接断线感知。通过 socket2 统一设置，避免 tokio
//! TcpStream 不提供 keepalive setter 的限制。

use std::time::Duration;

use socket2::{SockRef, TcpKeepalive};
use tokio::net::TcpStream;

use crate::constants::TCP_KEEPALIVE_INTERVAL;

/// 对 TCP 流启用 `TCP_NODELAY` 与 keepalive。
pub fn configure_tcp_stream(stream: &TcpStream) -> std::io::Result<()> {
    let sock = SockRef::from(stream);
    sock.set_nodelay(true)?;
    sock.set_tcp_keepalive(
        &TcpKeepalive::new().with_time(Duration::from_secs(TCP_KEEPALIVE_INTERVAL)),
    )?;
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
