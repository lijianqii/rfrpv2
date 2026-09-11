//! TCP 连接参数统一配置（NODELAY / keepalive）。
//!
//! RDP 等交互式场景对延迟敏感：禁用 Nagle 可避免小包合并等待；
//! keepalive 用于长连接断线感知（空闲 SSH/RDP 会话在 NAT/防火墙表项过期后
//! 由内核探测发现，避免"终端卡住"）。
//!
//! keepalive 参数为**进程级**配置，启动时由配置注入（见 [`init_keepalive`]）；
//! 未注入时使用默认值（`TCP_KEEPALIVE_INTERVAL` / `TCP_KEEPALIVE_PROBE_INTERVAL`），
//! 可通过 `tcp_keepalive_secs = 0` 禁用。
//!
//! 历史说明：Windows 曾因**只设置空闲时间而未设置探测间隔**（interval=0）导致空闲连接
//! 约 30s 后被断开，因而一度禁用；现统一同时设置 time+interval（Windows 走
//! `SIO_KEEPALIVE_VALS`），与 Linux 行为一致。

use std::sync::OnceLock;
use std::time::Duration;

use socket2::SockRef;
use tokio::net::TcpStream;

use crate::constants::{TCP_KEEPALIVE_INTERVAL, TCP_KEEPALIVE_PROBE_INTERVAL};

/// 进程级 keepalive 参数。
#[derive(Clone, Copy, Debug)]
pub struct KeepaliveConfig {
    /// 空闲多久后开始探测；`Duration::ZERO` 表示禁用 keepalive。
    pub idle: Duration,
    /// 探测间隔。
    pub interval: Duration,
}

impl Default for KeepaliveConfig {
    fn default() -> Self {
        Self {
            idle: Duration::from_secs(TCP_KEEPALIVE_INTERVAL),
            interval: Duration::from_secs(TCP_KEEPALIVE_PROBE_INTERVAL),
        }
    }
}

impl KeepaliveConfig {
    /// 由配置项构造（秒；0 = 禁用）。
    pub fn from_secs(keepalive_secs: Option<u64>) -> Self {
        let secs = keepalive_secs.unwrap_or(TCP_KEEPALIVE_INTERVAL);
        Self {
            idle: Duration::from_secs(secs),
            interval: Duration::from_secs(TCP_KEEPALIVE_PROBE_INTERVAL),
        }
    }
}

static KEEPALIVE: OnceLock<KeepaliveConfig> = OnceLock::new();

/// 注入进程级 keepalive 参数（启动时调用一次；重复调用忽略）。
pub fn init_keepalive(cfg: KeepaliveConfig) {
    let _ = KEEPALIVE.set(cfg);
}

fn keepalive() -> KeepaliveConfig {
    KEEPALIVE.get().copied().unwrap_or_default()
}

/// 对 TCP 流启用 `TCP_NODELAY` 与 keepalive。
pub fn configure_tcp_stream(stream: &TcpStream) -> std::io::Result<()> {
    let sock = SockRef::from(stream);
    sock.set_nodelay(true)?;
    let cfg = keepalive();
    if !cfg.idle.is_zero() {
        set_tcp_keepalive(&sock, &cfg)?;
    }
    Ok(())
}

/// 设置 keepalive（所有平台统一：同时设置空闲时间与探测间隔）。
fn set_tcp_keepalive(sock: &SockRef, cfg: &KeepaliveConfig) -> std::io::Result<()> {
    use socket2::TcpKeepalive;
    sock.set_tcp_keepalive(
        &TcpKeepalive::new()
            .with_time(cfg.idle)
            .with_interval(cfg.interval),
    )
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

    #[tokio::test]
    async fn keepalive_applies_with_time_and_interval() {
        // 同时设置 time+interval 不应报错（Windows 历史问题是 interval=0）。
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let cfg = KeepaliveConfig {
            idle: Duration::from_secs(30),
            interval: Duration::from_secs(5),
        };
        set_tcp_keepalive(&SockRef::from(&client), &cfg).unwrap();
        set_tcp_keepalive(&SockRef::from(&server), &cfg).unwrap();
    }

    #[test]
    fn config_from_secs_disables_when_zero() {
        assert!(KeepaliveConfig::from_secs(Some(0)).idle.is_zero());
        assert_eq!(
            KeepaliveConfig::from_secs(None).idle,
            Duration::from_secs(TCP_KEEPALIVE_INTERVAL)
        );
        assert_eq!(
            KeepaliveConfig::from_secs(Some(120)).idle,
            Duration::from_secs(120)
        );
    }
}
