//! rfrps 集成测试共享工具。
#![allow(dead_code)]

use std::net::SocketAddr;

use base64::Engine;
use rfrp_common::config::{
    DashboardSection, LogSection, ProxySection, ServerConfig, ServerSection,
};
use rfrps::server::Server;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;

/// 测试端口分配（统一实现见 [`rfrp_common::testutil::free_port`]）。
#[allow(unused_imports)] // 各测试二进制按需使用，未使用时不报错。
pub use rfrp_common::testutil::free_port;

/// 基础服务端配置：回环地址、随机控制端口、关闭 TLS 与 Dashboard。
pub fn base_server_config() -> ServerConfig {
    ServerConfig {
        server: ServerSection {
            bind_addr: "127.0.0.1".into(),
            bind_port: 0,
            work_conn_tls: false,
            ..Default::default()
        },
        dashboard: None,
        proxy: ProxySection::default(),
        log: LogSection::default(),
    }
}

/// 带 Dashboard 的服务端配置（测试固定凭据）。
pub fn dashboard_config(dashboard_port: u16) -> ServerConfig {
    let mut cfg = base_server_config();
    cfg.dashboard = Some(DashboardSection {
        addr: format!("127.0.0.1:{dashboard_port}"),
        user: "admin".into(),
        password: "secret123".into(),
    });
    cfg
}

/// 启动服务端，返回任务句柄与实际监听地址。
pub async fn start_server(cfg: ServerConfig) -> (JoinHandle<()>, SocketAddr) {
    let server = Server::new(cfg).await.unwrap();
    let addr = server.local_addr();
    let task = tokio::spawn(async move {
        let _ = server.run().await;
    });
    (task, addr)
}

/// 极简 GET：返回（状态码, 响应体）。
pub async fn http_get(port: u16, path: &str, auth: Option<&str>) -> (u16, String) {
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let mut req = format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n");
    if let Some(auth) = auth {
        req.push_str(&format!("Authorization: Basic {auth}\r\n"));
    }
    req.push_str("\r\n");
    s.write_all(req.as_bytes()).await.unwrap();

    let mut resp = Vec::new();
    s.read_to_end(&mut resp).await.unwrap();
    let text = String::from_utf8_lossy(&resp).to_string();
    let status = text
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(0);
    (status, text)
}

/// Basic Auth 头的 base64 值。
pub fn basic_auth(user: &str, pass: &str) -> String {
    base64::engine::general_purpose::STANDARD.encode(format!("{user}:{pass}"))
}
