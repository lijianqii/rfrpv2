//! M5：Dashboard / 指标集成测试。

use std::time::Duration;

use base64::Engine;
use rfrp_common::config::{
    DashboardSection, LogSection, ProxySection, ServerConfig, ServerSection,
};
use rfrps::server::Server;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

fn server_config(dashboard_port: u16) -> ServerConfig {
    ServerConfig {
        server: ServerSection {
            bind_addr: "127.0.0.1".into(),
            bind_port: 0,
            token: "".into(),
            tls_enable: false,
            tls_cert: None,
            tls_key: None,
            work_conn_tls: false,
        },
        dashboard: Some(DashboardSection {
            addr: format!("127.0.0.1:{dashboard_port}"),
            user: "admin".into(),
            password: "secret123".into(),
        }),
        proxy: ProxySection::default(),
        log: LogSection::default(),
    }
}

async fn start_server(port: u16) -> (JoinHandle<()>, std::net::SocketAddr) {
    let server = Server::new(server_config(port)).await.unwrap();
    let addr = server.local_addr();
    let task = tokio::spawn(async move {
        let _ = server.run().await;
    });
    (task, addr)
}

async fn http_get(port: u16, path: &str, auth: Option<&str>) -> (u16, String) {
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

fn basic_auth(user: &str, pass: &str) -> String {
    base64::engine::general_purpose::STANDARD.encode(format!("{user}:{pass}"))
}

#[tokio::test]
async fn dashboard_requires_auth() {
    let port = free_port();
    let (srv, _addr) = start_server(port).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let (status, _) = http_get(port, "/api/status", None).await;
    assert_eq!(status, 401, "dashboard must require auth");

    let (status, body) =
        http_get(port, "/api/status", Some(&basic_auth("admin", "secret123"))).await;
    assert_eq!(status, 200, "authorized request should succeed: {body}");
    assert!(
        body.contains("\"sessions\""),
        "status json missing sessions: {body}"
    );

    srv.abort();
}

#[tokio::test]
async fn dashboard_metrics_and_page() {
    let port = free_port();
    let (srv, _addr) = start_server(port).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let auth = basic_auth("admin", "secret123");

    let (status, body) = http_get(port, "/metrics", Some(&auth)).await;
    assert_eq!(status, 200);
    assert!(
        body.contains("rfrp_connections_total"),
        "metrics missing counters: {body}"
    );
    assert!(body.contains("rfrp_active_connections"));

    let (status, body) = http_get(port, "/", Some(&auth)).await;
    assert_eq!(status, 200);
    assert!(body.contains("<html>"), "expected html page: {body}");

    let (status, _) = http_get(port, "/nope", Some(&auth)).await;
    assert_eq!(status, 404);

    srv.abort();
}

#[tokio::test]
async fn dashboard_rate_limits_excess_requests() {
    let port = free_port();
    let (srv, _addr) = start_server(port).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let auth = basic_auth("admin", "secret123");
    let mut statuses = Vec::new();
    for _ in 0..101 {
        let (status, _) = http_get(port, "/api/status", Some(&auth)).await;
        statuses.push(status);
    }
    assert_eq!(statuses[..100].iter().filter(|s| **s == 200).count(), 100);
    assert_eq!(statuses[100], 429, "101st request should be rate limited");

    srv.abort();
}
