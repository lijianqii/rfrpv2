//! M5：Dashboard 与真实客户端会话的集成测试。

mod common;

use std::time::Duration;

use base64::Engine;
use common::*;
use rfrp_common::config::{
    DashboardSection, LogSection, ProxySection, ServerConfig, ServerSection,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

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

async fn http_get(port: u16, path: &str) -> String {
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let auth = base64::engine::general_purpose::STANDARD.encode("admin:secret123");
    let req = format!(
        "GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nAuthorization: Basic {auth}\r\nConnection: close\r\n\r\n"
    );
    s.write_all(req.as_bytes()).await.unwrap();
    let mut resp = Vec::new();
    s.read_to_end(&mut resp).await.unwrap();
    String::from_utf8_lossy(&resp).to_string()
}

#[tokio::test]
async fn dashboard_lists_live_session() {
    let echo_port = spawn_echo().await;
    let dashboard_port = free_port();
    let (srv, addr) = start_server(server_config(dashboard_port)).await;
    let remote = free_port();
    let cli = start_client(client_config(
        addr,
        vec![tcp_proxy("ssh", echo_port, remote)],
        None,
    ))
    .await;
    assert!(
        wait_for_proxy(addr, remote, Duration::from_secs(5)).await,
        "proxy should become ready"
    );

    // 等会话注册进 dashboard 快照。
    let mut body = String::new();
    for _ in 0..30 {
        body = http_get(dashboard_port, "/api/status").await;
        if body.contains("ssh") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        body.contains("ssh"),
        "dashboard status should list the live session proxy: {body}"
    );
    assert!(
        body.contains("sessions"),
        "status should include sessions: {body}"
    );

    srv.abort();
    cli.abort();
}
