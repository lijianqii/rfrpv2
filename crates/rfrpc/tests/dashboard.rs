//! Dashboard 与真实客户端会话的集成测试。

mod common;

use std::time::Duration;

use base64::Engine;
use common::*;
use rfrp_common::config::{
    DashboardSection, LogSection, ProxySection, ServerConfig, ServerSection,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};

fn server_config(dashboard_port: u16) -> ServerConfig {
    ServerConfig {
        server: ServerSection {
            bind_addr: "127.0.0.1".into(),
            bind_port: 0,
            work_conn_tls: false,
            ..Default::default()
        },
        dashboard: Some(DashboardSection {
            addr: format!("127.0.0.1:{dashboard_port}"),
            user: "admin".into(),
            password: "secret123".into(),
            ..Default::default()
        }),
        proxy: ProxySection::default(),
        log: LogSection::default(),
    }
}

/// 从 Prometheus 文本中取某指标（`proxy="ssh"`）的值。
fn proxy_metric(body: &str, name: &str) -> u64 {
    body.lines()
        .find(|l| l.starts_with(&format!("{name}{{proxy=\"ssh\"}}")))
        .and_then(|l| l.rsplit(' ').next())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
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

/// 真实 UDP 代理链路：Dashboard /api/status 必须能看到每代理字节和会话数。
#[tokio::test]
async fn dashboard_exposes_udp_proxy_stats() {
    let echo_port = spawn_udp_echo().await;
    let dashboard_port = free_port();
    let (srv, addr) = start_server(server_config(dashboard_port)).await;
    let remote = free_port();
    let cli = start_client(client_config(
        addr,
        vec![udp_proxy("udp", echo_port, remote)],
        None,
    ))
    .await;
    wait_udp_ready(addr, remote).await;

    // 用同一个源端口建立已配对会话，避免把多个新会话的首次建连延迟混进统计。
    let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    sock.connect((addr.ip(), remote)).await.unwrap();
    let payload = b"udp-stats";
    for _ in 0..3 {
        sock.send(payload).await.unwrap();
        let mut buf = [0u8; 16];
        let n = tokio::time::timeout(Duration::from_secs(2), sock.recv(&mut buf))
            .await
            .expect("udp echo timeout")
            .expect("udp echo recv");
        assert_eq!(&buf[..n], payload, "unexpected UDP echo");
    }

    let mut body = String::new();
    let mut status: Option<serde_json::Value> = None;
    for _ in 0..30 {
        body = http_get(dashboard_port, "/api/status").await;
        let payload_body = body.split_once("\r\n\r\n").map(|(_, b)| b).unwrap_or("");
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(payload_body) {
            let bytes_up = json["proxy_stats"]
                .as_array()
                .and_then(|stats| stats.iter().find(|s| s["name"] == "udp"))
                .and_then(|s| s["bytes_up"].as_u64())
                .unwrap_or(0);
            if json["udp_sessions"].as_u64().unwrap_or(0) >= 1
                && bytes_up >= payload.len() as u64 * 3
            {
                status = Some(json);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let json = status.unwrap_or_else(|| panic!("UDP stats did not appear in dashboard: {body}"));
    let stats = json["proxy_stats"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["name"] == "udp")
        .expect("UDP proxy stats missing");
    assert!(
        stats["bytes_up"].as_u64().unwrap_or(0) >= payload.len() as u64 * 3,
        "per-proxy UDP up bytes missing: {json}"
    );
    assert!(
        stats["bytes_down"].as_u64().unwrap_or(0) >= payload.len() as u64 * 3,
        "per-proxy UDP down bytes missing: {json}"
    );
    assert!(
        stats["connections_total"].as_u64().unwrap_or(0) >= 1,
        "per-proxy UDP connections missing: {json}"
    );
    assert!(
        stats["active_connections"].as_u64().unwrap_or(0) >= 1,
        "per-proxy UDP active sessions missing: {json}"
    );

    let udp_sessions = json["udp_sessions"].as_u64().unwrap_or(0);
    assert!(udp_sessions >= 1, "global UDP sessions missing: {json}");
    assert!(
        json["active_sessions"].as_u64().unwrap_or(0) >= udp_sessions,
        "active_sessions must include UDP sessions: {json}"
    );
    assert!(
        json["metrics"]["udp_sessions_total"].as_u64().unwrap_or(0) >= 1,
        "global established UDP counter missing: {json}"
    );
    assert!(
        json["total_sessions"].as_u64().unwrap_or(0) >= 1,
        "total_sessions must include UDP sessions: {json}"
    );

    srv.abort();
    cli.abort();
}

/// 每代理指标：真实流量应计入对应代理（Dashboard /metrics 与 /api/status 可见）。
#[tokio::test]
async fn dashboard_exposes_per_proxy_traffic() {
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

    // 打两轮流量（1000 字节 ×2）。注意 wait_for_proxy 的就绪探测也会计入统计，
    // 因此这里断言下界而非精确值。
    let payload = vec![0x5Au8; 1000];
    for _ in 0..2 {
        expect_echo(remote, addr, &payload).await;
    }
    // 等字节计数 flush（连接关闭时写入，可能滞后于连接计数）。
    let mut body = String::new();
    for _ in 0..30 {
        body = http_get(dashboard_port, "/metrics").await;
        if proxy_metric(&body, "rfrp_proxy_bytes_up_total") >= 2000 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        proxy_metric(&body, "rfrp_proxy_bytes_up_total") >= 2000,
        "per-proxy up bytes should be >= 2000: {body}"
    );
    assert!(
        proxy_metric(&body, "rfrp_proxy_bytes_down_total") >= 2000,
        "per-proxy down bytes should be >= 2000: {body}"
    );
    assert!(
        proxy_metric(&body, "rfrp_proxy_connections_total") >= 2,
        "per-proxy connection count should be >= 2: {body}"
    );

    // /api/status 也应包含每代理统计。
    let status = http_get(dashboard_port, "/api/status").await;
    assert!(status.contains("proxy_stats"), "{status}");
    assert!(status.contains("\"name\": \"ssh\""), "{status}");

    srv.abort();
    cli.abort();
}
