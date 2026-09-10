//! 客户端状态端点集成测试（`[client] status_addr`）。

mod common;

use std::time::Duration;

use common::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// 极简 GET：返回响应体。
async fn http_get(port: u16, path: &str) -> std::io::Result<String> {
    let mut s = TcpStream::connect(("127.0.0.1", port)).await?;
    s.write_all(format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n").as_bytes())
        .await?;
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).await?;
    let text = String::from_utf8_lossy(&buf).into_owned();
    Ok(text
        .split("\r\n\r\n")
        .nth(1)
        .unwrap_or_default()
        .to_string())
}

async fn get_with_retry(port: u16, path: &str, timeout: Duration) -> String {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Ok(body) = http_get(port, path).await {
            if !body.is_empty() {
                return body;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "status endpoint not ready: {path}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn status_endpoint_serves_status_and_metrics() {
    let echo_port = spawn_echo().await;
    let (srv, addr) = start_server(server_config(0)).await;
    let remote = free_port();
    let status_port = free_port();

    let mut cfg = client_config(addr, vec![tcp_proxy("p1", echo_port, remote)], None);
    cfg.client.status_addr = Some(format!("127.0.0.1:{status_port}"));
    let cli = start_client(cfg).await;
    assert!(
        wait_for_proxy(addr, remote, Duration::from_secs(5)).await,
        "proxy should become ready"
    );

    // /api/status：版本、连接状态、代理清单。
    let body = get_with_retry(status_port, "/api/status", Duration::from_secs(5)).await;
    let json: serde_json::Value = serde_json::from_str(&body).expect("valid json");
    assert_eq!(json["connected"], true);
    assert!(json["rtt_ms"].as_u64().is_some());
    assert_eq!(json["proxies"].as_array().unwrap().len(), 1);
    assert_eq!(json["proxies"][0]["name"], "p1");
    assert!(json["uptime_seconds"].as_u64().is_some());

    // /metrics：Prometheus 文本含客户端指标。
    let metrics = get_with_retry(status_port, "/metrics", Duration::from_secs(5)).await;
    assert!(metrics.contains("rfrp_client_uptime_seconds"));
    assert!(metrics.contains("rfrp_client_connected 1"));
    assert!(metrics.contains("rfrp_client_work_conns_total"));
    assert!(metrics.contains("rfrp_client_reconnects_total"));
    // RTT 指标存在（首个心跳往返前为 0）。
    assert!(metrics.contains("rfrp_client_rtt_ms"), "{metrics}");

    // 状态页可访问。
    let html = get_with_retry(status_port, "/", Duration::from_secs(5)).await;
    assert!(html.contains("rfrp client"));

    srv.abort();
    cli.abort();
}

#[tokio::test]
async fn status_endpoint_unknown_path_404() {
    let echo_port = spawn_echo().await;
    let (srv, addr) = start_server(server_config(0)).await;
    let remote = free_port();
    let status_port = free_port();
    let mut cfg = client_config(addr, vec![tcp_proxy("p1", echo_port, remote)], None);
    cfg.client.status_addr = Some(format!("127.0.0.1:{status_port}"));
    let cli = start_client(cfg).await;
    assert!(wait_for_proxy(addr, remote, Duration::from_secs(5)).await);

    // 等端点就绪后请求未知路径。
    let _ = get_with_retry(status_port, "/api/status", Duration::from_secs(5)).await;
    let mut s = TcpStream::connect(("127.0.0.1", status_port))
        .await
        .unwrap();
    s.write_all(b"GET /nope HTTP/1.1\r\nHost: x\r\n\r\n")
        .await
        .unwrap();
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf);
    assert!(text.starts_with("HTTP/1.1 404"), "{text}");

    srv.abort();
    cli.abort();
}
