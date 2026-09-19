//! M5：Dashboard / 指标集成测试。

mod common;

use std::time::Duration;

use common::*;

#[tokio::test]
async fn dashboard_requires_auth() {
    let port = free_port();
    let (srv, _addr) = start_server(dashboard_config(port)).await;
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
    let (srv, _addr) = start_server(dashboard_config(port)).await;
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
    let (srv, _addr) = start_server(dashboard_config(port)).await;
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
