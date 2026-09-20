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
    assert!(body.contains("<!DOCTYPE html>"), "expected html page: {body}");
    assert!(body.contains("rfrp dashboard"), "expected board title: {body}");
    assert!(
        body.contains("rfrp-bootstrap"),
        "expected bootstrap status data: {body}"
    );
    assert!(
        body.contains("id=\"k-sessions\""),
        "expected kpi cards: {body}"
    );

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

#[tokio::test]
async fn dashboard_browser_gets_login_page() {
    // 浏览器直接访问 ip:port 时应拿到可填写的登录表单，而不是干巴巴的 Unauthorized。
    let port = free_port();
    let (srv, _addr) = start_server(dashboard_config(port)).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let (status, body) = http_get_browser(port, "/").await;
    assert_eq!(status, 401, "login page is returned for unauthenticated users");
    assert!(
        body.contains("action=\"/login\"") && body.contains("type=\"password\""),
        "expected a login form: {body}"
    );

    srv.abort();
}

#[tokio::test]
async fn dashboard_page_embeds_valid_bootstrap_json() {
    // 看板把初始状态内嵌为 JSON，前端首屏直接渲染。转义必须正确（可被 JSON.parse 解析），
    // 否则页面会白屏。
    let port = free_port();
    let (srv, _addr) = start_server(dashboard_config(port)).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let auth = basic_auth("admin", "secret123");
    let (status, body) = http_get(port, "/", Some(&auth)).await;
    assert_eq!(status, 200);

    let anchor = body
        .find("id=\"rfrp-bootstrap\"")
        .expect("bootstrap script tag present");
    let open = body[anchor..].find('>').expect("script open tag") + anchor + 1;
    let close = body[open..].find("</script>").expect("script close tag") + open;
    let json = &body[open..close];
    let v: serde_json::Value =
        serde_json::from_str(json).expect("bootstrap must be valid JSON");
    assert!(v.get("sessions").is_some());
    assert_eq!(v["metrics"]["accepting"], serde_json::json!(true));

    srv.abort();
}

#[tokio::test]
async fn dashboard_unauthorized_api_keeps_basic_challenge() {
    // 非浏览器客户端仍应收到合法的 WWW-Authenticate 头（曾漏写头部名导致浏览器不弹框）。
    let port = free_port();
    let (srv, _addr) = start_server(dashboard_config(port)).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let (status, resp) = http_get(port, "/api/status", None).await;
    assert_eq!(status, 401);
    assert!(
        resp.contains("WWW-Authenticate: Basic realm=\"rfrp dashboard\""),
        "missing basic auth challenge: {resp}"
    );

    srv.abort();
}

#[tokio::test]
async fn dashboard_form_login_grants_cookie_session() {
    let port = free_port();
    let (srv, _addr) = start_server(dashboard_config(port)).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 正确凭据：302 回首页并下发会话 Cookie。
    let (status, resp) =
        http_post_form(port, "/login", "user=admin&password=secret123").await;
    assert_eq!(status, 302, "successful login should redirect: {resp}");
    let cookie = response_cookie(&resp, "rfrp_dashboard").expect("session cookie set");

    // 带上 Cookie 访问首页：应放行。
    let (status, body) =
        http_get_with_headers(port, "/", &format!("Cookie: {cookie}\r\n")).await;
    assert_eq!(status, 200, "cookie session should be authorized: {body}");
    assert!(body.contains("rfrp dashboard"));

    // 错误密码：返回登录页（401），不下发 Cookie。
    let (status, resp) = http_post_form(port, "/login", "user=admin&password=wrong").await;
    assert_eq!(status, 401);
    assert!(resp.contains("action=\"/login\""));
    assert!(response_cookie(&resp, "rfrp_dashboard").is_none());

    srv.abort();
}
