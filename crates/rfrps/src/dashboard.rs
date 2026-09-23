//! Dashboard：Basic Auth + 状态 API + Prometheus 指标。

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine;
use rfrp_common::auth::verify_token;
use rfrp_common::config::DashboardSection;
use rfrp_common::util::accept::AcceptRetry;
use rfrp_common::util::http::{html_escape, read_request_head, write_response};
use rfrp_common::util::ratelimit::RateLimiter;
use serde_json::json;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

use crate::state::ServerState;

/// 表单登录成功后下发的会话 Cookie 名。
const SESSION_COOKIE: &str = "rfrp_dashboard";
/// 登录表单请求体上限（防大体积 POST 占用任务与内存）。
const MAX_LOGIN_BODY: usize = 4096;

/// Dashboard HTTP 服务主循环。
pub async fn run_dashboard(
    listener: TcpListener,
    cfg: DashboardSection,
    state: Arc<ServerState>,
    shutdown: CancellationToken,
) {
    let limiter = Arc::new(RateLimiter::new(100, Duration::from_secs(60)));
    // accept 出错不得结束循环：Dashboard 是进程级监听，自行退出后只能靠重启恢复。
    let mut retry = AcceptRetry::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, peer)) => {
                        retry.record_ok();
                        tracing::debug!(%peer, "dashboard connection");
                        let cfg = cfg.clone();
                        let state = state.clone();
                        let limiter = limiter.clone();
                        tokio::spawn(async move {
                            let _ = handle_request(stream, &cfg, &state, &limiter, peer).await;
                        });
                    }
                    Err(e) => {
                        let backoff = retry.record_err();
                        if retry.should_log() {
                            tracing::warn!(
                                consecutive = retry.consecutive(),
                                error = %e,
                                "dashboard accept error; retrying"
                            );
                        }
                        tokio::time::sleep(backoff).await;
                    }
                }
            }
            _ = shutdown.cancelled() => {
                tracing::info!("dashboard shutting down");
                break;
            }
        }
    }
}

async fn handle_request(
    mut stream: TcpStream,
    cfg: &DashboardSection,
    state: &Arc<ServerState>,
    limiter: &RateLimiter,
    peer: SocketAddr,
) -> std::io::Result<()> {
    let head = match read_request_head(
        &mut stream,
        Duration::from_secs(rfrp_common::constants::HTTP_HEAD_TIMEOUT),
    )
    .await?
    {
        Some(h) => h,
        None => return Ok(()),
    };

    if !limiter.allow(peer.ip(), Instant::now()) {
        return write_response(&mut stream, 429, "text/plain", "Too Many Requests\n", None).await;
    }

    let req = ParsedHead::parse(&head);

    // /healthz 免鉴权（仅暴露 up/down，供监控/负载均衡探活）。
    if req.path == "/healthz" {
        let (status, body) = health_response(state);
        return write_response(&mut stream, status, "text/plain", body, None).await;
    }

    let authenticated = authorized(&req, cfg);

    // 表单登录：POST /login 校验凭据，成功后下发会话 Cookie 并跳转首页。
    if req.method.eq_ignore_ascii_case("POST") && req.path == "/login" {
        if req.content_length > MAX_LOGIN_BODY {
            return write_response(&mut stream, 400, "text/plain", "Bad Request\n", None).await;
        }
        let body = read_body(&mut stream, &head, req.content_length).await?;
        let (user, pass) = parse_login_form(&body);
        if verify_token(&cfg.user, &user) && verify_token(&cfg.password, &pass) {
            let value = base64::engine::general_purpose::STANDARD.encode(format!("{user}:{pass}"));
            let headers = format!(
                "Location: /\r\nSet-Cookie: {SESSION_COOKIE}={value}; HttpOnly; SameSite=Strict; Path=/"
            );
            return write_response(&mut stream, 302, "text/plain", "", Some(&headers)).await;
        }
        tracing::warn!(%peer, "dashboard login failed");
        return write_login_page(&mut stream, Some("用户名或密码错误")).await;
    }

    if !authenticated {
        // 浏览器导航请求（Accept: text/html）返回登录页；其余（curl/脚本）返回标准
        // 401 + WWW-Authenticate，保留 Basic Auth 客户端的兼容性。
        if req.accept_html || req.path == "/login" {
            return write_login_page(&mut stream, None).await;
        }
        return write_response(
            &mut stream,
            401,
            "text/plain",
            "Unauthorized\n",
            Some("WWW-Authenticate: Basic realm=\"rfrp dashboard\""),
        )
        .await;
    }

    // 已登录：/login 直接回首页，/logout 清 Cookie 后回首页。
    if req.path == "/login" {
        return write_response(&mut stream, 302, "text/plain", "", Some("Location: /")).await;
    }
    if req.path == "/logout" {
        let headers = format!(
            "Location: /\r\nSet-Cookie: {SESSION_COOKIE}=; Max-Age=0; HttpOnly; SameSite=Strict; Path=/"
        );
        return write_response(&mut stream, 302, "text/plain", "", Some(&headers)).await;
    }

    match req.path.as_str() {
        "/" => {
            let body = render_html(state);
            write_response(&mut stream, 200, "text/html; charset=utf-8", &body, None).await
        }
        "/api/status" => {
            let body = serde_json::to_string_pretty(&status_json(state)).unwrap_or_default();
            write_response(&mut stream, 200, "application/json", &body, None).await
        }
        "/metrics" => {
            let body = render_metrics(state);
            write_response(&mut stream, 200, "text/plain; version=0.0.4", &body, None).await
        }
        _ => write_response(&mut stream, 404, "text/plain", "Not Found\n", None).await,
    }
}

/// 解析后的请求头关键字段（一次解析，多处复用）。
#[derive(Default)]
struct ParsedHead {
    method: String,
    path: String,
    content_length: usize,
    accept_html: bool,
    authorization: Option<String>,
    cookie: Option<String>,
}

impl ParsedHead {
    fn parse(head: &[u8]) -> Self {
        let mut headers = [httparse::EMPTY_HEADER; 32];
        let mut req = httparse::Request::new(&mut headers);
        let mut out = Self {
            method: "GET".into(),
            path: "/".into(),
            ..Default::default()
        };
        if let Ok(httparse::Status::Complete(_)) = req.parse(head) {
            if let Some(m) = req.method {
                out.method = m.to_string();
            }
            if let Some(p) = req.path {
                out.path = p.to_string();
            }
            for h in req.headers.iter() {
                let value = std::str::from_utf8(h.value).unwrap_or("").trim();
                if h.name.eq_ignore_ascii_case("content-length") {
                    out.content_length = value.parse().unwrap_or(0);
                } else if h.name.eq_ignore_ascii_case("accept") {
                    out.accept_html = value.to_ascii_lowercase().contains("text/html");
                } else if h.name.eq_ignore_ascii_case("authorization") {
                    out.authorization = Some(value.to_string());
                } else if h.name.eq_ignore_ascii_case("cookie") {
                    out.cookie = Some(value.to_string());
                }
            }
        }
        out
    }
}

/// 健康检查响应：accept 循环正常返回 200，否则 503（供探活与告警）。
fn health_response(state: &Arc<ServerState>) -> (u16, &'static str) {
    if state.metrics.is_accepting() {
        (200, "ok\n")
    } else {
        (503, "unhealthy: accept loop failing\n")
    }
}

/// 鉴权：接受 `Authorization: Basic` 头（脚本/兼容）或登录后的会话 Cookie。
fn authorized(req: &ParsedHead, cfg: &DashboardSection) -> bool {
    if let Some(auth) = req.authorization.as_deref() {
        if let Some(encoded) = auth.strip_prefix("Basic ") {
            if let Some((user, pass)) = decode_basic(encoded.trim()) {
                return verify_token(&cfg.user, &user) && verify_token(&cfg.password, &pass);
            }
        }
    }
    if let Some(cookie) = req.cookie.as_deref() {
        if let Some(value) = cookie_value(cookie, SESSION_COOKIE) {
            if let Some((user, pass)) = decode_basic(&value) {
                return verify_token(&cfg.user, &user) && verify_token(&cfg.password, &pass);
            }
        }
    }
    false
}

/// 解码 `user:password`（Basic 头与会话 Cookie 的值同为 base64(user:pass)）。
fn decode_basic(encoded: &str) -> Option<(String, String)> {
    let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(encoded) else {
        return None;
    };
    let Ok(text) = String::from_utf8(decoded) else {
        return None;
    };
    let (user, pass) = text.split_once(':')?;
    Some((user.to_string(), pass.to_string()))
}

/// 从 `Cookie:` 头中取出指定名字的值。
fn cookie_value(cookie: &str, name: &str) -> Option<String> {
    cookie.split(';').find_map(|pair| {
        let (k, v) = pair.split_once('=')?;
        (k.trim() == name).then(|| v.trim().to_string())
    })
}

/// 读取表单 POST 的请求体；`head` 中可能已包含部分/全部 body 字节。
async fn read_body(
    stream: &mut TcpStream,
    head: &[u8],
    content_length: usize,
) -> std::io::Result<Vec<u8>> {
    let start = head
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|i| i + 4)
        .unwrap_or(head.len());
    let mut body = head[start..].to_vec();
    while body.len() < content_length {
        let mut tmp = [0u8; 512];
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            break;
        }
        let need = content_length - body.len();
        body.extend_from_slice(&tmp[..n.min(need)]);
    }
    body.truncate(content_length);
    Ok(body)
}

/// 解析 `application/x-www-form-urlencoded` 登录表单，返回 `(user, password)`。
fn parse_login_form(body: &[u8]) -> (String, String) {
    let text = String::from_utf8_lossy(body);
    let mut user = String::new();
    let mut pass = String::new();
    for pair in text.split('&') {
        let Some((k, v)) = pair.split_once('=') else {
            continue;
        };
        match percent_decode(k).as_str() {
            "user" | "username" => user = percent_decode(v),
            "password" | "pass" => pass = percent_decode(v),
            _ => {}
        }
    }
    (user, pass)
}

/// 表单值的百分号解码（`+` → 空格，`%XX` → 字节）。
fn percent_decode(s: &str) -> String {
    fn hex(b: u8) -> Option<u8> {
        match b {
            b'0'..=b'9' => Some(b - b'0'),
            b'a'..=b'f' => Some(b - b'a' + 10),
            b'A'..=b'F' => Some(b - b'A' + 10),
            _ => None,
        }
    }
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => match (hex(bytes[i + 1]), hex(bytes[i + 2])) {
                (Some(h), Some(l)) => {
                    out.push(h * 16 + l);
                    i += 3;
                }
                _ => {
                    out.push(b'%');
                    i += 1;
                }
            },
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// 返回登录页（HTML 表单）。无 `WWW-Authenticate`，浏览器会渲染表单而非弹框。
async fn write_login_page(stream: &mut TcpStream, error: Option<&str>) -> std::io::Result<()> {
    let body = render_login_page(error);
    write_response(
        stream,
        401,
        "text/html; charset=utf-8",
        &body,
        Some("Cache-Control: no-store"),
    )
    .await
}

fn render_login_page(error: Option<&str>) -> String {
    let err = error
        .map(|e| format!("<p class=\"error\">{}</p>", html_escape(e)))
        .unwrap_or_default();
    format!(
        "<!DOCTYPE html><html lang=\"zh-CN\"><head><meta charset=\"utf-8\">\
         <meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\
         <title>rfrp dashboard 登录</title><style>\
         body{{font-family:system-ui,sans-serif;background:#f5f6f8;margin:0;display:flex;\
         align-items:center;justify-content:center;min-height:100vh}}\
         form{{background:#fff;padding:2rem;border-radius:8px;\
         box-shadow:0 2px 12px rgba(0,0,0,.08);width:280px}}\
         h1{{font-size:1.1rem;margin:0 0 1rem}}\
         label{{display:block;font-size:.85rem;color:#555;margin:.6rem 0 .2rem}}\
         input{{width:100%;box-sizing:border-box;padding:.5rem;border:1px solid #ccc;border-radius:4px}}\
         button{{margin-top:1.2rem;width:100%;padding:.55rem;border:0;border-radius:4px;\
         background:#2563eb;color:#fff;font-size:.95rem;cursor:pointer}}\
         .error{{color:#b91c1c;font-size:.85rem;margin:.6rem 0 0}}\
         </style></head><body>\
         <form method=\"post\" action=\"/login\">\
         <h1>rfrp dashboard</h1>{err}\
         <label for=\"u\">用户名</label>\
         <input id=\"u\" name=\"user\" autocomplete=\"username\" autofocus>\
         <label for=\"p\">密码</label>\
         <input id=\"p\" name=\"password\" type=\"password\" autocomplete=\"current-password\">\
         <button type=\"submit\">登录</button>\
         </form></body></html>"
    )
}

fn status_json(state: &Arc<ServerState>) -> serde_json::Value {
    let sessions = state.sessions.lock();
    let session_list: Vec<serde_json::Value> = sessions
        .values()
        .map(|s| {
            let proxies = s.proxies.lock();
            let proxy_list: Vec<serde_json::Value> = proxies
                .iter()
                .map(|(name, e)| {
                    json!({
                        "name": name,
                        "kind": serde_json::to_value(e.kind).unwrap_or_default(),
                        "remote_port": e.remote_port,
                        "domains": e.custom_domains,
                    })
                })
                .collect();
            json!({
                "run_id": s.run_id,
                "session_id": s.session_id,
                "proxies": proxy_list,
            })
        })
        .collect();

    drop(sessions);
    let g = state.gauges();

    // 每代理累计统计（按名字排序，便于阅读与测试）。
    let mut proxy_stats: Vec<(String, Arc<crate::metrics::ProxyStats>)> = state
        .proxy_stats
        .lock()
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    proxy_stats.sort_by(|a, b| a.0.cmp(&b.0));
    let proxy_stats: Vec<serde_json::Value> = proxy_stats
        .iter()
        .map(|(name, st)| {
            json!({
                "name": name,
                "bytes_up": st.bytes_up.load(std::sync::atomic::Ordering::Relaxed),
                "bytes_down": st.bytes_down.load(std::sync::atomic::Ordering::Relaxed),
                "connections_total": st.connections_total.load(std::sync::atomic::Ordering::Relaxed),
                "active_connections": st.active_connections.load(std::sync::atomic::Ordering::Relaxed),
            })
        })
        .collect();

    json!({
        "version": env!("CARGO_PKG_VERSION"),
        "uptime_seconds": state.metrics.uptime_secs(),
        "sessions": session_list,
        "pending_work": g.pending_work,
        "udp_sessions": g.udp_sessions,
        "proxies": g.proxies,
        "pooled_work_conns": g.pooled_work_conns,
        "rtt_ms": state.metrics.rtt_ms(),
        "proxy_stats": proxy_stats,
        "metrics": {
            "total_connections": state.metrics.total_connections.load(std::sync::atomic::Ordering::Relaxed),
            "active_connections": state.metrics.active_connections.load(std::sync::atomic::Ordering::Relaxed),
            "bytes_up": state.metrics.bytes_up.load(std::sync::atomic::Ordering::Relaxed),
            "bytes_down": state.metrics.bytes_down.load(std::sync::atomic::Ordering::Relaxed),
            "accepted_total": state.metrics.accepted_total.load(std::sync::atomic::Ordering::Relaxed),
            "accept_errors_total": state.metrics.accept_errors_total.load(std::sync::atomic::Ordering::Relaxed),
            "udp_dropped_total": state.metrics.udp_dropped_total.load(std::sync::atomic::Ordering::Relaxed),
            "accepting": state.metrics.is_accepting(),
        },
    })
}

fn render_metrics(state: &Arc<ServerState>) -> String {
    crate::metrics::render_prometheus(state)
}

/// 渲染看板页面：内嵌初始状态（首屏立即可见），随后由页面脚本每 5s 轮询刷新。
fn render_html(state: &Arc<ServerState>) -> String {
    // 内嵌 JSON 到 `<script type="application/json">`：把 `<`/`>`/`&` 转义为
    // `\uXXXX`，防止代理名等数据里的 `</script>` 提前结束脚本块（存储型 XSS）。
    let bootstrap = serde_json::to_string(&status_json(state))
        .unwrap_or_else(|_| "{}".into())
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('&', "\\u0026");
    include_str!("dashboard/page.html")
        .replace("__VERSION__", env!("CARGO_PKG_VERSION"))
        .replace("__BOOTSTRAP__", &bootstrap)
}

#[cfg(test)]
mod authorized_tests {
    use super::*;
    use rfrp_common::config::DashboardSection;

    fn cfg() -> DashboardSection {
        DashboardSection {
            addr: "127.0.0.1:7500".into(),
            user: "admin".into(),
            password: "secret123".into(),
        }
    }

    fn parsed(head: &str) -> ParsedHead {
        ParsedHead::parse(head.as_bytes())
    }

    #[test]
    fn authorized_accepts_valid_credentials() {
        let auth = base64::engine::general_purpose::STANDARD.encode("admin:secret123");
        let head = format!("GET / HTTP/1.1\r\nAuthorization: Basic {auth}\r\n\r\n");
        assert!(authorized(&parsed(&head), &cfg()));
    }

    #[test]
    fn authorized_rejects_wrong_credentials() {
        let cfg = cfg();
        let auth = base64::engine::general_purpose::STANDARD.encode("admin:wrongpass");
        let head = format!("GET / HTTP/1.1\r\nAuthorization: Basic {auth}\r\n\r\n");
        assert!(!authorized(&parsed(&head), &cfg));
    }

    #[test]
    fn authorized_rejects_malformed() {
        let cfg = cfg();
        assert!(
            !authorized(&parsed("GET / HTTP/1.1\r\n\r\n"), &cfg),
            "no auth header"
        );
        let head = "GET / HTTP/1.1\r\nAuthorization: Basic !!!\r\n\r\n";
        assert!(!authorized(&parsed(head), &cfg), "invalid base64");
        // 缺少冒号
        let auth = base64::engine::general_purpose::STANDARD.encode("adminsecret123");
        let head = format!("GET / HTTP/1.1\r\nAuthorization: Basic {auth}\r\n\r\n");
        assert!(!authorized(&parsed(&head), &cfg));
    }

    #[test]
    fn authorized_accepts_session_cookie() {
        // 表单登录后浏览器只带 Cookie（无 Authorization 头）。
        let cfg = cfg();
        let value = base64::engine::general_purpose::STANDARD.encode("admin:secret123");
        let head =
            format!("GET / HTTP/1.1\r\nCookie: theme=dark; {SESSION_COOKIE}={value}\r\n\r\n");
        assert!(authorized(&parsed(&head), &cfg));
        // 错误 Cookie 必须被拒。
        let bad = base64::engine::general_purpose::STANDARD.encode("admin:wrong");
        let head = format!("GET / HTTP/1.1\r\nCookie: {SESSION_COOKIE}={bad}\r\n\r\n");
        assert!(!authorized(&parsed(&head), &cfg));
    }

    #[test]
    fn login_form_parses_urlencoded_credentials() {
        let (u, p) = parse_login_form(b"user=admin&password=secret123");
        assert_eq!(u, "admin");
        assert_eq!(p, "secret123");
        // 百分号编码与 `+`（空格）需正确解码。
        let (u, p) = parse_login_form(b"username=a%40b.com&password=p%2Bq+r");
        assert_eq!(u, "a@b.com");
        assert_eq!(p, "p+q r");
        // 未知字段忽略，缺失字段为空。
        let (u, p) = parse_login_form(b"remember=1");
        assert!(u.is_empty() && p.is_empty());
    }

    #[test]
    fn login_page_has_form_and_no_raw_error() {
        let html = render_login_page(Some("<script>alert(1)</script>"));
        assert!(html.contains("method=\"post\" action=\"/login\""));
        assert!(
            !html.contains("<script>alert(1)</script>"),
            "error text must be escaped: {html}"
        );
    }
}

#[cfg(test)]
mod metrics_tests {
    use super::*;

    use crate::state::ServerState;

    #[test]
    fn healthz_reflects_accept_state() {
        let state = ServerState::new();
        assert_eq!(health_response(&state).0, 200);
        state.metrics.inc_accept_error();
        assert_eq!(health_response(&state).0, 503);
        state.metrics.inc_accepted();
        assert_eq!(health_response(&state).0, 200);
    }

    #[test]
    fn render_metrics_includes_session_gauge() {
        let state = ServerState::new();
        let session = crate::control::test_session("r");
        state.sessions.lock().insert("r".into(), session);

        let text = render_metrics(&state);
        assert!(
            text.contains("rfrp_sessions 1"),
            "expected 1 session: {text}"
        );
    }

    #[test]
    fn render_html_escapes_proxy_names() {
        // 代理名由认证客户端控制。看板把它内嵌进 `<script type="application/json">`，
        // 必须把 `<`/`>`/`&` 转义为 `\uXXXX`，否则 `</script>` 会提前结束脚本块（XSS）。
        let state = ServerState::new();
        let evil = "<script>alert(1)</script>";
        let st = state.proxy_stats_for(evil);
        st.connections_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let html = render_html(&state);
        assert!(
            !html.contains("<script>alert(1)</script>"),
            "raw proxy name must not be injected: {html}"
        );
        assert!(
            html.contains("\\u003cscript\\u003ealert(1)\\u003c/script\\u003e"),
            "escaped JSON proxy name expected: {html}"
        );
    }

    #[tokio::test]
    async fn status_json_includes_proxy_details() {
        // 看板需要展示代理的公网端口 / 域名，而不只是名字与类型。
        let state = ServerState::new();
        let session = crate::control::test_session("r");
        session.proxies.lock().insert(
            "web".into(),
            crate::control::test_entry_with(
                rfrp_common::protocol::msg::ProxyType::Http,
                None,
                &["dev.example.com"],
            ),
        );
        session.proxies.lock().insert(
            "ssh".into(),
            crate::control::test_entry_with(
                rfrp_common::protocol::msg::ProxyType::Tcp,
                Some(6000),
                &[],
            ),
        );
        state.sessions.lock().insert("r".into(), session);

        let json = status_json(&state);
        let proxies = json["sessions"][0]["proxies"].as_array().unwrap();
        let by_name = |n: &str| proxies.iter().find(|p| p["name"] == n).unwrap();
        assert_eq!(by_name("ssh")["remote_port"], 6000);
        assert_eq!(by_name("web")["domains"][0], "dev.example.com");
    }
}
