//! Dashboard：Basic Auth + 状态 API + Prometheus 指标。

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use base64::Engine;
use rfrp_common::auth::verify_token;
use rfrp_common::config::DashboardSection;
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

use crate::server::ServerState;

/// Dashboard HTTP 服务主循环。
pub async fn run_dashboard(
    listener: TcpListener,
    cfg: DashboardSection,
    state: Arc<ServerState>,
    shutdown: CancellationToken,
) {
    let limiter = Arc::new(RateLimiter::new(100, Duration::from_secs(60)));
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, peer)) => {
                        tracing::debug!(%peer, "dashboard connection");
                        let cfg = cfg.clone();
                        let state = state.clone();
                        let limiter = limiter.clone();
                        tokio::spawn(async move {
                            let _ = handle_request(stream, &cfg, &state, &limiter, peer).await;
                        });
                    }
                    Err(e) => {
                        tracing::warn!("dashboard accept error: {e}");
                        break;
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
    let head = match read_request_head(&mut stream).await? {
        Some(h) => h,
        None => return Ok(()),
    };

    if !limiter.allow(peer.ip(), Instant::now()) {
        return write_response(&mut stream, 429, "text/plain", "Too Many Requests\n", None).await;
    }

    let mut headers = [httparse::EMPTY_HEADER; 32];
    let mut req = httparse::Request::new(&mut headers);
    let path = match req.parse(&head) {
        Ok(httparse::Status::Complete(_)) => req.path.unwrap_or("/").to_string(),
        _ => "/".to_string(),
    };

    if !authorized(&head, cfg) {
        return write_response(
            &mut stream,
            401,
            "text/plain",
            "Unauthorized\n",
            Some("Basic realm=\"rfrp dashboard\""),
        )
        .await;
    }

    match path.as_str() {
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

/// 读取请求头（到 `\r\n\r\n` 为止），最大 8KB。
async fn read_request_head(stream: &mut TcpStream) -> std::io::Result<Option<Vec<u8>>> {
    let mut buf = Vec::with_capacity(1024);
    let mut tmp = [0u8; 1024];
    loop {
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            return Ok(None);
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            return Ok(Some(buf));
        }
        if buf.len() > 8192 {
            return Ok(Some(buf)); // 超限按整段处理，路径解析失败返回 /
        }
    }
}

fn authorized(head: &[u8], cfg: &DashboardSection) -> bool {
    let mut headers = [httparse::EMPTY_HEADER; 32];
    let mut req = httparse::Request::new(&mut headers);
    if req.parse(head).is_err() {
        return false;
    }
    let Some(auth) = req
        .headers
        .iter()
        .find(|h| h.name.eq_ignore_ascii_case("authorization"))
        .and_then(|h| std::str::from_utf8(h.value).ok())
    else {
        return false;
    };
    let Some(encoded) = auth.strip_prefix("Basic ") else {
        return false;
    };
    let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(encoded) else {
        return false;
    };
    let Ok(text) = String::from_utf8(decoded) else {
        return false;
    };
    let Some((user, pass)) = text.split_once(':') else {
        return false;
    };
    verify_token(&cfg.user, user) && verify_token(&cfg.password, pass)
}

fn status_json(state: &Arc<ServerState>) -> serde_json::Value {
    let sessions = state.sessions.lock().unwrap();
    let session_list: Vec<serde_json::Value> = sessions
        .values()
        .map(|s| {
            let proxies = s.proxies.lock().unwrap();
            let proxy_list: Vec<serde_json::Value> = proxies
                .iter()
                .map(|(name, e)| {
                    json!({
                        "name": name,
                        "kind": serde_json::to_value(e.kind).unwrap_or_default(),
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

    let udp_sessions: usize = state
        .udp
        .lock()
        .unwrap()
        .values()
        .map(|p| p.sessions.lock().unwrap().len())
        .sum();

    json!({
        "sessions": session_list,
        "pending_work": state.pending.lock().unwrap().len(),
        "udp_sessions": udp_sessions,
        "metrics": {
            "total_connections": state.metrics.total_connections.load(std::sync::atomic::Ordering::Relaxed),
            "active_connections": state.metrics.active_connections.load(std::sync::atomic::Ordering::Relaxed),
            "bytes_up": state.metrics.bytes_up.load(std::sync::atomic::Ordering::Relaxed),
            "bytes_down": state.metrics.bytes_down.load(std::sync::atomic::Ordering::Relaxed),
        },
    })
}

fn render_metrics(state: &Arc<ServerState>) -> String {
    let mut text = state.metrics.render();
    let sessions = state.sessions.lock().unwrap().len();
    text.push_str(&format!(
        "# TYPE rfrp_sessions gauge\nrfrp_sessions {sessions}\n"
    ));
    text
}

fn render_html(state: &Arc<ServerState>) -> String {
    let json = status_json(state);
    let sessions_html = json["sessions"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .map(|s| {
                    let proxies = s["proxies"]
                        .as_array()
                        .map(|ps| {
                            ps.iter()
                                .map(|p| format!("{} ({})", p["name"], p["kind"]))
                                .collect::<Vec<_>>()
                                .join(", ")
                        })
                        .unwrap_or_default();
                    format!(
                        "<tr><td>{}</td><td>{}</td><td>{}</td></tr>",
                        s["run_id"], s["session_id"], proxies
                    )
                })
                .collect::<Vec<_>>()
                .join("")
        })
        .unwrap_or_default();

    format!(
        "<html><head><title>rfrp dashboard</title></head><body>\
         <h1>rfrp dashboard</h1>\
         <h2>Metrics</h2><pre>{}</pre>\
         <h2>Sessions</h2>\
         <table border=1><tr><th>run_id</th><th>session_id</th><th>proxies</th></tr>{}</table>\
         </body></html>",
        state.metrics.render(),
        sessions_html
    )
}

async fn write_response(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &str,
    extra_header: Option<&str>,
) -> std::io::Result<()> {
    let reason = match status {
        200 => "OK",
        401 => "Unauthorized",
        404 => "Not Found",
        _ => "OK",
    };
    let mut resp = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n",
        body.len()
    );
    if let Some(h) = extra_header {
        resp.push_str(h);
        resp.push_str("\r\n");
    }
    resp.push_str("Connection: close\r\n\r\n");
    resp.push_str(body);
    stream.write_all(resp.as_bytes()).await
}

/// 简单的每 IP 请求限频（滑动窗口计数）。
struct RateLimiter {
    inner: Mutex<HashMap<std::net::IpAddr, (u32, Instant)>>,
    max: u32,
    window: Duration,
}

impl RateLimiter {
    fn new(max: u32, window: Duration) -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            max,
            window,
        }
    }

    fn allow(&self, ip: std::net::IpAddr, now: Instant) -> bool {
        let mut m = self.inner.lock().unwrap();
        if let Some((count, start)) = m.get_mut(&ip) {
            if now.duration_since(*start) >= self.window {
                *count = 1;
                *start = now;
                true
            } else if *count < self.max {
                *count += 1;
                true
            } else {
                false
            }
        } else {
            m.insert(ip, (1, now));
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_limiter_blocks_after_limit_and_resets() {
        let limiter = RateLimiter::new(3, Duration::from_secs(60));
        let ip: std::net::IpAddr = "127.0.0.1".parse().unwrap();
        let now = Instant::now();
        assert!(limiter.allow(ip, now));
        assert!(limiter.allow(ip, now));
        assert!(limiter.allow(ip, now));
        assert!(
            !limiter.allow(ip, now),
            "4th request in window should be blocked"
        );

        // 窗口过后重置。
        assert!(limiter.allow(ip, now + Duration::from_secs(61)));

        // 不同 IP 不受影响。
        let other: std::net::IpAddr = "127.0.0.2".parse().unwrap();
        assert!(limiter.allow(other, now));
    }
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

    #[test]
    fn authorized_accepts_valid_credentials() {
        let auth = base64::engine::general_purpose::STANDARD.encode("admin:secret123");
        let head = format!("GET / HTTP/1.1\r\nAuthorization: Basic {auth}\r\n\r\n");
        assert!(authorized(head.as_bytes(), &cfg()));
    }

    #[test]
    fn authorized_rejects_wrong_credentials() {
        let cfg = cfg();
        let auth = base64::engine::general_purpose::STANDARD.encode("admin:wrongpass");
        let head = format!("GET / HTTP/1.1\r\nAuthorization: Basic {auth}\r\n\r\n");
        assert!(!authorized(head.as_bytes(), &cfg));
    }

    #[test]
    fn authorized_rejects_malformed() {
        let cfg = cfg();
        assert!(
            !authorized(b"GET / HTTP/1.1\r\n\r\n", &cfg),
            "no auth header"
        );
        let head = b"GET / HTTP/1.1\r\nAuthorization: Basic !!!\r\n\r\n";
        assert!(!authorized(head, &cfg), "invalid base64");
        // 缺少冒号
        let auth = base64::engine::general_purpose::STANDARD.encode("adminsecret123");
        let head = format!("GET / HTTP/1.1\r\nAuthorization: Basic {auth}\r\n\r\n");
        assert!(!authorized(head.as_bytes(), &cfg));
    }
}

#[cfg(test)]
mod metrics_tests {
    use super::*;
    use crate::control::Session;
    use crate::server::ServerState;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::Mutex;
    use tokio::sync::{mpsc, Notify};

    #[test]
    fn render_metrics_includes_session_gauge() {
        let state = ServerState::new();
        let (tx, _rx) = mpsc::channel::<rfrp_common::protocol::msg::Message>(8);
        let session = Arc::new(Session {
            run_id: "r".into(),
            session_id: "s".into(),
            tx,
            proxies: Mutex::new(HashMap::new()),
            proxy_domains: Mutex::new(HashMap::new()),
            stop: Arc::new(Notify::new()),
            pools: Mutex::new(HashMap::new()),
        });
        state.sessions.lock().unwrap().insert("r".into(), session);

        let text = render_metrics(&state);
        assert!(
            text.contains("rfrp_sessions 1"),
            "expected 1 session: {text}"
        );
    }
}
