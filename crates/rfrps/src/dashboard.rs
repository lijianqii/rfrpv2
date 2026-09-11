//! Dashboard：Basic Auth + 状态 API + Prometheus 指标。

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine;
use rfrp_common::auth::verify_token;
use rfrp_common::config::DashboardSection;
use rfrp_common::util::http::{read_request_head, write_response};
use rfrp_common::util::ratelimit::RateLimiter;
use serde_json::json;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

use crate::state::ServerState;

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
    let head = match read_request_head(
        &mut stream,
        std::time::Duration::from_secs(rfrp_common::constants::HTTP_HEAD_TIMEOUT),
    )
    .await?
    {
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

    // /healthz 免鉴权（仅暴露 up/down，供监控/负载均衡探活）。
    if path == "/healthz" {
        let (status, body) = health_response(state);
        return write_response(&mut stream, status, "text/plain", body, None).await;
    }

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

/// 健康检查响应：accept 循环正常返回 200，否则 503（供探活与告警）。
fn health_response(state: &Arc<ServerState>) -> (u16, &'static str) {
    if state.metrics.is_accepting() {
        (200, "ok\n")
    } else {
        (503, "unhealthy: accept loop failing\n")
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

    drop(sessions);
    let g = state.gauges();

    // 每代理累计统计（按名字排序，便于阅读与测试）。
    let mut proxy_stats: Vec<(String, Arc<crate::metrics::ProxyStats>)> = state
        .proxy_stats
        .lock()
        .unwrap()
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
        },
    })
}

fn render_metrics(state: &Arc<ServerState>) -> String {
    crate::metrics::render_prometheus(state)
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

    let g = state.gauges();
    let uptime = state.metrics.uptime_secs();
    let mut stats_rows: Vec<(String, u64, u64, u64, i64)> = state
        .proxy_stats
        .lock()
        .unwrap()
        .iter()
        .map(|(k, st)| {
            (
                k.clone(),
                st.bytes_up.load(std::sync::atomic::Ordering::Relaxed),
                st.bytes_down.load(std::sync::atomic::Ordering::Relaxed),
                st.connections_total
                    .load(std::sync::atomic::Ordering::Relaxed),
                st.active_connections
                    .load(std::sync::atomic::Ordering::Relaxed),
            )
        })
        .collect();
    stats_rows.sort_by(|a, b| a.0.cmp(&b.0));
    let proxy_table: String = stats_rows
        .iter()
        .map(|(name, up, down, total, active)| {
            format!(
                "<tr><td>{name}</td><td>{up}</td><td>{down}</td><td>{total}</td><td>{active}</td></tr>"
            )
        })
        .collect();
    format!(
        "<html><head><title>rfrp dashboard</title>\
         <meta http-equiv=\"refresh\" content=\"5\"></head><body>\
         <h1>rfrp dashboard <small>v{}</small></h1>\
         <p>uptime: {}s | sessions: {} | proxies: {} | pending work: {} | udp sessions: {} | pooled work conns: {}</p>\
         <h2>Metrics</h2><pre>{}</pre>\
         <h2>Proxies</h2>\
         <table border=1><tr><th>name</th><th>bytes_up</th><th>bytes_down</th><th>connections</th><th>active</th></tr>{}</table>\
         <h2>Sessions</h2>\
         <table border=1><tr><th>run_id</th><th>session_id</th><th>proxies</th></tr>{}</table>\
         </body></html>",
        env!("CARGO_PKG_VERSION"),
        uptime,
        g.sessions,
        g.proxies,
        g.pending_work,
        g.udp_sessions,
        g.pooled_work_conns,
        crate::metrics::render_prometheus(state),
        proxy_table,
        sessions_html
    )
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
    use crate::state::ServerState;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::Mutex;
    use tokio::sync::{mpsc, Notify};

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
        let (tx, _rx) = mpsc::channel::<rfrp_common::protocol::msg::Message>(8);
        let session = Arc::new(Session {
            run_id: "r".into(),
            session_id: "s".into(),
            work_conn_token: "tok".into(),
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
