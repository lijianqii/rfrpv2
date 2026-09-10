//! 客户端状态端点（可选，`[client] status_addr` 配置后启用）。
//!
//! 提供 `/`（状态页）、`/api/status`（JSON）、`/metrics`（Prometheus 文本）。
//! 仅只读、无鉴权：默认应绑定回环地址；绑定非回环时启动会打印警告。

use std::sync::Arc;

use rfrp_common::config::ClientConfig;
use rfrp_common::util::http::{read_request_head, write_response};
use serde_json::json;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

use crate::metrics::ClientMetrics;

/// 状态端点主循环。
pub async fn run_status_server(
    listener: TcpListener,
    cfg: ClientConfig,
    metrics: Arc<ClientMetrics>,
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _peer)) => {
                        let cfg = cfg.clone();
                        let metrics = metrics.clone();
                        tokio::spawn(async move {
                            let _ = handle_request(stream, &cfg, &metrics).await;
                        });
                    }
                    Err(e) => {
                        tracing::warn!("status accept error: {e}");
                        break;
                    }
                }
            }
            _ = shutdown.cancelled() => {
                tracing::info!("status server shutting down");
                break;
            }
        }
    }
}

async fn handle_request(
    mut stream: TcpStream,
    cfg: &ClientConfig,
    metrics: &Arc<ClientMetrics>,
) -> std::io::Result<()> {
    let head = match read_request_head(&mut stream).await? {
        Some(h) => h,
        None => return Ok(()),
    };
    let mut headers = [httparse::EMPTY_HEADER; 32];
    let mut req = httparse::Request::new(&mut headers);
    let path = match req.parse(&head) {
        Ok(httparse::Status::Complete(_)) => req.path.unwrap_or("/").to_string(),
        _ => "/".to_string(),
    };

    match path.as_str() {
        "/" => {
            let body = render_html(cfg, metrics);
            write_response(&mut stream, 200, "text/html; charset=utf-8", &body, None).await
        }
        "/api/status" => {
            let body = serde_json::to_string_pretty(&status_json(cfg, metrics)).unwrap_or_default();
            write_response(&mut stream, 200, "application/json", &body, None).await
        }
        "/metrics" => {
            let body = metrics.render();
            write_response(&mut stream, 200, "text/plain; version=0.0.4", &body, None).await
        }
        _ => write_response(&mut stream, 404, "text/plain", "Not Found\n", None).await,
    }
}

fn status_json(cfg: &ClientConfig, metrics: &Arc<ClientMetrics>) -> serde_json::Value {
    let proxies: Vec<serde_json::Value> = cfg
        .proxies
        .iter()
        .map(|p| {
            json!({
                "name": p.name,
                "kind": serde_json::to_value(p.r#type).unwrap_or_default(),
                "local": format!("{}:{}", p.local_ip, p.local_port),
                "remote_port": p.remote_port,
                "domains": p.custom_domains,
                "pool_size": p.pool_size,
            })
        })
        .collect();
    json!({
        "version": env!("CARGO_PKG_VERSION"),
        "uptime_seconds": metrics.uptime_secs(),
        "connected": metrics.is_connected(),
        "rtt_ms": metrics.rtt_ms(),
        "server": format!("{}:{}", cfg.client.server_addr, cfg.client.server_port),
        "tls": cfg.client.tls_enable,
        "work_conn_tls": cfg.client.work_conn_tls,
        "proxies": proxies,
        "metrics": {
            "reconnects_total": metrics.reconnects_total.load(std::sync::atomic::Ordering::Relaxed),
            "work_conns_total": metrics.work_conns_total.load(std::sync::atomic::Ordering::Relaxed),
            "work_conn_failures_total": metrics.work_conn_failures_total.load(std::sync::atomic::Ordering::Relaxed),
            "proxy_register_failures_total": metrics.proxy_register_failures_total.load(std::sync::atomic::Ordering::Relaxed),
            "proxy_register_retry_success_total": metrics.proxy_register_retry_success_total.load(std::sync::atomic::Ordering::Relaxed),
        },
    })
}

fn render_html(cfg: &ClientConfig, metrics: &Arc<ClientMetrics>) -> String {
    let rows: String = cfg
        .proxies
        .iter()
        .map(|p| {
            format!(
                "<tr><td>{}</td><td>{:?}</td><td>{}:{}</td><td>{:?}</td><td>{}</td></tr>",
                p.name, p.r#type, p.local_ip, p.local_port, p.remote_port, p.pool_size
            )
        })
        .collect();
    format!(
        "<html><head><title>rfrp client status</title>\
         <meta http-equiv=\"refresh\" content=\"5\"></head><body>\
         <h1>rfrp client <small>v{}</small></h1>\
         <p>server: {}:{} | connected: {} | uptime: {}s</p>\
         <pre>{}</pre>\
         <h2>Proxies</h2>\
         <table border=1><tr><th>name</th><th>type</th><th>local</th><th>remote_port</th><th>pool</th></tr>{}</table>\
         </body></html>",
        env!("CARGO_PKG_VERSION"),
        cfg.client.server_addr,
        cfg.client.server_port,
        metrics.is_connected(),
        metrics.uptime_secs(),
        metrics.render(),
        rows
    )
}
