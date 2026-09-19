//! 极简 HTTP / 指标抓取辅助（Dashboard 与状态端点测试共用，见 [`mod@super`]）。
#![allow(dead_code)]

use base64::Engine;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// 拉取 Dashboard 的 `/metrics`（Basic Auth）。
pub async fn fetch_metrics(port: u16, user: &str, password: &str) -> String {
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let auth = base64::engine::general_purpose::STANDARD.encode(format!("{user}:{password}"));
    let req = format!(
        "GET /metrics HTTP/1.1\r\nHost: 127.0.0.1\r\nAuthorization: Basic {auth}\r\nConnection: close\r\n\r\n"
    );
    s.write_all(req.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).await.unwrap();
    String::from_utf8_lossy(&buf).into_owned()
}

/// 从 Prometheus 文本里取某个 counters 的值（缺失按 0 处理）。
pub fn metric_value(metrics: &str, name: &str) -> u64 {
    let prefix = format!("{name} ");
    metrics
        .lines()
        .find_map(|l| l.strip_prefix(&prefix)?.trim().parse().ok())
        .unwrap_or(0)
}
