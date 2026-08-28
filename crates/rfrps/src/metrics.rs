//! 服务端运行指标（Prometheus 文本格式）。

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

/// 运行指标。计数器用 `Arc<Atomic*>`，便于多处共享。
pub struct Metrics {
    /// 累计接受的用户连接数。
    pub total_connections: Arc<AtomicU64>,
    /// 当前活跃用户连接数。
    pub active_connections: Arc<AtomicI64>,
    /// 累计上行字节（外部 -> 本地）。
    pub bytes_up: Arc<AtomicU64>,
    /// 累计下行字节（本地 -> 外部）。
    pub bytes_down: Arc<AtomicU64>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            total_connections: Arc::new(AtomicU64::new(0)),
            active_connections: Arc::new(AtomicI64::new(0)),
            bytes_up: Arc::new(AtomicU64::new(0)),
            bytes_down: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl Metrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// 渲染为 Prometheus 文本格式。
    pub fn render(&self) -> String {
        format!(
            "# HELP rfrp_connections_total Total accepted user connections.\n\
             # TYPE rfrp_connections_total counter\n\
             rfrp_connections_total {}\n\
             # TYPE rfrp_active_connections gauge\n\
             rfrp_active_connections {}\n\
             # TYPE rfrp_bytes_up_total counter\n\
             rfrp_bytes_up_total {}\n\
             # TYPE rfrp_bytes_down_total counter\n\
             rfrp_bytes_down_total {}\n",
            self.total_connections.load(Ordering::Relaxed),
            self.active_connections.load(Ordering::Relaxed),
            self.bytes_up.load(Ordering::Relaxed),
            self.bytes_down.load(Ordering::Relaxed),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_contains_all_metrics() {
        let m = Metrics::new();
        m.total_connections.fetch_add(3, Ordering::Relaxed);
        let text = m.render();
        assert!(text.contains("rfrp_connections_total 3"));
        assert!(text.contains("rfrp_active_connections"));
        assert!(text.contains("rfrp_bytes_up_total"));
        assert!(text.contains("rfrp_bytes_down_total"));
    }
}
