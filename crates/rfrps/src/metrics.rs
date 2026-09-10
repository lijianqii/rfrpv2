//! 服务端运行指标（Prometheus 文本格式）。

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// 运行指标。计数器用 `Arc<Atomic*>`，便于多处共享。
pub struct Metrics {
    /// 进程启动时刻（用于 uptime 指标）。
    pub started: Instant,
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
            started: Instant::now(),
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

    /// 进程已运行秒数。
    pub fn uptime_secs(&self) -> u64 {
        self.started.elapsed().as_secs()
    }

    /// 渲染为 Prometheus 文本格式（仅进程计数器；gauge 由 [`crate::metrics::render_prometheus`] 补充）。
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

/// 渲染完整 Prometheus 文本：进程计数器（[`Metrics::render`]）+ 实时 gauge。
pub fn render_prometheus(state: &crate::state::ServerState) -> String {
    let g = state.gauges();
    let mut out = state.metrics.render();
    out.push_str(&format!(
        "# HELP rfrp_uptime_seconds Process uptime in seconds.\n\
         # TYPE rfrp_uptime_seconds gauge\n\
         rfrp_uptime_seconds {}\n\
         # HELP rfrp_sessions Active control sessions.\n\
         # TYPE rfrp_sessions gauge\n\
         rfrp_sessions {}\n\
         # HELP rfrp_proxies Registered proxies.\n\
         # TYPE rfrp_proxies gauge\n\
         rfrp_proxies {}\n\
         # HELP rfrp_pending_work User connections waiting for a work connection.\n\
         # TYPE rfrp_pending_work gauge\n\
         rfrp_pending_work {}\n\
         # HELP rfrp_udp_sessions Active UDP sessions.\n\
         # TYPE rfrp_udp_sessions gauge\n\
         rfrp_udp_sessions {}\n\
         # HELP rfrp_pooled_work_conns Idle pooled work connections.\n\
         # TYPE rfrp_pooled_work_conns gauge\n\
         rfrp_pooled_work_conns {}\n",
        state.metrics.uptime_secs(),
        g.sessions,
        g.proxies,
        g.pending_work,
        g.udp_sessions,
        g.pooled_work_conns,
    ));
    out
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

    #[test]
    fn render_prometheus_includes_gauges_and_uptime() {
        let state = crate::state::ServerState::new();
        let text = render_prometheus(&state);
        for needle in [
            "rfrp_uptime_seconds",
            "rfrp_sessions 0",
            "rfrp_proxies 0",
            "rfrp_pending_work 0",
            "rfrp_udp_sessions 0",
            "rfrp_pooled_work_conns 0",
            "rfrp_connections_total",
            "rfrp_bytes_up_total",
        ] {
            assert!(text.contains(needle), "missing {needle} in:\n{text}");
        }
    }

    #[test]
    fn render_reflects_all_counter_values() {
        // 渲染文本应包含各计数器的实际数值（Prometheus 兼容，§M5）。
        let m = Metrics::new();
        m.total_connections.fetch_add(10, Ordering::Relaxed);
        m.active_connections.fetch_add(4, Ordering::Relaxed);
        m.bytes_up.fetch_add(1024, Ordering::Relaxed);
        m.bytes_down.fetch_add(2048, Ordering::Relaxed);
        let text = m.render();
        assert!(text.contains("rfrp_connections_total 10"));
        assert!(text.contains("rfrp_active_connections 4"));
        assert!(text.contains("rfrp_bytes_up_total 1024"));
        assert!(text.contains("rfrp_bytes_down_total 2048"));
        assert!(text.contains("# TYPE rfrp_connections_total counter"));
        assert!(text.contains("# TYPE rfrp_active_connections gauge"));
    }
}
