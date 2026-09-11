//! 服务端运行指标（Prometheus 文本格式）。

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// 运行指标。计数器用 `Arc<Atomic*>`，便于多处共享。
pub struct Metrics {
    /// 进程启动时刻（用于 uptime 指标）。
    pub started: Instant,
    /// 最近一次控制链路 RTT（毫秒；0 = 尚未测得）。
    pub rtt_ms: AtomicU64,
    /// 主监听端口累计接受的 TCP 连接数（含控制/工作连接）。
    pub accepted_total: AtomicU64,
    /// accept 错误累计次数。
    pub accept_errors_total: AtomicU64,
    /// UDP 因待配对会话达到上限而丢弃的包数。
    pub udp_dropped_total: AtomicU64,
    /// accept 循环当前是否正常（连续失败后置 false，恢复后置 true）。
    pub accepting: AtomicBool,
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
            rtt_ms: AtomicU64::new(0),
            accepted_total: AtomicU64::new(0),
            accept_errors_total: AtomicU64::new(0),
            udp_dropped_total: AtomicU64::new(0),
            accepting: AtomicBool::new(true),
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

    /// 记录一次成功 accept。
    pub fn inc_accepted(&self) {
        self.accepted_total.fetch_add(1, Ordering::Relaxed);
        self.accepting.store(true, Ordering::Relaxed);
    }

    /// 记录一次 accept 错误。
    pub fn inc_accept_error(&self) {
        self.accept_errors_total.fetch_add(1, Ordering::Relaxed);
        self.accepting.store(false, Ordering::Relaxed);
    }

    /// 记录一次因 UDP pending 上限而丢弃的包。
    pub fn inc_udp_dropped(&self) {
        self.udp_dropped_total.fetch_add(1, Ordering::Relaxed);
    }

    /// accept 循环是否正常（供 /healthz 与指标使用）。
    pub fn is_accepting(&self) -> bool {
        self.accepting.load(Ordering::Relaxed)
    }

    /// 记录控制链路 RTT（毫秒）。
    pub fn set_rtt_ms(&self, ms: u64) {
        self.rtt_ms.store(ms, Ordering::Relaxed);
    }

    /// 最近一次控制链路 RTT（毫秒；0 = 未测得）。
    pub fn rtt_ms(&self) -> u64 {
        self.rtt_ms.load(Ordering::Relaxed)
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
             rfrp_bytes_down_total {}\n\
             # HELP rfrp_rtt_ms Control connection round-trip time in milliseconds (0 = unknown).\n\
             # TYPE rfrp_rtt_ms gauge\n\
             rfrp_rtt_ms {}\n\
             # HELP rfrp_accepted_total Accepted TCP connections on the control port.\n\
             # TYPE rfrp_accepted_total counter\n\
             rfrp_accepted_total {}\n\
             # HELP rfrp_accept_errors_total Accept errors.\n\
             # TYPE rfrp_accept_errors_total counter\n\
             rfrp_accept_errors_total {}\n\
             # HELP rfrp_udp_dropped_total UDP datagrams dropped (pending session limit).\n\
             # TYPE rfrp_udp_dropped_total counter\n\
             rfrp_udp_dropped_total {}\n\
             # HELP rfrp_accepting Whether the accept loop is healthy (1/0).\n\
             # TYPE rfrp_accepting gauge\n\
             rfrp_accepting {}\n",
            self.total_connections.load(Ordering::Relaxed),
            self.active_connections.load(Ordering::Relaxed),
            self.bytes_up.load(Ordering::Relaxed),
            self.bytes_down.load(Ordering::Relaxed),
            self.rtt_ms(),
            self.accepted_total.load(Ordering::Relaxed),
            self.accept_errors_total.load(Ordering::Relaxed),
            self.udp_dropped_total.load(Ordering::Relaxed),
            u8::from(self.is_accepting()),
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
