//! 客户端运行指标（状态端点 / Prometheus 文本用）。
//!
//! 进程级、跨重连累计；控制循环与工作连接任务共享同一实例。

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

/// 客户端指标计数器。
pub struct ClientMetrics {
    /// 进程启动时刻（uptime）。
    pub started: Instant,
    /// 控制连接当前是否已登录成功（gauge 0/1）。
    pub connected: AtomicBool,
    /// 控制连接重连次数（每次进入重连退避计一次）。
    pub reconnects_total: AtomicU64,
    /// 成功建立的工作连接数。
    pub work_conns_total: AtomicU64,
    /// 工作连接建立失败次数（未知代理/建连失败/本地服务不可达等）。
    pub work_conn_failures_total: AtomicU64,
    /// 最近一次控制链路 RTT（毫秒；0 = 尚未测得）。
    pub rtt_ms: AtomicU64,
    /// 代理注册失败次数（含可重试与不可重试）。
    pub proxy_register_failures_total: AtomicU64,
    /// 后台重试后注册成功的代理数。
    pub proxy_register_retry_success_total: AtomicU64,
}

impl Default for ClientMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientMetrics {
    pub fn new() -> Self {
        Self {
            started: Instant::now(),
            connected: AtomicBool::new(false),
            reconnects_total: AtomicU64::new(0),
            work_conns_total: AtomicU64::new(0),
            work_conn_failures_total: AtomicU64::new(0),
            rtt_ms: AtomicU64::new(0),
            proxy_register_failures_total: AtomicU64::new(0),
            proxy_register_retry_success_total: AtomicU64::new(0),
        }
    }

    /// 进程已运行秒数。
    pub fn uptime_secs(&self) -> u64 {
        self.started.elapsed().as_secs()
    }

    pub fn set_connected(&self, v: bool) {
        self.connected.store(v, Ordering::Relaxed);
    }

    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }

    /// 记录控制链路 RTT（毫秒）。
    pub fn set_rtt_ms(&self, ms: u64) {
        self.rtt_ms.store(ms, Ordering::Relaxed);
    }

    /// 最近一次控制链路 RTT（毫秒；0 = 未测得）。
    pub fn rtt_ms(&self) -> u64 {
        self.rtt_ms.load(Ordering::Relaxed)
    }

    pub fn inc_reconnect(&self) {
        self.reconnects_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_work_conn(&self) {
        self.work_conns_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_work_conn_failure(&self) {
        self.work_conn_failures_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_proxy_register_failure(&self) {
        self.proxy_register_failures_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_proxy_register_retry_success(&self) {
        self.proxy_register_retry_success_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// 渲染 Prometheus 文本。
    pub fn render(&self) -> String {
        format!(
            "# HELP rfrp_client_uptime_seconds Client process uptime in seconds.\n\
             # TYPE rfrp_client_uptime_seconds gauge\n\
             rfrp_client_uptime_seconds {}\n\
             # HELP rfrp_client_rtt_ms Control connection round-trip time in milliseconds (0 = unknown).\n\
             # TYPE rfrp_client_rtt_ms gauge\n\
             rfrp_client_rtt_ms {}\n\
             # HELP rfrp_client_connected Whether the control connection is logged in (1/0).\n\
             # TYPE rfrp_client_connected gauge\n\
             rfrp_client_connected {}\n\
             # HELP rfrp_client_reconnects_total Control connection reconnects.\n\
             # TYPE rfrp_client_reconnects_total counter\n\
             rfrp_client_reconnects_total {}\n\
             # HELP rfrp_client_work_conns_total Work connections established.\n\
             # TYPE rfrp_client_work_conns_total counter\n\
             rfrp_client_work_conns_total {}\n\
             # HELP rfrp_client_work_conn_failures_total Work connection failures.\n\
             # TYPE rfrp_client_work_conn_failures_total counter\n\
             rfrp_client_work_conn_failures_total {}\n\
             # HELP rfrp_client_proxy_register_failures_total Proxy registration failures.\n\
             # TYPE rfrp_client_proxy_register_failures_total counter\n\
             rfrp_client_proxy_register_failures_total {}\n\
             # HELP rfrp_client_proxy_register_retry_success_total Proxies registered after retry.\n\
             # TYPE rfrp_client_proxy_register_retry_success_total counter\n\
             rfrp_client_proxy_register_retry_success_total {}\n",
            self.uptime_secs(),
            self.rtt_ms(),
            u8::from(self.is_connected()),
            self.reconnects_total.load(Ordering::Relaxed),
            self.work_conns_total.load(Ordering::Relaxed),
            self.work_conn_failures_total.load(Ordering::Relaxed),
            self.proxy_register_failures_total.load(Ordering::Relaxed),
            self.proxy_register_retry_success_total
                .load(Ordering::Relaxed),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_render_and_increment() {
        let m = ClientMetrics::new();
        let text = m.render();
        assert!(text.contains("rfrp_client_connected 0"));
        assert!(text.contains("rfrp_client_reconnects_total 0"));

        m.set_connected(true);
        m.inc_reconnect();
        m.inc_work_conn();
        m.inc_work_conn_failure();
        m.inc_proxy_register_failure();
        m.inc_proxy_register_retry_success();
        let text = m.render();
        assert!(text.contains("rfrp_client_connected 1"));
        assert!(text.contains("rfrp_client_reconnects_total 1"));
        assert!(text.contains("rfrp_client_work_conns_total 1"));
        assert!(text.contains("rfrp_client_work_conn_failures_total 1"));
        assert!(text.contains("rfrp_client_proxy_register_failures_total 1"));
        assert!(text.contains("rfrp_client_proxy_register_retry_success_total 1"));
    }
}
