//! 服务端共享状态与待处理工作连接。

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use rfrp_common::constants::MAX_ACTIVE_CONNECTIONS;
use rfrp_common::util::stream::BoxedStream;
use tokio_util::sync::CancellationToken;

use crate::metrics::Metrics;

/// 一条等待工作连接到达的「待处理」项。工作连接到达后取出 `user` 与之桥接。
pub struct PendingWork {
    pub proxy_name: String,
    pub session_id: String,
    /// 用户侧流（类型擦除，兼容明文/TLS/vhost 已读缓冲包装）。
    pub user: Option<BoxedStream>,
}

/// 服务端共享状态（所有 accept 任务共享）。
pub struct ServerState {
    /// 全局自增 work_id 生成器（从 1 开始，0 为保留值，见 DESIGN §6.2.1）。
    pub work_id: AtomicU64,
    /// work_id → 待处理用户连接。工作连接到达后消费。
    pub pending: Mutex<HashMap<u64, PendingWork>>,
    /// run_id → 控制会话。重连时按 run_id 定位旧会话并清理（§8.3）。
    pub sessions: Mutex<HashMap<String, Arc<crate::control::Session>>>,
    /// UDP 代理运行状态（proxy_name -> UdpProxy）。
    pub udp: Mutex<HashMap<String, Arc<crate::udp::UdpProxy>>>,
    /// 运行指标（连接数/流量）。
    pub metrics: Arc<Metrics>,
    /// 最大并发用户连接数（防 DoS 兜底，测试可调小）。
    pub max_active: AtomicI64,
    /// 优雅退出令牌：信号触发后，accept 循环与所有长连接任务据此退出（§14.4）。
    pub shutdown: CancellationToken,
}

impl ServerState {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            work_id: AtomicU64::new(0),
            pending: Mutex::new(HashMap::new()),
            sessions: Mutex::new(HashMap::new()),
            udp: Mutex::new(HashMap::new()),
            metrics: Arc::new(Metrics::new()),
            max_active: AtomicI64::new(MAX_ACTIVE_CONNECTIONS),
            shutdown: CancellationToken::new(),
        })
    }

    /// 分配下一个 work_id（≥1）。
    pub fn next_work_id(&self) -> u64 {
        self.work_id.fetch_add(1, Ordering::SeqCst) + 1
    }
}
