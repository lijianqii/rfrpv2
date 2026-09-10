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
    /// proxy_name → run_id：预热池工作连接按名 O(1) 定位所属会话，
    /// 避免每次池连接到达时全表扫描（§8.2）。与 `sessions` 同键（run_id），
    /// 重连替换会话后映射仍有效，清理时按名移除。
    pub proxy_index: Mutex<HashMap<String, String>>,
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
            proxy_index: Mutex::new(HashMap::new()),
            udp: Mutex::new(HashMap::new()),
            metrics: Arc::new(Metrics::new()),
            max_active: AtomicI64::new(MAX_ACTIVE_CONNECTIONS),
            shutdown: CancellationToken::new(),
        })
    }

    /// 分配下一个 work_id（≥1）。原子 RMW 已保证唯一性，Relaxed 足够。
    pub fn next_work_id(&self) -> u64 {
        self.work_id.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// 记录 proxy_name 归属的会话（run_id），注册成功后调用。
    pub fn index_proxy(&self, proxy_name: &str, run_id: &str) {
        self.proxy_index
            .lock()
            .unwrap()
            .insert(proxy_name.to_string(), run_id.to_string());
    }

    /// 批量移除 proxy 归属记录（会话清理时调用）。按名移除，与 run_id 无关：
    /// 即使同一 run_id 的新会话已重新登记同名代理，条目内容也一致，移除无副作用。
    pub fn unindex_proxies(&self, names: impl IntoIterator<Item = String>) {
        let mut idx = self.proxy_index.lock().unwrap();
        for n in names {
            idx.remove(&n);
        }
    }

    /// 按 proxy_name 定位所属会话（O(1)）。
    pub fn session_for_proxy(&self, proxy_name: &str) -> Option<Arc<crate::control::Session>> {
        let idx = self.proxy_index.lock().unwrap();
        let run_id = idx.get(proxy_name)?;
        let sessions = self.sessions.lock().unwrap();
        sessions.get(run_id).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    fn test_session(run_id: &str) -> Arc<crate::control::Session> {
        let (tx, _rx) = mpsc::channel(8);
        Arc::new(crate::control::Session {
            run_id: run_id.into(),
            session_id: "sid".into(),
            tx,
            proxies: Mutex::new(HashMap::new()),
            proxy_domains: Mutex::new(HashMap::new()),
            stop: Arc::new(tokio::sync::Notify::new()),
            pools: Mutex::new(HashMap::new()),
        })
    }

    #[test]
    fn proxy_index_roundtrip() {
        // proxy_name → run_id 索引：注册后可 O(1) 定位会话，清理后失效（§8.2）。
        let state = ServerState::new();
        let s = test_session("r1");
        state.sessions.lock().unwrap().insert("r1".into(), s);

        // 未注册时查不到。
        assert!(state.session_for_proxy("web").is_none());

        state.index_proxy("web", "r1");
        state.index_proxy("udp-x", "r1");
        let found = state.session_for_proxy("web").expect("indexed");
        assert_eq!(found.run_id, "r1");
        assert!(state.session_for_proxy("udp-x").is_some());

        // 按名清理：同名代理的归属一并失效。
        state.unindex_proxies(vec!["web".into(), "udp-x".into()]);
        assert!(state.session_for_proxy("web").is_none());
        assert!(state.session_for_proxy("udp-x").is_none());
    }

    #[test]
    fn proxy_index_survives_session_replace_same_run_id() {
        // 同一 run_id 重连替换会话后，索引仍指向新会话（§8.3 去重语义）。
        let state = ServerState::new();
        let old = test_session("r1");
        state.sessions.lock().unwrap().insert("r1".into(), old);
        state.index_proxy("web", "r1");

        // 新会话替换旧会话（旧会话代理已清理，索引条目内容相同 → 无副作用）。
        let new = test_session("r1");
        state.unindex_proxies(vec!["web".into()]);
        state
            .sessions
            .lock()
            .unwrap()
            .insert("r1".into(), new.clone());
        state.index_proxy("web", "r1");

        let found = state.session_for_proxy("web").expect("re-indexed");
        assert!(Arc::ptr_eq(&found, &new));
    }
}
