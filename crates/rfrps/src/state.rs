//! 服务端共享状态与待处理工作连接。
//!
//! # 锁顺序
//!
//! 本结构被所有 accept / 控制 / 数据面任务共享，加锁顺序必须统一，否则会出现
//! 反向持锁导致的死锁。已确立的嵌套顺序（从左到右，不可反向获取）：
//!
//! ```text
//! proxy_index → sessions
//! ```
//!
//! 即 `session_for_proxy` 先 `proxy_index` 再 `sessions`。`sessions` 与各会话内部的
//! `proxies` / `pools` / `proxy_domains` **不嵌套**：`gauges` / `status_json` 先克隆
//! 会话句柄、释放 `sessions` 锁，再逐个读取内部表；注册路径也是先在会话锁内写映射、
//! 再在锁外写全局索引。需要跨层操作时（如会话替换后的清理），先收集结果、
//! **释放锁之后**再获取另一把锁：见 control.rs 中 `cleanup`。
//! `pending` / `udp` / `proxy_stats` / `login_failures` 彼此独立，不参与上述嵌套。

use parking_lot::Mutex;
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rfrp_common::constants::{
    LOGIN_FAILURE_LIMIT, LOGIN_FAILURE_WINDOW, MAX_ACTIVE_CONNECTIONS, MAX_SESSIONS,
    MAX_SESSIONS_PER_IP, WORK_CONN_REQUEST_INTERVAL_MS,
};
use rfrp_common::util::stream::BoxedStream;
use tokio_util::sync::CancellationToken;

use crate::metrics::{Metrics, ProxyStats};

/// 一条等待工作连接到达的「待处理」项。工作连接到达后取出 `user` 与之桥接。
pub struct PendingWork {
    pub proxy_name: String,
    pub session_id: String,
    /// 用户侧流（类型擦除，兼容明文/TLS/vhost 已读缓冲包装）。
    pub user: Option<BoxedStream>,
    /// 登记时刻：供单一周期扫描任务判定超时（见 `listener::sweep_expired_pending`）。
    pub created: Instant,
}

/// 瞬时 gauge 快照（Dashboard/Prometheus 共用，避免各自重复遍历加锁）。
#[derive(Debug, Clone, Copy, Default)]
pub struct Gauges {
    /// 活跃控制会话数。
    pub sessions: usize,
    /// 已注册代理总数（含 TCP/UDP/HTTP/HTTPS）。
    pub proxies: usize,
    /// 等待工作连接的用户连接数。
    pub pending_work: usize,
    /// UDP 活跃会话数（所有 UDP 代理求和）。
    pub udp_sessions: usize,
    /// 池中空闲工作连接数（所有会话求和）。
    pub pooled_work_conns: usize,
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
    /// domain → (run_id, proxy_name)：vhost 路由按域名 O(1) 定位代理。
    ///
    /// 此前每次 vhost 请求都要持 `sessions` 锁遍历所有会话及其 `proxy_domains`，
    /// 请求量或会话数一上来就会串行化在这把锁上；改为与 `proxy_index` 对称的
    /// 全局索引，注册/清理时维护。
    pub domain_index: Mutex<HashMap<String, (String, String)>>,
    /// UDP 代理运行状态（proxy_name -> UdpProxy）。
    pub udp: Mutex<HashMap<String, Arc<crate::udp::UdpProxy>>>,
    /// 登录失败计数（IP -> (失败次数, 窗口起点)），用于登录限速（防 token 穷举）。
    pub login_failures: Mutex<HashMap<IpAddr, (u32, Instant)>>,
    /// 每代理累计统计（proxy_name -> 计数）；会话清理时移除。
    pub proxy_stats: Mutex<HashMap<String, Arc<ProxyStats>>>,
    /// 运行指标（连接数/流量）。
    pub metrics: Arc<Metrics>,
    /// 最大并发用户连接数（防 DoS 兜底，测试可调小）。
    pub max_active: AtomicI64,
    /// 全局并发控制会话上限（测试可调小）。
    pub max_sessions: AtomicUsize,
    /// 单来源 IP 的并发控制会话上限（测试可调小）。
    pub max_sessions_per_ip: AtomicUsize,
    /// 重发 `ReqWorkConn` 的间隔（毫秒，测试可调小）。
    pub pending_request_interval_ms: AtomicU64,
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
            domain_index: Mutex::new(HashMap::new()),
            udp: Mutex::new(HashMap::new()),
            login_failures: Mutex::new(HashMap::new()),
            proxy_stats: Mutex::new(HashMap::new()),
            metrics: Arc::new(Metrics::new()),
            max_active: AtomicI64::new(MAX_ACTIVE_CONNECTIONS),
            max_sessions: AtomicUsize::new(MAX_SESSIONS),
            max_sessions_per_ip: AtomicUsize::new(MAX_SESSIONS_PER_IP),
            pending_request_interval_ms: AtomicU64::new(WORK_CONN_REQUEST_INTERVAL_MS),
            shutdown: CancellationToken::new(),
        })
    }

    /// 取（或创建）某代理的统计计数器。
    pub fn proxy_stats_for(&self, proxy_name: &str) -> Arc<ProxyStats> {
        self.proxy_stats
            .lock()
            .entry(proxy_name.to_string())
            .or_default()
            .clone()
    }

    /// 移除若干代理的统计（会话清理时调用）。
    pub fn remove_proxy_stats(&self, names: &[String]) {
        let mut m = self.proxy_stats.lock();
        for n in names {
            m.remove(n);
        }
    }

    /// 该 IP 当前是否允许尝试登录（窗口内失败次数未超限）。
    pub fn login_allowed(&self, ip: IpAddr) -> bool {
        let mut m = self.login_failures.lock();
        let window = Duration::from_secs(LOGIN_FAILURE_WINDOW);
        match m.get(&ip) {
            Some((count, start)) if start.elapsed() < window => *count < LOGIN_FAILURE_LIMIT,
            Some(_) => {
                m.remove(&ip);
                true
            }
            None => true,
        }
    }

    /// 记录一次登录失败；条目过多时清理过期项（防内存增长）。
    pub fn record_login_failure(&self, ip: IpAddr) {
        let window = Duration::from_secs(LOGIN_FAILURE_WINDOW);
        let mut m = self.login_failures.lock();
        match m.get_mut(&ip) {
            Some((count, start)) if start.elapsed() < window => *count += 1,
            Some(e) => *e = (1, Instant::now()),
            None => {
                m.insert(ip, (1, Instant::now()));
            }
        }
        if m.len() > 4096 {
            m.retain(|_, (_, start)| start.elapsed() < window);
        }
    }

    /// 登录成功后清除该 IP 的失败计数。
    pub fn clear_login_failures(&self, ip: IpAddr) {
        self.login_failures.lock().remove(&ip);
    }

    /// 采样瞬时 gauge。
    ///
    /// 先快照会话 Arc 再逐个统计：`sessions` 锁只在克隆句柄时短暂持有，
    /// 不会因为遍历各会话的内部表而拖住登录/注销路径。
    pub fn gauges(&self) -> Gauges {
        let sessions: Vec<Arc<crate::control::Session>> =
            self.sessions.lock().values().cloned().collect();
        let mut g = Gauges {
            sessions: sessions.len(),
            ..Default::default()
        };
        for s in &sessions {
            g.proxies += s.proxies.lock().len();
            g.pooled_work_conns += s.pools.lock().values().map(|v| v.len()).sum::<usize>();
        }
        g.pending_work = self.pending.lock().len();
        g.udp_sessions = self
            .udp
            .lock()
            .values()
            .map(|p| p.sessions.lock().len())
            .sum();
        g
    }

    /// 分配下一个 work_id（≥1）。原子 RMW 已保证唯一性，Relaxed 足够。
    pub fn next_work_id(&self) -> u64 {
        self.work_id.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// 重发 `ReqWorkConn` 的间隔（下限 1ms，避免被误配成 0 造成热循环）。
    pub fn pending_request_interval(&self) -> Duration {
        Duration::from_millis(
            self.pending_request_interval_ms
                .load(Ordering::Relaxed)
                .max(1),
        )
    }

    /// 记录 proxy_name 归属的会话（run_id），注册成功后调用。
    pub fn index_proxy(&self, proxy_name: &str, run_id: &str) {
        self.proxy_index
            .lock()
            .insert(proxy_name.to_string(), run_id.to_string());
    }

    /// 批量移除 proxy 归属记录（会话清理时调用）。按名移除，与 run_id 无关：
    /// 即使同一 run_id 的新会话已重新登记同名代理，条目内容也一致，移除无副作用。
    pub fn unindex_proxies(&self, names: impl IntoIterator<Item = String>) {
        let mut idx = self.proxy_index.lock();
        for n in names {
            idx.remove(&n);
        }
    }

    /// 按 proxy_name 定位所属会话（O(1)）。
    pub fn session_for_proxy(&self, proxy_name: &str) -> Option<Arc<crate::control::Session>> {
        let idx = self.proxy_index.lock();
        let run_id = idx.get(proxy_name)?;
        let sessions = self.sessions.lock();
        sessions.get(run_id).cloned()
    }

    /// 登记域名归属（vhost 路由用，注册成功后调用）。
    pub fn index_domain(&self, domain: &str, run_id: &str, proxy_name: &str) {
        self.domain_index.lock().insert(
            domain.to_string(),
            (run_id.to_string(), proxy_name.to_string()),
        );
    }

    /// 批量移除域名归属（会话清理时调用）。按域名移除，与 run_id 无关：
    /// 同一 run_id 重连后若已重新登记同名域名，条目内容一致，移除无副作用
    /// （清理发生在旧会话注销、新会话注册之前）。
    pub fn unindex_domains(&self, domains: impl IntoIterator<Item = String>) {
        let mut idx = self.domain_index.lock();
        for d in domains {
            idx.remove(&d);
        }
    }

    /// 按域名定位所属会话与代理名（O(1)，vhost 路由热路径）。
    pub fn session_for_domain(&self, host: &str) -> Option<(Arc<crate::control::Session>, String)> {
        let (run_id, proxy_name) = {
            let idx = self.domain_index.lock();
            let (run_id, proxy_name) = idx.get(host)?;
            (run_id.clone(), proxy_name.clone())
        };
        let sessions = self.sessions.lock();
        sessions.get(&run_id).cloned().map(|s| (s, proxy_name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::test_session;

    #[test]
    fn login_rate_limit_blocks_after_failures() {
        let state = ServerState::new();
        let ip: IpAddr = "203.0.113.7".parse().unwrap();
        assert!(state.login_allowed(ip));
        for _ in 0..LOGIN_FAILURE_LIMIT {
            assert!(state.login_allowed(ip), "still under the limit");
            state.record_login_failure(ip);
        }
        assert!(!state.login_allowed(ip), "must block after limit reached");
        // 成功登录清除计数后恢复。
        state.clear_login_failures(ip);
        assert!(state.login_allowed(ip));
        // 其他 IP 不受影响。
        assert!(state.login_allowed("203.0.113.8".parse().unwrap()));
    }

    #[test]
    fn proxy_index_roundtrip() {
        // proxy_name → run_id 索引：注册后可 O(1) 定位会话，清理后失效（§8.2）。
        let state = ServerState::new();
        let s = test_session("r1");
        state.sessions.lock().insert("r1".into(), s);

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
    fn domain_index_roundtrip() {
        // vhost 路由按域名 O(1) 定位会话；清理后必须失效，否则会命中已注销的会话。
        let state = ServerState::new();
        let s = test_session("r1");
        state.sessions.lock().insert("r1".into(), s.clone());

        assert!(state.session_for_domain("dev.example.com").is_none());

        state.index_domain("dev.example.com", "r1", "web");
        let (found, proxy) = state
            .session_for_domain("dev.example.com")
            .expect("indexed domain");
        assert_eq!(proxy, "web");
        assert!(Arc::ptr_eq(&found, &s));

        state.unindex_domains(vec!["dev.example.com".into()]);
        assert!(state.session_for_domain("dev.example.com").is_none());
    }

    #[test]
    fn domain_index_ignores_stale_run_id() {
        // 索引指向已不存在的会话（例如会话已被替换/清理）时不得 panic，返回 None。
        let state = ServerState::new();
        state.index_domain("dev.example.com", "ghost", "web");
        assert!(state.session_for_domain("dev.example.com").is_none());
    }

    #[tokio::test]
    async fn gauges_count_sessions_proxies_and_pools() {
        let state = ServerState::new();
        let s1 = test_session("r1");
        // 一个代理 + 两条池连接
        s1.proxies.lock().insert(
            "web".into(),
            crate::control::test_entry(rfrp_common::protocol::msg::ProxyType::Tcp),
        );
        s1.pools
            .lock()
            .insert("web".into(), vec![Box::new(tokio::io::duplex(8).0)]);
        state.sessions.lock().insert("r1".into(), s1);
        state.pending.lock().insert(
            1,
            PendingWork {
                proxy_name: "web".into(),
                session_id: "sid".into(),
                user: None,
                created: Instant::now(),
            },
        );

        let g = state.gauges();
        assert_eq!(g.sessions, 1);
        assert_eq!(g.proxies, 1);
        assert_eq!(g.pooled_work_conns, 1);
        assert_eq!(g.pending_work, 1);
        assert_eq!(g.udp_sessions, 0);
    }

    #[test]
    fn proxy_index_survives_session_replace_same_run_id() {
        // 同一 run_id 重连替换会话后，索引仍指向新会话（§8.3 去重语义）。
        let state = ServerState::new();
        let old = test_session("r1");
        state.sessions.lock().insert("r1".into(), old);
        state.index_proxy("web", "r1");

        // 新会话替换旧会话（旧会话代理已清理，索引条目内容相同 → 无副作用）。
        let new = test_session("r1");
        state.unindex_proxies(vec!["web".into()]);
        state.sessions.lock().insert("r1".into(), new.clone());
        state.index_proxy("web", "r1");

        let found = state.session_for_proxy("web").expect("re-indexed");
        assert!(Arc::ptr_eq(&found, &new));
    }
}
