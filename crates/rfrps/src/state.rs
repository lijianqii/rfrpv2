//! 服务端共享状态与待处理工作连接。
//!
//! # 锁顺序
//!
//! 本结构被所有 accept / 控制 / 数据面任务共享，加锁顺序必须统一，否则会出现
//! 反向持锁导致的死锁。已确立的嵌套顺序（从左到右，不可反向获取）：
//!
//! ```text
//! proxy_index → sessions
//! domain_index → sessions
//! ```
//!
//! 即 `session_for_proxy`/`session_for_domain` 先查全局索引，再查 `sessions`。
//! `sessions` 与各会话内部的
//! `proxies` / `pools` / `proxy_domains` **不嵌套**：`gauges` / `status_json` 先克隆
//! 会话句柄、释放 `sessions` 锁，再逐个读取内部表；注册路径也是先在会话锁内写映射、
//! 再在锁外写全局索引。需要跨层操作时（如会话替换后的清理），先收集结果、
//! **释放锁之后**再获取另一把锁：见 control.rs 中 `cleanup`。
//! `pending` / `udp` / `proxy_stats` / `login_failures` 彼此独立，不参与上述嵌套。

use parking_lot::Mutex;
use std::collections::hash_map::{DefaultHasher, Entry};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::net::IpAddr;
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rfrp_common::config::ServerConfig;
use rfrp_common::constants::{
    LOGIN_FAILURE_LIMIT, LOGIN_FAILURE_WINDOW, MAX_ACTIVE_CONNECTIONS, MAX_SESSIONS,
    MAX_SESSIONS_PER_IP, MAX_TOTAL_DOMAINS, MAX_TOTAL_POOLED_WORK_CONNS, MAX_TOTAL_PROXIES,
    MAX_UNAUTHENTICATED_CONNECTIONS, WORK_CONN_REQUEST_INTERVAL_MS,
};
use rfrp_common::util::stream::BoxedStream;
use rfrp_common::util::tasks::TaskGroup;
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

/// 全局资源（代理名/域名/UDP 运行态）的所有者。
///
/// `session_id` 是区分“同一 `run_id` 的新旧会话”的关键：客户端重连替换会话时
/// `run_id` 不变，只有 `session_id` 能证明清理/删除操作是否仍属于当前会话。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceOwner {
    pub run_id: String,
    pub session_id: String,
}

impl ResourceOwner {
    fn of(session: &crate::control::Session) -> Self {
        Self {
            run_id: session.run_id.clone(),
            session_id: session.session_id.clone(),
        }
    }
}

/// 全局资源预留失败原因：名称/域名冲突，或总量上限。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReserveError {
    Conflict,
    Limit,
}

/// 首帧握手阶段的并发许可；drop 时归还计数。
pub struct HandshakeGuard {
    state: Arc<ServerState>,
}

impl Drop for HandshakeGuard {
    fn drop(&mut self) {
        self.state.handshakes_active.fetch_sub(1, Ordering::Relaxed);
    }
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
    /// proxy_name → owner：预热池工作连接按名 O(1) 定位所属会话，
    /// 避免每次池连接到达时全表扫描（§8.2）。条目保存 run_id + session_id，
    /// 注册时原子预留，清理时按 owner 条件删除。
    pub proxy_index: Mutex<HashMap<String, ResourceOwner>>,
    /// domain → (owner, proxy_name)：vhost 路由按域名 O(1) 定位代理。
    ///
    /// 此前每次 vhost 请求都要持 `sessions` 锁遍历所有会话及其 `proxy_domains`，
    /// 请求量或会话数一上来就会串行化在这把锁上；改为与 `proxy_index` 对称的
    /// 全局索引，注册时原子预留，清理时按 owner 条件删除。
    pub domain_index: Mutex<HashMap<String, (ResourceOwner, String)>>,
    /// UDP 代理运行状态（proxy_name -> UdpProxy）。
    pub udp: Mutex<HashMap<String, Arc<crate::udp::UdpProxy>>>,
    /// 登录失败计数（IP -> (失败次数, 窗口起点)），用于登录限速（防 token 穷举）。
    pub login_failures: Mutex<HashMap<IpAddr, (u32, Instant)>>,
    /// 每 IP 登录尝试的分片异步锁：把 `login_allowed` 检查与失败记录串行化，
    /// 关闭并发尝试同时通过检查、随后一起记失败的竞态。64 个分片避免无界表。
    login_locks: Vec<tokio::sync::Mutex<()>>,
    /// 每代理累计统计（proxy_name -> 计数）；会话清理时移除。
    pub proxy_stats: Mutex<HashMap<String, Arc<ProxyStats>>>,
    /// 运行指标（连接数/流量）。
    pub metrics: Arc<Metrics>,
    /// 最大并发用户连接数（防 DoS 兜底，测试可调小）。
    pub max_active: AtomicI64,
    /// 全局代理总数上限。
    pub max_total_proxies: AtomicUsize,
    /// 全局域名索引总数上限。
    pub max_total_domains: AtomicUsize,
    /// 全局预热工作连接总数上限。
    pub max_total_pooled_work_conns: AtomicUsize,
    /// 当前已入池的预热工作连接总数。
    pub pooled_work_conns: AtomicUsize,
    /// 全局并发控制会话上限（测试可调小）。
    pub max_sessions: AtomicUsize,
    /// 单来源 IP 的并发控制会话上限（测试可调小）。
    pub max_sessions_per_ip: AtomicUsize,
    /// 重发 `ReqWorkConn` 的间隔（毫秒，测试可调小）。
    pub pending_request_interval_ms: AtomicU64,
    /// 当前处于首帧握手阶段的连接数。
    pub handshakes_active: AtomicUsize,
    /// 数据面与监听任务跟踪器：池命中的用户桥接、vhost/Dashboard 每连接任务、
    /// 代理监听和 UDP 监听都在这里登记，退出时按 grace 统一等待。
    pub data_tasks: TaskGroup,
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
            login_locks: (0..64).map(|_| tokio::sync::Mutex::new(())).collect(),
            proxy_stats: Mutex::new(HashMap::new()),
            metrics: Arc::new(Metrics::new()),
            max_active: AtomicI64::new(MAX_ACTIVE_CONNECTIONS),
            max_total_proxies: AtomicUsize::new(MAX_TOTAL_PROXIES),
            max_total_domains: AtomicUsize::new(MAX_TOTAL_DOMAINS),
            max_total_pooled_work_conns: AtomicUsize::new(MAX_TOTAL_POOLED_WORK_CONNS),
            pooled_work_conns: AtomicUsize::new(0),
            max_sessions: AtomicUsize::new(MAX_SESSIONS),
            max_sessions_per_ip: AtomicUsize::new(MAX_SESSIONS_PER_IP),
            pending_request_interval_ms: AtomicU64::new(WORK_CONN_REQUEST_INTERVAL_MS),
            handshakes_active: AtomicUsize::new(0),
            data_tasks: TaskGroup::new(),
            shutdown: CancellationToken::new(),
        })
    }

    /// 从服务端配置注入资源上限；直接构造 `ServerState` 的测试保持默认值。
    pub fn configure_limits(&self, cfg: &ServerConfig) {
        self.max_active
            .store(cfg.server.max_active_connections.max(1), Ordering::Relaxed);
        self.max_total_proxies
            .store(cfg.server.max_total_proxies.max(1), Ordering::Relaxed);
        self.max_total_domains
            .store(cfg.server.max_total_domains.max(1), Ordering::Relaxed);
        self.max_total_pooled_work_conns.store(
            cfg.server.max_total_pooled_work_conns.max(1),
            Ordering::Relaxed,
        );
    }

    /// 取（或创建）某代理的统计计数器（测试/无 owner 场景）。
    pub fn proxy_stats_for(&self, proxy_name: &str) -> Arc<ProxyStats> {
        self.proxy_stats_for_owner(proxy_name, "")
    }

    /// 取（或创建）某代理的统计计数器，并记录归属会话。
    pub fn proxy_stats_for_owner(&self, proxy_name: &str, session_id: &str) -> Arc<ProxyStats> {
        let mut m = self.proxy_stats.lock();
        if let Some(st) = m.get(proxy_name) {
            if st.owner_session_id == session_id {
                return st.clone();
            }
        }
        let st = Arc::new(ProxyStats {
            owner_session_id: session_id.to_string(),
            ..Default::default()
        });
        m.insert(proxy_name.to_string(), st.clone());
        st
    }

    /// 移除若干代理的统计；仅当归属会话匹配时删除（会话清理时调用）。
    pub fn remove_proxy_stats(&self, names: &[String], session_id: &str) {
        let mut m = self.proxy_stats.lock();
        for n in names {
            if m.get(n).is_some_and(|st| st.owner_session_id == session_id) {
                m.remove(n);
            }
        }
    }

    /// 取该 IP 对应的登录尝试分片锁。
    ///
    /// 调用方在“检查是否限速 → 校验 token → 记录失败/清除”期间持有它，
    /// 使同名 IP 的并发登录不能同时绕过失败次数上限。
    pub fn login_lock(&self, ip: IpAddr) -> &tokio::sync::Mutex<()> {
        let mut h = DefaultHasher::new();
        ip.hash(&mut h);
        &self.login_locks[(h.finish() as usize) % self.login_locks.len()]
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
        // 表上限：先清理过期项；若窗口内活跃 IP 仍过多，淘汰最早条目，
        // 避免大量来源 IP 撑爆内存（登录仍受每 IP 失败次数限制）。
        // 注意这里在插入之后检查：新 IP 插入后 len 会变成 4097。
        if m.len() > 4096 {
            m.retain(|_, (_, start)| start.elapsed() < window);
            while m.len() > 4096 {
                if let Some(oldest) = m
                    .iter()
                    .min_by_key(|(_, (_, start))| *start)
                    .map(|(ip, _)| *ip)
                {
                    m.remove(&oldest);
                } else {
                    break;
                }
            }
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

    /// 尝试占用一个全局预热工作连接名额。
    pub fn try_acquire_pooled_slot(&self) -> bool {
        let max = self.max_total_pooled_work_conns.load(Ordering::Relaxed);
        // 保持 MSRV 1.75：新工具链的 try_update 更晚引入。
        #[allow(deprecated)]
        self.pooled_work_conns
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                (cur < max).then_some(cur + 1)
            })
            .is_ok()
    }

    /// 归还预热工作连接名额；饱和到 0，避免异常路径把计数减成负数。
    pub fn release_pooled_slots(&self, count: usize) {
        if count == 0 {
            return;
        }
        let mut cur = self.pooled_work_conns.load(Ordering::Relaxed);
        loop {
            let next = cur.saturating_sub(count);
            if self
                .pooled_work_conns
                .compare_exchange_weak(cur, next, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return;
            }
            cur = self.pooled_work_conns.load(Ordering::Relaxed);
        }
    }

    /// 重发 `ReqWorkConn` 的间隔（下限 1ms，避免被误配成 0 造成热循环）。
    pub fn pending_request_interval(&self) -> Duration {
        Duration::from_millis(
            self.pending_request_interval_ms
                .load(Ordering::Relaxed)
                .max(1),
        )
    }

    /// 原子预留 proxy_name；已存在且不属于同一会话时返回 false。
    ///
    /// DESIGN §6.6 要求 proxy_name 全局唯一；预留发生在 bind 之前，失败路径必须
    /// 调用 [`Self::release_proxy`] 回滚，避免名字被永久占用。
    pub fn reserve_proxy(
        &self,
        proxy_name: &str,
        session: &crate::control::Session,
    ) -> std::result::Result<(), ReserveError> {
        let owner = ResourceOwner::of(session);
        let mut idx = self.proxy_index.lock();
        // 新 key 才占全局名额；同名预留/陈旧接管不增加条目数。
        if !idx.contains_key(proxy_name)
            && idx.len() >= self.max_total_proxies.load(Ordering::Relaxed)
        {
            return Err(ReserveError::Limit);
        }
        match idx.entry(proxy_name.to_string()) {
            Entry::Vacant(e) => {
                e.insert(owner);
                Ok(())
            }
            Entry::Occupied(mut e) => {
                if e.get() == &owner {
                    Ok(())
                } else {
                    // 旧会话在 bind 期间被重连替换时可能留下“陈旧预留”。只有确认
                    // 旧 owner 已不是当前会话，才允许接管；否则仍是真正的名称冲突。
                    let existing = e.get().clone();
                    let still_current = self
                        .sessions
                        .lock()
                        .get(&existing.run_id)
                        .is_some_and(|s| s.session_id == existing.session_id);
                    if still_current {
                        Err(ReserveError::Conflict)
                    } else {
                        e.insert(owner);
                        Ok(())
                    }
                }
            }
        }
    }

    /// 条件释放 proxy_name：仅当归属仍是本会话时删除。
    pub fn release_proxy(&self, proxy_name: &str, session: &crate::control::Session) {
        let owner = ResourceOwner::of(session);
        let mut idx = self.proxy_index.lock();
        if idx.get(proxy_name).is_some_and(|o| o == &owner) {
            idx.remove(proxy_name);
        }
    }

    /// 直接登记 proxy_name 归属（测试/内部使用，不检查全局唯一）。
    pub fn index_proxy(&self, proxy_name: &str, session: &crate::control::Session) {
        self.proxy_index
            .lock()
            .insert(proxy_name.to_string(), ResourceOwner::of(session));
    }

    /// 条件移除 proxy 归属记录（会话清理时调用）。
    pub fn unindex_proxies(
        &self,
        names: impl IntoIterator<Item = String>,
        session: &crate::control::Session,
    ) {
        let owner = ResourceOwner::of(session);
        let mut idx = self.proxy_index.lock();
        for n in names {
            if idx.get(&n).is_some_and(|o| o == &owner) {
                idx.remove(&n);
            }
        }
    }

    /// 按 proxy_name 定位所属会话（O(1)）；索引指向旧 session_id 时视为失效。
    pub fn session_for_proxy(&self, proxy_name: &str) -> Option<Arc<crate::control::Session>> {
        let owner = {
            let idx = self.proxy_index.lock();
            idx.get(proxy_name)?.clone()
        };
        let sessions = self.sessions.lock();
        sessions
            .get(&owner.run_id)
            .filter(|s| s.session_id == owner.session_id)
            .cloned()
    }

    /// 原子预留一组域名；任一域名已属于其他会话/其他代理时返回 false。
    pub fn reserve_domains(
        &self,
        domains: &[String],
        session: &crate::control::Session,
        proxy_name: &str,
    ) -> std::result::Result<(), ReserveError> {
        let owner = ResourceOwner::of(session);
        let mut idx = self.domain_index.lock();
        let mut new_keys = std::collections::HashSet::new();
        for d in domains {
            match idx.get(d) {
                None => {
                    new_keys.insert(d.as_str());
                }
                Some((existing_owner, existing_proxy)) => {
                    if existing_owner == &owner && existing_proxy == proxy_name {
                        continue;
                    }
                    // 同 proxy_name 预留：旧会话已失效的域名条目可以被新会话接管，
                    // 否则 cleanup 漏删/在途注册会让新会话永久 DomainConflict。
                    let existing = existing_owner.clone();
                    let still_current = self
                        .sessions
                        .lock()
                        .get(&existing.run_id)
                        .is_some_and(|s| s.session_id == existing.session_id);
                    if still_current {
                        return Err(ReserveError::Conflict);
                    }
                }
            }
        }
        if idx.len().saturating_add(new_keys.len()) > self.max_total_domains.load(Ordering::Relaxed)
        {
            return Err(ReserveError::Limit);
        }
        for d in domains {
            idx.insert(d.clone(), (owner.clone(), proxy_name.to_string()));
        }
        Ok(())
    }

    /// 条件释放一组域名：仅删除仍属于本会话/本代理的条目。
    pub fn release_domains(
        &self,
        domains: &[String],
        session: &crate::control::Session,
        proxy_name: &str,
    ) {
        let owner = ResourceOwner::of(session);
        let mut idx = self.domain_index.lock();
        for d in domains {
            if idx
                .get(d)
                .is_some_and(|(o, p)| o == &owner && p == proxy_name)
            {
                idx.remove(d);
            }
        }
    }

    /// 直接登记域名归属（测试/内部使用，不检查全局唯一）。
    pub fn index_domain(&self, domain: &str, session: &crate::control::Session, proxy_name: &str) {
        self.domain_index.lock().insert(
            domain.to_string(),
            (ResourceOwner::of(session), proxy_name.to_string()),
        );
    }

    /// 条件移除域名归属（会话清理时调用）。
    pub fn unindex_domains(
        &self,
        domains: impl IntoIterator<Item = String>,
        session: &crate::control::Session,
    ) {
        let owner = ResourceOwner::of(session);
        let mut idx = self.domain_index.lock();
        for d in domains {
            if idx.get(&d).is_some_and(|(o, _)| o == &owner) {
                idx.remove(&d);
            }
        }
    }

    /// 按域名定位所属会话与代理名（O(1)，vhost 路由热路径）。
    pub fn session_for_domain(&self, host: &str) -> Option<(Arc<crate::control::Session>, String)> {
        let (owner, proxy_name) = {
            let idx = self.domain_index.lock();
            let (owner, proxy_name) = idx.get(host)?;
            (owner.clone(), proxy_name.clone())
        };
        let sessions = self.sessions.lock();
        sessions
            .get(&owner.run_id)
            .filter(|s| s.session_id == owner.session_id)
            .cloned()
            .map(|s| (s, proxy_name))
    }

    /// 当前 `sessions[run_id]` 是否仍是这个 session（同 run_id 重连后旧 session 为 false）。
    pub fn is_current_session(&self, session: &crate::control::Session) -> bool {
        self.sessions
            .lock()
            .get(&session.run_id)
            .is_some_and(|s| s.session_id == session.session_id)
    }

    /// 获取一个首帧握手许可；达到 [`MAX_UNAUTHENTICATED_CONNECTIONS`] 时返回 None。
    pub fn begin_handshake(self: &Arc<Self>) -> Option<HandshakeGuard> {
        loop {
            let n = self.handshakes_active.load(Ordering::Relaxed);
            if n >= MAX_UNAUTHENTICATED_CONNECTIONS {
                return None;
            }
            if self
                .handshakes_active
                .compare_exchange_weak(n, n + 1, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return Some(HandshakeGuard {
                    state: self.clone(),
                });
            }
        }
    }

    /// 控制链路 RTT（毫秒）：取所有活跃会话中的**最大**值（最差链路）。
    ///
    /// 此前是单一全局值，多会话时被“最后一个回包的会话”覆盖，语义不明。
    /// 逐会话 RTT 见 [`crate::control::Session::rtt_ms`]（Dashboard `/api/status` 给出）。
    /// 无活跃会话时返回 0（与“未测得”一致）。
    pub fn rtt_ms(&self) -> u64 {
        // 与 `gauges()` 一样：先快照会话句柄再逐个读取，不在持锁期间遍历会话内部表。
        let sessions: Vec<Arc<crate::control::Session>> =
            self.sessions.lock().values().cloned().collect();
        sessions
            .iter()
            .map(|s| s.rtt_ms.load(Ordering::Relaxed))
            .max()
            .unwrap_or(0)
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
        // proxy_name → owner 索引：注册后可 O(1) 定位会话，清理后失效（§8.2）。
        let state = ServerState::new();
        let s = test_session("r1");
        state.sessions.lock().insert("r1".into(), s.clone());

        // 未注册时查不到。
        assert!(state.session_for_proxy("web").is_none());

        assert!((state.reserve_proxy("web", &s)).is_ok());
        assert!((state.reserve_proxy("udp-x", &s)).is_ok());
        let found = state.session_for_proxy("web").expect("indexed");
        assert_eq!(found.run_id, "r1");
        assert!(state.session_for_proxy("udp-x").is_some());

        // 条件清理：归属仍是本会话时失效。
        state.unindex_proxies(vec!["web".into(), "udp-x".into()], &s);
        assert!(state.session_for_proxy("web").is_none());
        assert!(state.session_for_proxy("udp-x").is_none());
    }

    #[test]
    fn proxy_name_is_globally_unique() {
        // DESIGN §6.6：不同会话不得注册同名代理。
        let state = ServerState::new();
        let a = test_session("ra");
        let b = test_session("rb");
        state.sessions.lock().insert("ra".into(), a.clone());
        state.sessions.lock().insert("rb".into(), b.clone());

        assert!((state.reserve_proxy("web", &a)).is_ok());
        assert!((state.reserve_proxy("web", &b)).is_err());
        assert!(Arc::ptr_eq(&state.session_for_proxy("web").unwrap(), &a));

        // A 清理后 B 才能注册。
        state.unindex_proxies(vec!["web".into()], &a);
        assert!((state.reserve_proxy("web", &b)).is_ok());
        assert!(Arc::ptr_eq(&state.session_for_proxy("web").unwrap(), &b));
    }

    #[test]
    fn domain_index_roundtrip() {
        // vhost 路由按域名 O(1) 定位会话；清理后必须失效，否则会命中已注销的会话。
        let state = ServerState::new();
        let s = test_session("r1");
        state.sessions.lock().insert("r1".into(), s.clone());

        assert!(state.session_for_domain("dev.example.com").is_none());

        state.index_domain("dev.example.com", &s, "web");
        let (found, proxy) = state
            .session_for_domain("dev.example.com")
            .expect("indexed domain");
        assert_eq!(proxy, "web");
        assert!(Arc::ptr_eq(&found, &s));

        state.unindex_domains(vec!["dev.example.com".into()], &s);
        assert!(state.session_for_domain("dev.example.com").is_none());
    }

    #[test]
    fn domain_index_ignores_stale_run_id() {
        // 索引指向已不存在的会话（例如会话已被替换/清理）时不得 panic，返回 None。
        let state = ServerState::new();
        let ghost = test_session("ghost");
        state.index_domain("dev.example.com", &ghost, "web");
        assert!(state.session_for_domain("dev.example.com").is_none());
    }

    #[test]
    fn stale_reservations_can_be_taken_over() {
        // 旧会话在 bind 中被替换、cleanup 漏掉预留时，新会话应能接管而不是永久 NameExists。
        let state = ServerState::new();
        let old = test_session("r1");
        assert!((state.reserve_proxy("web", &old)).is_ok());
        let domain = vec!["web.example.com".to_string()];
        assert!((state.reserve_domains(&domain, &old, "web")).is_ok());

        let mut new = test_session("r1");
        Arc::get_mut(&mut new)
            .expect("new session not shared yet")
            .session_id = "new-session".into();
        state.sessions.lock().insert("r1".into(), new.clone());

        assert!(
            state.reserve_proxy("web", &new).is_ok(),
            "stale name must be stealable"
        );
        assert!(
            state.reserve_domains(&domain, &new, "web").is_ok(),
            "stale domain must be stealable"
        );
        assert!(Arc::ptr_eq(&state.session_for_proxy("web").unwrap(), &new));
    }

    #[test]
    fn domain_reservation_is_atomic_and_owner_checked() {
        // 两个会话抢同一域名：只有第一个成功；清理只删除自己的条目。
        let state = ServerState::new();
        let a = test_session("ra");
        let b = test_session("rb");
        state.sessions.lock().insert("ra".into(), a.clone());
        state.sessions.lock().insert("rb".into(), b.clone());

        let domain = vec!["web.example.com".to_string()];
        assert!((state.reserve_domains(&domain, &a, "web-a")).is_ok());
        assert!((state.reserve_domains(&domain, &b, "web-b")).is_err());
        assert!(state.session_for_domain("web.example.com").is_some());

        // A 的 cleanup 不会删除 B 的条目（B 没抢到，但条件删除仍应安全）。
        state.unindex_domains(domain.clone(), &b);
        assert!(state.session_for_domain("web.example.com").is_some());
        state.unindex_domains(domain, &a);
        assert!(state.session_for_domain("web.example.com").is_none());
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
    fn proxy_index_owner_prevents_old_cleanup_from_deleting_new_session() {
        // 同一 run_id 重连替换：新旧 session_id 不同，旧 cleanup 不得删除新条目。
        let state = ServerState::new();
        let old = test_session("r1");
        let mut new = test_session("r1");
        Arc::get_mut(&mut new)
            .expect("new session not shared yet")
            .session_id = "new-session".into();
        state.sessions.lock().insert("r1".into(), old.clone());
        assert!((state.reserve_proxy("web", &old)).is_ok());

        // 替换会话（真实登录路径会先 cleanup(old)，这里验证条件删除本身）。
        state.sessions.lock().insert("r1".into(), new.clone());
        state.unindex_proxies(vec!["web".into()], &old);
        // 旧 owner 的索引不再匹配当前 session，查询应为 None。
        assert!(state.session_for_proxy("web").is_none());
        // 新会话可重新预留。
        assert!((state.reserve_proxy("web", &new)).is_ok());

        let found = state.session_for_proxy("web").expect("re-indexed");
        assert!(Arc::ptr_eq(&found, &new));
    }

    #[test]
    fn handshake_guard_enforces_limit() {
        let state = ServerState::new();
        let guards: Vec<_> = (0..MAX_UNAUTHENTICATED_CONNECTIONS)
            .map(|_| state.begin_handshake().expect("under limit"))
            .collect();
        assert!(
            state.begin_handshake().is_none(),
            "must reject beyond limit"
        );
        drop(guards);
        assert!(
            state.begin_handshake().is_some(),
            "permits must be returned on drop"
        );
    }
}

#[cfg(test)]
mod global_limit_tests {
    use super::*;
    use crate::control::test_session;

    #[test]
    fn proxy_global_limit_is_enforced_and_released() {
        let state = ServerState::new();
        let session = test_session("r1");
        state.max_total_proxies.store(1, Ordering::Relaxed);
        assert!(state.reserve_proxy("a", &session).is_ok());
        assert_eq!(state.reserve_proxy("b", &session), Err(ReserveError::Limit));
        state.release_proxy("a", &session);
        assert!(state.reserve_proxy("b", &session).is_ok());
    }

    #[test]
    fn domain_global_limit_is_enforced_and_released() {
        let state = ServerState::new();
        let session = test_session("r1");
        state.max_total_domains.store(1, Ordering::Relaxed);
        let a = vec!["a.example.com".to_string()];
        let b = vec!["b.example.com".to_string()];
        assert!(state.reserve_domains(&a, &session, "web-a").is_ok());
        assert_eq!(
            state.reserve_domains(&b, &session, "web-b"),
            Err(ReserveError::Limit)
        );
        state.release_domains(&a, &session, "web-a");
        assert!(state.reserve_domains(&b, &session, "web-b").is_ok());
    }

    #[test]
    fn configure_limits_sets_all_resource_fields() {
        let state = ServerState::new();
        let mut cfg = ServerConfig::default();
        cfg.server.max_active_connections = 123;
        cfg.server.max_total_proxies = 456;
        cfg.server.max_total_domains = 789;
        cfg.server.max_total_pooled_work_conns = 321;
        state.configure_limits(&cfg);
        assert_eq!(state.max_active.load(Ordering::Relaxed), 123);
        assert_eq!(state.max_total_proxies.load(Ordering::Relaxed), 456);
        assert_eq!(state.max_total_domains.load(Ordering::Relaxed), 789);
        assert_eq!(
            state.max_total_pooled_work_conns.load(Ordering::Relaxed),
            321
        );
    }

    #[test]
    fn repeated_proxy_reservation_is_idempotent() {
        let state = ServerState::new();
        let session = test_session("r1");
        assert!(state.reserve_proxy("web", &session).is_ok());
        assert!(
            state.reserve_proxy("web", &session).is_ok(),
            "same owner must be allowed to re-reserve"
        );
    }

    #[test]
    fn repeated_domain_reservation_is_idempotent() {
        let state = ServerState::new();
        let session = test_session("r1");
        let domains = vec!["a.example.com".to_string()];
        assert!(state.reserve_domains(&domains, &session, "web").is_ok());
        assert!(
            state.reserve_domains(&domains, &session, "web").is_ok(),
            "same owner/proxy must be allowed to re-reserve"
        );
    }

    #[test]
    fn pooled_slots_are_saturated() {
        let state = ServerState::new();
        state
            .max_total_pooled_work_conns
            .store(1, Ordering::Relaxed);
        assert!(state.try_acquire_pooled_slot());
        assert!(!state.try_acquire_pooled_slot());
        state.release_pooled_slots(1);
        assert!(state.try_acquire_pooled_slot());
        state.release_pooled_slots(100);
        assert_eq!(state.pooled_work_conns.load(Ordering::Relaxed), 0);
    }
}
