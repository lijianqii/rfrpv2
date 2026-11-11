//! UDP 代理：会话映射 + 4 字节长度前缀分帧（DESIGN §8.6）。

use parking_lot::Mutex;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use rfrp_common::constants::{
    MAX_PENDING_UDP_SESSIONS, MAX_UDP_PACKET_SIZE, MAX_UDP_SESSIONS_PER_PROXY,
    UDP_BUF_INITIAL_BYTES, UDP_PACKET_POOL_MAX_BUF_BYTES, UDP_PROXY_QUEUE_MAX_BYTES,
    UDP_READ_BATCH, UDP_RECV_BATCH, UDP_SESSION_QUEUE_DEPTH, UDP_WRITE_BATCH,
    WORK_CONN_TIMEOUT_RFRPS,
};
use rfrp_common::error::Result;
use rfrp_common::protocol::msg::*;
use rfrp_common::util::accept::AcceptRetry;
use rfrp_common::util::control::send_with_timeout;
use rfrp_common::util::udp::{enlarge_recv_buffer, write_udp_frames_buffered, UdpFrameBuf};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::control::Session;
use crate::state::ServerState;

/// 每个 UDP 代理保留的最大空闲包缓冲数。
const UDP_PACKET_POOL_MAX: usize = 256;

/// 服务端上行队列中的 UDP 数据报：持有从池中借出的缓冲。
///
/// 成功批量写入后由 [`recycle_packets`] 归还；通道满/写失败等丢弃路径随
/// `UdpPacket` 一起释放，不进入池（避免为了归还而给 Drop 加锁）。
/// `budget` 记录该包在代理队列中的字节额度，任何丢弃路径都会在 Drop 中归还。
pub struct UdpPacket {
    data: BytesMut,
    budget: Option<Arc<AtomicUsize>>,
    weight: usize,
}

impl UdpPacket {
    fn new(data: BytesMut, budget: Option<Arc<AtomicUsize>>, weight: usize) -> Self {
        Self {
            data,
            budget,
            weight,
        }
    }

    fn into_buf(mut self) -> BytesMut {
        // 数据已被消费：这里主动归还额度，随后 Drop 不再重复扣减。
        if let Some(budget) = self.budget.take() {
            budget.fetch_sub(self.weight, Ordering::Relaxed);
        }
        std::mem::take(&mut self.data)
    }
}

impl Drop for UdpPacket {
    fn drop(&mut self) {
        if let Some(budget) = self.budget.take() {
            budget.fetch_sub(self.weight, Ordering::Relaxed);
        }
    }
}

impl AsRef<[u8]> for UdpPacket {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

impl Deref for UdpPacket {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

/// 从池中借出缓冲并拷贝一个数据报；池空时按常见 RDP 包大小起步。
///
/// `budget` 是代理级队列字节计数器：入队前先占用额度，失败直接丢包；
/// 包在写成功/丢弃时通过 `Drop` 归还。
fn alloc_udp_packet(
    pool: &Arc<Mutex<Vec<BytesMut>>>,
    data: &[u8],
    budget: Option<&Arc<AtomicUsize>>,
) -> Option<UdpPacket> {
    if let Some(budget) = budget {
        let prev = budget.fetch_add(data.len(), Ordering::Relaxed);
        if prev.saturating_add(data.len()) > UDP_PROXY_QUEUE_MAX_BYTES {
            budget.fetch_sub(data.len(), Ordering::Relaxed);
            return None;
        }
    }
    let mut buf = pool
        .lock()
        .pop()
        .unwrap_or_else(|| BytesMut::with_capacity(data.len().max(2048)));
    buf.clear();
    buf.extend_from_slice(data);
    Some(UdpPacket::new(buf, budget.map(Arc::clone), data.len()))
}

/// 批量归还一批已写入的包缓冲：整批只加一次锁，避免每包 Drop 抢锁。
fn recycle_packets(pool: &Arc<Mutex<Vec<BytesMut>>>, packets: &mut Vec<UdpPacket>) {
    if packets.is_empty() {
        return;
    }
    let mut pool = pool.lock();
    for p in packets.drain(..) {
        // 先消费 packet 归还字节额度；池满或缓冲过大时直接丢弃该缓冲。
        let buf = p.into_buf();
        if pool.len() >= UDP_PACKET_POOL_MAX || buf.capacity() > UDP_PACKET_POOL_MAX_BUF_BYTES {
            continue;
        }
        pool.push(buf);
    }
}

/// 记录 UDP 上行字节：全局总量 + 每代理统计。
fn record_up(proxy: &UdpProxy, n: usize) {
    let n = n as u64;
    proxy
        .metrics
        .bytes_up
        .fetch_add(n, std::sync::atomic::Ordering::Relaxed);
    proxy
        .stats
        .bytes_up
        .fetch_add(n, std::sync::atomic::Ordering::Relaxed);
}

/// 记录 UDP 下行字节：全局总量 + 每代理统计。
fn record_down(proxy: &UdpProxy, n: usize) {
    let n = n as u64;
    proxy
        .metrics
        .bytes_down
        .fetch_add(n, std::sync::atomic::Ordering::Relaxed);
    proxy
        .stats
        .bytes_down
        .fetch_add(n, std::sync::atomic::Ordering::Relaxed);
}

/// UDP 会话建立：累计连接数 +1，活跃会话数 +1。
fn record_session_open(proxy: &UdpProxy) {
    proxy
        .metrics
        .udp_sessions_total
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    proxy
        .stats
        .connections_total
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    proxy
        .stats
        .active_connections
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

/// UDP 会话关闭：活跃会话数 -1（饱和到 0，避免异常路径把计数减成负数）。
fn record_session_close(proxy: &UdpProxy, count: i64) {
    let count = count.max(0);
    if count == 0 {
        return;
    }
    let active = &proxy.stats.active_connections;
    let mut cur = active.load(std::sync::atomic::Ordering::Relaxed);
    loop {
        let next = (cur - count).max(0);
        match active.compare_exchange_weak(
            cur,
            next,
            std::sync::atomic::Ordering::Relaxed,
            std::sync::atomic::Ordering::Relaxed,
        ) {
            Ok(_) => return,
            Err(actual) => cur = actual,
        }
    }
}

/// 等待工作连接配对的 UDP 会话（首个数据包触发 ReqWorkConn 后暂存）。
pub struct PendingUdp {
    pub client: SocketAddr,
    pub tx: mpsc::Sender<UdpPacket>,
    pub rx: mpsc::Receiver<UdpPacket>,
    pub created: Instant,
}

/// 已配对的 UDP 会话。
pub struct UdpSession {
    /// 建立该会话的工作连接 ID：用于 handler 退出时判断条目是否仍属于自己，
    /// 避免旧 handler 误删同一 client 的新会话（ABA）。
    pub work_id: u64,
    pub tx: mpsc::Sender<UdpPacket>,
    pub last_active: Instant,
    /// 会话级取消：sweep 超时/被新会话替换时中断可能阻塞的工作连接写/下行发送。
    pub cancel: CancellationToken,
}

/// 单个 UDP 代理的运行状态（监听 socket + 会话表）。
pub struct UdpProxy {
    pub socket: Arc<UdpSocket>,
    pub sessions: Mutex<HashMap<SocketAddr, UdpSession>>,
    pub pending_by_id: Mutex<HashMap<u64, PendingUdp>>,
    pub pending_client: Mutex<HashMap<SocketAddr, u64>>,
    pub metrics: Arc<crate::metrics::Metrics>,
    /// 每代理统计（Dashboard / Prometheus）；不能复用全局 `Metrics`。
    pub stats: Arc<crate::metrics::ProxyStats>,
    pub packet_pool: Arc<Mutex<Vec<BytesMut>>>,
    /// 所有队列中尚未消费的 UDP 数据报总字节数（代理级内存上界）。
    pub queue_bytes: Arc<AtomicUsize>,
    pub session_timeout: Duration,
    pub pending_timeout: Duration,
    /// 归属会话 ID：工作连接/清理时校验，防止同名/旧会话操作误伤。
    pub owner_session_id: String,
    /// 已配对会话上限。
    pub max_sessions: usize,
    /// 会话级停止信号：控制会话被清理时触发，用于结束仍在途的 UDP 工作连接，
    /// 释放其持有的 `Arc<UdpProxy>`（进而释放 UDP socket / 端口）。
    pub stop: CancellationToken,
}

/// 注册 UDP 代理：绑定 UDP socket 并启动监听循环。
pub async fn register_udp_proxy(
    proxy_name: String,
    remote_port: u16,
    session: &Arc<Session>,
    state: &Arc<ServerState>,
    bind_addr: &str,
    session_timeout: Duration,
) -> std::result::Result<JoinHandle<()>, ProxyError> {
    let socket = match UdpSocket::bind((bind_addr, remote_port)).await {
        Ok(s) => s,
        Err(e) => {
            // 具体原因只写服务端日志（DESIGN §8.5）。
            tracing::warn!(%proxy_name, remote_port, error = %e, "failed to bind udp proxy port");
            return Err(if e.kind() == std::io::ErrorKind::AddrInUse {
                ProxyError::PortOccupied
            } else {
                ProxyError::Internal
            });
        }
    };
    // 突发时先由内核缓冲吸收（best-effort；内核按 rmem_max 截断）。
    match enlarge_recv_buffer(&socket) {
        Ok(bytes) => tracing::debug!(%proxy_name, bytes, "udp recv buffer enlarged"),
        Err(e) => tracing::debug!(%proxy_name, error = %e, "failed to enlarge udp recv buffer"),
    }
    let stats = state.proxy_stats_for_owner(&proxy_name, &session.session_id);
    let proxy = Arc::new(UdpProxy {
        socket: Arc::new(socket),
        sessions: Mutex::new(HashMap::new()),
        pending_by_id: Mutex::new(HashMap::new()),
        pending_client: Mutex::new(HashMap::new()),
        metrics: state.metrics.clone(),
        stats,
        packet_pool: Arc::new(Mutex::new(Vec::new())),
        queue_bytes: Arc::new(AtomicUsize::new(0)),
        session_timeout,
        pending_timeout: Duration::from_secs(WORK_CONN_TIMEOUT_RFRPS),
        owner_session_id: session.session_id.clone(),
        max_sessions: MAX_UDP_SESSIONS_PER_PROXY,
        stop: CancellationToken::new(),
    });
    state.udp.lock().insert(proxy_name.clone(), proxy.clone());

    let session = session.clone();
    let state = state.clone();
    let shutdown = state.shutdown.clone();
    let tracker = state.data_tasks.clone();
    let handle = tracker.spawn(async move {
        run_udp_listener(proxy, proxy_name, session, state, shutdown).await;
    });
    Ok(handle)
}

async fn run_udp_listener(
    proxy: Arc<UdpProxy>,
    proxy_name: String,
    session: Arc<Session>,
    state: Arc<ServerState>,
    shutdown: CancellationToken,
) {
    // 清理周期取两个超时中较小者的 1/4：pending 超时（默认 10s）远小于会话超时
    // （默认 300s），若只按后者计算，过期的 pending 会占用上限数十秒。
    let sweep_period = Duration::from_secs(
        (proxy.session_timeout.min(proxy.pending_timeout).as_secs() / 4).max(1),
    );
    let mut sweep_iv = tokio::time::interval(sweep_period);
    sweep_iv.tick().await; // 消耗首次立即 tick
    let mut buf = vec![0u8; MAX_UDP_PACKET_SIZE];
    // recv 出错同样用退避策略：一次瞬时错误不得结束监听循环（见下方说明）。
    let mut recv_retry = AcceptRetry::new();
    loop {
        tokio::select! {
            r = proxy.socket.recv_from(&mut buf) => {
                match r {
                    Ok((n, peer)) => {
                        recv_retry.record_ok();
                        handle_datagram(&proxy, &proxy_name, &session, &state, peer, &buf[..n]).await;
                        drain_socket_batch(&proxy, &proxy_name, &session, &state, &mut buf).await;
                    }
                    Err(e) => {
                        // 瞬时错误（Windows 上此前的 send_to 若引来 ICMP port unreachable，
                        // 后续 recv_from 会返回 WSAECONNRESET，但 socket 之后仍可用）不得
                        // 结束监听循环：否则 UDP 代理会静默失效（不再收包，端口却仍绑定）。
                        let backoff = recv_retry.record_err();
                        if recv_retry.should_log() {
                            tracing::warn!(
                                proxy = %proxy_name,
                                consecutive = recv_retry.consecutive(),
                                error = %e,
                                "udp recv error; retrying"
                            );
                        }
                        tokio::time::sleep(backoff).await;
                    }
                }
            }
            _ = sweep_iv.tick() => sweep(&proxy),
            _ = shutdown.cancelled() => {
                tracing::info!(proxy = %proxy_name, "udp listener shutting down");
                break;
            }
            // 会话清理/重连替换：即使 cleanup 在注册竞态中漏掉了该任务，也必须
            // 退出，否则监听任务持有的 Arc<Session> 会让端口永久存活。
            _ = proxy.stop.cancelled() => {
                tracing::info!(proxy = %proxy_name, "udp proxy stopped, closing listener");
                break;
            }
            _ = session.stop.cancelled() => {
                tracing::info!(proxy = %proxy_name, "udp session stopped, closing listener");
                break;
            }
        }
    }
}

async fn handle_datagram(
    proxy: &Arc<UdpProxy>,
    proxy_name: &str,
    session: &Arc<Session>,
    state: &Arc<ServerState>,
    peer: SocketAddr,
    data: &[u8],
) {
    // 已配对会话：直接转发到工作连接。
    let tx = {
        let mut sessions = proxy.sessions.lock();
        match sessions.get_mut(&peer) {
            Some(s) => {
                s.last_active = Instant::now();
                Some(s.tx.clone())
            }
            None => None,
        }
    };
    if let Some(tx) = tx {
        record_up(proxy, data.len());
        // 背压保护：所有客户端共用同一个收包循环，工作连接消费慢时不得在这里
        // await（否则整代理数据报被阻塞）。通道满即丢包并计数——UDP 语义允许丢失。
        // 先看容量再分配：满队列时不必先拷贝一个包再丢弃。
        if tx.capacity() == 0 {
            proxy.metrics.inc_udp_dropped();
            tracing::debug!(
                proxy = %proxy_name, %peer,
                "udp session backpressure; dropping datagram"
            );
            return;
        }
        let Some(packet) = alloc_udp_packet(&proxy.packet_pool, data, Some(&proxy.queue_bytes))
        else {
            proxy.metrics.inc_udp_dropped();
            tracing::debug!(
                proxy = %proxy_name, %peer,
                "udp queue byte budget reached; dropping datagram"
            );
            return;
        };
        if tx.try_send(packet).is_err() {
            proxy.metrics.inc_udp_dropped();
            tracing::debug!(
                proxy = %proxy_name, %peer,
                "udp session backpressure; dropping datagram"
            );
        }
        return;
    }

    // 已有待配对请求：继续投递到暂存通道。
    let id = { proxy.pending_client.lock().get(&peer).copied() };
    if let Some(id) = id {
        let tx = { proxy.pending_by_id.lock().get(&id).map(|p| p.tx.clone()) };
        if let Some(tx) = tx {
            // 待配对窗口期的包同样属于上行流量，之前漏计了这里的字节数。
            record_up(proxy, data.len());
            // 同会话背压：待配对窗口期也不得阻塞收包循环。
            if tx.capacity() == 0 {
                proxy.metrics.inc_udp_dropped();
                tracing::debug!(
                    proxy = %proxy_name, %peer,
                    "udp pending backpressure; dropping datagram"
                );
                return;
            }
            let Some(packet) = alloc_udp_packet(&proxy.packet_pool, data, Some(&proxy.queue_bytes))
            else {
                proxy.metrics.inc_udp_dropped();
                tracing::debug!(
                    proxy = %proxy_name, %peer,
                    "udp queue byte budget reached; dropping datagram"
                );
                return;
            };
            if tx.try_send(packet).is_err() {
                proxy.metrics.inc_udp_dropped();
                tracing::debug!(
                    proxy = %proxy_name, %peer,
                    "udp pending backpressure; dropping datagram"
                );
            }
        }
        return;
    }

    // 待配对会话上限：伪造源地址可制造大量 pending，并把每个包放大为一次
    // 工作连接请求（客户端 → 服务端 + 本地服务），超限直接丢弃。
    // 达到上限时先清理过期项，避免一次突发把上限长期占满、饿死后续合法会话。
    if proxy.pending_by_id.lock().len() >= MAX_PENDING_UDP_SESSIONS {
        purge_expired_pending(proxy, Instant::now());
    }
    if proxy.pending_by_id.lock().len() >= MAX_PENDING_UDP_SESSIONS {
        proxy.metrics.inc_udp_dropped();
        tracing::debug!(
            proxy = %proxy_name, %peer,
            "udp pending session limit reached; dropping datagram"
        );
        return;
    }

    // 首个数据包：建立待配对项并请求工作连接，同时把该数据包先入队，
    // 避免工作连接建立期间丢包（DESIGN §8.6）。
    let (tx, rx) = mpsc::channel::<UdpPacket>(UDP_SESSION_QUEUE_DEPTH);
    let work_id = state.next_work_id();
    record_up(proxy, data.len());
    let Some(packet) = alloc_udp_packet(&proxy.packet_pool, data, Some(&proxy.queue_bytes)) else {
        proxy.metrics.inc_udp_dropped();
        tracing::debug!(
            proxy = %proxy_name, %peer,
            "udp queue byte budget reached; dropping datagram"
        );
        return;
    };
    let _ = tx.send(packet).await;
    proxy.pending_by_id.lock().insert(
        work_id,
        PendingUdp {
            client: peer,
            tx: tx.clone(),
            rx,
            created: Instant::now(),
        },
    );
    proxy.pending_client.lock().insert(peer, work_id);
    tracing::debug!(proxy = %proxy_name, work_id, %peer, "udp session pending; requesting work conn");

    // 请求工作连接：与按需 TCP 路径一致，用带超时发送避免控制通道拥堵时挂起
    // （背压死锁修复的同源残留，见 util/control.rs）。失败时清理待配对项。
    let tx_ctl = session.tx.clone();
    let pname = proxy_name.to_string();
    let proxy_cleanup = proxy.clone();
    let tracker = state.data_tasks.clone();
    tracker.spawn(async move {
        if !send_with_timeout(
            &tx_ctl,
            Message::ReqWorkConn(ReqWorkConn {
                proxy_name: pname,
                work_id,
            }),
        )
        .await
        {
            remove_pending(&proxy_cleanup, work_id);
        }
    });
}

/// 清理超时会话与超时待配对项。
fn sweep(proxy: &Arc<UdpProxy>) {
    let now = Instant::now();
    let mut sessions = proxy.sessions.lock();
    sessions.retain(|_, s| {
        if now.duration_since(s.last_active) >= proxy.session_timeout {
            // 先取消再删除：即使 handler 正阻塞在工作连接写/下行发送上，
            // 也会在 select! 的取消分支中退出，而不是等对端主动关闭。
            s.cancel.cancel();
            // 计数与删除在同一把锁内更新，避免与 Dashboard 采样产生不一致。
            record_session_close(proxy, 1);
            false
        } else {
            true
        }
    });
    drop(sessions);

    // 删除 `UdpSession` 会 drop 其唯一的 sender（handler 不再保留 `pending.tx`），
    // 对应 `rx.recv_many` 随即可见 0 并退出，不需要额外的停止信号。
    purge_expired_pending(proxy, now);
}

/// 移除已超过 `pending_timeout` 的待配对项，返回清理条数。
fn purge_expired_pending(proxy: &Arc<UdpProxy>, now: Instant) -> usize {
    let expired: Vec<u64> = proxy
        .pending_by_id
        .lock()
        .iter()
        .filter(|(_, p)| now.duration_since(p.created) >= proxy.pending_timeout)
        .map(|(id, _)| *id)
        .collect();
    let n = expired.len();
    for id in expired {
        remove_pending(proxy, id);
    }
    n
}

/// 摘除一个待配对项（同时清理 `pending_client` 反查表）。
fn remove_pending(proxy: &Arc<UdpProxy>, work_id: u64) -> Option<PendingUdp> {
    let pending = proxy.pending_by_id.lock().remove(&work_id)?;
    let mut by_client = proxy.pending_client.lock();
    // 仅当反查表仍指向该 work_id 时才删除：该源地址期间可能已建立**新的**待配对项，
    // 无条件按地址删除会误删新映射（导致后续数据报重复建连/丢包）。
    if by_client.get(&pending.client) == Some(&work_id) {
        by_client.remove(&pending.client);
    }
    Some(pending)
}

/// 同一唤醒内继续排空 UDP 接收缓冲：减少任务唤醒与系统调用次数，
/// 突发时也更不容易把数据报堆到应用层队列里丢包（上限 [`UDP_RECV_BATCH`]）。
async fn drain_socket_batch(
    proxy: &Arc<UdpProxy>,
    proxy_name: &str,
    session: &Arc<Session>,
    state: &Arc<ServerState>,
    buf: &mut [u8],
) {
    for _ in 1..UDP_RECV_BATCH {
        match proxy.socket.try_recv_from(buf) {
            Ok((n, peer)) => {
                handle_datagram(proxy, proxy_name, session, state, peer, &buf[..n]).await;
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(e) => {
                tracing::warn!(proxy = %proxy_name, error = %e, "udp recv error");
                break;
            }
        }
    }
}

/// 工作连接到达后进入 UDP 分帧循环（DESIGN §8.6）。
pub async fn handle_udp_work_conn(
    proxy: Arc<UdpProxy>,
    work_id: u64,
    stream: rfrp_common::util::stream::BoxedStream,
) -> Result<()> {
    let PendingUdp { client, tx, rx, .. } = match remove_pending(&proxy, work_id) {
        Some(p) => p,
        None => {
            tracing::warn!(work_id, "no pending udp session for work_id");
            return Ok(());
        }
    };
    // sender 只存放在 sessions 表中（handler 不再保留 `pending.tx`）：sweep 删除
    // 会话时 sender 被 drop，下面的 `rx.recv_many` 返回 0，handler 自然退出。
    // 另加会话级取消令牌，用于中断可能阻塞的写操作。
    let cancel = CancellationToken::new();
    if !register_session(&proxy, client, tx, work_id, cancel.clone()) {
        tracing::warn!(
            client = %client, work_id,
            limit = proxy.max_sessions,
            "udp session limit reached; closing work connection"
        );
        return Ok(());
    }

    tracing::info!(client = %client, work_id, "udp work connection established");
    let mut rx = rx;
    let mut stream = stream;
    // 上行批量写缓冲 + 批量收集：把"队列里已就绪的多个数据报"合并成一次 write。
    // 从小容量起步，按实际突发增长，避免每个会话固定占 64 KiB。
    let mut write_buf = Vec::with_capacity(UDP_BUF_INITIAL_BYTES);
    let mut batch: Vec<UdpPacket> = Vec::with_capacity(UDP_WRITE_BATCH);
    // 下行：一次 read 解析多帧，避免每包一次任务唤醒。
    let mut reader = UdpFrameBuf::new();
    let mut down: Vec<Bytes> = Vec::with_capacity(UDP_READ_BATCH);

    loop {
        tokio::select! {
            n = rx.recv_many(&mut batch, UDP_WRITE_BATCH) => {
                if n == 0 {
                    break; // 通道关闭（会话结束）
                }
                touch_session(&proxy, client, work_id);
                let write = tokio::select! {
                    r = write_udp_frames_buffered(&mut stream, &mut write_buf, &batch) => r,
                    _ = cancel.cancelled() => break,
                    _ = proxy.stop.cancelled() => break,
                };
                if let Err(e) = write {
                    tracing::warn!(work_id, error = %e, "udp write frames error");
                    break;
                }
                recycle_packets(&proxy.packet_pool, &mut batch);
            }
            r = reader.read_batch(&mut stream, &mut down, UDP_READ_BATCH) => {
                match r {
                    Ok(n) if n > 0 => {
                        touch_session(&proxy, client, work_id);
                        tokio::select! {
                            _ = forward_to_client(&proxy, client, &down, work_id) => {}
                            _ = cancel.cancelled() => break,
                            _ = proxy.stop.cancelled() => break,
                        }
                    }
                    Ok(_) => break, // EOF
                    Err(e) => {
                        tracing::warn!(work_id, error = %e, "udp read frame error");
                        break;
                    }
                }
            }
            // 控制会话已清理：结束本工作连接，避免其 Arc<UdpProxy> 一直占着 UDP 端口
            // （否则客户端重连后重新注册同一 UDP 代理会得到 port occupied）。
            _ = proxy.stop.cancelled() => {
                tracing::debug!(work_id, "udp work connection stopped by session cleanup");
                break;
            }
            _ = cancel.cancelled() => {
                tracing::debug!(work_id, "udp session cancelled");
                break;
            }
        }
    }

    remove_session_if_owner(&proxy, client, work_id);
    tracing::debug!(client = %client, work_id, "udp work connection closed");
    Ok(())
}

/// 只删除仍属于本次 handler 的会话条目（防同一 client 新会话被旧 handler 误删）。
fn remove_session_if_owner(proxy: &Arc<UdpProxy>, client: SocketAddr, work_id: u64) {
    let mut sessions = proxy.sessions.lock();
    if sessions.get(&client).is_some_and(|s| s.work_id == work_id) {
        sessions.remove(&client);
        record_session_close(proxy, 1);
    }
}

/// 把一批下行数据报回发给用户侧（统计下行字节；单个 send 失败不影响其余包）。
async fn forward_to_client(
    proxy: &Arc<UdpProxy>,
    client: SocketAddr,
    frames: &[Bytes],
    work_id: u64,
) {
    // 旧 handler 可能已经读到下行数据，但此时会话已被新 work_id 取代；
    // 不能再把旧响应发给同一 client，否则会造成新旧会话串流。
    if !proxy
        .sessions
        .lock()
        .get(&client)
        .is_some_and(|s| s.work_id == work_id)
    {
        tracing::debug!(
            work_id,
            "udp session no longer current; dropping downstream batch"
        );
        return;
    }
    for d in frames {
        record_down(proxy, d.len());
        if let Err(e) = proxy.socket.send_to(d, client).await {
            tracing::warn!(work_id, error = %e, "udp send_to client error");
        }
    }
}

/// 登记已配对的 UDP 会话（工作连接就绪后调用）；超过上限返回 false。
fn register_session(
    proxy: &Arc<UdpProxy>,
    client: SocketAddr,
    tx: mpsc::Sender<UdpPacket>,
    work_id: u64,
    cancel: CancellationToken,
) -> bool {
    let mut sessions = proxy.sessions.lock();
    let replacing = sessions.contains_key(&client);
    if !replacing && sessions.len() >= proxy.max_sessions {
        return false;
    }
    // 同一 client 已有旧会话：先取消它，确保旧 handler 即使阻塞在写操作也能退出。
    if let Some(old) = sessions.get(&client) {
        old.cancel.cancel();
    }
    sessions.insert(
        client,
        UdpSession {
            work_id,
            tx,
            last_active: Instant::now(),
            cancel,
        },
    );

    // 新工作连接对应一次新的 UDP 会话；替换同一 client 时活跃数不变。
    // 在持有 sessions 锁时更新计数，避免与 sweep 删除并发导致计数漂移。
    if replacing {
        proxy
            .metrics
            .udp_sessions_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        proxy
            .stats
            .connections_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    } else {
        record_session_open(proxy);
    }
    true
}

/// 更新 UDP 会话的最后活跃时间（仅当条目仍属于本次工作连接）。
fn touch_session(proxy: &Arc<UdpProxy>, client: SocketAddr, work_id: u64) {
    if let Some(s) = proxy.sessions.lock().get_mut(&client) {
        if s.work_id == work_id {
            s.last_active = Instant::now();
        }
    }
}

/// 判断代理是否为 UDP 类型。
pub fn is_udp_proxy(state: &ServerState, proxy_name: &str) -> bool {
    state.udp.lock().contains_key(proxy_name)
}

/// 取 UDP 代理运行状态。
pub fn get_udp_proxy(state: &ServerState, proxy_name: &str) -> Option<Arc<UdpProxy>> {
    state.udp.lock().get(proxy_name).cloned()
}

#[cfg(test)]
mod tests;
