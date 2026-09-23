//! UDP 代理：会话映射 + 4 字节长度前缀分帧（DESIGN §8.6）。

use parking_lot::Mutex;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use rfrp_common::constants::{
    MAX_PENDING_UDP_SESSIONS, MAX_UDP_PACKET_SIZE, UDP_READ_BATCH, UDP_RECV_BATCH,
    UDP_SESSION_QUEUE_DEPTH, UDP_WRITE_BATCH, WORK_CONN_TIMEOUT_RFRPS,
};
use rfrp_common::error::Result;
use rfrp_common::protocol::msg::*;
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

/// 服务端上行队列中的 UDP 数据报：持有从池中借出的缓冲，Drop 时归还。
pub struct UdpPacket {
    data: BytesMut,
}

impl UdpPacket {
    fn new(data: BytesMut) -> Self {
        Self { data }
    }

    fn into_buf(self) -> BytesMut {
        self.data
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
fn alloc_udp_packet(pool: &Arc<Mutex<Vec<BytesMut>>>, data: &[u8]) -> UdpPacket {
    let mut buf = pool
        .lock()
        .pop()
        .unwrap_or_else(|| BytesMut::with_capacity(data.len().max(2048)));
    buf.clear();
    buf.extend_from_slice(data);
    UdpPacket::new(buf)
}

/// 批量归还一批已写入的包缓冲：整批只加一次锁，避免每包 Drop 抢锁。
fn recycle_packets(pool: &Arc<Mutex<Vec<BytesMut>>>, packets: &mut Vec<UdpPacket>) {
    if packets.is_empty() {
        return;
    }
    let mut pool = pool.lock();
    for p in packets.drain(..) {
        if pool.len() >= UDP_PACKET_POOL_MAX {
            break;
        }
        pool.push(p.into_buf());
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
    pub tx: mpsc::Sender<UdpPacket>,
    pub last_active: Instant,
}

/// 单个 UDP 代理的运行状态（监听 socket + 会话表）。
pub struct UdpProxy {
    pub socket: Arc<UdpSocket>,
    pub sessions: Mutex<HashMap<SocketAddr, UdpSession>>,
    pub pending_by_id: Mutex<HashMap<u64, PendingUdp>>,
    pub pending_client: Mutex<HashMap<SocketAddr, u64>>,
    pub metrics: Arc<crate::metrics::Metrics>,
    pub packet_pool: Arc<Mutex<Vec<BytesMut>>>,
    pub session_timeout: Duration,
    pub pending_timeout: Duration,
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
    let proxy = Arc::new(UdpProxy {
        socket: Arc::new(socket),
        sessions: Mutex::new(HashMap::new()),
        pending_by_id: Mutex::new(HashMap::new()),
        pending_client: Mutex::new(HashMap::new()),
        metrics: state.metrics.clone(),
        packet_pool: Arc::new(Mutex::new(Vec::new())),
        session_timeout,
        pending_timeout: Duration::from_secs(WORK_CONN_TIMEOUT_RFRPS),
        stop: CancellationToken::new(),
    });
    state.udp.lock().insert(proxy_name.clone(), proxy.clone());

    let session = session.clone();
    let state = state.clone();
    let shutdown = state.shutdown.clone();
    let handle = tokio::spawn(async move {
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
    // 清理周期取超时的 1/4：使会话/待配对项的实际存活时间接近配置超时
    // （周期等于超时时，最坏会存活 2× 超时）。
    let sweep_period = Duration::from_secs((proxy.session_timeout.as_secs() / 4).max(1));
    let mut sweep_iv = tokio::time::interval(sweep_period);
    sweep_iv.tick().await; // 消耗首次立即 tick
    let mut buf = vec![0u8; MAX_UDP_PACKET_SIZE];
    loop {
        tokio::select! {
            r = proxy.socket.recv_from(&mut buf) => {
                match r {
                    Ok((n, peer)) => {
                        handle_datagram(&proxy, &proxy_name, &session, &state, peer, &buf[..n]).await;
                        drain_socket_batch(&proxy, &proxy_name, &session, &state, &mut buf).await;
                    }
                    Err(e) => {
                        tracing::warn!(proxy = %proxy_name, error = %e, "udp recv error");
                        break;
                    }
                }
            }
            _ = sweep_iv.tick() => sweep(&proxy),
            _ = shutdown.cancelled() => {
                tracing::info!(proxy = %proxy_name, "udp listener shutting down");
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
        proxy
            .metrics
            .bytes_up
            .fetch_add(data.len() as u64, std::sync::atomic::Ordering::Relaxed);
        // 背压保护：所有客户端共用同一个收包循环，工作连接消费慢时不得在这里
        // await（否则整代理数据报被阻塞）。通道满即丢包并计数——UDP 语义允许丢失。
        if tx
            .try_send(alloc_udp_packet(&proxy.packet_pool, data))
            .is_err()
        {
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
            // 同会话背压：待配对窗口期也不得阻塞收包循环。
            if tx
                .try_send(alloc_udp_packet(&proxy.packet_pool, data))
                .is_err()
            {
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
    proxy
        .metrics
        .bytes_up
        .fetch_add(data.len() as u64, std::sync::atomic::Ordering::Relaxed);
    let _ = tx.send(alloc_udp_packet(&proxy.packet_pool, data)).await;
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
    tokio::spawn(async move {
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
    sessions.retain(|_, s| now.duration_since(s.last_active) < proxy.session_timeout);
    drop(sessions);

    let expired: Vec<u64> = proxy
        .pending_by_id
        .lock()
        .iter()
        .filter(|(_, p)| now.duration_since(p.created) >= proxy.pending_timeout)
        .map(|(id, _)| *id)
        .collect();
    for id in expired {
        remove_pending(proxy, id);
    }
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
    let pending = match remove_pending(&proxy, work_id) {
        Some(p) => p,
        None => {
            tracing::warn!(work_id, "no pending udp session for work_id");
            return Ok(());
        }
    };
    let client = pending.client;
    register_session(&proxy, client, pending.tx.clone());

    tracing::info!(client = %client, work_id, "udp work connection established");
    let mut rx = pending.rx;
    let mut stream = stream;
    // 上行批量写缓冲 + 批量收集：把"队列里已就绪的多个数据报"合并成一次 write。
    let mut write_buf = Vec::with_capacity(MAX_UDP_PACKET_SIZE);
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
                touch_session(&proxy, client);
                if let Err(e) = write_udp_frames_buffered(&mut stream, &mut write_buf, &batch).await {
                    tracing::warn!(work_id, error = %e, "udp write frames error");
                    break;
                }
                recycle_packets(&proxy.packet_pool, &mut batch);
            }
            r = reader.read_batch(&mut stream, &mut down, UDP_READ_BATCH) => {
                match r {
                    Ok(n) if n > 0 => {
                        touch_session(&proxy, client);
                        forward_to_client(&proxy, client, &down, work_id).await;
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
        }
    }

    proxy.sessions.lock().remove(&client);
    tracing::debug!(client = %client, work_id, "udp work connection closed");
    Ok(())
}

/// 把一批下行数据报回发给用户侧（统计下行字节；单个 send 失败不影响其余包）。
async fn forward_to_client(
    proxy: &Arc<UdpProxy>,
    client: SocketAddr,
    frames: &[Bytes],
    work_id: u64,
) {
    for d in frames {
        proxy
            .metrics
            .bytes_down
            .fetch_add(d.len() as u64, std::sync::atomic::Ordering::Relaxed);
        if let Err(e) = proxy.socket.send_to(d, client).await {
            tracing::warn!(work_id, error = %e, "udp send_to client error");
        }
    }
}

/// 登记已配对的 UDP 会话（工作连接就绪后调用）。
fn register_session(proxy: &Arc<UdpProxy>, client: SocketAddr, tx: mpsc::Sender<UdpPacket>) {
    proxy.sessions.lock().insert(
        client,
        UdpSession {
            tx,
            last_active: Instant::now(),
        },
    );
}

/// 更新 UDP 会话的最后活跃时间（双向流量都算活跃）。
fn touch_session(proxy: &Arc<UdpProxy>, client: SocketAddr) {
    if let Some(s) = proxy.sessions.lock().get_mut(&client) {
        s.last_active = Instant::now();
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
