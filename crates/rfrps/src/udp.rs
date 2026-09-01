//! UDP 代理：会话映射 + 4 字节长度前缀分帧（DESIGN §8.6）。

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rfrp_common::constants::{MAX_UDP_PACKET_SIZE, UDP_SESSION_TIMEOUT, WORK_CONN_TIMEOUT_RFRPS};
use rfrp_common::error::Result;
use rfrp_common::protocol::msg::*;
use rfrp_common::util::udp::{read_udp_frame, write_udp_frame};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::control::Session;
use crate::server::ServerState;

/// 等待工作连接配对的 UDP 会话（首个数据包触发 ReqWorkConn 后暂存）。
pub struct PendingUdp {
    pub client: SocketAddr,
    pub tx: mpsc::Sender<Vec<u8>>,
    pub rx: mpsc::Receiver<Vec<u8>>,
    pub created: Instant,
}

/// 已配对的 UDP 会话。
pub struct UdpSession {
    pub tx: mpsc::Sender<Vec<u8>>,
    pub last_active: Instant,
}

/// 单个 UDP 代理的运行状态（监听 socket + 会话表）。
pub struct UdpProxy {
    pub socket: Arc<UdpSocket>,
    pub sessions: Mutex<HashMap<SocketAddr, UdpSession>>,
    pub pending_by_id: Mutex<HashMap<u64, PendingUdp>>,
    pub pending_client: Mutex<HashMap<SocketAddr, u64>>,
    pub metrics: Arc<crate::metrics::Metrics>,
    pub session_timeout: Duration,
    pub pending_timeout: Duration,
}

/// 注册 UDP 代理：绑定 UDP socket 并启动监听循环。
pub async fn register_udp_proxy(
    proxy_name: String,
    remote_port: u16,
    session: &Arc<Session>,
    state: &Arc<ServerState>,
    bind_addr: &str,
) -> Result<JoinHandle<()>> {
    let socket = UdpSocket::bind((bind_addr, remote_port))
        .await
        .map_err(|_| rfrp_common::Error::Config("internal error".into()))?;
    let proxy = Arc::new(UdpProxy {
        socket: Arc::new(socket),
        sessions: Mutex::new(HashMap::new()),
        pending_by_id: Mutex::new(HashMap::new()),
        pending_client: Mutex::new(HashMap::new()),
        metrics: state.metrics.clone(),
        session_timeout: Duration::from_secs(UDP_SESSION_TIMEOUT),
        pending_timeout: Duration::from_secs(WORK_CONN_TIMEOUT_RFRPS),
    });
    state
        .udp
        .lock()
        .unwrap()
        .insert(proxy_name.clone(), proxy.clone());

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
    let mut sweep_iv = tokio::time::interval(Duration::from_secs(UDP_SESSION_TIMEOUT));
    sweep_iv.tick().await; // 消耗首次立即 tick
    let mut buf = vec![0u8; MAX_UDP_PACKET_SIZE];
    loop {
        tokio::select! {
            r = proxy.socket.recv_from(&mut buf) => {
                match r {
                    Ok((n, peer)) => {
                        handle_datagram(&proxy, &proxy_name, &session, &state, peer, &buf[..n]).await;
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
        let mut sessions = proxy.sessions.lock().unwrap();
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
        let _ = tx.send(data.to_vec()).await;
        return;
    }

    // 已有待配对请求：继续投递到暂存通道。
    let id = { proxy.pending_client.lock().unwrap().get(&peer).copied() };
    if let Some(id) = id {
        let tx = {
            proxy
                .pending_by_id
                .lock()
                .unwrap()
                .get(&id)
                .map(|p| p.tx.clone())
        };
        if let Some(tx) = tx {
            let _ = tx.send(data.to_vec()).await;
        }
        return;
    }

    // 首个数据包：建立待配对项并请求工作连接，同时把该数据包先入队，
    // 避免工作连接建立期间丢包（DESIGN §8.6）。
    let (tx, rx) = mpsc::channel::<Vec<u8>>(16);
    let work_id = state.next_work_id();
    proxy
        .metrics
        .bytes_up
        .fetch_add(data.len() as u64, std::sync::atomic::Ordering::Relaxed);
    let _ = tx.send(data.to_vec()).await;
    proxy.pending_by_id.lock().unwrap().insert(
        work_id,
        PendingUdp {
            client: peer,
            tx: tx.clone(),
            rx,
            created: Instant::now(),
        },
    );
    proxy.pending_client.lock().unwrap().insert(peer, work_id);
    tracing::debug!(proxy = %proxy_name, work_id, %peer, "udp session pending; requesting work conn");

    let tx_ctl = session.tx.clone();
    let pname = proxy_name.to_string();
    tokio::spawn(async move {
        let _ = tx_ctl
            .send(Message::ReqWorkConn(ReqWorkConn {
                proxy_name: pname,
                work_id,
            }))
            .await;
    });
}

/// 清理超时会话与超时待配对项。
fn sweep(proxy: &Arc<UdpProxy>) {
    let now = Instant::now();
    let mut sessions = proxy.sessions.lock().unwrap();
    sessions.retain(|_, s| now.duration_since(s.last_active) < proxy.session_timeout);
    drop(sessions);

    let mut pending = proxy.pending_by_id.lock().unwrap();
    let expired: Vec<u64> = pending
        .iter()
        .filter(|(_, p)| now.duration_since(p.created) >= proxy.pending_timeout)
        .map(|(id, _)| *id)
        .collect();
    for id in expired {
        if let Some(p) = pending.remove(&id) {
            proxy.pending_client.lock().unwrap().remove(&p.client);
        }
    }
}

/// 工作连接到达后进入 UDP 分帧循环（DESIGN §8.6）。
pub async fn handle_udp_work_conn(
    proxy: Arc<UdpProxy>,
    work_id: u64,
    stream: rfrp_common::util::stream::BoxedStream,
) -> Result<()> {
    let pending = proxy.pending_by_id.lock().unwrap().remove(&work_id);
    let pending = match pending {
        Some(p) => p,
        None => {
            tracing::warn!(work_id, "no pending udp session for work_id");
            return Ok(());
        }
    };
    proxy.pending_client.lock().unwrap().remove(&pending.client);

    {
        let mut sessions = proxy.sessions.lock().unwrap();
        sessions.insert(
            pending.client,
            UdpSession {
                tx: pending.tx.clone(),
                last_active: Instant::now(),
            },
        );
    }

    tracing::info!(client = %pending.client, work_id, "udp work connection established");
    let mut rx = pending.rx;
    let mut stream = stream;

    loop {
        tokio::select! {
            data = rx.recv() => {
                match data {
                    Some(d) => {
                        touch_session(&proxy, pending.client);
                        if let Err(e) = write_udp_frame(&mut stream, &d).await {
                            tracing::warn!(work_id, error = %e, "udp write frame error");
                            break;
                        }
                    }
                    None => break,
                }
            }
            r = read_udp_frame(&mut stream) => {
                match r {
                    Ok(Some(d)) => {
                        touch_session(&proxy, pending.client);
                        proxy
                            .metrics
                            .bytes_down
                            .fetch_add(d.len() as u64, std::sync::atomic::Ordering::Relaxed);
                        if let Err(e) = proxy.socket.send_to(&d, pending.client).await {
                            tracing::warn!(work_id, error = %e, "udp send_to client error");
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        tracing::warn!(work_id, error = %e, "udp read frame error");
                        break;
                    }
                }
            }
        }
    }

    proxy.sessions.lock().unwrap().remove(&pending.client);
    tracing::debug!(client = %pending.client, work_id, "udp work connection closed");
    Ok(())
}

/// 更新 UDP 会话的最后活跃时间（双向流量都算活跃）。
fn touch_session(proxy: &Arc<UdpProxy>, client: SocketAddr) {
    if let Some(s) = proxy.sessions.lock().unwrap().get_mut(&client) {
        s.last_active = Instant::now();
    }
}

/// 判断代理是否为 UDP 类型。
pub fn is_udp_proxy(state: &ServerState, proxy_name: &str) -> bool {
    state.udp.lock().unwrap().contains_key(proxy_name)
}

/// 取 UDP 代理运行状态。
pub fn get_udp_proxy(state: &ServerState, proxy_name: &str) -> Option<Arc<UdpProxy>> {
    state.udp.lock().unwrap().get(proxy_name).cloned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::Metrics;

    async fn test_proxy(session_timeout: Duration, pending_timeout: Duration) -> Arc<UdpProxy> {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        Arc::new(UdpProxy {
            socket: Arc::new(socket),
            sessions: Mutex::new(HashMap::new()),
            pending_by_id: Mutex::new(HashMap::new()),
            pending_client: Mutex::new(HashMap::new()),
            metrics: Arc::new(Metrics::new()),
            session_timeout,
            pending_timeout,
        })
    }

    #[tokio::test]
    async fn sweep_removes_expired_session_and_pending() {
        let proxy = test_proxy(Duration::from_millis(50), Duration::from_millis(50)).await;
        let (tx, _rx) = mpsc::channel::<Vec<u8>>(4);
        let old = Instant::now() - Duration::from_secs(1);

        proxy.sessions.lock().unwrap().insert(
            "127.0.0.1:1".parse().unwrap(),
            UdpSession {
                tx: tx.clone(),
                last_active: old,
            },
        );
        proxy.pending_by_id.lock().unwrap().insert(
            1,
            PendingUdp {
                client: "127.0.0.1:2".parse().unwrap(),
                tx: tx.clone(),
                rx: mpsc::channel(4).1,
                created: old,
            },
        );
        proxy
            .pending_client
            .lock()
            .unwrap()
            .insert("127.0.0.1:2".parse().unwrap(), 1);
        // 新鲜会话应保留。
        proxy.sessions.lock().unwrap().insert(
            "127.0.0.1:3".parse().unwrap(),
            UdpSession {
                tx: tx.clone(),
                last_active: Instant::now(),
            },
        );

        sweep(&proxy);

        assert!(!proxy
            .sessions
            .lock()
            .unwrap()
            .contains_key(&"127.0.0.1:1".parse().unwrap()));
        assert!(proxy
            .sessions
            .lock()
            .unwrap()
            .contains_key(&"127.0.0.1:3".parse().unwrap()));
        assert!(proxy.pending_by_id.lock().unwrap().is_empty());
        assert!(proxy.pending_client.lock().unwrap().is_empty());
    }
}
