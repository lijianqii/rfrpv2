//! 服务端主控：监听 accept 循环，按首帧区分控制/工作连接。

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rfrp_common::config::ServerConfig;
use rfrp_common::constants::{GRACEFUL_SHUTDOWN_TIMEOUT, HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT};
use rfrp_common::error::Result;
use rfrp_common::protocol::frame::read_one_frame;
use rfrp_common::protocol::msg::{MSG_LOGIN, MSG_START_WORK_CONN};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

use crate::control;
use crate::work;

/// 一条等待工作连接到达的「待处理」项。工作连接到达后取出 `user` 与之桥接。
pub struct PendingWork {
    pub proxy_name: String,
    pub session_id: String,
    pub user: Option<TcpStream>,
}

/// 服务端共享状态（所有 accept 任务共享）。
pub struct ServerState {
    /// 全局自增 work_id 生成器（从 1 开始，0 为保留值，见 DESIGN §6.2.1）。
    pub work_id: AtomicU64,
    /// work_id → 待处理用户连接。工作连接到达后消费。
    pub pending: Mutex<HashMap<u64, PendingWork>>,
    /// run_id → 控制会话。重连时按 run_id 定位旧会话并清理（§8.3）。
    pub sessions: Mutex<HashMap<String, Arc<crate::control::Session>>>,
    /// 优雅退出令牌：信号触发后，accept 循环与所有长连接任务据此退出（§14.4）。
    pub shutdown: CancellationToken,
}

impl ServerState {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            work_id: AtomicU64::new(0),
            pending: Mutex::new(HashMap::new()),
            sessions: Mutex::new(HashMap::new()),
            shutdown: CancellationToken::new(),
        })
    }

    /// 分配下一个 work_id（≥1）。
    pub fn next_work_id(&self) -> u64 {
        self.work_id.fetch_add(1, Ordering::SeqCst) + 1
    }
}

/// rfrps 服务端实例。
pub struct Server {
    config: ServerConfig,
    listener: TcpListener,
    state: Arc<ServerState>,
    /// 优雅退出宽限期：停止接收后等待在途连接结束的最长时间（§14.4）。
    grace: Duration,
}

impl Server {
    /// 绑定 `config.server.bind_addr:bind_port`。`bind_port=0` 由 OS 分配。
    pub async fn new(config: ServerConfig) -> Result<Self> {
        let listener = TcpListener::bind(config.server.bind_socket_addr()?).await?;
        Ok(Self {
            config,
            listener,
            state: ServerState::new(),
            grace: Duration::from_secs(GRACEFUL_SHUTDOWN_TIMEOUT),
        })
    }

    /// 实际监听地址（含 OS 分配的端口）。
    pub fn local_addr(&self) -> SocketAddr {
        self.listener.local_addr().unwrap()
    }

    /// 覆盖优雅退出宽限期（主要用于测试）。
    pub fn with_grace(mut self, grace: Duration) -> Self {
        self.grace = grace;
        self
    }

    /// 返回可被外部触发的退出令牌（终止信号处理器与测试共用）。
    pub fn shutdown_token(&self) -> CancellationToken {
        self.state.shutdown.clone()
    }

    /// accept 循环：每条连接派生一个任务。
    /// 收到退出信号（Ctrl+C / SIGTERM / 令牌取消）后停止接收新连接，
    /// 在 grace 宽限期内让在途连接自然结束，超时后由任务取消/进程退出强制关闭（§14.4）。
    pub async fn run(self) -> Result<()> {
        let shutdown = self.state.shutdown.clone();
        // 监听 OS 终止信号，触发统一退出令牌。
        let sig = spawn_signal_watcher(shutdown.clone());
        loop {
            tokio::select! {
                res = self.listener.accept() => {
                    match res {
                        Ok((stream, peer)) => {
                            let state = self.state.clone();
                            let config = self.config.clone();
                            tracing::debug!(%peer, "accepted connection");
                            tokio::spawn(async move {
                                if let Err(e) = handle_connection(stream, state, config).await {
                                    tracing::warn!("connection error: {e}");
                                }
                            });
                        }
                        Err(e) => {
                            tracing::warn!("accept error: {e}");
                            break;
                        }
                    }
                }
                _ = shutdown.cancelled() => {
                    tracing::info!("shutdown signal received; draining in-flight connections");
                    break;
                }
            }
        }
        // 优雅期：让在途连接自然结束；超时后返回，由取消令牌与进程退出强制清理。
        tokio::time::sleep(self.grace).await;
        sig.abort();
        Ok(())
    }
}

/// 读取首帧，按类型分派到控制连接或工作连接处理。
async fn handle_connection(
    stream: TcpStream,
    state: Arc<ServerState>,
    config: ServerConfig,
) -> Result<()> {
    let (frame, stream) = read_one_frame(stream).await?;
    match frame.msg_type {
        MSG_LOGIN => {
            control::handle_control_login(
                frame,
                stream,
                state,
                config,
                Duration::from_secs(HEARTBEAT_INTERVAL),
                Duration::from_secs(HEARTBEAT_TIMEOUT),
            )
            .await
        }
        MSG_START_WORK_CONN => work::handle_work_connection(frame, stream, state).await,
        other => {
            tracing::warn!("unexpected first frame msg_type={other:#x}, closing");
            Ok(())
        }
    }
}

/// 监听 OS 终止信号（Ctrl+C / SIGTERM），触发统一的退出令牌。
/// 仅当令牌未被取消时才等待信号，避免重复触发。
fn spawn_signal_watcher(shutdown: CancellationToken) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if shutdown.is_cancelled() {
            return;
        }
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm = match signal(SignalKind::terminate()) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!("install SIGTERM handler failed: {e}");
                    let _ = tokio::signal::ctrl_c().await;
                    shutdown.cancel();
                    return;
                }
            };
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = sigterm.recv() => {}
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
        shutdown.cancel();
    })
}
