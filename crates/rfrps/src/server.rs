//! 服务端主控：监听 accept 循环，按首帧区分控制/工作连接。

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rfrp_common::config::ServerConfig;
use rfrp_common::constants::{HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT};
use rfrp_common::error::Result;
use rfrp_common::protocol::frame::read_one_frame;
use rfrp_common::protocol::msg::{MSG_LOGIN, MSG_START_WORK_CONN};
use tokio::net::{TcpListener, TcpStream};

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
}

impl ServerState {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            work_id: AtomicU64::new(0),
            pending: Mutex::new(HashMap::new()),
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
}

impl Server {
    /// 绑定 `config.server.bind_addr:bind_port`。`bind_port=0` 由 OS 分配。
    pub async fn new(config: ServerConfig) -> Result<Self> {
        let listener = TcpListener::bind(config.server.bind_socket_addr()?).await?;
        Ok(Self {
            config,
            listener,
            state: ServerState::new(),
        })
    }

    /// 实际监听地址（含 OS 分配的端口）。
    pub fn local_addr(&self) -> SocketAddr {
        self.listener.local_addr().unwrap()
    }

    /// accept 循环：每条连接派生一个任务。
    pub async fn run(self) -> Result<()> {
        loop {
            let (stream, peer) = self.listener.accept().await?;
            let state = self.state.clone();
            let config = self.config.clone();
            tracing::debug!(%peer, "accepted connection");
            tokio::spawn(async move {
                if let Err(e) = handle_connection(stream, state, config).await {
                    tracing::warn!("connection error: {e}");
                }
            });
        }
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
