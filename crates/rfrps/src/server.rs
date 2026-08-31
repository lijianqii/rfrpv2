//! 服务端主控：监听 accept 循环，按首帧区分控制/工作连接。

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rfrp_common::config::ServerConfig;
use rfrp_common::constants::{
    FIRST_FRAME_TIMEOUT, GRACEFUL_SHUTDOWN_TIMEOUT, HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT,
    MAX_ACTIVE_CONNECTIONS,
};
use rfrp_common::crypto::{ServerTls, ServerTlsStream};
use rfrp_common::error::Result;
use rfrp_common::protocol::frame::read_one_frame;
use rfrp_common::protocol::msg::{MSG_LOGIN, MSG_START_WORK_CONN};
use rfrp_common::util::signal::spawn_signal_watcher;
use rfrp_common::util::stream::BoxedStream;
use rfrp_common::util::tcp::configure_tcp_stream;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::control;
use crate::metrics::Metrics;
use crate::work;

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
            shutdown: CancellationToken::new(),
        })
    }

    /// 分配下一个 work_id（≥1）。
    pub fn next_work_id(&self) -> u64 {
        self.work_id.fetch_add(1, Ordering::SeqCst) + 1
    }
}

/// 将 CLI 参数覆盖到服务端配置（DESIGN §9.3）。
///
/// 返回 `Err` 表示参数解析失败（如 `--bind` 不是合法 `ADDR:PORT`）。
pub fn apply_cli_overrides(
    cfg: &mut ServerConfig,
    bind: Option<String>,
    token: Option<String>,
    tls_enable: Option<bool>,
    work_conn_tls: Option<bool>,
) -> std::result::Result<(), String> {
    if let Some(bind) = bind {
        let addr: SocketAddr = bind
            .parse()
            .map_err(|e| format!("invalid --bind '{bind}': {e}"))?;
        cfg.server.bind_addr = addr.ip().to_string();
        cfg.server.bind_port = addr.port();
    }
    if let Some(token) = token {
        cfg.server.token = token;
    }
    if let Some(v) = tls_enable {
        cfg.server.tls_enable = v;
    }
    if let Some(v) = work_conn_tls {
        cfg.server.work_conn_tls = v;
    }
    Ok(())
}

/// rfrps 服务端实例。
pub struct Server {
    config: ServerConfig,
    listener: TcpListener,
    state: Arc<ServerState>,
    /// 优雅退出宽限期：停止接收后等待在途连接结束的最长时间（§14.4）。
    grace: Duration,
    /// 控制/工作连接 TLS acceptor（按配置按需加载）。
    tls: Option<ServerTls>,
    /// HTTP vhost 监听（可选）。
    vhost_http: Option<TcpListener>,
    /// HTTPS vhost 监听 + TLS acceptor（可选）。
    vhost_https: Option<(TcpListener, ServerTls)>,
    /// Dashboard 监听（可选）。
    dashboard: Option<TcpListener>,
}

impl Server {
    /// 绑定 `config.server.bind_addr:bind_port`。`bind_port=0` 由 OS 分配。
    pub async fn new(config: ServerConfig) -> Result<Self> {
        let listener = TcpListener::bind(config.server.bind_socket_addr()?).await?;
        let vhost_http = match config.proxy.vhost_http_port {
            Some(port) => {
                let addr = (config.server.bind_addr.as_str(), port);
                Some(TcpListener::bind(addr).await?)
            }
            None => None,
        };
        let dashboard = match &config.dashboard {
            Some(d) => {
                let addr: SocketAddr = d.addr.parse().map_err(|e| {
                    rfrp_common::Error::Config(format!("invalid dashboard addr: {e}"))
                })?;
                Some(TcpListener::bind(addr).await?)
            }
            None => None,
        };
        let vhost_https = match config.proxy.vhost_https_port {
            Some(port) => {
                let cert = config.proxy.vhost_tls_cert.as_deref().ok_or_else(|| {
                    rfrp_common::Error::Config(
                        "vhost_https_port requires vhost_tls_cert and vhost_tls_key".into(),
                    )
                })?;
                let key = config.proxy.vhost_tls_key.as_deref().ok_or_else(|| {
                    rfrp_common::Error::Config(
                        "vhost_https_port requires vhost_tls_cert and vhost_tls_key".into(),
                    )
                })?;
                let listener = TcpListener::bind((config.server.bind_addr.as_str(), port)).await?;
                let tls = ServerTls::new(std::path::Path::new(cert), std::path::Path::new(key))?;
                Some((listener, tls))
            }
            None => None,
        };
        let tls = if config.server.tls_enable || config.server.work_conn_tls {
            let cert = config.server.tls_cert.as_deref().ok_or_else(|| {
                rfrp_common::Error::Config("tls_cert is required when TLS is enabled".into())
            })?;
            let key = config.server.tls_key.as_deref().ok_or_else(|| {
                rfrp_common::Error::Config("tls_key is required when TLS is enabled".into())
            })?;
            Some(ServerTls::new(
                std::path::Path::new(cert),
                std::path::Path::new(key),
            )?)
        } else {
            None
        };
        Ok(Self {
            config,
            listener,
            state: ServerState::new(),
            grace: Duration::from_secs(GRACEFUL_SHUTDOWN_TIMEOUT),
            tls,
            vhost_http,
            vhost_https,
            dashboard,
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
        let tls = self.tls.clone();
        let vhost_http = self.vhost_http;
        let vhost_https = self.vhost_https;
        let dashboard = self.dashboard;
        let dashboard_cfg = self.config.dashboard.clone();
        let mut tasks = JoinSet::new();
        // 监听 OS 终止信号，触发统一退出令牌。
        let sig = spawn_signal_watcher(shutdown.clone());
        // HTTP vhost 监听循环（可选）。
        if let Some(listener) = vhost_http {
            let state = self.state.clone();
            let shutdown = shutdown.clone();
            tasks.spawn(async move {
                crate::vhost::run_http_vhost(listener, state, shutdown).await;
            });
        }
        // Dashboard 监听循环（可选）。
        if let (Some(listener), Some(cfg)) = (dashboard, dashboard_cfg) {
            let state = self.state.clone();
            let shutdown = shutdown.clone();
            tasks.spawn(async move {
                crate::dashboard::run_dashboard(listener, cfg, state, shutdown).await;
            });
        }
        // HTTPS vhost 监听循环（可选）。
        if let Some((listener, tls)) = vhost_https {
            let state = self.state.clone();
            let shutdown = shutdown.clone();
            tasks.spawn(async move {
                crate::vhost::run_https_vhost(listener, tls, state, shutdown).await;
            });
        }
        loop {
            tokio::select! {
                res = self.listener.accept() => {
                    match res {
                        Ok((stream, peer)) => {
                            if let Err(e) = configure_tcp_stream(&stream) {
                                tracing::warn!(%peer, error = %e, "failed to configure TCP stream");
                            }
                            // 并发连接数兜底（防 DoS）。
                            let active = self.state.metrics.active_connections.load(Ordering::Relaxed);
                            if active >= MAX_ACTIVE_CONNECTIONS {
                                tracing::warn!(%peer, active, "too many active connections, rejecting");
                                continue;
                            }
                            let state = self.state.clone();
                            let config = self.config.clone();
                            let tls = tls.clone();
                            tracing::debug!(%peer, "accepted connection");
                            tasks.spawn(async move {
                                if let Err(e) = handle_connection(stream, state, config, tls).await {
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
        // 优雅期：等待在途连接任务自然结束；无在途任务时立即返回，超时后强制返回。
        let deadline = Instant::now() + self.grace;
        while !tasks.is_empty() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            if tokio::time::timeout(remaining, tasks.join_next())
                .await
                .is_err()
            {
                break;
            }
        }
        sig.abort();
        Ok(())
    }
}

/// 读取首帧，按类型分派到控制连接或工作连接处理。
///
/// 同一 `bind_port` 上可能混有 TLS 与明文连接（取决于 `tls_enable` / `work_conn_tls`），
/// 因此先 peek 首字节：TLS 握手记录以 `0x16` 开头，普通 rfrp 帧以协议版本 `0x01` 开头。
async fn handle_connection(
    stream: TcpStream,
    state: Arc<ServerState>,
    config: ServerConfig,
    tls: Option<ServerTls>,
) -> Result<()> {
    let mut first = [0u8; 1];
    let n = stream.peek(&mut first).await?;
    if n == 0 {
        return Ok(());
    }
    let looks_like_tls = first[0] == 0x16;

    let maybe_tls = if let Some(tls) = tls {
        if looks_like_tls {
            MaybeTls::Tls(Box::new(tls.accept(stream).await?))
        } else {
            MaybeTls::Plain(stream)
        }
    } else {
        MaybeTls::Plain(stream)
    };
    let is_tls = matches!(&maybe_tls, MaybeTls::Tls(_));

    let (frame, stream) = match maybe_tls {
        MaybeTls::Plain(s) => {
            let (f, s) =
                tokio::time::timeout(Duration::from_secs(FIRST_FRAME_TIMEOUT), read_one_frame(s))
                    .await
                    .map_err(|_| rfrp_common::Error::Other("first frame timeout".into()))??;
            (f, Box::new(s) as BoxedStream)
        }
        MaybeTls::Tls(s) => {
            let (f, s) =
                tokio::time::timeout(Duration::from_secs(FIRST_FRAME_TIMEOUT), read_one_frame(*s))
                    .await
                    .map_err(|_| rfrp_common::Error::Other("first frame timeout".into()))??;
            (f, Box::new(s) as BoxedStream)
        }
    };

    match frame.msg_type {
        MSG_LOGIN => {
            if config.server.tls_enable && !is_tls {
                tracing::warn!("plaintext login rejected: tls_enable=true");
                return Ok(());
            }
            if !config.server.tls_enable && is_tls {
                tracing::warn!("TLS login rejected: tls_enable=false");
                return Ok(());
            }
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
        MSG_START_WORK_CONN => {
            if config.server.work_conn_tls && !is_tls {
                tracing::warn!("plaintext work connection rejected: work_conn_tls=true");
                return Ok(());
            }
            work::handle_work_connection(frame, stream, state).await
        }
        other => {
            tracing::warn!("unexpected first frame msg_type={other:#x}, closing");
            Ok(())
        }
    }
}

/// 服务端 accept 后可能是明文 TCP 或 TLS 流。
enum MaybeTls {
    Plain(TcpStream),
    Tls(Box<ServerTlsStream<TcpStream>>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use rfrp_common::config::ServerConfig;

    #[test]
    fn apply_cli_overrides_sets_fields() {
        let mut cfg = ServerConfig::default();
        apply_cli_overrides(
            &mut cfg,
            Some("0.0.0.0:8000".into()),
            Some("token".into()),
            Some(true),
            Some(false),
        )
        .unwrap();
        assert_eq!(cfg.server.bind_addr, "0.0.0.0");
        assert_eq!(cfg.server.bind_port, 8000);
        assert_eq!(cfg.server.token, "token");
        assert!(cfg.server.tls_enable);
        assert!(!cfg.server.work_conn_tls);
    }

    #[test]
    fn apply_cli_overrides_rejects_bad_bind() {
        let mut cfg = ServerConfig::default();
        assert!(
            apply_cli_overrides(&mut cfg, Some("not-an-addr".into()), None, None, None).is_err()
        );
    }
}
