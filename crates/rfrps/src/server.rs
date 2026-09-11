//! 服务端主控：监听 accept 循环，按首帧区分控制/工作连接。

use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use rfrp_common::config::ServerConfig;
use rfrp_common::constants::{
    FIRST_FRAME_TIMEOUT, GRACEFUL_SHUTDOWN_TIMEOUT, HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT,
    MAX_CONSECUTIVE_ACCEPT_ERRORS, SERVER_ALIVE_LOG_INTERVAL, TLS_HANDSHAKE_TIMEOUT,
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
use crate::work;

use crate::state::ServerState;

/// rfrps 服务端实例。
pub struct Server {
    config: Arc<ServerConfig>,
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
            config: Arc::new(config),
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

    /// 覆盖最大并发用户连接数（主要用于测试）。
    pub fn with_max_active(self, max: i64) -> Self {
        self.state.max_active.store(max, Ordering::Relaxed);
        self
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
        let config = self.config.clone();
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
        // 存活摘要：周期性输出计数，便于区分"进程卡死"与"网络不可达"
        // （客户端连不上时，看 accepted_total 是否增长即可判断 SYN 是否到达）。
        {
            let state = self.state.clone();
            let sd = shutdown.clone();
            tasks.spawn(async move {
                let mut iv = tokio::time::interval(Duration::from_secs(SERVER_ALIVE_LOG_INTERVAL));
                iv.tick().await; // 消耗首次立即 tick
                loop {
                    tokio::select! {
                        _ = iv.tick() => {
                            let g = state.gauges();
                            tracing::info!(
                                uptime_secs = state.metrics.uptime_secs(),
                                accepted_total = state.metrics.accepted_total.load(Ordering::Relaxed),
                                accept_errors_total = state.metrics.accept_errors_total.load(Ordering::Relaxed),
                                sessions = g.sessions,
                                proxies = g.proxies,
                                active_connections = state.metrics.active_connections.load(Ordering::Relaxed),
                                "rfrps alive"
                            );
                        }
                        _ = sd.cancelled() => break,
                    }
                }
            });
        }
        let mut consecutive_accept_errors: u32 = 0;
        loop {
            tokio::select! {
                res = self.listener.accept() => {
                    match res {
                        Ok((stream, peer)) => {
                            consecutive_accept_errors = 0;
                            self.state.metrics.inc_accepted();
                            if let Err(e) = configure_tcp_stream(&stream) {
                                tracing::warn!(%peer, error = %e, "failed to configure TCP stream");
                            }
                            let state = self.state.clone();
                            let config = config.clone();
                            let tls = tls.clone();
                            tracing::debug!(%peer, "accepted connection");
                            tasks.spawn(async move {
                                if let Err(e) = handle_connection(
                                    stream,
                                    state,
                                    config,
                                    tls,
                                    Duration::from_secs(FIRST_FRAME_TIMEOUT),
                                )
                                .await
                                {
                                    tracing::warn!(%peer, error = %e, "connection error");
                                }
                            });
                        }
                        Err(e) => {
                            // 瞬时错误（对端握手期重置、fd 耗尽等）不应终止 accept 循环：
                            // 退避重试，避免"服务端仍在运行却不再接受连接"的静默故障。
                            consecutive_accept_errors += 1;
                            self.state.metrics.inc_accept_error();
                            tracing::warn!(
                                consecutive = consecutive_accept_errors,
                                error = %e,
                                "accept error; retrying"
                            );
                            if consecutive_accept_errors >= MAX_CONSECUTIVE_ACCEPT_ERRORS {
                                tracing::error!(
                                    consecutive = consecutive_accept_errors,
                                    "accept loop failed repeatedly; exiting for supervisor restart"
                                );
                                sig.abort();
                                return Err(rfrp_common::Error::Other(
                                    "accept loop failed repeatedly".into(),
                                ));
                            }
                            let backoff_ms = (consecutive_accept_errors as u64 * 100).min(1000);
                            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                        }
                    }
                }
                // 回收已完成任务：JoinSet 会保留已完成任务的条目直到被 join，
                // 长期运行下按连接数累积（实测约 257B/连接）。此处持续回收。
                Some(_) = tasks.join_next(), if !tasks.is_empty() => {}
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
    config: Arc<ServerConfig>,
    tls: Option<ServerTls>,
    first_frame_timeout: Duration,
) -> Result<()> {
    let peer_ip = stream
        .peer_addr()
        .map(|a| a.ip())
        .unwrap_or(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED));
    // peek 也必须有超时：连接后不发任何字节的对端（端口扫描、半开连接）会
    // 永久挂住一个任务与套接字；10s 内无首字节直接关闭。
    let mut first = [0u8; 1];
    let n = match tokio::time::timeout(first_frame_timeout, stream.peek(&mut first)).await {
        Ok(r) => r?,
        Err(_) => {
            tracing::debug!("connection sent no first byte within timeout; closing");
            return Ok(());
        }
    };
    if n == 0 {
        return Ok(());
    }
    let looks_like_tls = first[0] == 0x16;

    let maybe_tls = if let Some(tls) = tls {
        if looks_like_tls {
            // 半截 TLS 握手（发送 ClientHello 后停住）不得长期占用任务与套接字。
            match tokio::time::timeout(
                Duration::from_secs(TLS_HANDSHAKE_TIMEOUT),
                tls.accept(stream),
            )
            .await
            {
                Ok(r) => MaybeTls::Tls(Box::new(r?)),
                Err(_) => {
                    tracing::debug!("TLS handshake timeout; closing");
                    return Ok(());
                }
            }
        } else {
            MaybeTls::Plain(stream)
        }
    } else {
        MaybeTls::Plain(stream)
    };
    let is_tls = matches!(&maybe_tls, MaybeTls::Tls(_));

    let (frame, stream) = match maybe_tls {
        MaybeTls::Plain(s) => {
            let (f, s) = tokio::time::timeout(first_frame_timeout, read_one_frame(s))
                .await
                .map_err(|_| rfrp_common::Error::Other("first frame timeout".into()))??;
            (f, Box::new(s) as BoxedStream)
        }
        MaybeTls::Tls(s) => {
            let (f, s) = tokio::time::timeout(first_frame_timeout, read_one_frame(*s))
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
                peer_ip,
                state,
                (*config).clone(),
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

    #[tokio::test]
    async fn silent_connection_is_dropped_by_peek_timeout() {
        // 连接后不发任何字节的对端（端口扫描/半开连接）不得永久挂住任务与套接字。
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _client = TcpStream::connect(addr).await.unwrap(); // 只连接，不发数据
        let (server, _) = listener.accept().await.unwrap();

        let state = ServerState::new();
        let cfg = Arc::new(ServerConfig::default());
        let started = std::time::Instant::now();
        let res = handle_connection(
            server,
            state,
            cfg,
            None,
            Duration::from_millis(150), // 测试用短超时
        )
        .await;
        assert!(res.is_ok(), "silent connection should be closed cleanly");
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "must not hang: {:?}",
            started.elapsed()
        );
    }
}
