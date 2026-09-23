//! 服务端主控：监听 accept 循环，按首帧区分控制/工作连接。

use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use rfrp_common::config::ServerConfig;
use rfrp_common::constants::{
    FIRST_FRAME_TIMEOUT, SERVER_ALIVE_LOG_INTERVAL, TLS_HANDSHAKE_TIMEOUT, WORK_CONN_TIMEOUT_RFRPS,
};
use rfrp_common::crypto::{ServerTls, ServerTlsStream};
use rfrp_common::error::{Error, Result};
use rfrp_common::protocol::frame::read_one_frame;
use rfrp_common::protocol::msg::{MSG_LOGIN, MSG_START_WORK_CONN};
use rfrp_common::util::accept::AcceptRetry;
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
    /// 心跳参数（来自配置，缺省 30s 间隔 / 10s 超时，见 §8.3）。
    heartbeat_interval: Duration,
    heartbeat_timeout: Duration,
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
        // 用 `(host, port)` 元组形式绑定：允许 `bind_addr` 为域名（如 `localhost`），
        // 与 vhost / 代理端口监听保持一致。
        let bind = format!("{}:{}", config.server.bind_addr, config.server.bind_port);
        let listener =
            TcpListener::bind((config.server.bind_addr.as_str(), config.server.bind_port))
                .await
                .map_err(|e| bind_error("control listener", &bind, e))?;
        let vhost_http =
            bind_optional(&config.server.bind_addr, config.proxy.vhost_http_port).await?;
        let dashboard = bind_dashboard(&config).await?;
        let vhost_https = bind_vhost_https(&config).await?;
        let tls = load_control_tls(&config)?;
        // 在 config 移入 Arc 之前取出生效的心跳参数。
        let heartbeat_interval = config.server.heartbeat_interval();
        let heartbeat_timeout = config.server.heartbeat_timeout();
        let grace = config.server.grace();
        Ok(Self {
            config: Arc::new(config),
            listener,
            state: ServerState::new(),
            grace,
            heartbeat_interval,
            heartbeat_timeout,
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
        let heartbeat_interval = self.heartbeat_interval;
        let heartbeat_timeout = self.heartbeat_timeout;
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
        // 待处理工作连接超时清理：单周期扫描整张表，替代"每个用户连接派生一个 sleep
        // 任务"（高连接速率下后者会累积大量睡眠任务）。扫描周期 1s，超时取
        // WORK_CONN_TIMEOUT_RFRPS，实际清理时间落在 [timeout, timeout+1s]。
        {
            let state = self.state.clone();
            let sd = shutdown.clone();
            tasks.spawn(async move {
                let timeout = Duration::from_secs(WORK_CONN_TIMEOUT_RFRPS);
                let mut iv = tokio::time::interval(Duration::from_secs(1));
                iv.tick().await; // 消耗首次立即 tick
                loop {
                    tokio::select! {
                        _ = iv.tick() => {
                            let n = crate::listener::sweep_expired_pending(&state, timeout).await;
                            if n > 0 {
                                tracing::debug!(count = n, "closed timed-out pending user connections");
                            }
                        }
                        _ = sd.cancelled() => break,
                    }
                }
            });
        }
        // 与代理/vhost/Dashboard 共用同一套退避策略；区别是主监听连续失败达到阈值后
        // 判定不可恢复，进程以非零码退出交服务管理器重启（见 util::accept 说明）。
        let mut accept_retry = AcceptRetry::new();
        loop {
            tokio::select! {
                res = self.listener.accept() => {
                    match res {
                        Ok((stream, peer)) => {
                            accept_retry.record_ok();
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
                                    heartbeat_interval,
                                    heartbeat_timeout,
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
                            let backoff = accept_retry.record_err();
                            self.state.metrics.inc_accept_error();
                            if accept_retry.should_log() {
                                tracing::warn!(
                                    consecutive = accept_retry.consecutive(),
                                    error = %e,
                                    "accept error; retrying"
                                );
                            }
                            if accept_retry.is_fatal() {
                                tracing::error!(
                                    consecutive = accept_retry.consecutive(),
                                    "accept loop failed repeatedly; exiting for supervisor restart"
                                );
                                sig.abort();
                                return Err(Error::Other("accept loop failed repeatedly".into()));
                            }
                            tokio::time::sleep(backoff).await;
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

/// 绑定可选监听端口（`None` = 该监听未启用）。
async fn bind_optional(bind_addr: &str, port: Option<u16>) -> Result<Option<TcpListener>> {
    match port {
        Some(port) => {
            let addr = format!("{bind_addr}:{port}");
            let listener = TcpListener::bind((bind_addr, port))
                .await
                .map_err(|e| bind_error("http vhost listener", &addr, e))?;
            Ok(Some(listener))
        }
        None => Ok(None),
    }
}

/// 绑定失败时带上"绑到哪个地址"的上下文。
///
/// 否则日志只剩 `address already in use`，多监听场景下无法定位是控制口、vhost 还是
/// Dashboard 端口冲突。
fn bind_error(what: &str, addr: &str, e: std::io::Error) -> Error {
    Error::Other(format!(
        "failed to bind {what} {addr}: {}",
        rfrp_common::error::describe_io_error(&e)
    ))
}

/// Dashboard 监听（可选）。地址格式已在配置校验阶段检查过。
async fn bind_dashboard(config: &ServerConfig) -> Result<Option<TcpListener>> {
    let Some(d) = &config.dashboard else {
        return Ok(None);
    };
    let addr: SocketAddr = d
        .addr
        .parse()
        .map_err(|e| Error::Config(format!("invalid dashboard addr: {e}")))?;
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| bind_error("dashboard listener", &d.addr, e))?;
    Ok(Some(listener))
}

/// HTTPS vhost 监听 + 证书（可选；启用时必须同时提供证书与私钥）。
async fn bind_vhost_https(config: &ServerConfig) -> Result<Option<(TcpListener, ServerTls)>> {
    let Some(port) = config.proxy.vhost_https_port else {
        return Ok(None);
    };
    let missing =
        || Error::Config("vhost_https_port requires vhost_tls_cert and vhost_tls_key".into());
    let cert = config.proxy.vhost_tls_cert.as_deref().ok_or_else(missing)?;
    let key = config.proxy.vhost_tls_key.as_deref().ok_or_else(missing)?;
    let addr = format!("{}:{port}", config.server.bind_addr);
    let listener = TcpListener::bind((config.server.bind_addr.as_str(), port))
        .await
        .map_err(|e| bind_error("https vhost listener", &addr, e))?;
    let tls = ServerTls::new(std::path::Path::new(cert), std::path::Path::new(key))?;
    Ok(Some((listener, tls)))
}

/// 控制链路 / 工作连接共用的 TLS acceptor（可选）。
fn load_control_tls(config: &ServerConfig) -> Result<Option<ServerTls>> {
    if !(config.server.tls_enable || config.server.work_conn_tls) {
        return Ok(None);
    }
    let missing = |field: &str| {
        Error::Config(format!(
            "{field} is required when tls_enable or work_conn_tls is set"
        ))
    };
    let cert = config
        .server
        .tls_cert
        .as_deref()
        .ok_or_else(|| missing("tls_cert"))?;
    let key = config
        .server
        .tls_key
        .as_deref()
        .ok_or_else(|| missing("tls_key"))?;
    Ok(Some(ServerTls::new(
        std::path::Path::new(cert),
        std::path::Path::new(key),
    )?))
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
    heartbeat_interval: Duration,
    heartbeat_timeout: Duration,
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
                heartbeat_interval,
                heartbeat_timeout,
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
            Duration::from_secs(30),
            Duration::from_secs(10),
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
