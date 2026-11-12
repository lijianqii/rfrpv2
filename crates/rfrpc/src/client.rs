//! 客户端主控：连接服务端、登录、按配置串行注册代理，长驻控制循环；
//! 断开后按指数退避重连，并复用 run_id 恢复代理（DESIGN §8.1 / §8.3 / §6.6）。

use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result as AnyResult;
use rfrp_common::config::{ClientConfig, ClientProxy};
use rfrp_common::constants::{
    CONNECT_TIMEOUT, LOGIN_TIMEOUT, MAX_RUN_ID_LEN, MIN_STABLE_CONNECTION_SECS, NEW_PROXY_TIMEOUT,
    POOL_SIZE_MAX, PREHEAT_MAX_CONCURRENT, PROTOCOL_VERSION, PROXY_REGISTER_RETRY_INITIAL,
    PROXY_REGISTER_RETRY_MAX, PROXY_REGISTER_RETRY_MAX_DELAY,
    PROXY_REGISTER_RETRY_PERSISTENT_DELAY_SECS, PROXY_REGISTER_RETRY_PERSISTENT_MAX,
    RECONNECT_BACKOFF_INITIAL, RECONNECT_BACKOFF_MAX, TLS_HANDSHAKE_TIMEOUT, WORK_ID_POOL_RESERVED,
};
use rfrp_common::crypto::ClientTls;
use rfrp_common::error::Result as RfrpResult;
use rfrp_common::protocol::msg::*;
use rfrp_common::util::control::{send_with_timeout_reserved, CONTROL_HEARTBEAT_RESERVE};
use rfrp_common::util::platform::default_run_id_path;
use rfrp_common::util::signal::spawn_signal_watcher;
use rfrp_common::util::stream::BoxedStream;
use rfrp_common::util::tcp::configure_tcp_stream;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;

use crate::control;
use crate::metrics::ClientMetrics;

/// 客户端运行时共享状态（控制循环与注册逻辑共享）。
pub struct ClientState {
    pub server_addr: std::net::SocketAddr,
    /// 重连身份标识（持久化复用，DESIGN §6.6）。
    pub run_id: String,
    /// proxy_name → 代理配置（工作连接按名 O(1) 查找）。
    pub proxies: HashMap<String, ClientProxy>,
    /// proxy_name → NewProxyResp 的一次性回传通道（注册时 await）。
    pub resps: Mutex<HashMap<String, oneshot::Sender<NewProxyResp>>>,
    /// Login 结果一次性回传通道（连接时 await，用于区分致命/可恢复失败）。
    pub login_tx: Mutex<Option<oneshot::Sender<LoginResp>>>,
    /// 客户端 TLS 配置（控制链路和工作连接共用；仅任一 TLS 开启时存在）。
    pub tls: Option<ClientTls>,
    /// 工作连接实际是否使用 TLS（由服务端 LoginResp 偏好覆盖，DESIGN §6.5）。
    pub work_conn_tls: Mutex<bool>,
    /// 工作连接鉴权令牌（来自 LoginResp，随 StartWorkConn 上送）。
    pub work_conn_token: Mutex<Option<String>>,
    /// 正在建立/使用中的按需工作连接 `(proxy_name, work_id)`。
    ///
    /// 服务端在 10s pending 窗口内每 3s 重发 `ReqWorkConn`；客户端必须去重，
    /// 否则本地服务建立慢时会并发创建多条连接，之后多余工作连接被服务端拒绝。
    pub inflight_work: Mutex<HashSet<(String, u64)>>,
    /// 预热工作连接的全局并发上限。
    pub preheat_limit: Arc<tokio::sync::Semaphore>,
    /// 进程级运行指标（跨重连累计）。
    pub metrics: Arc<ClientMetrics>,
}

/// 单次连接的结果：决定上层是否重连（DESIGN §8.1 / §8.3）。
enum ConnectOutcome {
    /// 需要重连。`connected` 表示本次是否已经成功建立过控制会话；
    /// `lived` 从登录成功开始计时（不含 DNS/建连/TLS/登录等待），仅对
    /// `connected=true` 有意义。
    Reconnect { connected: bool, lived: Duration },
    /// 致命错误（鉴权 / 版本不兼容），不应重连。
    Fatal(String),
}

/// rfrpc 客户端实例。
pub struct Client {
    config: ClientConfig,
    /// 缓存的客户端 TLS 配置（重建连接时复用，避免每次重连读 CA 文件）。
    tls: Option<ClientTls>,
    /// 心跳发送间隔（控制连接保活/失联检测）。
    heartbeat_interval: Duration,
    /// 心跳响应等待超时：超时判定控制连接已死并触发重连。
    heartbeat_timeout: Duration,
    /// 进程级运行指标。
    metrics: Arc<ClientMetrics>,
    /// 优雅退出令牌：信号或外部触发后停止重连并退出（§14.4）。
    shutdown: CancellationToken,
}

/// 按配置构建客户端 TLS；不启用 TLS 时返回 `None`。
///
/// 只要配置了 `tls_server_name` 就构建，以便服务端在 LoginResp 中要求工作连接
/// 升级到 TLS 时（DESIGN §6.5 决策表）可以立即使用。
fn build_tls(config: &ClientConfig) -> RfrpResult<Option<ClientTls>> {
    if config.client.tls_enable
        || config.client.work_conn_tls
        || config.client.tls_server_name.is_some()
    {
        Ok(Some(ClientTls::new(&config.client)?))
    } else {
        Ok(None)
    }
}

/// 仅校验 TLS 材料（CA/服务端名称）可解析，不建立连接。
pub fn validate_tls_config(config: &ClientConfig) -> RfrpResult<()> {
    build_tls(config).map(|_| ())
}

/// 状态端点任务守卫：`run` 的任何返回路径（含致命错误）都取消监听并 abort。
struct StatusTask {
    task: JoinHandle<()>,
    shutdown: CancellationToken,
}

impl Drop for StatusTask {
    fn drop(&mut self) {
        self.shutdown.cancel();
        self.task.abort();
    }
}

impl Client {
    pub fn new(config: ClientConfig) -> RfrpResult<Self> {
        let tls = build_tls(&config)?;
        Ok(Self {
            heartbeat_interval: config.client.heartbeat_interval(),
            heartbeat_timeout: config.client.heartbeat_timeout(),
            config,
            tls,
            metrics: Arc::new(ClientMetrics::new()),
            shutdown: CancellationToken::new(),
        })
    }

    /// 覆盖心跳间隔与响应超时（主要用于测试：缩短失联检测时间）。
    pub fn with_heartbeat(mut self, interval: Duration, timeout: Duration) -> Self {
        self.heartbeat_interval = interval;
        self.heartbeat_timeout = timeout;
        self
    }

    /// 返回可被外部触发的退出令牌（终止信号处理器与测试共用）。
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// 长驻运行：连接 → 注册 → 控制循环；断开后按指数退避重连（§8.3）。
    /// 仅当收到致命 Login 失败（鉴权 / 版本不兼容）时返回错误退出。
    pub async fn run(self) -> AnyResult<()> {
        let run_id = self.load_or_create_run_id();
        let shutdown = self.shutdown.clone();
        // 监听 OS 终止信号，触发统一退出令牌。
        let sig = spawn_signal_watcher(shutdown.clone());
        // 可选状态端点（[client] status_addr）：绑定失败只告警，不影响隧道功能。
        let _status_task = if let Some(addr) = &self.config.client.status_addr {
            match tokio::net::TcpListener::bind(addr).await {
                Ok(listener) => {
                    if !addr.starts_with("127.")
                        && !addr.starts_with("localhost")
                        && !addr.starts_with("[::1]")
                    {
                        tracing::warn!(%addr, "status endpoint bound to non-loopback address; it has no authentication");
                    }
                    tracing::info!(%addr, "status endpoint listening");
                    let cfg = self.config.clone();
                    let metrics = self.metrics.clone();
                    let sd = shutdown.clone();
                    let task = tokio::spawn(async move {
                        crate::status::run_status_server(listener, cfg, metrics, sd).await;
                    });
                    Some(StatusTask {
                        task,
                        shutdown: shutdown.clone(),
                    })
                }
                Err(e) => {
                    tracing::error!(%addr, error = %e, "failed to bind status endpoint");
                    None
                }
            }
        } else {
            None
        };
        let mut backoff = Duration::from_secs(RECONNECT_BACKOFF_INITIAL);
        let mut attempt: u32 = 0;
        loop {
            if shutdown.is_cancelled() {
                tracing::info!("shutdown requested, exiting client");
                break;
            }
            attempt += 1;
            tracing::debug!(attempt, "connection attempt");
            match self.connect_once(&run_id, &shutdown).await {
                Ok(ConnectOutcome::Fatal(reason)) => {
                    sig.abort();
                    return Err(anyhow::anyhow!("login fatal: {reason}"));
                }
                Ok(ConnectOutcome::Reconnect { connected, lived }) => {
                    if shutdown.is_cancelled() {
                        break;
                    }
                    self.metrics.inc_reconnect();
                    // 仅"稳定连接"重置退避：会话刚建立即断开（抖动/被顶替）时保持退避增长，
                    // 避免 1s 间隔的重连风暴。计时从登录成功开始，避免慢解析/建连被误判为稳定。
                    if should_reset_backoff(connected, lived) {
                        attempt = 0;
                        backoff = Duration::from_secs(RECONNECT_BACKOFF_INITIAL);
                    }
                    tracing::info!(
                        attempt,
                        backoff_secs = backoff.as_secs(),
                        "control closed, reconnecting"
                    );
                    if !wait_for_reconnect(backoff, &shutdown).await {
                        break;
                    }
                    backoff = (backoff * 2).min(Duration::from_secs(RECONNECT_BACKOFF_MAX));
                }
                Err(e) => {
                    if shutdown.is_cancelled() {
                        break;
                    }
                    self.metrics.inc_reconnect();
                    tracing::warn!(attempt, error = %e, "transient error, reconnecting");
                    if !wait_for_reconnect(backoff, &shutdown).await {
                        break;
                    }
                    backoff = (backoff * 2).min(Duration::from_secs(RECONNECT_BACKOFF_MAX));
                }
            }
        }
        sig.abort();
        Ok(())
    }

    /// 单次连接：建连、登录、注册代理、长驻控制循环直到断开。
    async fn connect_once(
        &self,
        run_id: &str,
        shutdown: &CancellationToken,
    ) -> AnyResult<ConnectOutcome> {
        // 允许 server_addr 为域名：每次建连都重新解析（IP 字面量则直接使用）。
        // 双栈域名可能返回多个地址：在同一个 CONNECT_TIMEOUT 内逐个尝试，
        // 避免首个地址不可达时永远连不上；解析本身也受该截止时间约束。
        let deadline = tokio::time::Instant::now() + Duration::from_secs(CONNECT_TIMEOUT);
        let server_addrs = tokio::select! {
            _ = shutdown.cancelled() => {
                return Ok(ConnectOutcome::Reconnect { connected: false, lived: Duration::ZERO });
            }
            resolved = tokio::time::timeout_at(
                deadline,
                self.config.client.resolve_server_addrs(),
            ) => match resolved {
                Ok(Ok(addrs)) => addrs,
                Ok(Err(e)) => {
                    tracing::warn!(error = %e, "resolve server_addr failed");
                    return Err(anyhow::anyhow!(e));
                }
                Err(_) => {
                    tracing::warn!("resolve server_addr timeout");
                    return Err(anyhow::anyhow!("resolve server_addr timeout"));
                }
            }
        };

        let mut connected = None;
        let mut last_err = None;
        for addr in server_addrs {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            let attempt = tokio::select! {
                _ = shutdown.cancelled() => {
                    return Ok(ConnectOutcome::Reconnect { connected: false, lived: Duration::ZERO });
                }
                result = tokio::time::timeout(remaining, TcpStream::connect(addr)) => result,
            };
            match attempt {
                Ok(Ok(stream)) => {
                    connected = Some((stream, addr));
                    break;
                }
                Ok(Err(e)) => {
                    tracing::warn!(server = %addr, error = %e, "connect failed");
                    last_err = Some(anyhow::anyhow!(e));
                }
                Err(_) => {
                    tracing::warn!(server = %addr, "connect timeout");
                    last_err = Some(anyhow::anyhow!("connect timeout"));
                    break;
                }
            }
        }
        let (stream, server_addr) = match connected {
            Some(v) => v,
            None => {
                return Err(last_err.unwrap_or_else(|| {
                    anyhow::anyhow!("no resolved server address could connect")
                }));
            }
        };
        let tls = self.tls.clone();
        if let Err(e) = configure_tcp_stream(&stream) {
            tracing::warn!(error = %e, "failed to configure control TCP stream");
        }
        tracing::info!(
            server = %server_addr,
            local = %stream.local_addr().map(|a| a.to_string()).unwrap_or_else(|_| "?".into()),
            "connected to server"
        );

        // 控制链路 TLS（仅 tls_enable=true 时启用；工作连接 TLS 由各工作连接按需决定）。
        let stream: BoxedStream = if self.config.client.tls_enable {
            let tls = tls.as_ref().expect("tls built above");
            // 控制链路 TLS 握手必须有超时：服务端/中间设备接受 TCP 但不完成握手时，
            // 否则客户端会永久挂起、既不重连也没有日志（工作连接 TLS 已有超时）。
            match tokio::time::timeout(
                Duration::from_secs(TLS_HANDSHAKE_TIMEOUT),
                tls.connect(stream),
            )
            .await
            {
                Ok(Ok(s)) => Box::new(s),
                // 证书校验/协议协商失败是配置问题，重试不会自愈：按致命处理并退出，
                // 避免无限退避重连且不给用户明确信号（连接类错误仍是瞬时问题）。
                Ok(Err(rfrp_common::Error::Auth(msg))) => {
                    tracing::error!(
                        error = %msg,
                        "control TLS handshake failed; not reconnecting \
                         (check tls_ca / tls_server_name / server certificate)"
                    );
                    return Ok(ConnectOutcome::Fatal(msg));
                }
                Ok(Err(e)) => return Err(anyhow::anyhow!(e)),
                Err(_) => {
                    tracing::warn!("control TLS handshake timeout");
                    return Err(anyhow::anyhow!("control TLS handshake timeout"));
                }
            }
        } else {
            Box::new(stream)
        };

        let state = Arc::new(ClientState {
            server_addr,
            run_id: run_id.to_string(),
            proxies: self
                .config
                .proxies
                .iter()
                .map(|p| (p.name.clone(), p.clone()))
                .collect(),
            resps: Mutex::new(HashMap::new()),
            login_tx: Mutex::new(None),
            tls,
            work_conn_tls: Mutex::new(self.config.client.work_conn_tls),
            work_conn_token: Mutex::new(None),
            inflight_work: Mutex::new(HashSet::new()),
            preheat_limit: Arc::new(tokio::sync::Semaphore::new(PREHEAT_MAX_CONCURRENT)),
            metrics: self.metrics.clone(),
        });
        let (tx, rx) = mpsc::channel::<Message>(64);
        let (login_otx, login_orx) = oneshot::channel();
        let mut connected_at = None;
        state.login_tx.lock().replace(login_otx);

        // 每次控制会话使用独立子令牌：单次连接失败时可以只清理本会话的
        // writer/heartbeat，而不影响全局退出令牌。
        let session_shutdown = shutdown.child_token();
        let ctrl = tokio::spawn(control::control_loop(
            stream,
            rx,
            state.clone(),
            self.config.clone(),
            session_shutdown.clone(),
            self.heartbeat_interval,
            self.heartbeat_timeout,
        ));

        // 等待 Login 结果，区分致命 / 可恢复失败（§8.1）。
        match tokio::time::timeout(Duration::from_secs(LOGIN_TIMEOUT), login_orx).await {
            Ok(Ok(resp)) => {
                if resp.ok {
                    // 服务端的 work_conn_tls 偏好可以覆盖客户端配置；若服务端要求
                    // TLS 而客户端没有可用 TLS 配置，必须立即失败并说明原因，
                    // 否则登录/注册都成功、所有工作连接却静默失败。
                    let work_tls = match resolve_work_tls(
                        resp.work_conn_tls,
                        self.config.client.work_conn_tls,
                        state.tls.is_some(),
                    ) {
                        Ok(v) => v,
                        Err(msg) => {
                            tracing::error!(
                                error = msg,
                                "login rejected: unusable TLS configuration"
                            );
                            stop_control(ctrl, &session_shutdown).await;
                            return Ok(ConnectOutcome::Fatal(msg.into()));
                        }
                    };
                    *state.work_conn_tls.lock() = work_tls;
                    self.metrics.set_work_conn_tls(work_tls);
                    *state.work_conn_token.lock() = resp.work_conn_token.clone();
                    // 协商结果回传（DESIGN §6.4）：旧服务端不填该字段时按“沿用自己声明的版本”处理。
                    let negotiated = resp.protocol_version.unwrap_or(PROTOCOL_VERSION);
                    if negotiated != PROTOCOL_VERSION {
                        // 服务端协商到了不同版本：本次连接仍可用，但语义可能不同，值得留痕。
                        tracing::warn!(
                            server_version = negotiated,
                            client_version = PROTOCOL_VERSION,
                            "server negotiated a different protocol version"
                        );
                    } else {
                        tracing::debug!(version = negotiated, "protocol version negotiated");
                    }
                    self.metrics.set_connected(true);
                    connected_at = Some(std::time::Instant::now());
                }
                if !resp.ok {
                    let reason = resp.error.clone().unwrap_or_else(|| "auth failed".into());
                    // 服务端对鉴权失败不回显 error（DESIGN §10.2）；此时按致命错误处理，不重连。
                    // 限速等临时拒绝必须带 error（如 "rate limited"），走可重连路径。
                    if login_rejection_is_fatal(&resp) {
                        tracing::error!(error = %reason, "login fatal; not reconnecting");
                        // 中止控制任务：服务端若未主动断开，await 会永久挂住导致进程无法退出。
                        stop_control(ctrl, &session_shutdown).await;
                        return Ok(ConnectOutcome::Fatal(reason));
                    }
                    tracing::warn!(error = ?resp.error, "login rejected; reconnecting");
                    stop_control(ctrl, &session_shutdown).await;
                    return Ok(ConnectOutcome::Reconnect {
                        connected: false,
                        lived: Duration::ZERO,
                    });
                }
            }
            Ok(Err(_)) => {
                tracing::warn!("login response channel dropped; reconnecting");
                // 必须中止控制任务：仅 return 会把任务分离，导致心跳/写任务与旧连接泄漏，
                // 每次登录失败都累积一个常驻任务。
                stop_control(ctrl, &session_shutdown).await;
                return Ok(ConnectOutcome::Reconnect {
                    connected: false,
                    lived: Duration::ZERO,
                });
            }
            Err(_) => {
                tracing::warn!("login response timeout; reconnecting");
                stop_control(ctrl, &session_shutdown).await;
                return Ok(ConnectOutcome::Reconnect {
                    connected: false,
                    lived: Duration::ZERO,
                });
            }
        }

        tracing::info!(count = self.config.proxies.len(), "registering proxies");
        // 可重试失败的代理（如端口被旧会话占用），交给后台任务退避重试（§6.6）。
        let mut retryable: Vec<ClientProxy> = Vec::new();
        let mut persistent: Vec<ClientProxy> = Vec::new();
        // 按配置文件声明顺序串行注册（DESIGN §6.6），而不是 HashMap 的随机顺序。
        for p in &self.config.proxies {
            match register_one_proxy(&tx, &state, p, shutdown).await {
                RegisterOutcome::Ok => {}
                RegisterOutcome::Retryable => retryable.push(p.clone()),
                RegisterOutcome::Persistent => persistent.push(p.clone()),
                RegisterOutcome::Failed => {}
                RegisterOutcome::ConnLost => {
                    // 注册阶段控制连接已断：统一收尾，避免状态端点继续显示已连接。
                    stop_control(ctrl, &session_shutdown).await;
                    self.metrics.set_connected(false);
                    anyhow::bail!(
                        "control connection closed or congested during proxy registration"
                    )
                }
            }
        }

        // 后台重试：运行时冲突用短退避（端口最长约 40s 会被旧会话心跳超时释放），
        // 配置类失败用长退避（服务端改完配置即可恢复，无需重启客户端）。
        if !retryable.is_empty() || !persistent.is_empty() {
            let tx_retry = tx.clone();
            let state_retry = state.clone();
            let shutdown_retry = shutdown.clone();
            tokio::spawn(async move {
                retry_registration(tx_retry, state_retry, retryable, persistent, shutdown_retry)
                    .await;
            });
        }

        let _ = ctrl.await;
        // 兜底清理未消费的注册响应通道（正常情况下各路径已各自移除）。
        state.resps.lock().clear();
        self.metrics.set_connected(false);
        Ok(ConnectOutcome::Reconnect {
            connected: true,
            lived: connected_at.map(|t| t.elapsed()).unwrap_or_default(),
        })
    }

    /// 生成或复用 run_id 并持久化（§6.6 / §8.3 重连身份）。
    fn load_or_create_run_id(&self) -> String {
        let path = resolve_run_id_path(&self.config.client.run_id_file);
        if let Some(s) = read_run_id_limited(&path) {
            if !s.is_empty() && s.len() <= MAX_RUN_ID_LEN && uuid::Uuid::parse_str(&s).is_ok() {
                set_file_mode_0600(&path);
                return s;
            }
        }
        let rid = uuid::Uuid::new_v4().to_string();
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        // 先写同目录临时文件，设置 0600 后原子 rename，避免写到一半留下
        // 空/截断的 run_id 文件，也避免权限设置在明文写入之后才生效。
        let name = path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("run_id");
        let tmp = path.with_file_name(format!(".{name}.{}.tmp", uuid::Uuid::new_v4()));
        match write_run_id_temp(&tmp, &rid) {
            Ok(()) => {
                set_file_mode_0600(&tmp);
                match replace_run_id_file(&tmp, &path) {
                    Ok(()) => sync_parent_dir(&path),
                    Err(e) => {
                        tracing::warn!(error = %e, "failed to persist run_id");
                        let _ = std::fs::remove_file(&tmp);
                    }
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to write run_id temp file");
                let _ = std::fs::remove_file(&tmp);
            }
        }
        rid
    }
}

/// 只读取有限长度，避免指向超大文件时无界分配。
fn read_run_id_limited(path: &std::path::Path) -> Option<String> {
    use std::io::Read;
    let file = std::fs::File::open(path).ok()?;
    let mut text = String::new();
    file.take((MAX_RUN_ID_LEN + 1) as u64)
        .read_to_string(&mut text)
        .ok()?;
    Some(text.trim().to_string())
}

fn write_run_id_temp(path: &std::path::Path, rid: &str) -> std::io::Result<()> {
    let mut options = std::fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    std::io::Write::write_all(&mut file, rid.as_bytes())?;
    file.sync_all()
}

/// 优先原子替换；Windows 目标已存在时先备份旧文件，失败则恢复。
fn replace_run_id_file(tmp: &std::path::Path, path: &std::path::Path) -> std::io::Result<()> {
    match std::fs::rename(tmp, path) {
        Ok(()) => Ok(()),
        Err(first) => {
            let name = path
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("run_id");
            let backup = path.with_file_name(format!(".{name}.{}.bak", uuid::Uuid::new_v4()));
            let had_old = path.exists();
            if had_old {
                std::fs::rename(path, &backup)?;
            }
            match std::fs::rename(tmp, path) {
                Ok(()) => {
                    if had_old {
                        let _ = std::fs::remove_file(&backup);
                    }
                    Ok(())
                }
                Err(e) => {
                    if had_old {
                        if let Err(restore) = std::fs::rename(&backup, path) {
                            tracing::warn!(
                                first_error = %first,
                                restore_error = %restore,
                                "failed to restore run_id backup"
                            );
                        }
                    }
                    Err(e)
                }
            }
        }
    }
}

#[cfg(unix)]
fn sync_parent_dir(path: &std::path::Path) {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        if let Ok(dir) = std::fs::File::open(parent) {
            if let Err(e) = dir.sync_all() {
                tracing::warn!(error = %e, "failed to sync run_id directory");
            }
        }
    }
}

#[cfg(not(unix))]
fn sync_parent_dir(_path: &std::path::Path) {}

/// 解析配置中的 `run_id_file`。
///
/// 按 DESIGN §9.2，空字符串表示使用默认路径 `~/.rfrp/run_id`。
fn resolve_run_id_path(run_id_file: &Option<String>) -> PathBuf {
    match run_id_file {
        Some(p) if !p.trim().is_empty() => PathBuf::from(p),
        _ => default_run_id_path(),
    }
}

/// 取消并收尾一次控制会话；正常情况下让 `control_loop` 走完 writer/heartbeat 清理，
/// 超时再 abort，避免 `ctrl.abort()` 直接丢弃父任务导致子任务悬挂。
async fn stop_control<T>(ctrl: JoinHandle<T>, session_shutdown: &CancellationToken) {
    let abort = ctrl.abort_handle();
    session_shutdown.cancel();
    if tokio::time::timeout(Duration::from_secs(2), ctrl)
        .await
        .is_err()
    {
        abort.abort();
    }
}

/// 等待下次重连；若期间收到退出信号则返回 `false`。
async fn wait_for_reconnect(backoff: Duration, shutdown: &CancellationToken) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(backoff) => true,
        _ = shutdown.cancelled() => false,
    }
}

/// 连接结束后是否应重置重连退避。
///
/// 仅在本次成功建立过控制会话、且存活时间达到 [`MIN_STABLE_CONNECTION_SECS`]
/// 时重置；否则保持退避增长，避免"建立即断开"场景下的 1s 重连风暴。
fn should_reset_backoff(connected: bool, lived: Duration) -> bool {
    connected && lived >= Duration::from_secs(MIN_STABLE_CONNECTION_SECS)
}

/// 解析本次会话是否使用工作连接 TLS，并确认客户端具备 TLS 配置。
///
/// 服务端偏好优先（DESIGN §6.5）；但“要求 TLS”必须有可用的 `ClientTls`，
/// 否则登录成功后所有工作连接都会失败，属于配置错误，应快速致命退出。
fn resolve_work_tls(
    server_preference: Option<bool>,
    client_default: bool,
    tls_configured: bool,
) -> std::result::Result<bool, &'static str> {
    let use_tls = server_preference.unwrap_or(client_default);
    if use_tls && !tls_configured {
        return Err(
            "server requires work connection TLS, but client TLS is not configured \
             (set tls_server_name/tls_ca or enable client TLS)",
        );
    }
    Ok(use_tls)
}

/// 登录拒绝是否属于不可重试的致命错误。
fn login_rejection_is_fatal(resp: &LoginResp) -> bool {
    let lower = resp.error.as_deref().unwrap_or("").to_ascii_lowercase();
    resp.error.is_none()
        || lower.contains("version mismatch")
        || lower.contains("auth failed")
        // 服务端要求 TLS 而客户端未启用：属于配置错误，重试不会自愈。
        || lower.contains("tls required")
}

/// Unix 下将 run_id 文件权限设为 0600；其他平台静默跳过（§6.6）。
#[cfg(unix)]
fn set_file_mode_0600(path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt;
    if let Err(e) = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)) {
        tracing::warn!(path = %path.display(), error = %e, "failed to set run_id permissions");
    }
}
#[cfg(not(unix))]
fn set_file_mode_0600(_path: &std::path::Path) {}

/// 单个代理注册的结果分类。
enum RegisterOutcome {
    /// 注册成功。
    Ok,
    /// 失败且可重试（运行时冲突，DESIGN §6.6）。
    Retryable,
    /// 失败且不可立即重试，但可能随服务端配置/状态变化恢复：长退避后台重试。
    Persistent,
    /// 失败且重试无意义（纯客户端配置错误），已记录日志。
    Failed,
    /// 控制连接已断，无法继续注册。
    ConnLost,
}

/// 注册单个代理并按其结果分类；成功后按需预热工作连接池。
async fn register_one_proxy(
    tx: &mpsc::Sender<Message>,
    state: &Arc<ClientState>,
    p: &ClientProxy,
    shutdown: &CancellationToken,
) -> RegisterOutcome {
    let (otx, orx) = oneshot::channel();
    state.resps.lock().insert(p.name.clone(), otx);
    let sent = tokio::select! {
        _ = shutdown.cancelled() => false,
        ok = send_with_timeout_reserved(
            tx,
            Message::NewProxy(new_proxy_from_config(p)),
            CONTROL_HEARTBEAT_RESERVE,
        ) => ok,
    };
    if !sent {
        // 控制连接已断或正在退出：清理未消费的响应通道。
        state.resps.lock().remove(&p.name);
        return RegisterOutcome::ConnLost;
    }
    let response = tokio::select! {
        _ = shutdown.cancelled() => {
            state.resps.lock().remove(&p.name);
            return RegisterOutcome::ConnLost;
        }
        result = tokio::time::timeout(Duration::from_secs(NEW_PROXY_TIMEOUT), orx) => result,
    };
    match response {
        Ok(Ok(resp)) if resp.ok => {
            tracing::info!(proxy = %p.name, "proxy registered");
            spawn_preheat(state, p);
            RegisterOutcome::Ok
        }
        Ok(Ok(resp)) => {
            let code = resp.error.as_deref().and_then(ProxyError::from_code);
            state.metrics.inc_proxy_register_failure();
            if code.is_some_and(ProxyError::is_retryable) {
                tracing::warn!(
                    proxy = %p.name, code = ?resp.error,
                    "proxy registration rejected (retryable, will retry in background)"
                );
                RegisterOutcome::Retryable
            } else if code.is_some_and(ProxyError::is_persistently_retryable) {
                // 如服务端尚未放行 allow_ports：重试不会立刻自愈，但服务端改完配置即可恢复，
                // 且不需要重启客户端，因此用长退避继续尝试。
                tracing::warn!(
                    proxy = %p.name, code = ?resp.error,
                    hint = registration_hint(code),
                    "proxy registration rejected (will retry in background with long backoff)"
                );
                RegisterOutcome::Persistent
            } else {
                tracing::error!(
                    proxy = %p.name, code = ?resp.error,
                    hint = registration_hint(code),
                    "proxy registration rejected (not retryable)"
                );
                RegisterOutcome::Failed
            }
        }
        Ok(Err(_)) => {
            state.resps.lock().remove(&p.name);
            tracing::warn!(proxy = %p.name, "registration response channel dropped");
            RegisterOutcome::Failed
        }
        Err(_) => {
            // 超时后服务端可能仍会回包，但已按可重试处理；此处移除条目避免累积，
            // 迟到的响应由控制循环查不到条目而丢弃。
            state.resps.lock().remove(&p.name);
            tracing::warn!(proxy = %p.name, "registration response timeout");
            RegisterOutcome::Retryable
        }
    }
}

/// 针对不可重试的注册失败给出可操作提示（避免用户去翻文档）。
fn registration_hint(code: Option<ProxyError>) -> &'static str {
    match code {
        Some(ProxyError::PortNotAllowed) => "检查服务端 [proxy].allow_ports 是否放行该 remote_port",
        Some(ProxyError::NameExists) => "代理名已被占用：修改 [[proxy]].name，或等待旧会话释放",
        Some(ProxyError::InvalidType) => "代理 type 非法：仅支持 tcp/udp/http/https",
        Some(ProxyError::InvalidField) => {
            "[[proxy]] 字段缺失或格式错误：检查 remote_port / custom_domains 等"
        }
        Some(ProxyError::TooManyProxies) => "单会话代理数超过服务端上限：减少 [[proxy]] 条目",
        Some(ProxyError::TooManyDomains) => {
            "服务端 vhost 域名总数超过上限：减少域名或提高 max_total_domains"
        }
        Some(ProxyError::DomainConflict) => "域名与其他代理冲突（通常可重试）",
        Some(ProxyError::PortOccupied) => "端口被占用（通常可重试）",
        Some(ProxyError::Internal) | None => "查看服务端日志获取详细原因",
    }
}

/// 工作连接池预热（pool_size>0，§8.2）：按池大小预建工作连接，命中后由服务端补充。
fn spawn_preheat(state: &Arc<ClientState>, p: &ClientProxy) {
    // 服务端当前明确拒绝 UDP 的 work_id=0 池化连接（见 rfrps/src/work.rs）；
    // 预热 UDP 只会建立一条注定被关闭的连接，因此直接跳过。
    // DESIGN §8.2/§9.4 若未来决定支持 UDP 池化，再移除此分支。
    if p.r#type == ProxyType::Udp || p.pool_size == 0 {
        return;
    }
    let state = state.clone();
    let p = p.clone();
    // 每个代理一个预热任务，并在全局 Semaphore 下串行建立连接：
    // 避免 128 代理 × pool_size=32 时瞬间创建数千条连接/任务。
    tokio::spawn(async move {
        for _ in 0..p.pool_size.min(POOL_SIZE_MAX) {
            let Ok(permit) = state.preheat_limit.clone().acquire_owned().await else {
                return;
            };
            let req = ReqWorkConn {
                proxy_name: p.name.clone(),
                work_id: WORK_ID_POOL_RESERVED,
            };
            let _ = crate::workconn::handle_work_conn(req, state.clone()).await;
            drop(permit);
        }
    });
}

/// 后台注册重试。
///
/// - `retryable`（端口占用 / 域名冲突）：退避 2s→30s，最多 [`PROXY_REGISTER_RETRY_MAX`] 轮
///   （约 2.5 分钟），覆盖旧会话心跳超时释放端口的窗口；
/// - `persistent`（配置类失败，如 `port not allowed`）：固定
///   [`PROXY_REGISTER_RETRY_PERSISTENT_DELAY_SECS`] 秒、最多
///   [`PROXY_REGISTER_RETRY_PERSISTENT_MAX`] 轮（约 10 分钟），便于服务端改完配置后
///   自动恢复，而不必等客户端重连。
///
/// 两段都感知退出信号；控制连接断开（发送失败）即结束，重连后由新一轮注册接管。
async fn retry_registration(
    tx: mpsc::Sender<Message>,
    state: Arc<ClientState>,
    retryable: Vec<ClientProxy>,
    persistent: Vec<ClientProxy>,
    shutdown: CancellationToken,
) {
    retry_loop(
        &tx,
        &state,
        retryable,
        &shutdown,
        Duration::from_secs(PROXY_REGISTER_RETRY_INITIAL),
        Duration::from_secs(PROXY_REGISTER_RETRY_MAX_DELAY),
        PROXY_REGISTER_RETRY_MAX,
    )
    .await;
    retry_loop(
        &tx,
        &state,
        persistent,
        &shutdown,
        Duration::from_secs(PROXY_REGISTER_RETRY_PERSISTENT_DELAY_SECS),
        Duration::from_secs(PROXY_REGISTER_RETRY_PERSISTENT_DELAY_SECS),
        PROXY_REGISTER_RETRY_PERSISTENT_MAX,
    )
    .await;
}

/// 单个退避重试循环：`delay` 从 `initial` 起翻倍增长、封顶 `max_delay`，最多 `rounds` 轮。
async fn retry_loop(
    tx: &mpsc::Sender<Message>,
    state: &Arc<ClientState>,
    mut pending: Vec<ClientProxy>,
    shutdown: &CancellationToken,
    initial: Duration,
    max_delay: Duration,
    rounds: u32,
) {
    if pending.is_empty() {
        return;
    }
    let mut delay = initial;
    for round in 1..=rounds {
        // 退避期间响应退出信号，避免进程退出时还要等满一个退避周期。
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = shutdown.cancelled() => return,
        }
        delay = (delay * 2).min(max_delay);
        let mut still = Vec::new();
        for p in pending {
            match register_one_proxy(tx, state, &p, shutdown).await {
                RegisterOutcome::Ok => {
                    state.metrics.inc_proxy_register_retry_success();
                    tracing::info!(proxy = %p.name, round, "proxy registered after retry");
                }
                // Retryable/Persistent 留到下一轮；Failed 是纯客户端配置错误，
                // 继续重试只会刷日志和拖延时间，直接放弃。
                RegisterOutcome::Retryable | RegisterOutcome::Persistent => still.push(p),
                RegisterOutcome::Failed => {}
                RegisterOutcome::ConnLost => return,
            }
        }
        pending = still;
        if pending.is_empty() {
            return;
        }
    }
    for p in pending {
        tracing::warn!(
            proxy = %p.name,
            "proxy still unavailable after retries; will retry on next reconnect"
        );
    }
}

/// 由配置条目构造 `NewProxy` 控制消息。
pub fn new_proxy_from_config(p: &ClientProxy) -> NewProxy {
    NewProxy {
        proxy_name: p.name.clone(),
        r#type: p.r#type,
        remote_port: p.remote_port,
        custom_domains: p.custom_domains.clone(),
    }
}

/// 测试用客户端状态构造器：所有测试共用一份，避免 `ClientState` 增字段时逐个构造点修改。
#[cfg(test)]
pub(crate) fn test_state(
    server_addr: std::net::SocketAddr,
    run_id: &str,
    proxies: HashMap<String, ClientProxy>,
    work_conn_tls: bool,
) -> Arc<ClientState> {
    Arc::new(ClientState {
        server_addr,
        run_id: run_id.to_string(),
        proxies,
        resps: Mutex::new(HashMap::new()),
        login_tx: Mutex::new(None),
        tls: None,
        work_conn_tls: Mutex::new(work_conn_tls),
        work_conn_token: Mutex::new(None),
        inflight_work: Mutex::new(HashSet::new()),
        preheat_limit: Arc::new(tokio::sync::Semaphore::new(PREHEAT_MAX_CONCURRENT)),
        metrics: Arc::new(crate::metrics::ClientMetrics::new()),
    })
}

#[cfg(test)]
mod tests;
