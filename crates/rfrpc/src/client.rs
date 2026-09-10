//! 客户端主控：连接服务端、登录、按配置串行注册代理，长驻控制循环；
//! 断开后按指数退避重连，并复用 run_id 恢复代理（DESIGN §8.1 / §8.3 / §6.6）。

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::Result as AnyResult;
use rfrp_common::config::{ClientConfig, ClientProxy};
use rfrp_common::constants::{
    CONNECT_TIMEOUT, HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT, LOGIN_TIMEOUT, MAX_RUN_ID_LEN,
    NEW_PROXY_TIMEOUT, RECONNECT_BACKOFF_INITIAL, RECONNECT_BACKOFF_MAX, WORK_ID_POOL_RESERVED,
};
use rfrp_common::crypto::ClientTls;
use rfrp_common::error::Result as RfrpResult;
use rfrp_common::protocol::msg::*;
use rfrp_common::util::control::send_with_timeout;
use rfrp_common::util::platform::default_run_id_path;
use rfrp_common::util::signal::spawn_signal_watcher;
use rfrp_common::util::stream::BoxedStream;
use rfrp_common::util::tcp::configure_tcp_stream;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;

use crate::control;

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
}

/// 单次连接的结果：决定上层是否重连（DESIGN §8.1 / §8.3）。
enum ConnectOutcome {
    /// 需要重连。`connected` 表示本次是否已经成功建立过控制会话；
    /// 若为 `true`，退避计时器应重置，避免用历史大退避惩罚一次健康的长连接。
    Reconnect { connected: bool },
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
    /// 优雅退出令牌：信号或外部触发后停止重连并退出（§14.4）。
    shutdown: CancellationToken,
}

impl Client {
    pub fn new(config: ClientConfig) -> RfrpResult<Self> {
        // 只要配置了 tls_server_name 就构建 TLS 客户端，以便服务端在 LoginResp 中要求
        // 工作连接升级到 TLS 时（DESIGN §6.5 决策表）可以立即使用。
        let tls = if config.client.tls_enable
            || config.client.work_conn_tls
            || config.client.tls_server_name.is_some()
        {
            Some(ClientTls::new(&config.client)?)
        } else {
            None
        };
        Ok(Self {
            config,
            tls,
            heartbeat_interval: Duration::from_secs(HEARTBEAT_INTERVAL),
            heartbeat_timeout: Duration::from_secs(HEARTBEAT_TIMEOUT),
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
                Ok(ConnectOutcome::Reconnect { connected }) => {
                    if shutdown.is_cancelled() {
                        break;
                    }
                    if connected {
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
        let server_addr = self.config.client.server_socket_addr()?;
        let tls = self.tls.clone();
        // 带超时建连：防火墙静默丢包时 connect 可能阻塞约 2 分钟（Linux），
        // 期间无法感知失败、也无法进入退避重试。
        let stream = match tokio::time::timeout(
            Duration::from_secs(CONNECT_TIMEOUT),
            TcpStream::connect(server_addr),
        )
        .await
        {
            Ok(Ok(s)) => {
                if let Err(e) = configure_tcp_stream(&s) {
                    tracing::warn!(error = %e, "failed to configure control TCP stream");
                }
                s
            }
            Ok(Err(e)) => {
                tracing::warn!(error = %e, "connect failed");
                return Err(anyhow::anyhow!(e));
            }
            Err(_) => {
                tracing::warn!(server = %server_addr, "connect timeout");
                return Err(anyhow::anyhow!("connect timeout"));
            }
        };
        tracing::info!(server = %server_addr, "connected to server");

        // 控制链路 TLS（仅 tls_enable=true 时启用；工作连接 TLS 由各工作连接按需决定）。
        let stream: BoxedStream = if self.config.client.tls_enable {
            let tls = tls.as_ref().expect("tls built above");
            Box::new(tls.connect(stream).await?)
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
        });
        let (tx, rx) = mpsc::channel::<Message>(64);
        let (login_otx, login_orx) = oneshot::channel();
        state.login_tx.lock().unwrap().replace(login_otx);

        let ctrl = tokio::spawn(control::control_loop(
            stream,
            rx,
            state.clone(),
            self.config.clone(),
            shutdown.clone(),
            self.heartbeat_interval,
            self.heartbeat_timeout,
        ));

        // 等待 Login 结果，区分致命 / 可恢复失败（§8.1）。
        match tokio::time::timeout(Duration::from_secs(LOGIN_TIMEOUT), login_orx).await {
            Ok(Ok(resp)) => {
                if resp.ok {
                    *state.work_conn_tls.lock().unwrap() = resp
                        .work_conn_tls
                        .unwrap_or(self.config.client.work_conn_tls);
                }
                if !resp.ok {
                    let reason = resp.error.clone().unwrap_or_else(|| "auth failed".into());
                    let lower = reason.to_lowercase();
                    // 服务端对鉴权失败不回显 error（DESIGN §10.2）；此时按致命错误处理，不重连。
                    if resp.error.is_none()
                        || lower.contains("version mismatch")
                        || lower.contains("auth failed")
                    {
                        tracing::error!(error = %reason, "login fatal; not reconnecting");
                        let _ = ctrl.await;
                        return Ok(ConnectOutcome::Fatal(reason));
                    }
                    tracing::warn!(error = ?resp.error, "login rejected; reconnecting");
                    let _ = ctrl.await;
                    return Ok(ConnectOutcome::Reconnect { connected: false });
                }
            }
            Ok(Err(_)) => {
                tracing::warn!("login response channel dropped; reconnecting");
                return Ok(ConnectOutcome::Reconnect { connected: false });
            }
            Err(_) => {
                tracing::warn!("login response timeout; reconnecting");
                return Ok(ConnectOutcome::Reconnect { connected: false });
            }
        }

        tracing::info!(count = state.proxies.len(), "registering proxies");
        let mut preheat = Vec::new();
        for p in state.proxies.values() {
            let (otx, orx) = oneshot::channel();
            state.resps.lock().unwrap().insert(p.name.clone(), otx);
            let np = new_proxy_from_config(p);
            if !send_with_timeout(&tx, Message::NewProxy(np)).await {
                anyhow::bail!("control connection closed or congested during proxy registration");
            }
            match tokio::time::timeout(Duration::from_secs(NEW_PROXY_TIMEOUT), orx).await {
                Ok(Ok(resp)) => {
                    if resp.ok {
                        tracing::info!(proxy = %p.name, "proxy registered");
                        if p.pool_size > 0 {
                            preheat.push(p.clone());
                        }
                    } else {
                        tracing::warn!(proxy = %p.name, error = ?resp.error, "proxy registration rejected");
                    }
                }
                Ok(Err(_)) => {
                    tracing::warn!(proxy = %p.name, "registration response channel dropped")
                }
                Err(_) => tracing::warn!(proxy = %p.name, "registration response timeout"),
            }
        }

        // 工作连接池预热（pool_size>0，§8.2）：按池大小预建工作连接，命中后由服务端补充。
        for p in &preheat {
            for _ in 0..p.pool_size {
                let req = ReqWorkConn {
                    proxy_name: p.name.clone(),
                    work_id: WORK_ID_POOL_RESERVED,
                };
                let st = state.clone();
                tokio::spawn(async move {
                    let _ = crate::workconn::handle_work_conn(req, st).await;
                });
            }
        }

        let _ = ctrl.await;
        Ok(ConnectOutcome::Reconnect { connected: true })
    }

    /// 生成或复用 run_id 并持久化（§6.6 / §8.3 重连身份）。
    fn load_or_create_run_id(&self) -> String {
        let path = resolve_run_id_path(&self.config.client.run_id_file);
        if let Ok(s) = std::fs::read_to_string(&path) {
            let s = s.trim().to_string();
            if !s.is_empty() && s.len() <= MAX_RUN_ID_LEN && uuid::Uuid::parse_str(&s).is_ok() {
                return s;
            }
        }
        let rid = uuid::Uuid::new_v4().to_string();
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        match std::fs::File::create(&path) {
            Ok(mut f) => {
                let _ = std::io::Write::write_all(&mut f, rid.as_bytes());
                set_file_mode_0600(&path);
            }
            Err(e) => tracing::warn!(error = %e, "failed to persist run_id"),
        }
        rid
    }
}

/// 解析配置中的 `run_id_file`。
///
/// 按 DESIGN §9.2，空字符串表示使用默认路径 `~/.rfrp/run_id`。
fn resolve_run_id_path(run_id_file: &Option<String>) -> PathBuf {
    match run_id_file {
        Some(p) if !p.trim().is_empty() => PathBuf::from(p),
        _ => default_run_id_path(),
    }
}

/// 等待下次重连；若期间收到退出信号则返回 `false`。
async fn wait_for_reconnect(backoff: Duration, shutdown: &CancellationToken) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(backoff) => true,
        _ = shutdown.cancelled() => false,
    }
}

/// Unix 下将 run_id 文件权限设为 0600；其他平台静默跳过（§6.6）。
#[cfg(unix)]
fn set_file_mode_0600(path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt;
    let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
}
#[cfg(not(unix))]
fn set_file_mode_0600(_path: &std::path::Path) {}

/// 由配置条目构造 `NewProxy` 控制消息。
pub fn new_proxy_from_config(p: &ClientProxy) -> NewProxy {
    NewProxy {
        proxy_name: p.name.clone(),
        r#type: p.r#type,
        remote_port: p.remote_port,
        custom_domains: p.custom_domains.clone(),
    }
}

#[cfg(test)]
mod tests;
