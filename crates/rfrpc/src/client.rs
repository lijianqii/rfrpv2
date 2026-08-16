//! 客户端主控：连接服务端、登录、按配置串行注册代理，长驻控制循环；
//! 断开后按指数退避重连，并复用 run_id 恢复代理（DESIGN §8.1 / §8.3 / §6.6）。

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::Result as AnyResult;
use rfrp_common::config::{ClientConfig, ClientProxy};
use rfrp_common::constants::{
    MAX_RUN_ID_LEN, RECONNECT_BACKOFF_INITIAL, RECONNECT_BACKOFF_MAX, WORK_ID_POOL_RESERVED,
};
use rfrp_common::error::Result as RfrpResult;
use rfrp_common::protocol::msg::*;
use rfrp_common::util::platform::default_run_id_path;
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
    pub proxies: Vec<ClientProxy>,
    /// proxy_name → NewProxyResp 的一次性回传通道（注册时 await）。
    pub resps: Mutex<HashMap<String, oneshot::Sender<NewProxyResp>>>,
    /// Login 结果一次性回传通道（连接时 await，用于区分致命/可恢复失败）。
    pub login_tx: Mutex<Option<oneshot::Sender<LoginResp>>>,
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
    /// 优雅退出令牌：信号或外部触发后停止重连并退出（§14.4）。
    shutdown: CancellationToken,
}

impl Client {
    pub fn new(config: ClientConfig) -> RfrpResult<Self> {
        Ok(Self {
            config,
            shutdown: CancellationToken::new(),
        })
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
        let sig = spawn_client_signal_watcher(shutdown.clone());
        let mut backoff = Duration::from_secs(RECONNECT_BACKOFF_INITIAL);
        loop {
            if shutdown.is_cancelled() {
                tracing::info!("shutdown requested, exiting client");
                break;
            }
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
                        backoff = Duration::from_secs(RECONNECT_BACKOFF_INITIAL);
                    }
                    tracing::info!(
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
                    tracing::warn!(error = %e, "transient error, reconnecting");
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
        let stream = match TcpStream::connect(server_addr).await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "connect failed");
                return Err(anyhow::anyhow!(e));
            }
        };
        tracing::info!(server = %server_addr, "connected to server");

        let state = Arc::new(ClientState {
            server_addr,
            run_id: run_id.to_string(),
            proxies: self.config.proxies.clone(),
            resps: Mutex::new(HashMap::new()),
            login_tx: Mutex::new(None),
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
        ));

        // 等待 Login 结果，区分致命 / 可恢复失败（§8.1）。
        match tokio::time::timeout(Duration::from_secs(10), login_orx).await {
            Ok(Ok(resp)) => {
                if !resp.ok {
                    let reason = resp
                        .error
                        .clone()
                        .unwrap_or_else(|| "login rejected".into());
                    let lower = reason.to_lowercase();
                    if lower.contains("version mismatch") || lower.contains("auth failed") {
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
        for p in &state.proxies {
            let (otx, orx) = oneshot::channel();
            state.resps.lock().unwrap().insert(p.name.clone(), otx);
            let np = new_proxy_from_config(p);
            if tx.send(Message::NewProxy(np)).await.is_err() {
                anyhow::bail!("control connection closed during proxy registration");
            }
            match tokio::time::timeout(Duration::from_secs(10), orx).await {
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
                let cfg = self.config.clone();
                tokio::spawn(async move {
                    let _ = crate::workconn::handle_work_conn(req, st, cfg).await;
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
            if !s.is_empty() && s.len() <= MAX_RUN_ID_LEN {
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

/// 监听 OS 终止信号（Ctrl+C / SIGTERM），触发统一的退出令牌。
fn spawn_client_signal_watcher(shutdown: CancellationToken) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if shutdown.is_cancelled() {
            return;
        }
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigint = match signal(SignalKind::interrupt()) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!("install SIGINT handler failed: {e}");
                    let _ = tokio::signal::ctrl_c().await;
                    shutdown.cancel();
                    return;
                }
            };
            let mut sigterm = match signal(SignalKind::terminate()) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!("install SIGTERM handler failed: {e}");
                    let _ = tokio::signal::ctrl_c().await;
                    shutdown.cancel();
                    return;
                }
            };
            tracing::info!("OS signal handler installed (SIGINT/SIGTERM)");
            tokio::select! {
                _ = sigint.recv() => {}
                _ = sigterm.recv() => {}
            }
        }
        #[cfg(not(unix))]
        {
            tracing::info!("OS signal handler installed (Ctrl-C)");
            let _ = tokio::signal::ctrl_c().await;
        }
        shutdown.cancel();
    })
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

#[cfg(test)]
mod tests {
    use super::*;
    use rfrp_common::config::{ClientConfig, ClientProxy, ClientSection};
    use rfrp_common::constants::MAX_RUN_ID_LEN;
    use rfrp_common::protocol::msg::{NewProxy, ProxyType};

    #[test]
    fn new_proxy_from_config_maps_fields() {
        let p = ClientProxy {
            name: "web".into(),
            r#type: ProxyType::Tcp,
            local_ip: "10.0.0.2".into(),
            local_port: 22,
            remote_port: Some(8022),
            custom_domains: Some(vec!["a.example.com".into()]),
            pool_size: 2,
        };
        let np: NewProxy = new_proxy_from_config(&p);
        assert_eq!(np.proxy_name, "web");
        assert_eq!(np.r#type, ProxyType::Tcp);
        assert_eq!(np.remote_port, Some(8022));
        assert_eq!(np.custom_domains, Some(vec!["a.example.com".into()]));
    }

    #[test]
    fn run_id_persisted_and_reused() {
        let dir = std::env::temp_dir().join(format!("rfrp-test-{}", uuid::Uuid::new_v4()));
        let path = dir.join("run_id");
        let r1 = Client::new(cfg_for(&path)).unwrap().load_or_create_run_id();
        let r2 = Client::new(cfg_for(&path)).unwrap().load_or_create_run_id();
        assert!(!r1.is_empty());
        assert_eq!(r1, r2, "run_id must be reused across starts");
        let _ = std::fs::remove_dir_all(&dir);
    }

    // 构造仅覆盖 run_id_file 的 ClientConfig，避免依赖完整字段。
    fn cfg_for(path: &std::path::Path) -> ClientConfig {
        ClientConfig {
            client: ClientSection {
                run_id_file: Some(path.to_string_lossy().to_string()),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn run_id_empty_file_regenerates() {
        // 文件存在但内容为空/空白：应重新生成非空 run_id（§6.6）。
        let dir = std::env::temp_dir().join(format!("rfrp-test-{}", uuid::Uuid::new_v4()));
        let path = dir.join("run_id");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(&path, "   \n").unwrap();
        let rid = Client::new(cfg_for(&path)).unwrap().load_or_create_run_id();
        assert!(!rid.trim().is_empty());
        assert_ne!(rid, "   \n");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn run_id_too_long_regenerates() {
        // 文件内容超过 MAX_RUN_ID_LEN：应重新生成合规 run_id（§6.6）。
        let dir = std::env::temp_dir().join(format!("rfrp-test-{}", uuid::Uuid::new_v4()));
        let path = dir.join("run_id");
        std::fs::create_dir_all(&dir).unwrap();
        let long = "x".repeat(MAX_RUN_ID_LEN + 1);
        std::fs::write(&path, &long).unwrap();
        let rid = Client::new(cfg_for(&path)).unwrap().load_or_create_run_id();
        assert_ne!(rid, long);
        assert!(rid.len() <= MAX_RUN_ID_LEN);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn run_id_empty_string_uses_default_path() {
        // DESIGN §9.2：`run_id_file = ""` 应等价于未配置，使用默认 `~/.rfrp/run_id`。
        let none = resolve_run_id_path(&None);
        let empty = resolve_run_id_path(&Some(String::new()));
        let blank = resolve_run_id_path(&Some("   ".to_string()));
        assert_eq!(empty, none);
        assert_eq!(blank, none);

        let custom = resolve_run_id_path(&Some("/tmp/custom-run-id".to_string()));
        assert_eq!(custom, PathBuf::from("/tmp/custom-run-id"));
    }

    #[tokio::test]
    async fn reconnect_delay_is_interruptible_by_shutdown() {
        // 若退出信号落在退避 sleep 期间，wait_for_reconnect 应立即返回 false，
        // 避免客户端在 30s 退避期间无法及时退出（§8.3 / §14.4）。
        let shutdown = CancellationToken::new();
        let shutdown_for_task = shutdown.clone();
        let start = tokio::time::Instant::now();
        let task = tokio::spawn(async move {
            wait_for_reconnect(
                Duration::from_secs(RECONNECT_BACKOFF_MAX),
                &shutdown_for_task,
            )
            .await
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        shutdown.cancel();

        let ok = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("wait_for_reconnect must return promptly after shutdown")
            .unwrap();
        assert!(!ok, "shutdown during backoff should abort the wait");
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "should not wait out the full backoff after shutdown"
        );
    }
}
