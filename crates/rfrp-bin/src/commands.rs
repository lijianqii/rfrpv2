//! 子命令执行：`server` / `client` / `client status`。
//!
//! 统一流程：加载配置 → 应用 CLI 覆盖 → 校验 →（`--check` 打印摘要并退出）→
//! 初始化日志 → 进入长驻循环。

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::summary::{
    init_logging, log_client_summary, log_server_summary, print_client_config_summary,
    print_server_config_summary, LogOverrides,
};
use crate::watch::{watch_config_file, ConfigWatcher};

/// 服务端子命令参数。
#[derive(Clone)]
pub struct ServerArgs {
    pub config: PathBuf,
    pub bind: Option<String>,
    pub token: Option<String>,
    pub tls_enable: Option<bool>,
    pub work_conn_tls: Option<bool>,
    pub grace_secs: Option<u64>,
    pub check: bool,
    pub watch_exit: bool,
}

/// 客户端子命令参数。
#[derive(Clone)]
pub struct ClientArgs {
    pub config: PathBuf,
    pub server: Option<String>,
    pub token: Option<String>,
    pub tls_enable: Option<bool>,
    pub work_conn_tls: Option<bool>,
    pub check: bool,
    pub watch_exit: bool,
}

/// 加载服务端配置并应用 CLI 覆盖；启动和 watch 退出校验共用。
fn load_server_cfg(
    args: &ServerArgs,
) -> std::result::Result<rfrp_common::config::ServerConfig, String> {
    let mut cfg = rfrp_common::config::load_server_config(&args.config).map_err(|e| {
        format!(
            "failed to load server config {}: {e}",
            args.config.display()
        )
    })?;
    rfrps::cli::apply_cli_overrides(
        &mut cfg,
        args.bind.clone(),
        args.token.clone(),
        args.tls_enable,
        args.work_conn_tls,
    )
    .map_err(|e| e.to_string())?;
    cfg.validate()
        .map_err(|e| format!("server config invalid after CLI overrides: {e}"))?;
    // 提前解析证书/私钥：`Server::new` 会先绑端口再加载 TLS，若只在启动时发现
    // 证书损坏，exit 模式已经退出、restart 模式也会多一次无谓的重启/回滚。
    rfrps::validate_tls_config(&cfg)
        .map_err(|e| format!("server TLS config invalid after CLI overrides: {e}"))?;
    Ok(cfg)
}

/// 加载客户端配置并应用 CLI 覆盖；启动和 watch 退出校验共用。
fn load_client_cfg(
    args: &ClientArgs,
) -> std::result::Result<rfrp_common::config::ClientConfig, String> {
    let mut cfg = rfrp_common::config::load_client_config(&args.config).map_err(|e| {
        format!(
            "failed to load client config {}: {e}",
            args.config.display()
        )
    })?;
    rfrpc::cli::apply_cli_overrides(
        &mut cfg,
        args.server.clone(),
        args.token.clone(),
        args.tls_enable,
        args.work_conn_tls,
    )
    .map_err(|e| e.to_string())?;
    cfg.validate()
        .map_err(|e| format!("client config invalid after CLI overrides: {e}"))?;
    rfrpc::validate_tls_config(&cfg)
        .map_err(|e| format!("client TLS config invalid after CLI overrides: {e}"))?;
    Ok(cfg)
}

struct ExitWatch {
    _watcher: ConfigWatcher,
    rx: tokio::sync::mpsc::Receiver<()>,
}

struct ExitWatcher {
    _watcher: ConfigWatcher,
    task: Option<JoinHandle<()>>,
}

impl ExitWatcher {
    async fn stop(mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
    }
}

/// 在主配置加载前打开监听，启动期间的变更也会留在队列里。
fn open_exit_watch(path: &Path) -> std::result::Result<ExitWatch, String> {
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    let watcher = watch_config_file(path, tx).map_err(|e| format!("cannot watch config: {e}"))?;
    Ok(ExitWatch {
        _watcher: watcher,
        rx,
    })
}

/// 从 `host:port` / `[v6]:port` 形式的监听地址提取端口。
fn endpoint_port(addr: &str) -> Option<u16> {
    addr.rsplit_once(':')?.1.parse().ok()
}

struct ListenerTarget {
    label: &'static str,
    display: String,
    host: String,
    /// `None` 表示 `host` 本身已是完整 socket 地址（dashboard）。
    port: Option<u16>,
    port_key: u16,
}

/// 对 exit 模式的新配置做监听端口预检：只检查旧进程当前没有占用的新端口。
///
/// 旧进程持有的端口会在退出时释放，不能用来 bind，否则“只改控制端口”也会对
/// 未变化的 vhost 端口误报冲突；旧配置未出现的新端口才需要提前验证。
async fn preflight_server_listeners(
    old: &rfrp_common::config::ServerConfig,
    new: &rfrp_common::config::ServerConfig,
) -> std::result::Result<(), String> {
    use std::collections::BTreeSet;

    use tokio::net::TcpListener;

    let mut old_ports = BTreeSet::new();
    old_ports.insert(old.server.bind_port);
    old_ports.extend(old.proxy.vhost_http_port);
    old_ports.extend(old.proxy.vhost_https_port);
    if let Some(d) = &old.dashboard {
        old_ports.extend(endpoint_port(&d.addr));
    }

    let mut targets = vec![ListenerTarget {
        label: "control listener",
        display: format!("{}:{}", new.server.bind_addr, new.server.bind_port),
        host: new.server.bind_addr.clone(),
        port: Some(new.server.bind_port),
        port_key: new.server.bind_port,
    }];
    for (label, port) in [
        ("http vhost listener", new.proxy.vhost_http_port),
        ("https vhost listener", new.proxy.vhost_https_port),
    ] {
        if let Some(port) = port {
            targets.push(ListenerTarget {
                label,
                display: format!("{}:{port}", new.server.bind_addr),
                host: new.server.bind_addr.clone(),
                port: Some(port),
                port_key: port,
            });
        }
    }
    if let Some(d) = &new.dashboard {
        targets.push(ListenerTarget {
            label: "dashboard listener",
            display: d.addr.clone(),
            host: d.addr.clone(),
            port: None,
            port_key: endpoint_port(&d.addr).unwrap_or(0),
        });
    }

    if old.server.bind_addr != new.server.bind_addr {
        // 端口可能没变，不能靠 old_ports 跳过；用端口 0 验证新地址本地可绑定。
        TcpListener::bind((new.server.bind_addr.as_str(), 0))
            .await
            .map_err(|e| format!("preflight bind address {}: {e}", new.server.bind_addr))?;
    }
    let old_dashboard = old.dashboard.as_ref().map(|d| d.addr.as_str());
    let new_dashboard = new.dashboard.as_ref().map(|d| d.addr.as_str());
    if old_dashboard != new_dashboard {
        if let Some(d) = &new.dashboard {
            if let Ok(addr) = d.addr.parse::<std::net::SocketAddr>() {
                TcpListener::bind((addr.ip(), 0))
                    .await
                    .map_err(|e| format!("preflight bind dashboard address {}: {e}", addr.ip()))?;
            }
        }
    }

    for target in targets {
        if target.port_key != 0 && old_ports.contains(&target.port_key) {
            continue;
        }
        let result = match target.port {
            Some(port) => TcpListener::bind((target.host.as_str(), port)).await,
            None => TcpListener::bind(target.host.as_str()).await,
        };
        result.map_err(|e| format!("preflight bind {} {}: {e}", target.label, target.display))?;
    }
    Ok(())
}

/// `--watch-config=exit`：配置变化且校验通过后触发优雅退出。
fn spawn_server_exit_watcher(
    args: ServerArgs,
    baseline: rfrp_common::config::ServerConfig,
    watch: ExitWatch,
    shutdown: CancellationToken,
    requested: Arc<AtomicBool>,
) -> ExitWatcher {
    let ExitWatch { _watcher, mut rx } = watch;
    let task = tokio::spawn(async move {
        loop {
            let event = tokio::select! {
                _ = shutdown.cancelled() => break,
                event = rx.recv() => event,
            };
            let Some(()) = event else {
                break;
            };
            tokio::time::sleep(Duration::from_millis(300)).await;
            while rx.try_recv().is_ok() {}
            match load_server_cfg(&args) {
                Ok(new_cfg) => match preflight_server_listeners(&baseline, &new_cfg).await {
                    Ok(()) => {
                        tracing::info!("valid config change; exiting for restart");
                        requested.store(true, Ordering::Relaxed);
                        shutdown.cancel();
                        break;
                    }
                    Err(e) => tracing::warn!(
                        error = %e,
                        "new config listener preflight failed; keeping current process"
                    ),
                },
                Err(e) => {
                    tracing::warn!(error = %e, "config change invalid; keeping current process")
                }
            }
        }
    });
    ExitWatcher {
        _watcher,
        task: Some(task),
    }
}

fn spawn_client_exit_watcher(
    args: ClientArgs,
    watch: ExitWatch,
    shutdown: CancellationToken,
    requested: Arc<AtomicBool>,
) -> ExitWatcher {
    let ExitWatch { _watcher, mut rx } = watch;
    let task = tokio::spawn(async move {
        loop {
            let event = tokio::select! {
                _ = shutdown.cancelled() => break,
                event = rx.recv() => event,
            };
            let Some(()) = event else {
                break;
            };
            tokio::time::sleep(Duration::from_millis(300)).await;
            while rx.try_recv().is_ok() {}
            match load_client_cfg(&args) {
                Ok(_) => {
                    tracing::info!("valid config change; exiting for restart");
                    requested.store(true, Ordering::Relaxed);
                    shutdown.cancel();
                    break;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "config change invalid; keeping current process")
                }
            }
        }
    });
    ExitWatcher {
        _watcher,
        task: Some(task),
    }
}

/// supervisor 子进程从 stdin 接收 shutdown；EOF（父进程退出）也触发清理。
fn spawn_supervised_stdin(shutdown: CancellationToken) {
    if std::env::var_os("RFRP_SUPERVISOR_PIPE").is_none() {
        return;
    }
    tokio::spawn(async move {
        let mut lines = BufReader::new(tokio::io::stdin()).lines();
        loop {
            match lines.next_line().await {
                Ok(Some(line)) if line.trim() == "shutdown" => break,
                Ok(Some(_)) => {}
                Ok(None) | Err(_) => break,
            }
        }
        shutdown.cancel();
    });
}

/// 服务端：加载配置 → CLI 覆盖 → 校验 → 日志 → accept 循环直到退出。
pub async fn run_server(args: ServerArgs, log: LogOverrides) -> ExitCode {
    let exit_watch = if args.watch_exit && !args.check {
        match open_exit_watch(&args.config) {
            Ok(watch) => Some(watch),
            Err(e) => {
                eprintln!("{e}");
                return ExitCode::FAILURE;
            }
        }
    } else {
        None
    };
    let cfg = match load_server_cfg(&args) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    let effective_grace = match args.grace_secs {
        Some(g) if g > rfrp_common::constants::GRACEFUL_SHUTDOWN_TIMEOUT_MAX_SECS => {
            eprintln!(
                "invalid --grace-secs {g}: max is {}",
                rfrp_common::constants::GRACEFUL_SHUTDOWN_TIMEOUT_MAX_SECS
            );
            return ExitCode::FAILURE;
        }
        Some(g) => Duration::from_secs(g),
        None => cfg.server.grace(),
    };
    if args.check {
        print_server_config_summary(&args.config, &cfg, effective_grace);
        return ExitCode::SUCCESS;
    }
    init_logging(&cfg.log, &log);
    log_server_summary(&cfg, effective_grace);
    rfrp_common::util::tcp::init_keepalive(rfrp_common::util::tcp::KeepaliveConfig::from_secs(
        cfg.server.tcp_keepalive_secs,
    ));

    let cfg_snapshot = cfg.clone();
    let server = match rfrps::Server::new(cfg).await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "failed to start server");
            return ExitCode::FAILURE;
        }
    };
    // 优雅退出宽限期可由 CLI 覆盖（配置文件为 [server].grace_secs，见 §14.4）。
    let server = server.with_grace(effective_grace);
    let restart_requested = Arc::new(AtomicBool::new(false));
    let exit_watcher = exit_watch.map(|watch| {
        spawn_server_exit_watcher(
            args.clone(),
            cfg_snapshot,
            watch,
            server.shutdown_token(),
            restart_requested.clone(),
        )
    });
    spawn_supervised_stdin(server.shutdown_token());

    tracing::info!(addr = %server.local_addr(), "rfrps listening");
    let result = server.run().await;
    if let Some(watcher) = exit_watcher {
        watcher.stop().await;
    }
    match result {
        Ok(()) if restart_requested.load(Ordering::Relaxed) => ExitCode::from(75),
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!(error = %e, "rfrps exited with error");
            ExitCode::FAILURE
        }
    }
}

/// 客户端：加载配置 → CLI 覆盖 → 校验 → 日志 → 长驻运行（重连直到退出）。
pub async fn run_client(args: ClientArgs, log: LogOverrides) -> ExitCode {
    let exit_watch = if args.watch_exit && !args.check {
        match open_exit_watch(&args.config) {
            Ok(watch) => Some(watch),
            Err(e) => {
                eprintln!("{e}");
                return ExitCode::FAILURE;
            }
        }
    } else {
        None
    };
    let cfg = match load_client_cfg(&args) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    if args.check {
        print_client_config_summary(&args.config, &cfg);
        return ExitCode::SUCCESS;
    }
    init_logging(&cfg.log, &log);
    log_client_summary(&cfg);
    rfrp_common::util::tcp::init_keepalive(rfrp_common::util::tcp::KeepaliveConfig::from_secs(
        cfg.client.tcp_keepalive_secs,
    ));

    let client = match rfrpc::Client::new(cfg) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "failed to create client");
            return ExitCode::FAILURE;
        }
    };
    let restart_requested = Arc::new(AtomicBool::new(false));
    let exit_watcher = exit_watch.map(|watch| {
        spawn_client_exit_watcher(
            args.clone(),
            watch,
            client.shutdown_token(),
            restart_requested.clone(),
        )
    });
    spawn_supervised_stdin(client.shutdown_token());

    let result = client.run().await;
    if let Some(watcher) = exit_watcher {
        watcher.stop().await;
    }
    match result {
        Ok(()) if restart_requested.load(Ordering::Relaxed) => ExitCode::from(75),
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!(error = %e, "rfrpc exited with error");
            ExitCode::FAILURE
        }
    }
}

/// `client status` 子命令：查询本地客户端状态端点并打印 `/api/status` 的 JSON。
pub async fn run_status(config: PathBuf, addr: Option<String>) -> ExitCode {
    let cfg = match rfrp_common::config::load_client_config(&config) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("failed to load client config {}: {e}", config.display());
            return ExitCode::FAILURE;
        }
    };
    let target = addr.or_else(|| cfg.client.status_addr.clone());
    let Some(target) = target else {
        eprintln!(
            "未启用状态端点：请在配置里设置 [client].status_addr（如 \"127.0.0.1:7400\"），\
             或用 --addr 指定"
        );
        return ExitCode::FAILURE;
    };
    match fetch_status(&target).await {
        Ok(body) => {
            print!("{body}");
            if !body.ends_with('\n') {
                println!();
            }
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("failed to query status endpoint {target}: {e}");
            ExitCode::FAILURE
        }
    }
}

/// 极简 HTTP GET：连接状态端点、读完整响应、校验 200 后返回 body。
///
/// 整体加超时：端点无响应时 `client status` 不应永久挂起。
async fn fetch_status(addr: &str) -> std::io::Result<String> {
    match tokio::time::timeout(Duration::from_secs(5), fetch_status_inner(addr)).await {
        Ok(result) => result,
        Err(_) => Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "status request timed out",
        )),
    }
}

async fn fetch_status_inner(addr: &str) -> std::io::Result<String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut stream = tokio::net::TcpStream::connect(addr).await?;
    let req = format!("GET /api/status HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).await?;
    let mut resp = Vec::new();
    stream.read_to_end(&mut resp).await?;

    let text = String::from_utf8_lossy(&resp).into_owned();
    let (head, body) = text.split_once("\r\n\r\n").unwrap_or((text.as_str(), ""));
    let status = head.lines().next().unwrap_or("");
    if !status.contains(" 200 ") {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("unexpected response: {status}"),
        ));
    }
    Ok(body.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rfrp_common::config::{ProxySection, ServerConfig, ServerSection};
    use std::net::TcpListener as StdTcpListener;

    fn free_port() -> u16 {
        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    }

    #[tokio::test]
    async fn exit_preflight_ignores_ports_still_held_by_old_process() {
        // 旧进程仍占着未变化的 vhost 端口；只改控制端口时不能因此误报冲突。
        let vhost = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let vhost_port = vhost.local_addr().unwrap().port();
        let old = ServerConfig {
            server: ServerSection {
                bind_addr: "127.0.0.1".into(),
                bind_port: free_port(),
                ..Default::default()
            },
            proxy: ProxySection {
                vhost_http_port: Some(vhost_port),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut new = old.clone();
        new.server.bind_port = free_port();
        preflight_server_listeners(&old, &new)
            .await
            .expect("unchanged listener ports must not be preflighted");
    }

    #[tokio::test]
    async fn exit_preflight_rejects_newly_added_occupied_port() {
        let blocker = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let blocked = blocker.local_addr().unwrap().port();
        let old = ServerConfig {
            server: ServerSection {
                bind_addr: "127.0.0.1".into(),
                bind_port: free_port(),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut new = old.clone();
        new.proxy.vhost_http_port = Some(blocked);
        let err = preflight_server_listeners(&old, &new)
            .await
            .expect_err("new occupied listener port must fail preflight");
        assert!(
            err.contains("http vhost listener"),
            "unexpected error: {err}"
        );
    }
}
