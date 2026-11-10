//! 子命令执行：`server` / `client` / `client status`。
//!
//! 统一流程：加载配置 → 应用 CLI 覆盖 → 校验 →（`--check` 打印摘要并退出）→
//! 初始化日志 → 进入长驻循环。

use std::path::PathBuf;
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
    Ok(cfg)
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

/// `--watch-config=exit`：配置变化且校验通过后触发优雅退出。
fn start_server_exit_watcher(
    args: ServerArgs,
    shutdown: CancellationToken,
    requested: Arc<AtomicBool>,
) -> std::result::Result<ExitWatcher, String> {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let watcher =
        watch_config_file(&args.config, tx).map_err(|e| format!("cannot watch config: {e}"))?;
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
    Ok(ExitWatcher {
        _watcher: watcher,
        task: Some(task),
    })
}

fn start_client_exit_watcher(
    args: ClientArgs,
    shutdown: CancellationToken,
    requested: Arc<AtomicBool>,
) -> std::result::Result<ExitWatcher, String> {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let watcher =
        watch_config_file(&args.config, tx).map_err(|e| format!("cannot watch config: {e}"))?;
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
    Ok(ExitWatcher {
        _watcher: watcher,
        task: Some(task),
    })
}

/// supervisor 子进程从 stdin 接收 shutdown；EOF（父进程退出）也触发清理。
fn spawn_supervised_stdin(shutdown: CancellationToken) {
    if std::env::var_os("RFRP_SUPERVISOR_PIPE").is_none() {
        return;
    }
    tokio::spawn(async move {
        tracing::info!("supervisor stdin pipe reader started");
        let mut lines = BufReader::new(tokio::io::stdin()).lines();
        loop {
            match lines.next_line().await {
                Ok(Some(line)) if line.trim() == "shutdown" => {
                    tracing::info!("supervisor stdin shutdown command received");
                    break;
                }
                Ok(Some(other)) => {
                    tracing::warn!(line = %other, "ignoring unknown supervisor stdin command");
                }
                Ok(None) => {
                    tracing::warn!("supervisor stdin EOF; shutting down child");
                    break;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "supervisor stdin read error; shutting down child");
                    break;
                }
            }
        }
        shutdown.cancel();
    });
}

/// 服务端：加载配置 → CLI 覆盖 → 校验 → 日志 → accept 循环直到退出。
pub async fn run_server(args: ServerArgs, log: LogOverrides) -> ExitCode {
    let cfg = match load_server_cfg(&args) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    if args.check {
        print_server_config_summary(&args.config, &cfg);
        return ExitCode::SUCCESS;
    }
    init_logging(&cfg.log, &log);
    log_server_summary(&cfg);
    rfrp_common::util::tcp::init_keepalive(rfrp_common::util::tcp::KeepaliveConfig::from_secs(
        cfg.server.tcp_keepalive_secs,
    ));

    let server = match rfrps::Server::new(cfg).await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "failed to start server");
            return ExitCode::FAILURE;
        }
    };
    // 优雅退出宽限期可由 CLI 覆盖（配置文件为 [server].grace_secs，见 §14.4）。
    let server = match args.grace_secs {
        Some(g) => {
            if g > rfrp_common::constants::GRACEFUL_SHUTDOWN_TIMEOUT_MAX_SECS {
                eprintln!(
                    "invalid --grace-secs {g}: max is {}",
                    rfrp_common::constants::GRACEFUL_SHUTDOWN_TIMEOUT_MAX_SECS
                );
                return ExitCode::FAILURE;
            }
            server.with_grace(Duration::from_secs(g))
        }
        None => server,
    };
    let restart_requested = Arc::new(AtomicBool::new(false));
    let exit_watcher = if args.watch_exit {
        match start_server_exit_watcher(
            args.clone(),
            server.shutdown_token(),
            restart_requested.clone(),
        ) {
            Ok(watcher) => Some(watcher),
            Err(e) => {
                tracing::error!(error = %e, "failed to start config watcher");
                return ExitCode::FAILURE;
            }
        }
    } else {
        None
    };
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
    let exit_watcher = if args.watch_exit {
        match start_client_exit_watcher(
            args.clone(),
            client.shutdown_token(),
            restart_requested.clone(),
        ) {
            Ok(watcher) => Some(watcher),
            Err(e) => {
                tracing::error!(error = %e, "failed to start config watcher");
                return ExitCode::FAILURE;
            }
        }
    } else {
        None
    };
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
