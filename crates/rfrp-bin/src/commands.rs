//! 子命令执行：`server` / `client` / `client status`。
//!
//! 统一流程：加载配置 → 应用 CLI 覆盖 → 校验 →（`--check` 打印摘要并退出）→
//! 初始化日志 → 进入长驻循环。

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use crate::summary::{
    init_logging, log_client_summary, log_server_summary, print_client_config_summary,
    print_server_config_summary, LogOverrides,
};

/// 服务端子命令参数。
pub struct ServerArgs {
    pub config: PathBuf,
    pub bind: Option<String>,
    pub token: Option<String>,
    pub tls_enable: Option<bool>,
    pub work_conn_tls: Option<bool>,
    pub grace_secs: Option<u64>,
    pub check: bool,
}

/// 客户端子命令参数。
pub struct ClientArgs {
    pub config: PathBuf,
    pub server: Option<String>,
    pub token: Option<String>,
    pub tls_enable: Option<bool>,
    pub work_conn_tls: Option<bool>,
    pub check: bool,
}

/// 服务端：加载配置 → CLI 覆盖 → 校验 → 日志 → accept 循环直到退出。
pub async fn run_server(args: ServerArgs, log: LogOverrides) -> ExitCode {
    let mut cfg = match rfrp_common::config::load_server_config(&args.config) {
        Ok(c) => c,
        Err(e) => {
            eprintln!(
                "failed to load server config {}: {e}",
                args.config.display()
            );
            return ExitCode::FAILURE;
        }
    };
    // CLI 参数覆盖配置文件（DESIGN §9.3），解析逻辑归属 rfrps。
    if let Err(e) = rfrps::cli::apply_cli_overrides(
        &mut cfg,
        args.bind,
        args.token,
        args.tls_enable,
        args.work_conn_tls,
    ) {
        eprintln!("{e}");
        return ExitCode::FAILURE;
    }
    if let Err(e) = cfg.validate() {
        eprintln!("server config invalid after CLI overrides: {e}");
        return ExitCode::FAILURE;
    }
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
        Some(g) => server.with_grace(Duration::from_secs(g)),
        None => server,
    };
    tracing::info!(addr = %server.local_addr(), "rfrps listening");
    match server.run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!(error = %e, "rfrps exited with error");
            ExitCode::FAILURE
        }
    }
}

/// 客户端：加载配置 → CLI 覆盖 → 校验 → 日志 → 长驻运行（重连直到退出）。
pub async fn run_client(args: ClientArgs, log: LogOverrides) -> ExitCode {
    let mut cfg = match rfrp_common::config::load_client_config(&args.config) {
        Ok(c) => c,
        Err(e) => {
            eprintln!(
                "failed to load client config {}: {e}",
                args.config.display()
            );
            return ExitCode::FAILURE;
        }
    };
    // CLI 参数覆盖配置文件（DESIGN §9.3），解析逻辑归属 rfrpc。
    if let Err(e) = rfrpc::cli::apply_cli_overrides(
        &mut cfg,
        args.server,
        args.token,
        args.tls_enable,
        args.work_conn_tls,
    ) {
        eprintln!("{e}");
        return ExitCode::FAILURE;
    }
    if let Err(e) = cfg.validate() {
        eprintln!("client config invalid after CLI overrides: {e}");
        return ExitCode::FAILURE;
    }
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
    match client.run().await {
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
async fn fetch_status(addr: &str) -> std::io::Result<String> {
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
