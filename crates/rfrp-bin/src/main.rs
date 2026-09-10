//! `rfrp` 二进制入口。
//!
//! 解析 CLI、加载配置、应用 CLI 覆盖、初始化日志，按子命令分派到
//! `rfrps::Server::run` 或 `rfrpc::Client::run`。

mod cli;
mod logging;

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::Parser;
use cli::{Cli, Commands};
use rfrp_common::config::LogSection;

/// 全局日志覆盖（CLI 参数，优先级高于配置文件 `[log]`）。
struct LogOverrides {
    level: Option<String>,
    output: Option<String>,
    format: Option<String>,
}

/// 服务端子命令参数。
struct ServerArgs {
    config: PathBuf,
    bind: Option<String>,
    token: Option<String>,
    tls_enable: Option<bool>,
    work_conn_tls: Option<bool>,
    grace_secs: Option<u64>,
}

/// 客户端子命令参数。
struct ClientArgs {
    config: PathBuf,
    server: Option<String>,
    token: Option<String>,
    tls_enable: Option<bool>,
    work_conn_tls: Option<bool>,
}

#[tokio::main]
async fn main() -> ExitCode {
    let Cli {
        command,
        log_level,
        log_output,
        log_format,
    } = Cli::parse();
    let log = LogOverrides {
        level: log_level,
        output: log_output,
        format: log_format,
    };

    match command {
        Commands::Server {
            config: Some(config),
            bind,
            token,
            tls_enable,
            work_conn_tls,
            grace_secs,
        } => {
            let args = ServerArgs {
                config,
                bind,
                token,
                tls_enable,
                work_conn_tls,
                grace_secs,
            };
            run_server(args, log).await
        }
        Commands::Server { config: None, .. } => {
            println!(
                "rfrp server: provide -c <config.toml> to start (see examples/rfrp-server.toml)"
            );
            ExitCode::SUCCESS
        }
        Commands::Client {
            config: Some(config),
            server,
            token,
            tls_enable,
            work_conn_tls,
        } => {
            let args = ClientArgs {
                config,
                server,
                token,
                tls_enable,
                work_conn_tls,
            };
            run_client(args, log).await
        }
        Commands::Client { config: None, .. } => {
            println!(
                "rfrp client: provide -c <config.toml> to start (see examples/rfrp-client.toml)"
            );
            ExitCode::SUCCESS
        }
    }
}

/// 按“CLI 参数 > 配置文件 > 默认值”合并日志设置并初始化。
fn init_logging(log: &LogSection, overrides: &LogOverrides) {
    logging::init_logging(
        overrides.level.as_deref().or(log.level.as_deref()),
        overrides.output.as_deref().or(log.output.as_deref()),
        overrides.format.as_deref().or(log.format.as_deref()),
    );
}

/// 服务端：加载配置 → CLI 覆盖 → 校验 → 日志 → accept 循环直到退出。
async fn run_server(args: ServerArgs, log: LogOverrides) -> ExitCode {
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
    init_logging(&cfg.log, &log);

    let server = match rfrps::Server::new(cfg).await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "failed to start server");
            return ExitCode::FAILURE;
        }
    };
    // 优雅退出宽限期可由 CLI 覆盖（运维可调，默认 30s，见 §14.4）。
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
async fn run_client(args: ClientArgs, log: LogOverrides) -> ExitCode {
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
    init_logging(&cfg.log, &log);

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
