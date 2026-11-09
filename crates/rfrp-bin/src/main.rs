//! `rfrp` 二进制入口。
//!
//! 解析 CLI 后按子命令分派：具体执行见 [`commands`]，启动/`--check` 摘要与日志
//! 初始化见 [`summary`]。

#![forbid(unsafe_code)]

mod cli;
mod commands;
mod logging;
mod reload;
mod summary;

/// 可选的高性能分配器（构建时加 `--features mimalloc` 启用）。
///
/// 默认用系统分配器：是否值得换要看实测（见 docs/BENCHMARKS.md 的 A/B 对比）。
/// 本项目数据面在稳态下几乎不分配（每连接几次、每字节零次），所以收益主要出现在
/// **高连接 churn**（大量短连接）场景，而不是大流量传输。
#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::process::ExitCode;

use clap::{CommandFactory, Parser};
use cli::{Cli, ClientAction, Commands};
use commands::{run_client, run_server, run_status, ClientArgs, ServerArgs};
use summary::LogOverrides;

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
            check,
        } => {
            let args = ServerArgs {
                config,
                bind,
                token,
                tls_enable,
                work_conn_tls,
                grace_secs,
                check,
            };
            run_server(args, log).await
        }
        Commands::Server { config: None, .. } => {
            eprintln!(
                "rfrp server: 需要 -c <config.toml>（示例见 examples/rfrp-server.toml）；\
                 用 `rfrp server --help` 查看全部参数"
            );
            ExitCode::FAILURE
        }
        Commands::Client {
            action: Some(ClientAction::Status { config, addr }),
            ..
        } => run_status(config, addr).await,
        Commands::Client {
            config: Some(config),
            server,
            token,
            tls_enable,
            work_conn_tls,
            check,
            ..
        } => {
            let args = ClientArgs {
                config,
                server,
                token,
                tls_enable,
                work_conn_tls,
                check,
            };
            run_client(args, log).await
        }
        Commands::Client { config: None, .. } => {
            eprintln!(
                "rfrp client: 需要 -c <config.toml>（示例见 examples/rfrp-client.toml）；\
                 用 `rfrp client --help` 查看全部参数"
            );
            ExitCode::FAILURE
        }
        Commands::Completions { shell } => {
            let mut cmd = Cli::command();
            clap_complete::generate(shell, &mut cmd, "rfrp", &mut std::io::stdout());
            ExitCode::SUCCESS
        }
    }
}
