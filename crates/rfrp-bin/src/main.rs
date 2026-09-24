//! `rfrp` 二进制入口。
//!
//! 解析 CLI 后按子命令分派：具体执行见 [`commands`]，启动/`--check` 摘要与日志
//! 初始化见 [`summary`]。

#![forbid(unsafe_code)]

mod cli;
mod commands;
mod logging;
mod summary;

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
