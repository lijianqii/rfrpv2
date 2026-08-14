//! `rfrp` 二进制入口。
//!
//! 解析 CLI、初始化日志，按子命令分派到 `rfrps::server::Server::run` 或
//! `rfrpc::client::Client::run`（M1 起接入真实逻辑）。

mod cli;
mod logging;

use std::process::ExitCode;

use clap::Parser;
use cli::{Cli, Commands};

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();

    // CLI 全局日志参数覆盖（优先级高于配置/环境变量，见 DESIGN §9.3）。
    let (lvl, _, _) = cli.log_overrides();
    logging::init_logging(lvl);

    match cli.command {
        // ---- server ----
        Commands::Server { config, .. } => match config {
            Some(path) => {
                let cfg = match rfrp_common::config::load_server_config(&path) {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::error!(?path, error = %e, "failed to load server config");
                        return ExitCode::FAILURE;
                    }
                };
                let server = match rfrps::server::Server::new(cfg).await {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::error!(error = %e, "failed to start server");
                        return ExitCode::FAILURE;
                    }
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
            None => {
                println!("rfrp server: provide -c <config.toml> to start (see examples/rfrp-server.toml)");
                ExitCode::SUCCESS
            }
        },

        // ---- client ----
        Commands::Client { config, .. } => match config {
            Some(path) => {
                let cfg = match rfrp_common::config::load_client_config(&path) {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::error!(?path, error = %e, "failed to load client config");
                        return ExitCode::FAILURE;
                    }
                };
                let client = match rfrpc::client::Client::new(cfg) {
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
            None => {
                println!("rfrp client: provide -c <config.toml> to start (see examples/rfrp-client.toml)");
                ExitCode::SUCCESS
            }
        },
    }
}
