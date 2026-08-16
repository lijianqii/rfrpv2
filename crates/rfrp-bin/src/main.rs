//! `rfrp` 二进制入口。
//!
//! 解析 CLI、加载配置、应用 CLI 覆盖、初始化日志，按子命令分派到
//! `rfrps::server::Server::run` 或 `rfrpc::client::Client::run`。

mod cli;
mod logging;

use std::process::ExitCode;

use clap::Parser;
use cli::{Cli, Commands};

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let log_level = cli.log_level.clone();
    let log_output = cli.log_output.clone();
    let log_format = cli.log_format.clone();

    match cli.command {
        // ---- server ----
        Commands::Server {
            config,
            bind,
            token,
            tls_enable,
            work_conn_tls,
            grace_secs,
        } => match config {
            Some(path) => {
                let mut cfg = match rfrp_common::config::load_server_config(&path) {
                    Ok(c) => c,
                    Err(e) => {
                        eprintln!("failed to load server config {}: {e}", path.display());
                        return ExitCode::FAILURE;
                    }
                };

                // CLI 参数覆盖配置文件（DESIGN §9.3）。
                if let Some(bind) = bind {
                    match bind.parse::<std::net::SocketAddr>() {
                        Ok(addr) => {
                            cfg.server.bind_addr = addr.ip().to_string();
                            cfg.server.bind_port = addr.port();
                        }
                        Err(e) => {
                            eprintln!("invalid --bind '{bind}': {e}");
                            return ExitCode::FAILURE;
                        }
                    }
                }
                if let Some(token) = token {
                    cfg.server.token = token;
                }
                if let Some(v) = tls_enable {
                    cfg.server.tls_enable = v;
                }
                if let Some(v) = work_conn_tls {
                    cfg.server.work_conn_tls = v;
                }
                if let Err(e) = cfg.validate() {
                    eprintln!("server config invalid after CLI overrides: {e}");
                    return ExitCode::FAILURE;
                }

                logging::init_logging(
                    log_level.as_deref().or(cfg.log.level.as_deref()),
                    log_output.as_deref().or(cfg.log.output.as_deref()),
                    log_format.as_deref().or(cfg.log.format.as_deref()),
                );

                let server = match rfrps::server::Server::new(cfg).await {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::error!(error = %e, "failed to start server");
                        return ExitCode::FAILURE;
                    }
                };
                // 优雅退出宽限期可由 CLI 覆盖（运维可调，默认 30s，见 §14.4）。
                let server = match grace_secs {
                    Some(g) => server.with_grace(std::time::Duration::from_secs(g)),
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
            None => {
                println!("rfrp server: provide -c <config.toml> to start (see examples/rfrp-server.toml)");
                ExitCode::SUCCESS
            }
        },

        // ---- client ----
        Commands::Client {
            config,
            server,
            token,
            tls_enable,
            work_conn_tls,
        } => match config {
            Some(path) => {
                let mut cfg = match rfrp_common::config::load_client_config(&path) {
                    Ok(c) => c,
                    Err(e) => {
                        eprintln!("failed to load client config {}: {e}", path.display());
                        return ExitCode::FAILURE;
                    }
                };

                // CLI 参数覆盖配置文件（DESIGN §9.3）。
                if let Some(server) = server {
                    match server.parse::<std::net::SocketAddr>() {
                        Ok(addr) => {
                            cfg.client.server_addr = addr.ip().to_string();
                            cfg.client.server_port = addr.port();
                        }
                        Err(e) => {
                            eprintln!("invalid --server '{server}': {e}");
                            return ExitCode::FAILURE;
                        }
                    }
                }
                if let Some(token) = token {
                    cfg.client.token = token;
                }
                if let Some(v) = tls_enable {
                    cfg.client.tls_enable = v;
                }
                if let Some(v) = work_conn_tls {
                    cfg.client.work_conn_tls = v;
                }
                if let Err(e) = cfg.validate() {
                    eprintln!("client config invalid after CLI overrides: {e}");
                    return ExitCode::FAILURE;
                }

                logging::init_logging(
                    log_level.as_deref().or(cfg.log.level.as_deref()),
                    log_output.as_deref().or(cfg.log.output.as_deref()),
                    log_format.as_deref().or(cfg.log.format.as_deref()),
                );

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
