//! 命令行参数定义（DESIGN §7.3）。
//!
//! 单一二进制 `rfrp`，通过子命令 `server` / `client` 切换角色。CLI 参数可覆盖
//! 配置文件同名字段；`-c` 与字段参数可组合。

use clap::{Parser, Subcommand};
use std::path::PathBuf;

/// rfrp — Rust Fast Reverse Proxy
#[derive(Parser, Debug)]
#[command(name = "rfrp", version, about = "Rust Fast Reverse Proxy")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,

    /// 覆盖 `[log].level`
    #[arg(long, global = true)]
    pub log_level: Option<String>,

    /// 覆盖 `[log].output`
    #[arg(long, global = true)]
    pub log_output: Option<String>,

    /// 覆盖 `[log].format`
    #[arg(long, global = true)]
    pub log_format: Option<String>,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// 以服务端模式运行（公网监听，接受客户端隧道）
    Server {
        /// 配置文件路径（必填，或与字段参数组合）
        #[arg(short, long)]
        config: Option<PathBuf>,
        /// 覆盖 `bind_addr` + `bind_port`
        #[arg(long)]
        bind: Option<String>,
        /// 覆盖 `token`
        #[arg(long)]
        token: Option<String>,
        /// 覆盖 `tls_enable`
        #[arg(long)]
        tls_enable: Option<bool>,
        /// 覆盖 `work_conn_tls`
        #[arg(long)]
        work_conn_tls: Option<bool>,
        /// 优雅退出宽限期（秒），覆盖默认 30s（见 §14.4）
        #[arg(long)]
        grace_secs: Option<u64>,
    },
    /// 以客户端模式运行（主动连接服务端，暴露本地服务）
    Client {
        /// 配置文件路径（必填，或与字段参数组合）
        #[arg(short, long)]
        config: Option<PathBuf>,
        /// 覆盖 `server_addr` + `server_port`
        #[arg(long)]
        server: Option<String>,
        /// 覆盖 `token`
        #[arg(long)]
        token: Option<String>,
        /// 覆盖 `tls_enable`
        #[arg(long)]
        tls_enable: Option<bool>,
        /// 覆盖 `work_conn_tls`
        #[arg(long)]
        work_conn_tls: Option<bool>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn parses_server_subcommand_with_all_overrides() {
        let cli = Cli::try_parse_from([
            "rfrp",
            "server",
            "-c",
            "examples/rfrp-server.toml",
            "--bind",
            "0.0.0.0:8000",
            "--token",
            "secret",
            "--tls-enable=true",
            "--work-conn-tls=true",
            "--grace-secs",
            "5",
            "--log-level",
            "debug",
        ])
        .unwrap();
        assert!(cli.log_level.as_deref() == Some("debug"));
        match cli.command {
            Commands::Server {
                config,
                bind,
                token,
                tls_enable,
                work_conn_tls,
                grace_secs,
            } => {
                assert_eq!(config, Some(PathBuf::from("examples/rfrp-server.toml")));
                assert_eq!(bind.as_deref(), Some("0.0.0.0:8000"));
                assert_eq!(token.as_deref(), Some("secret"));
                assert_eq!(tls_enable, Some(true));
                assert_eq!(work_conn_tls, Some(true));
                assert_eq!(grace_secs, Some(5));
            }
            other => panic!("expected server command, got {other:?}"),
        }
    }

    #[test]
    fn parses_client_subcommand() {
        let cli = Cli::try_parse_from([
            "rfrp",
            "client",
            "-c",
            "examples/rfrp-client.toml",
            "--server",
            "127.0.0.1:7000",
            "--tls-enable=false",
            "--log-format",
            "json",
        ])
        .unwrap();
        assert_eq!(cli.log_format.as_deref(), Some("json"));
        match cli.command {
            Commands::Client {
                config,
                server,
                token,
                tls_enable,
                work_conn_tls,
            } => {
                assert_eq!(config, Some(PathBuf::from("examples/rfrp-client.toml")));
                assert_eq!(server.as_deref(), Some("127.0.0.1:7000"));
                assert_eq!(token, None);
                assert_eq!(tls_enable, Some(false));
                assert_eq!(work_conn_tls, None);
            }
            other => panic!("expected client command, got {other:?}"),
        }
    }

    #[test]
    fn global_log_flags_accepted_without_subcommand_args() {
        // 全局参数可与子命令任意组合（§7.3）。
        let cli =
            Cli::try_parse_from(["rfrp", "--log-level", "warn", "server", "-c", "x.toml"]).unwrap();
        assert_eq!(cli.log_level.as_deref(), Some("warn"));
    }

    #[test]
    fn invalid_bool_value_rejected() {
        let err = Cli::try_parse_from(["rfrp", "server", "--tls-enable=maybe", "-c", "x.toml"])
            .unwrap_err();
        assert!(err.to_string().contains("invalid value"), "{err}");
    }

    #[test]
    fn unknown_subcommand_rejected() {
        let err = Cli::try_parse_from(["rfrp", "bogus"]).unwrap_err();
        assert!(err.to_string().contains("unrecognized subcommand"), "{err}");
    }
}
