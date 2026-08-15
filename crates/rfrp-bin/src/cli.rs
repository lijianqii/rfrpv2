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

impl Cli {
    /// 返回日志覆盖项 `(level, output, format)`。
    pub fn log_overrides(&self) -> (Option<&str>, Option<&str>, Option<&str>) {
        (
            self.log_level.as_deref(),
            self.log_output.as_deref(),
            self.log_format.as_deref(),
        )
    }
}
