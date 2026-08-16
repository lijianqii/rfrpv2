//! 配置结构、解析与校验。
//!
//! 服务端配置 `[server]`/`[dashboard]`/`[proxy]`/`[log]`，客户端配置
//! `[client]`/`[[proxy]]`/`[log]`。加载后做格式与一致性校验（DESIGN §9）。
//!
//! > 注意：服务端 `[proxy]` 是**单表**（端口范围 / vhost 监听等设置），
//! > 客户端 `[[proxy]]` 是**数组**（每条代理条目）。二者语义不同，分开定义。

mod client;
mod server;

pub use client::LogSection as ClientLogSection;
pub use client::{ClientConfig, ClientProxy, ClientSection};
pub use server::{DashboardSection, LogSection, ProxySection, ServerConfig, ServerSection};

use crate::error::{config, Result};
use std::path::Path;

/// 加载并校验服务端配置。
pub fn load_server_config(path: &Path) -> Result<ServerConfig> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| config(format!("cannot read {}: {e}", path.display())))?;
    let mut cfg: ServerConfig = toml::from_str(&text)?;
    let base = path.parent().unwrap_or_else(|| Path::new("."));
    resolve_opt_path(&mut cfg.server.tls_cert, base);
    resolve_opt_path(&mut cfg.server.tls_key, base);
    resolve_opt_path(&mut cfg.proxy.vhost_tls_cert, base);
    resolve_opt_path(&mut cfg.proxy.vhost_tls_key, base);
    cfg.validate()?;
    tracing::debug!(path = %path.display(), "server config loaded");
    Ok(cfg)
}

/// 加载并校验客户端配置。
pub fn load_client_config(path: &Path) -> Result<ClientConfig> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| config(format!("cannot read {}: {e}", path.display())))?;
    let mut cfg: ClientConfig = toml::from_str(&text)?;
    let base = path.parent().unwrap_or_else(|| Path::new("."));
    resolve_opt_path(&mut cfg.client.tls_ca, base);
    cfg.validate()?;
    tracing::debug!(path = %path.display(), "client config loaded");
    Ok(cfg)
}

/// 将相对路径解析为相对于配置文件所在目录的路径。
fn resolve_opt_path(value: &mut Option<String>, base: &Path) {
    if let Some(p) = value {
        let path = Path::new(p);
        if path.is_relative() {
            let joined = base.join(path);
            // 文件存在时规范化，去掉路径中的 `..`；不存在时保留 joined 用于后续报错。
            let resolved = joined
                .canonicalize()
                .unwrap_or(joined)
                .to_string_lossy()
                .to_string();
            tracing::debug!(original = %p, resolved = %resolved, "resolved relative config path");
            *value = Some(resolved);
        }
    }
}

/// 判定 `output` 字段是否指向文件（DESIGN §9.1 `output = "file:/path"`）。
pub fn is_file_output(output: &str) -> bool {
    output.starts_with("file:")
}
