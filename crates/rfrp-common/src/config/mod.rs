//! 配置结构、解析与校验。
//!
//! 服务端配置 `[server]`/`[dashboard]`/`[proxy]`/`[log]`，客户端配置
//! `[client]`/`[[proxy]]`/`[log]`。加载后做格式与一致性校验（DESIGN §9）。
//!
//! > 注意：服务端 `[proxy]` 是**单表**（端口范围 / vhost 监听等设置），
//! > 客户端 `[[proxy]]` 是**数组**（每条代理条目）。二者语义不同，分开定义。

mod client;
mod server;

/// `[log]` 日志设置（整段可选），服务端与客户端共用。
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct LogSection {
    #[serde(default)]
    pub level: Option<String>,
    #[serde(default)]
    pub output: Option<String>,
    #[serde(default)]
    pub format: Option<String>,
}

/// 兼容别名：客户端侧日志段与 `LogSection` 相同。
pub use LogSection as ClientLogSection;

pub use client::{ClientConfig, ClientProxy, ClientSection};
pub use server::{DashboardSection, ProxySection, ServerConfig, ServerSection};

use crate::constants::{
    HEARTBEAT_INTERVAL, HEARTBEAT_MAX_SECS, HEARTBEAT_MIN_SECS, HEARTBEAT_TIMEOUT,
};
use crate::error::{config, Result};
use serde::Deserialize;
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

/// 校验可配置的心跳参数（服务端/客户端共用，DESIGN §8.3 / §9.4）。
///
/// 约束：两项都在 [`HEARTBEAT_MIN_SECS`, `HEARTBEAT_MAX_SECS`] 内，
/// 且超时必须**小于**间隔——否则每轮心跳都会在等待回应时超时，
/// 造成刚建立就判死、无限重连。
pub(crate) fn validate_heartbeat(
    interval_secs: Option<u64>,
    timeout_secs: Option<u64>,
) -> Result<()> {
    for (field, value) in [
        ("heartbeat_interval_secs", interval_secs),
        ("heartbeat_timeout_secs", timeout_secs),
    ] {
        if let Some(v) = value {
            if !(HEARTBEAT_MIN_SECS..=HEARTBEAT_MAX_SECS).contains(&v) {
                return Err(config(format!(
                    "{field} {v} out of range {HEARTBEAT_MIN_SECS}-{HEARTBEAT_MAX_SECS}"
                )));
            }
        }
    }
    let interval = interval_secs.unwrap_or(HEARTBEAT_INTERVAL);
    let timeout = timeout_secs.unwrap_or(HEARTBEAT_TIMEOUT);
    if timeout >= interval {
        return Err(config(format!(
            "heartbeat_timeout_secs {timeout} must be less than heartbeat_interval_secs {interval}"
        )));
    }
    Ok(())
}

/// 说明"为什么必须要 TLS 配置"：点名真正被置位的字段。
///
/// `work_conn_tls` 默认 `true`，因此最小配置（只写 token）必然触发该分支；
/// 报错必须让用户知道是哪个字段在起作用，而不是含糊的 "tls_enable or work_conn_tls"。
pub(crate) fn tls_requirement_reason(tls_enable: bool, work_conn_tls: bool) -> &'static str {
    match (tls_enable, work_conn_tls) {
        (true, true) => "tls_enable=true and work_conn_tls=true",
        (true, false) => "tls_enable=true",
        (false, true) => "work_conn_tls=true (default)",
        (false, false) => "TLS is disabled",
    }
}

/// 纯明文部署的提示语：仅在 `work_conn_tls` 为真时附加（说明如何关闭）。
pub(crate) fn plaintext_hint(work_conn_tls: bool) -> &'static str {
    if work_conn_tls {
        "; set work_conn_tls=false for plaintext work connections"
    } else {
        ""
    }
}
