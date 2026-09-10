//! 客户端配置结构（DESIGN §9.2）。

use crate::config::LogSection;
use crate::constants::{MAX_CUSTOM_DOMAINS, MAX_DOMAIN_LEN, POOL_SIZE_WARN_THRESHOLD};
use crate::error::{config, Result};
use crate::protocol::msg::ProxyType;
use serde::Deserialize;
use std::net::IpAddr;
use std::path::Path;

fn default_local_ip() -> String {
    "127.0.0.1".to_string()
}
fn default_true() -> bool {
    true
}
fn default_pool_size() -> u32 {
    1
}

/// 简单的域名格式校验（字母/数字/连字符，标签长度限制）。
fn is_valid_domain(domain: &str) -> bool {
    if domain.is_empty() || domain.len() > MAX_DOMAIN_LEN {
        return false;
    }
    domain.split('.').all(|label| {
        !label.is_empty()
            && label.len() <= 63
            && !label.starts_with('-')
            && !label.ends_with('-')
            && label.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
    })
}

/// `[client]` 连接服务端与鉴权。
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ClientSection {
    pub server_addr: String,
    pub server_port: u16,
    #[serde(default)]
    pub token: String,
    #[serde(default)]
    pub tls_enable: bool,
    #[serde(default)]
    pub tls_server_name: Option<String>,
    /// 可选 CA 证书路径；用于自签证书场景。缺省时使用系统/webpki 内置根证书。
    #[serde(default)]
    pub tls_ca: Option<String>,
    #[serde(default = "default_true")]
    pub work_conn_tls: bool,
    #[serde(default)]
    pub run_id_file: Option<String>,
    /// 可选状态端点地址（如 `"127.0.0.1:7400"`）：提供 `/`、`/api/status`、`/metrics`。
    /// 默认关闭；仅只读、无鉴权，建议绑定回环地址。
    #[serde(default)]
    pub status_addr: Option<String>,
}

impl ClientSection {
    /// 服务端地址（`server_addr:server_port`）。
    pub fn server_socket_addr(&self) -> Result<std::net::SocketAddr> {
        let s = format!("{}:{}", self.server_addr, self.server_port);
        s.parse()
            .map_err(|e| config(format!("invalid server addr '{s}': {e}")))
    }
}

/// 单个 `[[proxy]]` 条目。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientProxy {
    pub name: String,
    pub r#type: ProxyType,
    #[serde(default = "default_local_ip")]
    pub local_ip: String,
    pub local_port: u16,
    #[serde(default)]
    pub remote_port: Option<u16>,
    #[serde(default)]
    pub custom_domains: Option<Vec<String>>,
    #[serde(default = "default_pool_size")]
    pub pool_size: u32,
}

impl ClientProxy {
    /// 校验本代理条目字段一致性（DESIGN §9.4）。
    pub fn validate(&self) -> Result<()> {
        self.local_ip
            .parse::<IpAddr>()
            .map_err(|_| config(format!("invalid local_ip: {}", self.local_ip)))?;
        if !(1..=65535).contains(&self.local_port) {
            return Err(config(format!(
                "proxy {} local_port {} out of range",
                self.name, self.local_port
            )));
        }
        match self.r#type {
            ProxyType::Tcp | ProxyType::Udp => {
                if self.remote_port.is_none() {
                    return Err(config(format!(
                        "proxy {} (type {:?}) requires remote_port",
                        self.name, self.r#type
                    )));
                }
            }
            ProxyType::Http | ProxyType::Https => {
                let d = self.custom_domains.as_ref().ok_or_else(|| {
                    config(format!(
                        "proxy {} (type {:?}) requires custom_domains",
                        self.name, self.r#type
                    ))
                })?;
                if d.is_empty() {
                    return Err(config(format!(
                        "proxy {} custom_domains must not be empty",
                        self.name
                    )));
                }
                if d.len() > MAX_CUSTOM_DOMAINS {
                    return Err(config(format!(
                        "proxy {} has {} custom_domains, max is {MAX_CUSTOM_DOMAINS}",
                        self.name,
                        d.len()
                    )));
                }
                for dom in d {
                    if !is_valid_domain(dom) {
                        return Err(config(format!("invalid domain: {dom}")));
                    }
                }
            }
        }
        if self.pool_size > POOL_SIZE_WARN_THRESHOLD {
            // 超出告警阈值但不拒绝（DESIGN §9.4）。
            tracing::warn!(
                proxy = %self.name,
                pool_size = self.pool_size,
                "pool_size exceeds recommended threshold {POOL_SIZE_WARN_THRESHOLD}"
            );
        }
        Ok(())
    }
}

/// 完整客户端配置。
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ClientConfig {
    #[serde(default)]
    pub client: ClientSection,
    /// 对应 TOML 的 `[[proxy]]`（数组表格）。
    #[serde(default, rename = "proxy")]
    pub proxies: Vec<ClientProxy>,
    #[serde(default)]
    pub log: LogSection,
}

impl ClientConfig {
    /// 校验配置（DESIGN §9.4）。
    pub fn validate(&self) -> Result<()> {
        if self.client.server_port == 0 {
            return Err(config("server_port must be > 0"));
        }
        if self.client.token.is_empty() {
            return Err(config("client token must not be empty"));
        }
        if let Some(addr) = &self.client.status_addr {
            addr.parse::<std::net::SocketAddr>()
                .map_err(|e| config(format!("invalid status_addr '{addr}': {e}")))?;
        }
        if (self.client.tls_enable || self.client.work_conn_tls)
            && self
                .client
                .tls_server_name
                .as_ref()
                .map(|s| s.is_empty())
                .unwrap_or(true)
        {
            return Err(config(
                "tls_server_name required when tls_enable or work_conn_tls is true",
            ));
        }
        if let Some(ca) = &self.client.tls_ca {
            if !Path::new(ca).is_file() {
                return Err(config(format!(
                    "tls_ca file not found or not readable: {ca}"
                )));
            }
        }

        let mut names = std::collections::HashSet::new();
        let mut ports = std::collections::HashSet::new();
        for p in &self.proxies {
            if p.name.is_empty() {
                return Err(config("proxy name must not be empty"));
            }
            if !names.insert(p.name.clone()) {
                return Err(config(format!("duplicate proxy name: {}", p.name)));
            }
            p.validate()?;
            if matches!(p.r#type, ProxyType::Tcp | ProxyType::Udp) {
                let rp = p.remote_port.expect("validated above");
                if !ports.insert(rp) {
                    return Err(config(format!("duplicate remote_port: {rp}")));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
