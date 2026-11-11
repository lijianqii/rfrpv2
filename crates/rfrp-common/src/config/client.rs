//! 客户端配置结构（DESIGN §9.2）。

use crate::config::{is_valid_domain, LogSection};
use crate::constants::{
    MAX_CUSTOM_DOMAINS, POOL_SIZE_DEFAULT, POOL_SIZE_MAX, POOL_SIZE_WARN_THRESHOLD,
};
use crate::error::{config, Result};
use crate::protocol::msg::ProxyType;
use serde::Deserialize;
use std::path::Path;

fn default_local_ip() -> String {
    "127.0.0.1".to_string()
}
fn default_true() -> bool {
    true
}
fn default_pool_size() -> u32 {
    POOL_SIZE_DEFAULT
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
    /// TCP keepalive 空闲时间（秒）；0 = 禁用。缺省 30。
    /// 用于空闲长连接（SSH/RDP）的断线感知；Windows 亦生效。
    #[serde(default)]
    pub tcp_keepalive_secs: Option<u64>,
    /// 心跳发送间隔（秒），缺省 30。
    /// 弱网/高延迟链路或希望更快感知服务端失联时可调小（见 §8.3）。
    #[serde(default)]
    pub heartbeat_interval_secs: Option<u64>,
    /// 心跳响应等待超时（秒），缺省 10；必须小于 `heartbeat_interval_secs`。
    #[serde(default)]
    pub heartbeat_timeout_secs: Option<u64>,
    /// 可选状态端点地址（如 `"127.0.0.1:7400"`）：提供 `/`、`/api/status`、`/metrics`。
    /// 默认关闭；仅只读、无鉴权，建议绑定回环地址。
    #[serde(default)]
    pub status_addr: Option<String>,
}

impl ClientSection {
    /// 解析服务端地址为全部候选 `SocketAddr`：IP 字面量直接使用，域名走 DNS。
    ///
    /// 每次建连都会调用，因此重连时会重新解析，能拿到 DNS 变更后的地址。
    /// 双栈域名可能返回多个地址，调用方应在总超时内逐个尝试。
    pub async fn resolve_server_addrs(&self) -> Result<Vec<std::net::SocketAddr>> {
        let host = self.server_addr.trim();
        let host = host
            .strip_prefix('[')
            .and_then(|h| h.strip_suffix(']'))
            .unwrap_or(host);
        if let Ok(ip) = host.parse::<std::net::IpAddr>() {
            return Ok(vec![std::net::SocketAddr::new(ip, self.server_port)]);
        }
        let addrs: Vec<std::net::SocketAddr> = tokio::net::lookup_host((host, self.server_port))
            .await
            .map_err(|e| {
                config(format!(
                    "cannot resolve server_addr '{}': {e}",
                    self.server_addr
                ))
            })?
            .collect();
        if addrs.is_empty() {
            return Err(config(format!(
                "server_addr '{}' resolved to no address",
                self.server_addr
            )));
        }
        Ok(addrs)
    }

    /// 兼容单地址调用：返回解析结果中的第一个地址。
    pub async fn resolve_server_addr(&self) -> Result<std::net::SocketAddr> {
        Ok(self.resolve_server_addrs().await?.remove(0))
    }

    /// 生效的心跳发送间隔（配置缺省时用默认值）。
    pub fn heartbeat_interval(&self) -> std::time::Duration {
        std::time::Duration::from_secs(
            self.heartbeat_interval_secs
                .unwrap_or(crate::constants::HEARTBEAT_INTERVAL),
        )
    }

    /// 生效的心跳响应超时（配置缺省时用默认值）。
    pub fn heartbeat_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_secs(
            self.heartbeat_timeout_secs
                .unwrap_or(crate::constants::HEARTBEAT_TIMEOUT),
        )
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
        // local_ip 允许域名（如 `db.internal`）：实际解析发生在建连时
        // （`TcpStream::connect((host, port))`）。这里只做非空/无空白的基础校验。
        if self.local_ip.trim().is_empty() || self.local_ip.chars().any(char::is_whitespace) {
            return Err(config(format!("invalid local_ip: {}", self.local_ip)));
        }
        if !(1..=65535).contains(&self.local_port) {
            return Err(config(format!(
                "proxy {} local_port {} out of range",
                self.name, self.local_port
            )));
        }
        match self.r#type {
            ProxyType::Tcp | ProxyType::Udp => match self.remote_port {
                None => {
                    return Err(config(format!(
                        "proxy {} (type {:?}) requires remote_port",
                        self.name, self.r#type
                    )))
                }
                Some(0) => {
                    return Err(config(format!(
                        "proxy {} (type {:?}) remote_port must be 1-65535",
                        self.name, self.r#type
                    )))
                }
                Some(_) => {}
            },
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
        if self.pool_size > POOL_SIZE_MAX {
            return Err(config(format!(
                "proxy {} pool_size {} exceeds max {POOL_SIZE_MAX}",
                self.name, self.pool_size
            )));
        }
        if self.pool_size > POOL_SIZE_WARN_THRESHOLD {
            // 超过推荐值但仍在硬上限内：只告警（DESIGN §9.4）。
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
    /// 归一化域名（小写），与 rfrps 的 vhost 路由/冲突判定保持一致。
    ///
    /// 服务端注册入口也会归一化（权威位置），这里提前做一次使客户端配置、
    /// 状态端点与上送协议的表示一致。
    pub fn normalize(&mut self) {
        for p in &mut self.proxies {
            if let Some(domains) = p.custom_domains.as_mut() {
                for d in domains.iter_mut() {
                    *d = d.to_lowercase();
                }
            }
        }
    }

    /// 校验配置（DESIGN §9.4）。
    pub fn validate(&self) -> Result<()> {
        if self.client.server_addr.trim().is_empty()
            || self.client.server_addr.chars().any(char::is_whitespace)
        {
            return Err(config(format!(
                "invalid server_addr: '{}'",
                self.client.server_addr
            )));
        }
        if self.client.server_port == 0 {
            return Err(config("server_port must be > 0"));
        }
        if self.client.token.is_empty() {
            return Err(config("client token must not be empty"));
        }
        if self.client.token.len() > crate::constants::MAX_TOKEN_LEN {
            return Err(config(format!(
                "client token length {} exceeds max {}",
                self.client.token.len(),
                crate::constants::MAX_TOKEN_LEN
            )));
        }
        if let Some(secs) = self.client.tcp_keepalive_secs {
            if secs > 3600 {
                return Err(config(format!(
                    "tcp_keepalive_secs {secs} out of range 0-3600"
                )));
            }
        }
        super::validate_heartbeat(
            self.client.heartbeat_interval_secs,
            self.client.heartbeat_timeout_secs,
        )?;
        if let Some(addr) = &self.client.status_addr {
            let parsed = addr
                .parse::<std::net::SocketAddr>()
                .map_err(|e| config(format!("invalid status_addr '{addr}': {e}")))?;
            if parsed.port() == 0 {
                return Err(config("status_addr port must be 1-65535"));
            }
        }
        if (self.client.tls_enable || self.client.work_conn_tls)
            && self
                .client
                .tls_server_name
                .as_ref()
                .map(|s| s.is_empty())
                .unwrap_or(true)
        {
            return Err(config(format!(
                "{} requires tls_server_name{}",
                super::tls_requirement_reason(self.client.tls_enable, self.client.work_conn_tls),
                super::plaintext_hint(self.client.work_conn_tls)
            )));
        }
        if let Some(ca) = &self.client.tls_ca {
            if !Path::new(ca).is_file() {
                return Err(config(format!(
                    "tls_ca file not found or not readable: {ca}"
                )));
            }
        }

        let mut names = std::collections::HashSet::new();
        // TCP 与 UDP 是**相互独立**的端口空间：同一个端口号可以同时被一个 TCP 代理和
        // 一个 UDP 代理使用（RDP 就是典型场景——客户端会把 UDP 发往与 TCP 相同的端口，
        // 见 DESIGN §9.4）。因此按协议分别判重。
        let mut tcp_ports = std::collections::HashSet::new();
        let mut udp_ports = std::collections::HashSet::new();
        for p in &self.proxies {
            if p.name.is_empty() {
                return Err(config("proxy name must not be empty"));
            }
            if !names.insert(p.name.clone()) {
                return Err(config(format!("duplicate proxy name: {}", p.name)));
            }
            p.validate()?;
            let (ports, kind) = match p.r#type {
                ProxyType::Tcp => (&mut tcp_ports, "tcp"),
                ProxyType::Udp => (&mut udp_ports, "udp"),
                // vhost 代理不占用独立端口（走共享监听），无端口唯一性问题。
                ProxyType::Http | ProxyType::Https => continue,
            };
            let rp = p.remote_port.expect("validated above");
            if !ports.insert(rp) {
                return Err(config(format!("duplicate remote_port for {kind}: {rp}")));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
