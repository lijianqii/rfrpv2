//! 服务端配置结构（DESIGN §9.1）。

use crate::config::LogSection;
use crate::error::{config, Result};
use serde::Deserialize;
use std::net::SocketAddr;
use std::path::Path;

fn default_bind_addr() -> String {
    "0.0.0.0".to_string()
}
fn default_bind_port() -> u16 {
    7000
}
fn default_true() -> bool {
    true
}

/// `[server]` 控制监听与鉴权。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerSection {
    #[serde(default = "default_bind_addr")]
    pub bind_addr: String,
    #[serde(default = "default_bind_port")]
    pub bind_port: u16,
    #[serde(default)]
    pub token: String,
    #[serde(default)]
    pub tls_enable: bool,
    #[serde(default)]
    pub tls_cert: Option<String>,
    #[serde(default)]
    pub tls_key: Option<String>,
    /// 是否要求工作连接走 TLS。默认 true（见 DESIGN §6.5）。
    #[serde(default = "default_true")]
    pub work_conn_tls: bool,
    /// TCP keepalive 空闲时间（秒）；0 = 禁用。缺省 30。
    /// 用于空闲长连接（SSH/RDP）的断线感知；Windows 亦生效。
    #[serde(default)]
    pub tcp_keepalive_secs: Option<u64>,
    /// 心跳发送间隔（秒），缺省 30。
    /// 弱网/高延迟链路可调大以降低误判断连的概率（见 §8.3）。
    #[serde(default)]
    pub heartbeat_interval_secs: Option<u64>,
    /// 心跳响应等待超时（秒），缺省 10；必须小于 `heartbeat_interval_secs`。
    #[serde(default)]
    pub heartbeat_timeout_secs: Option<u64>,
    /// UDP 代理会话无活动超时（秒），缺省 300。
    /// RDP-UDP 等交互式会话建议保持较大值，避免空闲期间频繁重建工作连接。
    #[serde(default)]
    pub udp_session_timeout_secs: Option<u64>,
}

impl Default for ServerSection {
    fn default() -> Self {
        Self {
            bind_addr: default_bind_addr(),
            bind_port: default_bind_port(),
            token: String::new(),
            tls_enable: false,
            tls_cert: None,
            tls_key: None,
            work_conn_tls: default_true(),
            tcp_keepalive_secs: None,
            heartbeat_interval_secs: None,
            heartbeat_timeout_secs: None,
            udp_session_timeout_secs: None,
        }
    }
}

impl ServerSection {
    /// 控制监听地址（`bind_addr:bind_port`）。
    pub fn bind_socket_addr(&self) -> Result<SocketAddr> {
        let s = format!("{}:{}", self.bind_addr, self.bind_port);
        s.parse()
            .map_err(|e| config(format!("invalid server bind addr '{s}': {e}")))
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

    /// 生效的 UDP 会话无活动超时（配置缺省时用默认值）。
    pub fn udp_session_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_secs(
            self.udp_session_timeout_secs
                .unwrap_or(crate::constants::UDP_SESSION_TIMEOUT),
        )
    }
}

/// `[dashboard]` 监控面板（整段可选，省略则不启用）。
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct DashboardSection {
    pub addr: String,
    pub user: String,
    pub password: String,
}

impl DashboardSection {
    pub fn validate(&self) -> Result<()> {
        // 地址必须可解析为带端口的 SocketAddr。
        self.addr
            .parse::<SocketAddr>()
            .map_err(|e| config(format!("invalid dashboard addr '{}': {e}", self.addr)))?;
        if self.user.is_empty() {
            return Err(config("dashboard.user must not be empty"));
        }
        if self.password.len() < 6 {
            return Err(config("dashboard.password must be at least 6 characters"));
        }
        Ok(())
    }
}

/// `[proxy]` 端口范围与 vhost 监听设置（单表，非数组）。
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ProxySection {
    /// 允许客户端使用的公网端口范围；空字符串 = 不限制（任意 1-65535）。
    #[serde(default)]
    pub allow_ports: String,
    #[serde(default)]
    pub vhost_http_port: Option<u16>,
    #[serde(default)]
    pub vhost_https_port: Option<u16>,
    #[serde(default)]
    pub vhost_tls_cert: Option<String>,
    #[serde(default)]
    pub vhost_tls_key: Option<String>,
}

impl ProxySection {
    /// 解析 `allow_ports` 为闭区间集合。空字符串返回空集合（表示不限制）。
    pub fn parse_allow_ports(&self) -> Result<Vec<(u16, u16)>> {
        let trimmed = self.allow_ports.trim();
        if trimmed.is_empty() {
            return Ok(Vec::new());
        }
        let mut ranges = Vec::new();
        for part in trimmed.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            if let Some((s, e)) = part.split_once('-') {
                let s: u16 = s
                    .trim()
                    .parse()
                    .map_err(|_| config(format!("invalid port range start: {s}")))?;
                let e: u16 = e
                    .trim()
                    .parse()
                    .map_err(|_| config(format!("invalid port range end: {e}")))?;
                if s > e {
                    return Err(config(format!("port range start > end: {part}")));
                }
                ranges.push((s, e));
            } else {
                let p: u16 = part
                    .parse()
                    .map_err(|_| config(format!("invalid port: {part}")))?;
                ranges.push((p, p));
            }
        }
        Ok(ranges)
    }

    /// 判断端口是否被允许。`allow_ports` 为空（不限制）时恒为 true。
    pub fn is_port_allowed(&self, port: u16) -> Result<bool> {
        let ranges = self.parse_allow_ports()?;
        if ranges.is_empty() {
            return Ok(true);
        }
        Ok(ranges.iter().any(|(s, e)| port >= *s && port <= *e))
    }
}

/// 校验证书/私钥文件存在且可读。
fn ensure_file_exists(path: &str, field: &str) -> Result<()> {
    if !Path::new(path).is_file() {
        return Err(config(format!(
            "{field} file not found or not readable: {path}"
        )));
    }
    Ok(())
}

/// 完整服务端配置。
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    #[serde(default)]
    pub server: ServerSection,
    #[serde(default)]
    pub dashboard: Option<DashboardSection>,
    #[serde(default)]
    pub proxy: ProxySection,
    #[serde(default)]
    pub log: LogSection,
}

impl ServerConfig {
    /// 校验 `heartbeat_interval_secs` / `heartbeat_timeout_secs`。
    fn validate_heartbeat(&self) -> Result<()> {
        super::validate_heartbeat(
            self.server.heartbeat_interval_secs,
            self.server.heartbeat_timeout_secs,
        )
    }

    /// 校验配置（DESIGN §9.4）。
    pub fn validate(&self) -> Result<()> {
        if !(1..=65535).contains(&self.server.bind_port) {
            return Err(config(format!(
                "bind_port {} out of range 1-65535",
                self.server.bind_port
            )));
        }
        if self.server.token.is_empty() {
            return Err(config("server token must not be empty"));
        }
        if self.server.tls_enable || self.server.work_conn_tls {
            let missing_tls = format!(
                "{} requires both tls_cert and tls_key{}",
                super::tls_requirement_reason(self.server.tls_enable, self.server.work_conn_tls),
                super::plaintext_hint(self.server.work_conn_tls)
            );
            let cert = self
                .server
                .tls_cert
                .as_deref()
                .ok_or_else(|| config(missing_tls.clone()))?;
            let key = self
                .server
                .tls_key
                .as_deref()
                .ok_or_else(|| config(missing_tls.clone()))?;
            ensure_file_exists(cert, "tls_cert")?;
            ensure_file_exists(key, "tls_key")?;
        }
        if let Some(secs) = self.server.tcp_keepalive_secs {
            if secs > 3600 {
                return Err(config(format!(
                    "tcp_keepalive_secs {secs} out of range 0-3600"
                )));
            }
        }
        if let Some(secs) = self.server.udp_session_timeout_secs {
            if !(crate::constants::UDP_SESSION_TIMEOUT_MIN_SECS
                ..=crate::constants::UDP_SESSION_TIMEOUT_MAX_SECS)
                .contains(&secs)
            {
                return Err(config(format!(
                    "udp_session_timeout_secs {secs} out of range {}-{}",
                    crate::constants::UDP_SESSION_TIMEOUT_MIN_SECS,
                    crate::constants::UDP_SESSION_TIMEOUT_MAX_SECS
                )));
            }
        }
        self.validate_heartbeat()?;
        // allow_ports 格式必须可解析。
        let _ = self.proxy.parse_allow_ports()?;
        for p in [self.proxy.vhost_http_port, self.proxy.vhost_https_port]
            .into_iter()
            .flatten()
        {
            if !(1..=65535).contains(&p) {
                return Err(config(format!("vhost port {p} out of range 1-65535")));
            }
        }
        if self.proxy.vhost_https_port.is_some() {
            let cert = self.proxy.vhost_tls_cert.as_deref().ok_or_else(|| {
                config("vhost_https_port requires vhost_tls_cert and vhost_tls_key")
            })?;
            let key = self.proxy.vhost_tls_key.as_deref().ok_or_else(|| {
                config("vhost_https_port requires vhost_tls_cert and vhost_tls_key")
            })?;
            ensure_file_exists(cert, "vhost_tls_cert")?;
            ensure_file_exists(key, "vhost_tls_key")?;
        }
        if let Some(d) = &self.dashboard {
            d.validate()?;
            let daddr: SocketAddr = d
                .addr
                .parse()
                .map_err(|e| config(format!("invalid dashboard addr '{}': {e}", d.addr)))?;
            let dport = daddr.port();
            if dport == self.server.bind_port {
                return Err(config("dashboard addr port must not equal bind_port"));
            }
            if self.proxy.vhost_http_port == Some(dport) {
                return Err(config("dashboard addr port must not equal vhost_http_port"));
            }
            if self.proxy.vhost_https_port == Some(dport) {
                return Err(config(
                    "dashboard addr port must not equal vhost_https_port",
                ));
            }
            if daddr.ip().is_unspecified() {
                tracing::warn!(
                    addr = %d.addr,
                    "dashboard bound to a non-loopback address; Basic Auth is plaintext, restrict access"
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
