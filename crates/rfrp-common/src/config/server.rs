//! 服务端配置结构（DESIGN §9.1）。

use crate::error::{config, Result};
use serde::Deserialize;
use std::net::SocketAddr;

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

/// `[log]` 日志设置（整段可选）。
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
        if (self.server.tls_enable || self.server.work_conn_tls)
            && (self.server.tls_cert.is_none() || self.server.tls_key.is_none())
        {
            return Err(config(
                "tls_enable=true or work_conn_tls=true requires both tls_cert and tls_key",
            ));
        }
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
        if self.proxy.vhost_https_port.is_some()
            && (self.proxy.vhost_tls_cert.is_none() || self.proxy.vhost_tls_key.is_none())
        {
            return Err(config(
                "vhost_https_port requires vhost_tls_cert and vhost_tls_key",
            ));
        }
        if let Some(d) = &self.dashboard {
            d.validate()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allow_ports_parsing_variants() {
        let cfg = ProxySection {
            allow_ports: "6000-6100,7001-7010".into(),
            ..Default::default()
        };
        let ranges = cfg.parse_allow_ports().unwrap();
        assert_eq!(ranges, vec![(6000, 6100), (7001, 7010)]);

        let cfg = ProxySection {
            allow_ports: "6000, 6005-6010 ,7000".into(),
            ..Default::default()
        };
        assert_eq!(
            cfg.parse_allow_ports().unwrap(),
            vec![(6000, 6000), (6005, 6010), (7000, 7000)]
        );

        let cfg = ProxySection {
            allow_ports: "6000".into(),
            ..Default::default()
        };
        assert_eq!(cfg.parse_allow_ports().unwrap(), vec![(6000, 6000)]);

        let cfg = ProxySection::default();
        assert!(cfg.parse_allow_ports().unwrap().is_empty());
    }

    #[test]
    fn allow_ports_invalid() {
        for bad in ["abc", "7000-", "8000-7000", "7000-abc"] {
            let cfg = ProxySection {
                allow_ports: bad.into(),
                ..Default::default()
            };
            assert!(cfg.parse_allow_ports().is_err(), "expected err for {bad}");
        }
    }

    #[test]
    fn is_port_allowed() {
        let cfg = ProxySection {
            allow_ports: "6000-6100".into(),
            ..Default::default()
        };
        assert!(cfg.is_port_allowed(6050).unwrap());
        assert!(!cfg.is_port_allowed(7000).unwrap());

        let cfg = ProxySection::default(); // 不限制
        assert!(cfg.is_port_allowed(22).unwrap());
    }

    #[test]
    fn server_validate_basic() {
        let cfg = ServerConfig {
            server: ServerSection {
                token: "x".into(),
                work_conn_tls: false,
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(cfg.validate().is_ok());

        let cfg = ServerConfig {
            server: ServerSection {
                token: "x".into(),
                tls_enable: true,
                tls_cert: None,
                tls_key: None,
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn dashboard_validate_rules() {
        let d = DashboardSection {
            addr: "0.0.0.0:7500".into(),
            user: "admin".into(),
            password: "secret".into(),
        };
        assert!(d.validate().is_ok());

        let weak = DashboardSection {
            addr: "0.0.0.0:7500".into(),
            user: "admin".into(),
            password: "123".into(),
        };
        assert!(weak.validate().is_err());

        let bad_addr = DashboardSection {
            addr: "not-an-addr".into(),
            user: "a".into(),
            password: "secret".into(),
        };
        assert!(bad_addr.validate().is_err());
    }

    /// 契约测试：完整服务端配置（含各段）能解析且内容正确。
    #[test]
    fn full_server_config_parses() {
        let toml = r#"
            [server]
            bind_addr = "127.0.0.1"
            bind_port = 7000
            token = "secret"
            work_conn_tls = false

            [dashboard]
            addr = "0.0.0.0:7500"
            user = "admin"
            password = "changeme"

            [proxy]
            allow_ports = "6000-6100,7001-7010"
            vhost_http_port = 80
            vhost_https_port = 443
            vhost_tls_cert = "./vhost-cert.pem"
            vhost_tls_key = "./vhost-key.pem"

            [log]
            level = "info"
        "#;
        let cfg: ServerConfig = toml::from_str(toml).expect("parse server config");
        assert_eq!(cfg.server.bind_addr, "127.0.0.1");
        assert_eq!(cfg.server.bind_port, 7000);
        assert_eq!(cfg.server.token, "secret");
        cfg.validate().unwrap();
        let dash = cfg.dashboard.expect("dashboard present");
        assert_eq!(dash.addr, "0.0.0.0:7500");
        assert_eq!(dash.user, "admin");
        assert_eq!(dash.password, "changeme");
        assert_eq!(cfg.proxy.allow_ports, "6000-6100,7001-7010");
        assert_eq!(cfg.proxy.vhost_http_port, Some(80));
        assert_eq!(cfg.proxy.vhost_https_port, Some(443));
    }

    #[test]
    fn malformed_toml_errors() {
        assert!(toml::from_str::<ServerConfig>("this = = = not valid toml").is_err());
    }

    #[test]
    fn wrong_field_type_errors() {
        // bind_port 是 u16，给字符串必须报错。
        let toml = r#"
            [server]
            bind_addr = "0.0.0.0"
            bind_port = "7000"
        "#;
        assert!(toml::from_str::<ServerConfig>(toml).is_err());
    }

    #[test]
    fn unknown_field_errors() {
        // deny_unknown_fields：TOML 键拼写错误应显式失败。
        let toml = r#"
            [server]
            bind_addr = "0.0.0.0"
            bind_port = 7000
            bind_potr_typo = 1
        "#;
        assert!(toml::from_str::<ServerConfig>(toml).is_err());
    }
}
