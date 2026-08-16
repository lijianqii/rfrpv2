//! 客户端配置结构（DESIGN §9.2）。

use crate::constants::{MAX_CUSTOM_DOMAINS, MAX_DOMAIN_LEN, POOL_SIZE_WARN_THRESHOLD};
use crate::error::{config, Result};
use crate::protocol::msg::ProxyType;
use serde::Deserialize;
use std::net::IpAddr;

fn default_local_ip() -> String {
    "127.0.0.1".to_string()
}
fn default_true() -> bool {
    true
}
fn default_pool_size() -> u32 {
    1
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
                    if dom.is_empty() || dom.len() > MAX_DOMAIN_LEN {
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
mod tests {
    use super::*;

    fn proxy(toml: &str) -> ClientProxy {
        toml::from_str(toml).unwrap()
    }

    #[test]
    fn tcp_proxy_parses() {
        let p = proxy(
            r#"
            name = "ssh"
            type = "tcp"
            local_ip = "127.0.0.1"
            local_port = 22
            remote_port = 6000
        "#,
        );
        assert_eq!(p.name, "ssh");
        assert_eq!(p.r#type, ProxyType::Tcp);
        assert_eq!(p.remote_port, Some(6000));
        assert_eq!(p.pool_size, 1); // default
        p.validate().unwrap();
    }

    #[test]
    fn http_proxy_requires_domains() {
        let p = proxy(
            r#"
            name = "web"
            type = "https"
            local_ip = "127.0.0.1"
            local_port = 8080
            custom_domains = ["dev.example.com"]
        "#,
        );
        assert_eq!(p.r#type, ProxyType::Https);
        p.validate().unwrap();
    }

    #[test]
    fn http_without_domains_fails() {
        let p = proxy(
            r#"
            name = "web"
            type = "http"
            local_ip = "127.0.0.1"
            local_port = 8080
        "#,
        );
        assert!(p.validate().is_err());
    }

    #[test]
    fn custom_domains_over_limit_fails() {
        let doms: Vec<String> = (0..17).map(|i| format!("d{i}.example.com")).collect();
        let p = ClientProxy {
            name: "web".into(),
            r#type: ProxyType::Http,
            local_ip: "127.0.0.1".into(),
            local_port: 80,
            remote_port: None,
            custom_domains: Some(doms),
            pool_size: 1,
        };
        assert!(p.validate().is_err());
    }

    #[test]
    fn custom_domains_at_limit_ok() {
        let doms: Vec<String> = (0..16).map(|i| format!("d{i}.example.com")).collect();
        let p = ClientProxy {
            name: "web".into(),
            r#type: ProxyType::Http,
            local_ip: "127.0.0.1".into(),
            local_port: 80,
            remote_port: None,
            custom_domains: Some(doms),
            pool_size: 1,
        };
        p.validate().unwrap();
    }

    #[test]
    fn work_conn_tls_requires_server_name() {
        // M3：work_conn_tls=true 时即使 tls_enable=false 也需要 tls_server_name。
        let cfg = ClientConfig {
            client: ClientSection {
                server_addr: "s.example.com".into(),
                server_port: 7000,
                token: "x".into(),
                work_conn_tls: true,
                tls_server_name: None,
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn empty_token_rejected() {
        let cfg = ClientConfig {
            client: ClientSection {
                server_addr: "s.example.com".into(),
                server_port: 7000,
                token: "".into(),
                work_conn_tls: false,
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn duplicate_names_fails() {
        let cfg = ClientConfig {
            client: ClientSection {
                server_addr: "s.example.com".into(),
                server_port: 7000,
                ..Default::default()
            },
            proxies: vec![
                ClientProxy {
                    name: "a".into(),
                    r#type: ProxyType::Tcp,
                    local_ip: "127.0.0.1".into(),
                    local_port: 22,
                    remote_port: Some(6000),
                    custom_domains: None,
                    pool_size: 1,
                },
                ClientProxy {
                    name: "a".into(),
                    r#type: ProxyType::Tcp,
                    local_ip: "127.0.0.1".into(),
                    local_port: 23,
                    remote_port: Some(6001),
                    custom_domains: None,
                    pool_size: 1,
                },
            ],
            log: Default::default(),
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn full_client_config_validates() {
        let toml = r#"
            [client]
            server_addr = "s.example.com"
            server_port = 7000
            token = "secret"
            work_conn_tls = false

            [[proxy]]
            name = "ssh"
            type = "tcp"
            local_port = 22
            remote_port = 6000

            [[proxy]]
            name = "web"
            type = "http"
            local_port = 8080
            custom_domains = ["dev.example.com"]
        "#;
        let cfg: ClientConfig = toml::from_str(toml).unwrap();
        // 关键回归保护：[[proxy]] 必须被解析为 proxies（曾因字段名 proxies vs TOML 键 proxy 不匹配而静默丢弃）。
        assert_eq!(cfg.proxies.len(), 2, "[[proxy]] entries must be parsed");
        assert_eq!(cfg.proxies[0].name, "ssh");
        assert_eq!(cfg.proxies[0].r#type, ProxyType::Tcp);
        assert_eq!(cfg.proxies[0].remote_port, Some(6000));
        assert_eq!(cfg.proxies[1].name, "web");
        assert_eq!(cfg.proxies[1].r#type, ProxyType::Http);
        assert_eq!(
            cfg.proxies[1].custom_domains,
            Some(vec!["dev.example.com".to_string()])
        );
        cfg.validate().unwrap();
    }

    /// 回归测试：明确断言 `[[proxy]]` 数组被解析且内容正确（不只检查解析成功）。
    #[test]
    fn proxy_array_not_silently_dropped() {
        let toml = r#"
            [client]
            server_addr = "127.0.0.1"
            server_port = 7000
            [[proxy]]
            name = "ssh"
            type = "tcp"
            local_port = 22
            remote_port = 6000
        "#;
        let cfg: ClientConfig = toml::from_str(toml).expect("parse");
        assert_eq!(cfg.proxies.len(), 1);
        let p = &cfg.proxies[0];
        assert_eq!(p.name, "ssh");
        assert_eq!(p.r#type, ProxyType::Tcp);
        assert_eq!(p.local_port, 22);
        assert_eq!(p.remote_port, Some(6000));
    }

    #[test]
    fn malformed_toml_errors() {
        let r = toml::from_str::<ClientConfig>("this = = = not valid toml");
        assert!(r.is_err());
    }

    #[test]
    fn wrong_field_type_errors() {
        // server_port 是 u16，给字符串必须报错（而非静默默认）。
        let toml = r#"
            [client]
            server_addr = "x"
            server_port = "7000"
        "#;
        assert!(toml::from_str::<ClientConfig>(toml).is_err());
    }

    #[test]
    fn unknown_field_errors() {
        // deny_unknown_fields：TOML 键拼写错误应显式失败，而非静默忽略。
        let toml = r#"
            [client]
            server_addr = "x"
            server_port = 7000
            servere_addr_typo = "y"
        "#;
        assert!(toml::from_str::<ClientConfig>(toml).is_err());
    }
}
