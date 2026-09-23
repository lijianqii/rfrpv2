//! 地址字符串解析：允许 `HOST:PORT` 中的 `HOST` 为域名、IPv4 或 `[IPv6]`。
//!
//! 这里只做**拆分与格式校验**，不做 DNS 解析——真正的解析交给
//! `tokio::net::{TcpStream,TcpListener}::connect/bind`（它们接受 `(&str, u16)` 元组
//! 并负责域名解析），这样重连时可以重新解析、拿到 DNS 变更后的地址。

use crate::error::{config, Result};

/// 拆分并校验 `HOST:PORT`。
///
/// - `HOST` 可为域名（`frp.example.com`）、IPv4（`1.2.3.4`）或带方括号的 IPv6（`[::1]`）；
/// - 裸 IPv6（如 `::1`）无法与 `HOST:PORT` 区分，因此 IPv6 **必须**加方括号；
/// - 端口范围为 1-65535。
pub fn split_host_port(s: &str) -> Result<(String, u16)> {
    let s = s.trim();
    let (host, port) = if let Some(rest) = s.strip_prefix('[') {
        rest.split_once("]:")
            .ok_or_else(|| config(format!("invalid address '{s}': expected [IPv6]:PORT")))?
    } else {
        s.rsplit_once(':')
            .ok_or_else(|| config(format!("invalid address '{s}': expected HOST:PORT")))?
    };
    let host = host.trim();
    if host.is_empty() {
        return Err(config(format!(
            "invalid address '{s}': host must not be empty"
        )));
    }
    let port: u16 = port
        .parse()
        .map_err(|_| config(format!("invalid address '{s}': port must be 1-65535")))?;
    if port == 0 {
        return Err(config(format!(
            "invalid address '{s}': port must be 1-65535"
        )));
    }
    Ok((host.to_string(), port))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_hostname_ipv4_and_bracketed_ipv6() {
        assert_eq!(
            split_host_port("frp.example.com:7000").unwrap(),
            ("frp.example.com".to_string(), 7000)
        );
        assert_eq!(
            split_host_port("127.0.0.1:7000").unwrap(),
            ("127.0.0.1".to_string(), 7000)
        );
        assert_eq!(
            split_host_port("[::1]:7000").unwrap(),
            ("::1".to_string(), 7000)
        );
        assert_eq!(
            split_host_port("  localhost:22  ").unwrap(),
            ("localhost".to_string(), 22)
        );
    }

    #[test]
    fn rejects_malformed_addresses() {
        for bad in [
            "",
            "no-port",
            ":7000",
            "host:",
            "host:0",
            "host:70000",
            "[::1]",
        ] {
            assert!(split_host_port(bad).is_err(), "{bad} must be rejected");
        }
    }
}
