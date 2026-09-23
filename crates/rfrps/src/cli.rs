//! 服务端 CLI 参数覆盖配置。

use rfrp_common::config::ServerConfig;
use rfrp_common::util::addr::split_host_port;

/// 将 CLI 参数覆盖到服务端配置（DESIGN §9.3）。
///
/// 返回 `Err` 表示参数解析失败（如 `--bind` 不是合法 `HOST:PORT`）。
pub fn apply_cli_overrides(
    cfg: &mut ServerConfig,
    bind: Option<String>,
    token: Option<String>,
    tls_enable: Option<bool>,
    work_conn_tls: Option<bool>,
) -> std::result::Result<(), String> {
    if let Some(bind) = bind {
        // HOST 允许域名（如 `localhost`）/ IPv4 / `[IPv6]`。
        let (host, port) = split_host_port(&bind).map_err(|e| e.to_string())?;
        cfg.server.bind_addr = host;
        cfg.server.bind_port = port;
    }
    if let Some(token) = token {
        cfg.server.token = token;
    }
    if let Some(v) = tls_enable {
        cfg.server.tls_enable = v;
    }
    if let Some(v) = work_conn_tls {
        cfg.server.work_conn_tls = v;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_cli_overrides_sets_fields() {
        let mut cfg = ServerConfig::default();
        apply_cli_overrides(
            &mut cfg,
            Some("0.0.0.0:8000".into()),
            Some("token".into()),
            Some(true),
            Some(false),
        )
        .unwrap();
        assert_eq!(cfg.server.bind_addr, "0.0.0.0");
        assert_eq!(cfg.server.bind_port, 8000);
        assert_eq!(cfg.server.token, "token");
        assert!(cfg.server.tls_enable);
        assert!(!cfg.server.work_conn_tls);
    }

    #[test]
    fn apply_cli_overrides_rejects_bad_bind() {
        let mut cfg = ServerConfig::default();
        assert!(
            apply_cli_overrides(&mut cfg, Some("not-an-addr".into()), None, None, None).is_err()
        );
    }

    #[test]
    fn apply_cli_overrides_accepts_hostname_bind() {
        let mut cfg = ServerConfig::default();
        apply_cli_overrides(&mut cfg, Some("localhost:7000".into()), None, None, None).unwrap();
        assert_eq!(cfg.server.bind_addr, "localhost");
        assert_eq!(cfg.server.bind_port, 7000);
    }
}
