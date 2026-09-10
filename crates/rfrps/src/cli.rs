//! 服务端 CLI 参数覆盖配置。

use std::net::SocketAddr;

use rfrp_common::config::ServerConfig;

/// 将 CLI 参数覆盖到服务端配置（DESIGN §9.3）。
///
/// 返回 `Err` 表示参数解析失败（如 `--bind` 不是合法 `ADDR:PORT`）。
pub fn apply_cli_overrides(
    cfg: &mut ServerConfig,
    bind: Option<String>,
    token: Option<String>,
    tls_enable: Option<bool>,
    work_conn_tls: Option<bool>,
) -> std::result::Result<(), String> {
    if let Some(bind) = bind {
        let addr: SocketAddr = bind
            .parse()
            .map_err(|e| format!("invalid --bind '{bind}': {e}"))?;
        cfg.server.bind_addr = addr.ip().to_string();
        cfg.server.bind_port = addr.port();
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
}
