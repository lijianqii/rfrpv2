//! 客户端 CLI 参数覆盖配置。

use rfrp_common::config::ClientConfig;

/// 将 CLI 参数覆盖到客户端配置（DESIGN §9.3）。
///
/// 返回 `Err` 表示参数解析失败（如 `--server` 不是合法 `ADDR:PORT`）。
pub fn apply_cli_overrides(
    cfg: &mut ClientConfig,
    server: Option<String>,
    token: Option<String>,
    tls_enable: Option<bool>,
    work_conn_tls: Option<bool>,
) -> std::result::Result<(), String> {
    if let Some(server) = server {
        let addr: std::net::SocketAddr = server
            .parse()
            .map_err(|e| format!("invalid --server '{server}': {e}"))?;
        cfg.client.server_addr = addr.ip().to_string();
        cfg.client.server_port = addr.port();
    }
    if let Some(token) = token {
        cfg.client.token = token;
    }
    if let Some(v) = tls_enable {
        cfg.client.tls_enable = v;
    }
    if let Some(v) = work_conn_tls {
        cfg.client.work_conn_tls = v;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_cli_overrides_sets_fields() {
        let mut cfg = ClientConfig::default();
        apply_cli_overrides(
            &mut cfg,
            Some("127.0.0.1:9000".into()),
            Some("token".into()),
            Some(true),
            Some(false),
        )
        .unwrap();
        assert_eq!(cfg.client.server_addr, "127.0.0.1");
        assert_eq!(cfg.client.server_port, 9000);
        assert_eq!(cfg.client.token, "token");
        assert!(cfg.client.tls_enable);
        assert!(!cfg.client.work_conn_tls);
    }

    #[test]
    fn apply_cli_overrides_rejects_bad_server() {
        let mut cfg = ClientConfig::default();
        assert!(
            apply_cli_overrides(&mut cfg, Some("not-an-addr".into()), None, None, None).is_err()
        );
    }
}
