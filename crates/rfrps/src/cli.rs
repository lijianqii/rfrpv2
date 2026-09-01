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
