//! 启动摘要、`--check` 配置摘要与日志初始化。
//!
//! 启动摘要与 `--check` 摘要都**不打印 token 明文**，只说明"已设置 + 长度"。

use rfrp_common::config::LogSection;

/// 全局日志覆盖（CLI 参数，优先级高于配置文件 `[log]`）。
pub struct LogOverrides {
    pub level: Option<String>,
    pub output: Option<String>,
    pub format: Option<String>,
}

/// 按“CLI 参数 > 配置文件 > 默认值”合并日志设置并初始化。
pub fn init_logging(log: &LogSection, overrides: &LogOverrides) {
    crate::logging::init_logging(
        overrides.level.as_deref().or(log.level.as_deref()),
        overrides.output.as_deref().or(log.output.as_deref()),
        overrides.format.as_deref().or(log.format.as_deref()),
    );
}

/// 打印服务端启动摘要（版本与关键配置；不打印 token）。
pub fn log_server_summary(cfg: &rfrp_common::config::ServerConfig) {
    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        bind = %format!("{}:{}", cfg.server.bind_addr, cfg.server.bind_port),
        tls = cfg.server.tls_enable,
        work_conn_tls = cfg.server.work_conn_tls,
        tcp_keepalive_secs = cfg.server.tcp_keepalive_secs.unwrap_or(30),
        udp_session_timeout_secs = cfg.server.udp_session_timeout().as_secs(),
        grace_secs = cfg.server.grace().as_secs(),
        allow_ports = if cfg.proxy.allow_ports.trim().is_empty() {
            "all"
        } else {
            cfg.proxy.allow_ports.as_str()
        },
        vhost_http = ?cfg.proxy.vhost_http_port,
        vhost_https = ?cfg.proxy.vhost_https_port,
        dashboard = cfg.dashboard.is_some(),
        log_level = cfg.log.level.as_deref().unwrap_or("info"),
        "rfrps starting"
    );
}

/// 打印客户端启动摘要（版本、服务端地址、代理清单；不打印 token）。
pub fn log_client_summary(cfg: &rfrp_common::config::ClientConfig) {
    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        server = %format!("{}:{}", cfg.client.server_addr, cfg.client.server_port),
        proxies = cfg.proxies.len(),
        tls = cfg.client.tls_enable,
        work_conn_tls = cfg.client.work_conn_tls,
        tcp_keepalive_secs = cfg.client.tcp_keepalive_secs.unwrap_or(30),
        run_id_file = ?cfg.client.run_id_file,
        "rfrpc starting"
    );
    for p in &cfg.proxies {
        tracing::info!(
            name = %p.name,
            kind = ?p.r#type,
            local = %format!("{}:{}", p.local_ip, p.local_port),
            remote_port = ?p.remote_port,
            domains = ?p.custom_domains,
            pool_size = p.pool_size,
            "proxy configured"
        );
    }
}

/// 密钥类字段的展示形式：只说明"已设置 + 长度"，绝不打印明文。
fn describe_secret(s: &str) -> String {
    format!("(set, {} chars)", s.chars().count())
}

/// `--check` 模式的服务端配置摘要（打印到 stdout，便于脚本/CI 捕获）。
pub fn print_server_config_summary(
    path: &std::path::Path,
    cfg: &rfrp_common::config::ServerConfig,
) {
    println!("server config OK: {}", path.display());
    println!("  bind: {}:{}", cfg.server.bind_addr, cfg.server.bind_port);
    println!("  token: {}", describe_secret(&cfg.server.token));
    println!(
        "  tls_enable: {}, work_conn_tls: {}",
        cfg.server.tls_enable, cfg.server.work_conn_tls
    );
    println!("  grace_secs: {}", cfg.server.grace().as_secs());
    println!(
        "  allow_ports: {}",
        if cfg.proxy.allow_ports.trim().is_empty() {
            "all".to_string()
        } else {
            cfg.proxy.allow_ports.clone()
        }
    );
    println!(
        "  vhost: http={:?}, https={:?}",
        cfg.proxy.vhost_http_port, cfg.proxy.vhost_https_port
    );
    println!(
        "  dashboard: {}",
        cfg.dashboard
            .as_ref()
            .map(|d| format!("{} (user={})", d.addr, d.user))
            .unwrap_or_else(|| "disabled".to_string())
    );
}

/// `--check` 模式的客户端配置摘要（打印到 stdout，便于脚本/CI 捕获）。
pub fn print_client_config_summary(
    path: &std::path::Path,
    cfg: &rfrp_common::config::ClientConfig,
) {
    println!("client config OK: {}", path.display());
    println!(
        "  server: {}:{}",
        cfg.client.server_addr, cfg.client.server_port
    );
    println!("  token: {}", describe_secret(&cfg.client.token));
    println!(
        "  tls_enable: {}, work_conn_tls: {}",
        cfg.client.tls_enable, cfg.client.work_conn_tls
    );
    println!("  proxies: {}", cfg.proxies.len());
    for p in &cfg.proxies {
        let remote = match (p.remote_port, &p.custom_domains) {
            (Some(rp), _) => format!("remote_port={rp}"),
            (None, Some(d)) => format!("domains={}", d.join(",")),
            (None, None) => String::new(),
        };
        println!(
            "    - {} {:?} {}:{} {} pool_size={}",
            p.name, p.r#type, p.local_ip, p.local_port, remote, p.pool_size
        );
    }
}
