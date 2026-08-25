//! 配置解析的「契约测试」：直接加载仓库自带的 `examples/rfrp-{server,client}.toml`
//! 并断言解析后的完整结构。
//!
//! 目的：防止 serde 字段名 / TOML 键不匹配导致**静默丢弃**（如 `[[proxy]]` 被忽略、
//! 或某段字段整体丢失）。这类 bug 若只检查「解析是否成功」会被漏掉，必须断言*内容*。

use std::path::{Path, PathBuf};

use rfrp_common::config::{load_client_config, load_server_config};
use rfrp_common::protocol::msg::ProxyType;

fn example_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../examples")
        .join(name)
}

#[test]
fn client_example_parses_with_proxies() {
    let cfg =
        load_client_config(&example_path("rfrp-client.toml")).expect("load example client config");

    // [client] 段
    assert_eq!(cfg.client.server_addr, "127.0.0.1");
    assert_eq!(cfg.client.server_port, 7000);
    assert_eq!(cfg.client.token, "shared-secret");
    assert!(cfg.client.tls_enable);
    assert_eq!(cfg.client.tls_server_name, Some("localhost".to_string()));
    assert!(
        cfg.client
            .tls_ca
            .as_deref()
            .map(|p| p.ends_with("examples/ca.pem"))
            .unwrap_or(false),
        "tls_ca should resolve relative to the example config dir"
    );
    assert!(cfg.client.work_conn_tls);

    // 关键回归：[[proxy]] 必须被解析为 proxies，不能静默丢弃。
    assert_eq!(cfg.proxies.len(), 1, "[[proxy]] entries must be parsed");
    assert_eq!(cfg.proxies[0].name, "ssh");
    assert_eq!(cfg.proxies[0].r#type, ProxyType::Tcp);
    assert_eq!(cfg.proxies[0].local_ip, "127.0.0.1");
    assert_eq!(cfg.proxies[0].local_port, 22);
    assert_eq!(cfg.proxies[0].remote_port, Some(6000));
}

#[test]
fn server_example_parses_with_sections() {
    let cfg =
        load_server_config(&example_path("rfrp-server.toml")).expect("load example server config");

    assert_eq!(cfg.server.bind_addr, "0.0.0.0");
    assert_eq!(cfg.server.bind_port, 7000);
    assert_eq!(cfg.server.token, "shared-secret");
    assert!(cfg.server.tls_enable);
    assert!(cfg.server.work_conn_tls);

    // [proxy] 段
    assert_eq!(cfg.proxy.allow_ports, "6000-6100,7001-7010");
    assert_eq!(cfg.proxy.vhost_http_port, Some(80));
    assert_eq!(cfg.proxy.vhost_https_port, Some(443));
    assert!(
        cfg.proxy
            .vhost_tls_cert
            .as_deref()
            .map(|p| p.ends_with("examples/vhost-cert.pem"))
            .unwrap_or(false),
        "vhost_tls_cert should resolve relative to the example config dir"
    );
    assert!(
        cfg.proxy
            .vhost_tls_key
            .as_deref()
            .map(|p| p.ends_with("examples/vhost-key.pem"))
            .unwrap_or(false),
        "vhost_tls_key should resolve relative to the example config dir"
    );

    // [dashboard] 段（整段可选，此处存在）
    let dash = cfg.dashboard.expect("dashboard section present");
    assert_eq!(dash.addr, "0.0.0.0:7500");
    assert_eq!(dash.user, "admin");
    assert_eq!(dash.password, "changeme");
}
