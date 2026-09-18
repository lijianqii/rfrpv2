//! 客户端配置加载与校验单元测试。

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
fn work_conn_tls_default_error_names_the_field() {
    // work_conn_tls 默认 true：最小客户端配置（只写 server/token）会因缺少
    // tls_server_name 失败，报错需点名真实字段并给出改法。
    // 走 TOML 反序列化路径：`work_conn_tls` 的默认值由 serde default 提供
    // （derived Default 会给出 false，与用户实际配置路径不一致）。
    let cfg: ClientConfig = toml::from_str(
        r#"
        [client]
        server_addr = "s.example.com"
        server_port = 7000
        token = "x"
    "#,
    )
    .unwrap();
    let err = cfg.validate().unwrap_err().to_string();
    assert!(err.contains("work_conn_tls=true (default)"), "{err}");
    assert!(err.contains("work_conn_tls=false"), "{err}");
}

#[test]
fn heartbeat_secs_range_and_ordering_validated() {
    let base = r#"
        [client]
        server_addr = "s.example.com"
        server_port = 7000
        token = "secret"
        work_conn_tls = false
    "#;
    let cfg = |interval: Option<u64>, timeout: Option<u64>| -> ClientConfig {
        let mut c: ClientConfig = toml::from_str(base).unwrap();
        c.client.heartbeat_interval_secs = interval;
        c.client.heartbeat_timeout_secs = timeout;
        c
    };

    assert!(cfg(None, None).validate().is_ok());
    assert!(cfg(Some(5), Some(2)).validate().is_ok());
    let d = cfg(None, None);
    assert_eq!(d.client.heartbeat_interval().as_secs(), 30);
    assert_eq!(d.client.heartbeat_timeout().as_secs(), 10);
    let c = cfg(Some(2), Some(1));
    assert_eq!(c.client.heartbeat_interval().as_secs(), 2);
    assert_eq!(c.client.heartbeat_timeout().as_secs(), 1);

    assert!(cfg(Some(0), None).validate().is_err());
    assert!(cfg(Some(3601), None).validate().is_err());
    assert!(cfg(None, Some(0)).validate().is_err());
    assert!(cfg(Some(10), Some(10)).validate().is_err());
    assert!(cfg(Some(10), Some(11)).validate().is_err());
}

#[test]
fn tls_ca_file_missing_rejected() {
    let cfg = ClientConfig {
        client: ClientSection {
            server_addr: "s.example.com".into(),
            server_port: 7000,
            token: "x".into(),
            work_conn_tls: false,
            tls_ca: Some("./definitely-missing-ca.pem".into()),
            ..Default::default()
        },
        ..Default::default()
    };
    assert!(cfg.validate().is_err());
}

#[test]
fn invalid_domain_rejected() {
    let p = ClientProxy {
        name: "web".into(),
        r#type: ProxyType::Http,
        local_ip: "127.0.0.1".into(),
        local_port: 80,
        remote_port: None,
        custom_domains: Some(vec!["-bad.example.com".into()]),
        pool_size: 1,
    };
    assert!(p.validate().is_err());
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
fn tcp_keepalive_secs_range_validated() {
    let base = r#"
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
    "#;
    let cfg = |secs: u64| -> ClientConfig {
        let mut c: ClientConfig = toml::from_str(base).unwrap();
        c.client.tcp_keepalive_secs = Some(secs);
        c
    };
    assert!(cfg(3600).validate().is_ok());
    assert!(cfg(0).validate().is_ok(), "0 = disable is valid");
    assert!(cfg(30).validate().is_ok());
    assert!(
        cfg(3601).validate().is_err(),
        "out of range must be rejected"
    );
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
