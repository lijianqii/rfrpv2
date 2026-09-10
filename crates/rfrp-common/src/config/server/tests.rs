//! 服务端配置加载与校验单元测试。

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
fn work_conn_tls_requires_certs() {
    // M3：work_conn_tls=true 时即使 tls_enable=false 也必须提供证书/私钥。
    let cfg = ServerConfig {
        server: ServerSection {
            token: "x".into(),
            work_conn_tls: true,
            ..Default::default()
        },
        ..Default::default()
    };
    assert!(cfg.validate().is_err());
}

#[test]
fn tls_cert_file_missing_rejected() {
    let cfg = ServerConfig {
        server: ServerSection {
            token: "x".into(),
            tls_enable: true,
            tls_cert: Some("./definitely-missing-cert.pem".into()),
            tls_key: Some("./definitely-missing-key.pem".into()),
            work_conn_tls: false,
            ..Default::default()
        },
        ..Default::default()
    };
    assert!(cfg.validate().is_err());
}

#[test]
fn vhost_cert_file_missing_rejected() {
    let cfg = ServerConfig {
        server: ServerSection {
            token: "x".into(),
            work_conn_tls: false,
            ..Default::default()
        },
        proxy: ProxySection {
            vhost_https_port: Some(443),
            vhost_tls_cert: Some("./definitely-missing-vhost-cert.pem".into()),
            vhost_tls_key: Some("./definitely-missing-vhost-key.pem".into()),
            ..Default::default()
        },
        ..Default::default()
    };
    assert!(cfg.validate().is_err());
}

#[test]
fn dashboard_port_conflict_rejected() {
    let cfg = ServerConfig {
        server: ServerSection {
            token: "x".into(),
            bind_port: 7000,
            work_conn_tls: false,
            ..Default::default()
        },
        dashboard: Some(DashboardSection {
            addr: "127.0.0.1:7000".into(),
            user: "admin".into(),
            password: "secret123".into(),
        }),
        ..Default::default()
    };
    assert!(cfg.validate().is_err());
}

#[test]
fn dashboard_nonloopback_valid_but_warns() {
    let cfg = ServerConfig {
        server: ServerSection {
            token: "x".into(),
            work_conn_tls: false,
            ..Default::default()
        },
        dashboard: Some(DashboardSection {
            addr: "0.0.0.0:7500".into(),
            user: "admin".into(),
            password: "secret123".into(),
        }),
        ..Default::default()
    };
    assert!(cfg.validate().is_ok());
}
#[test]
fn empty_token_rejected() {
    let cfg = ServerConfig {
        server: ServerSection {
            token: "".into(),
            work_conn_tls: false,
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
        vhost_tls_cert = "../../examples/vhost-cert.pem"
        vhost_tls_key = "../../examples/vhost-key.pem"

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
