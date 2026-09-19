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
fn work_conn_tls_default_error_names_the_field() {
    // 最小配置（只写 token）最常踩到这个坑：报错必须点名 work_conn_tls，
    // 并给出改法，而不是含糊地说 "tls_enable or work_conn_tls"。
    let cfg = ServerConfig {
        server: ServerSection {
            token: "x".into(),
            ..Default::default()
        },
        ..Default::default()
    };
    let err = cfg.validate().unwrap_err().to_string();
    assert!(err.contains("work_conn_tls=true (default)"), "{err}");
    assert!(err.contains("work_conn_tls=false"), "{err}");
}

#[test]
fn heartbeat_secs_range_and_ordering_validated() {
    let base = r#"
        [server]
        token = "x"
        work_conn_tls = false
    "#;
    let cfg = |interval: Option<u64>, timeout: Option<u64>| -> ServerConfig {
        let mut c: ServerConfig = toml::from_str(base).unwrap();
        c.server.heartbeat_interval_secs = interval;
        c.server.heartbeat_timeout_secs = timeout;
        c
    };

    // 缺省与合法值。
    assert!(cfg(None, None).validate().is_ok());
    assert!(cfg(Some(5), Some(1)).validate().is_ok());
    assert!(cfg(Some(60), Some(20)).validate().is_ok());
    assert!(cfg(Some(3600), Some(3599)).validate().is_ok());
    // 生效值（缺省 30/10）。
    let d = cfg(None, None);
    assert_eq!(d.server.heartbeat_interval().as_secs(), 30);
    assert_eq!(d.server.heartbeat_timeout().as_secs(), 10);
    let c = cfg(Some(60), Some(20));
    assert_eq!(c.server.heartbeat_interval().as_secs(), 60);
    assert_eq!(c.server.heartbeat_timeout().as_secs(), 20);

    // 越界。
    assert!(cfg(Some(0), None).validate().is_err());
    assert!(cfg(Some(3601), None).validate().is_err());
    assert!(cfg(None, Some(0)).validate().is_err());
    // 超时必须严格小于间隔：否则每轮心跳都会在等待回应时超时。
    assert!(cfg(Some(10), Some(10)).validate().is_err());
    assert!(cfg(Some(10), Some(11)).validate().is_err());
    assert!(cfg(Some(10), Some(9)).validate().is_ok());
}

#[test]
fn udp_session_timeout_default_and_range_validated() {
    let base = r#"
        [server]
        token = "x"
        work_conn_tls = false
    "#;
    let cfg = |secs: Option<u64>| -> ServerConfig {
        let mut c: ServerConfig = toml::from_str(base).unwrap();
        c.server.udp_session_timeout_secs = secs;
        c
    };

    // 默认 300 秒：RDP-UDP 空闲期间不应频繁重建工作连接。
    assert_eq!(cfg(None).server.udp_session_timeout().as_secs(), 300);
    assert!(cfg(None).validate().is_ok());
    assert_eq!(cfg(Some(600)).server.udp_session_timeout().as_secs(), 600);
    assert!(cfg(Some(600)).validate().is_ok());
    assert!(cfg(Some(1)).validate().is_ok());
    assert!(cfg(Some(24 * 60 * 60)).validate().is_ok());

    assert!(cfg(Some(0)).validate().is_err());
    assert!(cfg(Some(24 * 60 * 60 + 1)).validate().is_err());
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
