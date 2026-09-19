//! M4：UDP 代理集成测试（DESIGN §8.6）。

mod common;

use common::*;

#[tokio::test]
async fn udp_proxy_roundtrip() {
    init_logging();
    let echo_port = spawn_udp_echo().await;
    let (srv, addr) = start_server(server_config(0)).await;
    let remote = free_port();
    let cli = start_client(client_config(
        addr,
        vec![udp_proxy("udp", echo_port, remote)],
        None,
    ))
    .await;
    wait_udp_ready(addr, remote).await;

    // 小包
    assert!(
        udp_echo(addr, remote, b"hello udp").await,
        "small packet echo failed"
    );
    // 大包（1400 字节，接近常见 MTU）
    let big = vec![0xABu8; 1400];
    assert!(
        udp_echo(addr, remote, &big).await,
        "large packet echo failed"
    );
    // 多轮
    for i in 0..5 {
        assert!(udp_echo(addr, remote, format!("pkt-{i}").as_bytes()).await);
    }

    srv.abort();
    cli.abort();
}

#[tokio::test]
async fn udp_proxy_multiple_clients() {
    init_logging();
    let echo_port = spawn_udp_echo().await;
    let (srv, addr) = start_server(server_config(0)).await;
    let remote = free_port();
    let cli = start_client(client_config(
        addr,
        vec![udp_proxy("udp", echo_port, remote)],
        None,
    ))
    .await;
    wait_udp_ready(addr, remote).await;

    // 两个不同源端口的客户端各自建立会话并收到回声。
    let a = tokio::spawn(async move { udp_echo(addr, remote, b"from-a").await });
    let b = tokio::spawn(async move { udp_echo(addr, remote, b"from-b").await });
    assert!(a.await.unwrap(), "client A echo failed");
    assert!(b.await.unwrap(), "client B echo failed");

    srv.abort();
    cli.abort();
}

#[tokio::test]
async fn udp_proxy_with_tls_work_conn() {
    use rfrp_common::config::{
        ClientConfig, ClientLogSection, ClientSection, LogSection, ProxySection, ServerConfig,
        ServerSection,
    };

    let echo_port = spawn_udp_echo().await;
    let remote = free_port();
    let base = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let cert = base.join("../../examples/cert.pem");
    let key = base.join("../../examples/key.pem");
    let ca = base.join("../../examples/ca.pem");

    let server_cfg = ServerConfig {
        server: ServerSection {
            bind_addr: "127.0.0.1".into(),
            bind_port: 0,
            token: "".into(),
            tls_enable: false,
            tls_cert: Some(cert.to_string_lossy().to_string()),
            tls_key: Some(key.to_string_lossy().to_string()),
            work_conn_tls: true,
            tcp_keepalive_secs: None,
            heartbeat_interval_secs: None,
            heartbeat_timeout_secs: None,
        },
        dashboard: None,
        proxy: ProxySection {
            allow_ports: String::new(),
            vhost_http_port: None,
            vhost_https_port: None,
            vhost_tls_cert: None,
            vhost_tls_key: None,
        },
        log: LogSection::default(),
    };
    let (srv, addr) = start_server(server_cfg).await;

    let client_cfg = ClientConfig {
        client: ClientSection {
            server_addr: addr.ip().to_string(),
            server_port: addr.port(),
            token: "".into(),
            tls_enable: false,
            tls_server_name: Some("localhost".into()),
            tls_ca: Some(ca.to_string_lossy().to_string()),
            work_conn_tls: true,
            run_id_file: None,
            tcp_keepalive_secs: None,
            heartbeat_interval_secs: None,
            heartbeat_timeout_secs: None,
            status_addr: None,
        },
        proxies: vec![udp_proxy("udp", echo_port, remote)],
        log: ClientLogSection::default(),
    };
    let cli = start_client(client_cfg).await;
    wait_udp_ready(addr, remote).await;

    assert!(
        udp_echo(addr, remote, b"tls-udp").await,
        "udp over TLS work conn failed"
    );

    srv.abort();
    cli.abort();
}
