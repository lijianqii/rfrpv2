//! M4：HTTP/HTTPS vhost 集成测试。

mod common;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use common::*;
use rfrp_common::config::{ClientProxy, LogSection, ProxySection, ServerConfig, ServerSection};
use rfrp_common::crypto::ClientTls;
use rfrp_common::protocol::msg::ProxyType;
use rfrp_common::util::stream::BoxedStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// 本地 HTTP 服务：读请求头后返回固定响应。
async fn spawn_http_local() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((mut s, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buf = vec![0u8; 4096];
                let _ = s.read(&mut buf).await;
                let _ = s
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                    .await;
            });
        }
    });
    port
}

fn vhost_server_config(http_port: Option<u16>, https_port: Option<u16>) -> ServerConfig {
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let cert = base.join("../../examples/vhost-cert.pem");
    let key = base.join("../../examples/vhost-key.pem");
    ServerConfig {
        server: ServerSection {
            bind_addr: "127.0.0.1".into(),
            bind_port: 0,
            token: "".into(),
            tls_enable: false,
            tls_cert: None,
            tls_key: None,
            work_conn_tls: false,
        },
        dashboard: None,
        proxy: ProxySection {
            allow_ports: String::new(),
            vhost_http_port: http_port,
            vhost_https_port: https_port,
            vhost_tls_cert: Some(cert.to_string_lossy().to_string()),
            vhost_tls_key: Some(key.to_string_lossy().to_string()),
        },
        log: LogSection::default(),
    }
}

fn web_proxy(kind: ProxyType, local_port: u16, pool_size: u32) -> ClientProxy {
    ClientProxy {
        name: "web".into(),
        r#type: kind,
        local_ip: "127.0.0.1".into(),
        local_port,
        remote_port: None,
        custom_domains: Some(vec!["dev.example.com".into()]),
        pool_size,
    }
}

async fn start_vhost_stack(
    http_port: Option<u16>,
    https_port: Option<u16>,
    proxy: ClientProxy,
) -> (
    tokio::task::JoinHandle<()>,
    SocketAddr,
    tokio::task::JoinHandle<()>,
) {
    let (srv, addr) = start_server(vhost_server_config(http_port, https_port)).await;
    let cli = start_client(client_config(addr, vec![proxy], None)).await;
    (srv, addr, cli)
}

/// 通过 vhost 端口发送一次 HTTP 请求，返回响应文本（失败返回 None）。
async fn send_http_request(port: u16, host: &str, tls: Option<&ClientTls>) -> Option<String> {
    let tcp = TcpStream::connect(("127.0.0.1", port)).await.ok()?;
    let mut s: BoxedStream = match tls {
        Some(tls) => Box::new(tls.connect(tcp).await.ok()?),
        None => Box::new(tcp),
    };
    let req = format!("GET / HTTP/1.1\r\nHost: {host}\r\n\r\n");
    s.write_all(req.as_bytes()).await.ok()?;
    let mut buf = [0u8; 256];
    let n = tokio::time::timeout(Duration::from_secs(2), s.read(&mut buf))
        .await
        .ok()?
        .ok()?;
    if n == 0 {
        None
    } else {
        Some(String::from_utf8_lossy(&buf[..n]).to_string())
    }
}

/// 轮询直到 vhost 请求成功。
async fn wait_vhost_response(port: u16, host: &str, tls: Option<&ClientTls>) -> String {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(resp) = send_http_request(port, host, tls).await {
            return resp;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("vhost proxy did not become ready");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn http_vhost_routes_by_host() {
    let local_port = spawn_http_local().await;
    let vhost_port = free_port();
    let (srv, _addr, cli) = start_vhost_stack(
        Some(vhost_port),
        None,
        web_proxy(ProxyType::Http, local_port, 0),
    )
    .await;

    let resp = wait_vhost_response(vhost_port, "dev.example.com", None).await;
    assert!(
        resp.contains("200 OK") && resp.contains("ok"),
        "unexpected response: {resp}"
    );

    srv.abort();
    cli.abort();
}

#[tokio::test]
async fn http_vhost_host_variants() {
    let local_port = spawn_http_local().await;
    let vhost_port = free_port();
    let (srv, _addr, cli) = start_vhost_stack(
        Some(vhost_port),
        None,
        web_proxy(ProxyType::Http, local_port, 0),
    )
    .await;

    // 带端口与大小写变体都应命中同一代理。
    let resp = wait_vhost_response(vhost_port, "dev.example.com:8080", None).await;
    assert!(resp.contains("200 OK"), "port variant failed: {resp}");
    let resp = wait_vhost_response(vhost_port, "DEV.EXAMPLE.COM", None).await;
    assert!(resp.contains("200 OK"), "case variant failed: {resp}");

    srv.abort();
    cli.abort();
}

#[tokio::test]
async fn http_vhost_with_pool() {
    let local_port = spawn_http_local().await;
    let vhost_port = free_port();
    let (srv, _addr, cli) = start_vhost_stack(
        Some(vhost_port),
        None,
        web_proxy(ProxyType::Http, local_port, 1),
    )
    .await;

    let resp = wait_vhost_response(vhost_port, "dev.example.com", None).await;
    assert!(resp.contains("200 OK"), "pool vhost failed: {resp}");

    srv.abort();
    cli.abort();
}

#[tokio::test]
async fn http_vhost_rejects_unknown_host() {
    let local_port = spawn_http_local().await;
    let vhost_port = free_port();
    let (srv, _addr, cli) = start_vhost_stack(
        Some(vhost_port),
        None,
        web_proxy(ProxyType::Http, local_port, 0),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // 未知 Host：服务端应关闭连接（无响应）。
    assert!(
        send_http_request(vhost_port, "unknown.example.com", None)
            .await
            .is_none(),
        "unknown host should not be forwarded"
    );

    srv.abort();
    cli.abort();
}

#[tokio::test]
async fn https_vhost_routes_by_sni() {
    let local_port = spawn_http_local().await;
    let vhost_port = free_port();
    let (srv, _addr, cli) = start_vhost_stack(
        None,
        Some(vhost_port),
        web_proxy(ProxyType::Https, local_port, 0),
    )
    .await;

    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let cert = base.join("../../examples/vhost-cert.pem");
    let section = rfrp_common::config::ClientSection {
        tls_server_name: Some("dev.example.com".into()),
        tls_ca: Some(cert.to_string_lossy().to_string()),
        ..Default::default()
    };
    let tls = ClientTls::new(&section).unwrap();

    let resp = wait_vhost_response(vhost_port, "dev.example.com", Some(&tls)).await;
    assert!(
        resp.contains("200 OK") && resp.contains("ok"),
        "unexpected response: {resp}"
    );

    srv.abort();
    cli.abort();
}

#[tokio::test]
async fn https_vhost_falls_back_to_host_header_without_sni() {
    let local_port = spawn_http_local().await;
    let vhost_port = free_port();
    let (srv, _addr, cli) = start_vhost_stack(
        None,
        Some(vhost_port),
        web_proxy(ProxyType::Https, local_port, 0),
    )
    .await;

    // 用 IP 作为 server_name：rustls 对 IP 不发送 SNI，服务端应回退到 Host 头。
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let cert = base.join("../../examples/vhost-cert.pem");
    let section = rfrp_common::config::ClientSection {
        tls_server_name: Some("127.0.0.1".into()),
        tls_ca: Some(cert.to_string_lossy().to_string()),
        ..Default::default()
    };
    let tls = ClientTls::new(&section).unwrap();

    let resp = wait_vhost_response(vhost_port, "dev.example.com", Some(&tls)).await;
    assert!(
        resp.contains("200 OK"),
        "host-header fallback failed: {resp}"
    );

    srv.abort();
    cli.abort();
}

#[tokio::test]
async fn http_vhost_closes_on_oversized_head() {
    let local_port = spawn_http_local().await;
    let vhost_port = free_port();
    let (srv, _addr, cli) = start_vhost_stack(
        Some(vhost_port),
        None,
        web_proxy(ProxyType::Http, local_port, 0),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut s = TcpStream::connect(("127.0.0.1", vhost_port)).await.unwrap();
    // 超过 64KB 且不结束请求头：服务端应关闭连接。
    let big = vec![b'a'; 70 * 1024];
    s.write_all(&big).await.unwrap();
    let mut buf = [0u8; 16];
    let n = tokio::time::timeout(Duration::from_secs(2), s.read(&mut buf))
        .await
        .expect("connection should be closed by server");
    match n {
        Ok(0) => {}
        Ok(_) => panic!("oversized head should not be forwarded"),
        Err(_) => {} // RST 同样视为被关闭
    }

    srv.abort();
    cli.abort();
}

#[tokio::test]
async fn http_vhost_with_tls_work_conn() {
    let local_port = spawn_http_local().await;
    let vhost_port = free_port();
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let cert = base.join("../../examples/cert.pem");
    let key = base.join("../../examples/key.pem");
    let ca = base.join("../../examples/ca.pem");

    // 服务端：控制链路明文，工作连接 TLS。
    let server_cfg = ServerConfig {
        server: ServerSection {
            bind_addr: "127.0.0.1".into(),
            bind_port: 0,
            token: "".into(),
            tls_enable: false,
            tls_cert: Some(cert.to_string_lossy().to_string()),
            tls_key: Some(key.to_string_lossy().to_string()),
            work_conn_tls: true,
        },
        dashboard: None,
        proxy: ProxySection {
            allow_ports: String::new(),
            vhost_http_port: Some(vhost_port),
            vhost_https_port: None,
            vhost_tls_cert: None,
            vhost_tls_key: None,
        },
        log: LogSection::default(),
    };
    let (srv, addr) = start_server(server_cfg).await;

    let proxy = web_proxy(ProxyType::Http, local_port, 0);
    let client_cfg = rfrp_common::config::ClientConfig {
        client: rfrp_common::config::ClientSection {
            server_addr: addr.ip().to_string(),
            server_port: addr.port(),
            token: "".into(),
            tls_enable: false,
            tls_server_name: Some("localhost".into()),
            tls_ca: Some(ca.to_string_lossy().to_string()),
            work_conn_tls: true,
            run_id_file: None,
        },
        proxies: vec![proxy],
        log: rfrp_common::config::ClientLogSection::default(),
    };
    let cli = start_client(client_cfg).await;

    let resp = wait_vhost_response(vhost_port, "dev.example.com", None).await;
    assert!(
        resp.contains("200 OK"),
        "vhost over TLS work conn failed: {resp}"
    );

    srv.abort();
    cli.abort();
}

#[tokio::test]
async fn http_vhost_concurrent_requests() {
    let local_port = spawn_http_local().await;
    let vhost_port = free_port();
    let (srv, _addr, cli) = start_vhost_stack(
        Some(vhost_port),
        None,
        web_proxy(ProxyType::Http, local_port, 1),
    )
    .await;
    // 等第一个请求成功后，再并发压测。
    assert!(wait_vhost_response(vhost_port, "dev.example.com", None)
        .await
        .contains("200 OK"));

    let mut handles = Vec::new();
    for _ in 0..10 {
        handles.push(tokio::spawn(async move {
            send_http_request(vhost_port, "dev.example.com", None).await
        }));
    }
    for h in handles {
        let resp = h.await.unwrap().expect("concurrent vhost request failed");
        assert!(
            resp.contains("200 OK"),
            "unexpected concurrent response: {resp}"
        );
    }

    srv.abort();
    cli.abort();
}

#[tokio::test]
async fn http_vhost_recovers_after_client_reconnect() {
    let local_port = spawn_http_local().await;
    let vhost_port = free_port();
    let (srv, addr) = start_server(vhost_server_config(Some(vhost_port), None)).await;
    let run_id_file =
        std::env::temp_dir().join(format!("rfrp-vhost-{}.runid", uuid::Uuid::new_v4()));

    let cfg = |a: SocketAddr| {
        let mut c = client_config(a, vec![web_proxy(ProxyType::Http, local_port, 0)], None);
        c.client.run_id_file = Some(run_id_file.to_string_lossy().to_string());
        c
    };

    let cli1 = start_client(cfg(addr)).await;
    assert!(wait_vhost_response(vhost_port, "dev.example.com", None)
        .await
        .contains("200 OK"));

    // 模拟客户端控制连接断开。
    cli1.abort();
    tokio::time::sleep(Duration::from_millis(300)).await;

    // 新客户端复用 run_id 重连，vhost 应恢复。
    let cli2 = start_client(cfg(addr)).await;
    let resp = wait_vhost_response(vhost_port, "dev.example.com", None).await;
    assert!(
        resp.contains("200 OK"),
        "vhost did not recover after reconnect: {resp}"
    );

    srv.abort();
    cli2.abort();
    let _ = std::fs::remove_file(run_id_file);
}

#[tokio::test]
async fn https_vhost_sni_mismatch_falls_back_to_host() {
    let local_port = spawn_http_local().await;
    let vhost_port = free_port();
    let (srv, _addr, cli) = start_vhost_stack(
        None,
        Some(vhost_port),
        web_proxy(ProxyType::Https, local_port, 0),
    )
    .await;

    // SNI 用 localhost（证书合法但无代理注册），Host 头用 dev.example.com，
    // 服务端应回退到 Host 路由。
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let cert = base.join("../../examples/vhost-cert.pem");
    let section = rfrp_common::config::ClientSection {
        tls_server_name: Some("localhost".into()),
        tls_ca: Some(cert.to_string_lossy().to_string()),
        ..Default::default()
    };
    let tls = ClientTls::new(&section).unwrap();

    let resp = wait_vhost_response(vhost_port, "dev.example.com", Some(&tls)).await;
    assert!(
        resp.contains("200 OK"),
        "sni fallback to host failed: {resp}"
    );

    srv.abort();
    cli.abort();
}
