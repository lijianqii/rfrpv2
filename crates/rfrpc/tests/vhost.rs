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
