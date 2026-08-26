//! M4：HTTP vhost 全链路集成测试。

mod common;

use std::time::Duration;

use common::*;
use rfrp_common::config::{ClientProxy, LogSection, ProxySection, ServerConfig, ServerSection};
use rfrp_common::protocol::msg::ProxyType;
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

#[tokio::test]
async fn http_vhost_routes_by_host() {
    let local_port = spawn_http_local().await;
    let vhost_port = free_port();

    let server_cfg = ServerConfig {
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
            vhost_http_port: Some(vhost_port),
            vhost_https_port: None,
            vhost_tls_cert: None,
            vhost_tls_key: None,
        },
        log: LogSection::default(),
    };
    let (srv, addr) = start_server(server_cfg).await;

    let proxy = ClientProxy {
        name: "web".into(),
        r#type: ProxyType::Http,
        local_ip: "127.0.0.1".into(),
        local_port,
        remote_port: None,
        custom_domains: Some(vec!["dev.example.com".into()]),
        pool_size: 0,
    };
    let cli = start_client(client_config(addr, vec![proxy], None)).await;

    // 轮询 vhost 端口直到可用。
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let resp = loop {
        if let Ok(mut s) = TcpStream::connect(("127.0.0.1", vhost_port)).await {
            let _ = s
                .write_all(b"GET / HTTP/1.1\r\nHost: dev.example.com\r\n\r\n")
                .await;
            let mut buf = [0u8; 256];
            if let Ok(Ok(n)) = tokio::time::timeout(Duration::from_secs(2), s.read(&mut buf)).await
            {
                if n > 0 {
                    break String::from_utf8_lossy(&buf[..n]).to_string();
                }
            }
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("vhost proxy did not become ready");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert!(
        resp.contains("200 OK") && resp.contains("ok"),
        "unexpected vhost response: {resp}"
    );

    srv.abort();
    cli.abort();
}

#[tokio::test]
async fn http_vhost_rejects_unknown_host() {
    let local_port = spawn_http_local().await;
    let vhost_port = free_port();

    let server_cfg = ServerConfig {
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
            vhost_http_port: Some(vhost_port),
            vhost_https_port: None,
            vhost_tls_cert: None,
            vhost_tls_key: None,
        },
        log: LogSection::default(),
    };
    let (srv, addr) = start_server(server_cfg).await;

    let proxy = ClientProxy {
        name: "web".into(),
        r#type: ProxyType::Http,
        local_ip: "127.0.0.1".into(),
        local_port,
        remote_port: None,
        custom_domains: Some(vec!["dev.example.com".into()]),
        pool_size: 0,
    };
    let cli = start_client(client_config(addr, vec![proxy], None)).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // 未知 Host：服务端应关闭连接（无响应）。
    let mut s = TcpStream::connect(("127.0.0.1", vhost_port)).await.unwrap();
    s.write_all(b"GET / HTTP/1.1\r\nHost: unknown.example.com\r\n\r\n")
        .await
        .unwrap();
    let mut buf = [0u8; 16];
    let n = tokio::time::timeout(Duration::from_secs(2), s.read(&mut buf))
        .await
        .expect("connection should be closed by server")
        .unwrap();
    assert_eq!(n, 0, "unknown host should not be forwarded");

    srv.abort();
    cli.abort();
}
