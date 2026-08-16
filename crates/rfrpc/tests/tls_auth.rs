//! M3 安全相关集成测试：TLS 控制/工作连接、token 鉴权、服务端偏好覆盖。

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use rfrp_common::config::{
    ClientConfig, ClientLogSection, ClientProxy, ClientSection, LogSection, ProxySection,
    ServerConfig, ServerSection,
};
use rfrp_common::protocol::msg::ProxyType;
use rfrpc::client::Client;
use rfrps::server::Server;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

fn init_logging() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_writer(std::io::stderr)
        .try_init();
}

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

fn cert_paths() -> (PathBuf, PathBuf) {
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    (
        base.join("tests/certs/server-cert.pem"),
        base.join("tests/certs/server-key.pem"),
    )
}

fn server_config(tls_enable: bool, work_conn_tls: bool, token: &str) -> ServerConfig {
    let (cert, key) = cert_paths();
    ServerConfig {
        server: ServerSection {
            bind_addr: "127.0.0.1".into(),
            bind_port: 0,
            token: token.into(),
            tls_enable,
            tls_cert: Some(cert.to_string_lossy().to_string()),
            tls_key: Some(key.to_string_lossy().to_string()),
            work_conn_tls,
        },
        dashboard: None,
        proxy: ProxySection::default(),
        log: LogSection::default(),
    }
}

fn client_config(
    server_addr: SocketAddr,
    tls_enable: bool,
    work_conn_tls: bool,
    token: &str,
    proxies: Vec<ClientProxy>,
) -> ClientConfig {
    let (cert, _key) = cert_paths();
    ClientConfig {
        client: ClientSection {
            server_addr: server_addr.ip().to_string(),
            server_port: server_addr.port(),
            token: token.into(),
            tls_enable,
            tls_server_name: Some("localhost".into()),
            tls_ca: Some(cert.to_string_lossy().to_string()),
            work_conn_tls,
            run_id_file: None,
        },
        proxies,
        log: ClientLogSection::default(),
    }
}

fn tcp_proxy(name: &str, local_port: u16, remote_port: u16) -> ClientProxy {
    ClientProxy {
        name: name.into(),
        r#type: ProxyType::Tcp,
        local_ip: "127.0.0.1".into(),
        local_port,
        remote_port: Some(remote_port),
        custom_domains: None,
        pool_size: 1,
    }
}

async fn spawn_echo() -> u16 {
    let echo = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = echo.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((s, _)) = echo.accept().await {
            tokio::spawn(async move {
                let (mut r, mut w) = tokio::io::split(s);
                let _ = tokio::io::copy(&mut r, &mut w).await;
            });
        }
    });
    port
}

async fn start_server(cfg: ServerConfig) -> (JoinHandle<()>, SocketAddr) {
    let server = Server::new(cfg).await.unwrap();
    let addr = server.local_addr();
    let task = tokio::spawn(async move {
        let _ = server.run().await;
    });
    (task, addr)
}

async fn start_client(cfg: ClientConfig) -> JoinHandle<()> {
    tokio::spawn(async move {
        let _ = Client::new(cfg).unwrap().run().await;
    })
}

async fn wait_ready() {
    tokio::time::sleep(Duration::from_millis(300)).await;
}

async fn expect_echo(remote_port: u16, server_addr: SocketAddr, data: &[u8]) {
    let mut user = TcpStream::connect((server_addr.ip(), remote_port))
        .await
        .expect("connect to server remote_port");
    user.write_all(data).await.unwrap();
    let mut buf = vec![0u8; data.len()];
    user.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, data);
}

#[tokio::test]
async fn tls_control_and_work_roundtrip() {
    init_logging();
    let echo_port = spawn_echo().await;
    let (srv, addr) = start_server(server_config(true, true, "secret")).await;
    let remote = free_port();
    let cli = start_client(client_config(
        addr,
        true,
        true,
        "secret",
        vec![tcp_proxy("ssh", echo_port, remote)],
    ))
    .await;
    wait_ready().await;

    expect_echo(remote, addr, b"tls-hello").await;

    srv.abort();
    cli.abort();
}

#[tokio::test]
async fn wrong_token_is_fatal() {
    init_logging();
    let (srv, addr) = start_server(server_config(false, false, "secret")).await;
    let dir = std::env::temp_dir().join(format!("rfrp-tls-test-{}", uuid::Uuid::new_v4()));
    let run_id_file = dir.join("run_id");
    let mut cfg = client_config(addr, false, false, "wrong", vec![]);
    cfg.client.run_id_file = Some(run_id_file.to_string_lossy().to_string());

    let client = Client::new(cfg).unwrap();
    let res = tokio::time::timeout(Duration::from_secs(5), client.run()).await;
    assert!(
        res.is_ok(),
        "client must exit on auth failure, not reconnect forever"
    );
    assert!(res.unwrap().is_err(), "auth failure should yield Err");

    srv.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn tls_control_only_work_plaintext() {
    // 控制链路 TLS，但服务端/客户端都允许工作连接明文：验证同一端口混合识别。
    init_logging();
    let echo_port = spawn_echo().await;
    let (srv, addr) = start_server(server_config(true, false, "secret")).await;
    let remote = free_port();
    let cli = start_client(client_config(
        addr,
        true,
        false,
        "secret",
        vec![tcp_proxy("ssh", echo_port, remote)],
    ))
    .await;
    wait_ready().await;

    expect_echo(remote, addr, b"control-tls-work-plain").await;

    srv.abort();
    cli.abort();
}

#[tokio::test]
async fn work_conn_tls_upgrades_when_server_prefers_tls() {
    // 客户端配置 work_conn_tls=false，但服务端偏好 true；rfrpc 应升级为 TLS 工作连接。
    init_logging();
    let echo_port = spawn_echo().await;
    let (srv, addr) = start_server(server_config(false, true, "secret")).await;
    let remote = free_port();
    let cli = start_client(client_config(
        addr,
        false,
        false,
        "secret",
        vec![tcp_proxy("ssh", echo_port, remote)],
    ))
    .await;
    wait_ready().await;

    expect_echo(remote, addr, b"work-upgrade-ok").await;

    srv.abort();
    cli.abort();
}

#[tokio::test]
async fn work_conn_tls_follows_server_preference() {
    init_logging();
    // 客户端希望 work_conn_tls=true，但服务端偏好 false；rfrpc 应降级为明文工作连接。
    let echo_port = spawn_echo().await;
    let (srv, addr) = start_server(server_config(false, false, "secret")).await;
    let remote = free_port();
    let cli = start_client(client_config(
        addr,
        false,
        true,
        "secret",
        vec![tcp_proxy("ssh", echo_port, remote)],
    ))
    .await;
    wait_ready().await;

    expect_echo(remote, addr, b"downgrade-ok").await;

    srv.abort();
    cli.abort();
}
