//! rfrpc 集成测试共享工具。
#![allow(dead_code)]

use std::net::SocketAddr;
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

pub fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

pub async fn spawn_echo() -> u16 {
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

pub fn server_config(bind_port: u16) -> ServerConfig {
    ServerConfig {
        server: ServerSection {
            bind_addr: "127.0.0.1".into(),
            bind_port,
            token: "".into(),
            tls_enable: false,
            tls_cert: None,
            tls_key: None,
            work_conn_tls: false,
        },
        dashboard: None,
        proxy: ProxySection::default(),
        log: LogSection::default(),
    }
}

pub fn tcp_proxy(name: &str, local_port: u16, remote_port: u16) -> ClientProxy {
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

pub fn client_config(
    server_addr: SocketAddr,
    proxies: Vec<ClientProxy>,
    run_id_file: Option<String>,
) -> ClientConfig {
    ClientConfig {
        client: ClientSection {
            server_addr: server_addr.ip().to_string(),
            server_port: server_addr.port(),
            token: "".into(),
            tls_enable: false,
            tls_server_name: None,
            tls_ca: None,
            work_conn_tls: false,
            run_id_file,
        },
        proxies,
        log: ClientLogSection::default(),
    }
}

pub async fn start_server(cfg: ServerConfig) -> (JoinHandle<()>, SocketAddr) {
    let server = Server::new(cfg).await.unwrap();
    let addr = server.local_addr();
    let task = tokio::spawn(async move {
        let _ = server.run().await;
    });
    (task, addr)
}

pub async fn start_server_with_grace(
    cfg: ServerConfig,
    grace: Duration,
) -> (
    JoinHandle<()>,
    SocketAddr,
    tokio_util::sync::CancellationToken,
) {
    let server = Server::new(cfg).await.unwrap().with_grace(grace);
    let addr = server.local_addr();
    let sd = server.shutdown_token();
    let task = tokio::spawn(async move {
        let _ = server.run().await;
    });
    (task, addr, sd)
}

pub async fn start_client(cfg: ClientConfig) -> JoinHandle<()> {
    tokio::spawn(async move {
        let _ = Client::new(cfg).unwrap().run().await;
    })
}

/// 轮询直到代理端口可正常 echo，用于替代固定 sleep。
pub async fn wait_for_proxy(server_addr: SocketAddr, remote_port: u16, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if try_echo(server_addr, remote_port, b"ready").await.is_ok() {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

pub async fn try_echo(
    server_addr: SocketAddr,
    remote_port: u16,
    data: &[u8],
) -> std::io::Result<()> {
    let mut user = TcpStream::connect((server_addr.ip(), remote_port)).await?;
    user.write_all(data).await?;
    let mut buf = vec![0u8; data.len()];
    user.read_exact(&mut buf).await?;
    if buf.as_slice() == data {
        Ok(())
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "echo mismatch",
        ))
    }
}

pub async fn expect_echo(remote_port: u16, server_addr: SocketAddr, data: &[u8]) {
    try_echo(server_addr, remote_port, data)
        .await
        .expect("echo through proxy");
}
