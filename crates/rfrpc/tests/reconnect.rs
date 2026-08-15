//! 客户端断线重连与代理恢复集成测试（DESIGN §8.1 / §8.3 / M2b）。
//!
//! 模拟服务端崩溃后，客户端应检测控制连接断开、按指数退避重连，并复用 run_id
//! 恢复代理；在途用户无感地在新服务端上继续可用。

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
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

static TMP: AtomicU64 = AtomicU64::new(0);

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
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

async fn start_server() -> (JoinHandle<()>, SocketAddr) {
    let cfg = ServerConfig {
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
        proxy: ProxySection::default(),
        log: LogSection::default(),
    };
    let server = Server::new(cfg).await.unwrap();
    let addr = server.local_addr();
    let task = tokio::spawn(async move {
        let _ = server.run().await;
    });
    (task, addr)
}

/// 在同一地址重启服务端（模拟崩溃后恢复），绑定偶发占用时重试。
async fn start_server_on(port: u16) -> (JoinHandle<()>, SocketAddr) {
    let mut last_err = None;
    for _ in 0..40 {
        let cfg = ServerConfig {
            server: ServerSection {
                bind_addr: "127.0.0.1".into(),
                bind_port: port,
                token: "".into(),
                tls_enable: false,
                tls_cert: None,
                tls_key: None,
                work_conn_tls: false,
            },
            dashboard: None,
            proxy: ProxySection::default(),
            log: LogSection::default(),
        };
        match Server::new(cfg).await {
            Ok(server) => {
                let a = server.local_addr();
                let task = tokio::spawn(async move {
                    let _ = server.run().await;
                });
                return (task, a);
            }
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
    panic!("failed to rebind server on port {port}: {last_err:?}");
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

fn unique_run_id_file() -> PathBuf {
    let n = TMP.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!("rfrp-reconnect-{}-{}.runid", std::process::id(), n))
}

async fn start_client(
    server_addr: SocketAddr,
    proxies: Vec<ClientProxy>,
    run_id_file: PathBuf,
) -> JoinHandle<()> {
    let cfg = ClientConfig {
        client: ClientSection {
            server_addr: server_addr.ip().to_string(),
            server_port: server_addr.port(),
            token: "".into(),
            tls_enable: false,
            tls_server_name: None,
            work_conn_tls: false,
            run_id_file: Some(run_id_file.to_string_lossy().to_string()),
        },
        proxies,
        log: ClientLogSection::default(),
    };
    tokio::spawn(async move {
        let _ = Client::new(cfg).unwrap().run().await;
    })
}

async fn wait_ready() {
    tokio::time::sleep(Duration::from_millis(300)).await;
}

async fn try_echo(server_addr: SocketAddr, remote_port: u16, data: &[u8]) -> std::io::Result<()> {
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

/// 轮询直到 `f` 成功或超时，用于等待客户端完成重连与代理恢复。
async fn retry_until<F, Fut>(mut f: F, timeout: Duration) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = std::io::Result<()>>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if f().await.is_ok() {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
}

#[tokio::test]
async fn client_reconnects_and_recovers_proxy_after_server_restart() {
    let echo_port = spawn_echo().await;
    let (srv1, addr) = start_server().await;
    let remote = free_port();
    let run_id_file = unique_run_id_file();
    let cli = start_client(
        addr,
        vec![tcp_proxy("ssh", echo_port, remote)],
        run_id_file.clone(),
    )
    .await;
    wait_ready().await;

    // 初始可用。
    try_echo(addr, remote, b"before")
        .await
        .expect("proxy works before server crash");

    // 模拟服务端崩溃（直接中止任务，控制连接断开）。
    srv1.abort();
    // 客户端应检测断开并进入指数退避重连（不应退出）。

    // 在同一地址重启服务端。
    let (srv2, _) = start_server_on(addr.port()).await;

    // 轮询等待客户端重连并复用 run_id 恢复代理。
    let recovered = retry_until(|| try_echo(addr, remote, b"after"), Duration::from_secs(15)).await;
    assert!(
        recovered,
        "proxy must recover after client reconnects to restarted server"
    );

    srv2.abort();
    cli.abort();
    let _ = std::fs::remove_file(run_id_file);
}

#[tokio::test]
async fn client_reconnects_and_recovers_multiple_proxies() {
    let echo1 = spawn_echo().await;
    let echo2 = spawn_echo().await;
    let (srv1, addr) = start_server().await;
    let r1 = free_port();
    let r2 = free_port();
    let run_id_file = unique_run_id_file();
    let cli = start_client(
        addr,
        vec![tcp_proxy("ssh", echo1, r1), tcp_proxy("web", echo2, r2)],
        run_id_file.clone(),
    )
    .await;
    wait_ready().await;

    try_echo(addr, r1, b"to-ssh").await.unwrap();
    try_echo(addr, r2, b"to-web").await.unwrap();

    // 服务端崩溃 + 重启。
    srv1.abort();
    let (srv2, _) = start_server_on(addr.port()).await;

    let recovered = retry_until(
        || async {
            try_echo(addr, r1, b"ssh-again").await?;
            try_echo(addr, r2, b"web-again").await?;
            Ok::<(), std::io::Error>(())
        },
        Duration::from_secs(15),
    )
    .await;
    assert!(
        recovered,
        "multiple proxies must recover after client reconnect"
    );

    srv2.abort();
    cli.abort();
    let _ = std::fs::remove_file(run_id_file);
}
