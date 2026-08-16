//! 服务端健壮性/混沌集成测试（DESIGN §8.5 / §14.4 / M2d）。
//!
//! - 优雅退出宽限期内保留在途用户连接（不立即断）；
//! - 客户端控制连接断开后，服务端应回收代理监听（端口不再接受）。
//! - 同时起 rfrps + rfrpc，复用真实控制/工作连接通路。

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
use tokio_util::sync::CancellationToken;

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

async fn start_server_grace(grace: Duration) -> (JoinHandle<()>, SocketAddr, CancellationToken) {
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
    let server = Server::new(cfg).await.unwrap().with_grace(grace);
    let addr = server.local_addr();
    let sd = server.shutdown_token();
    let task = tokio::spawn(async move {
        let _ = server.run().await;
    });
    (task, addr, sd)
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
    std::env::temp_dir().join(format!("rfrp-chaos-{}-{}.runid", std::process::id(), n))
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
            tls_ca: None,
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

/// 轮询直到目标端口不再接受连接（监听已回收），或超时返回 false。
async fn wait_until_refused(target: SocketAddr, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        match TcpStream::connect(target).await {
            Ok(mut s) => {
                // 仍有人监听：连接成功，关闭后继续等待回收。
                let _ = s.shutdown().await;
                if tokio::time::Instant::now() >= deadline {
                    return false;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(_) => return true,
        }
    }
}

#[tokio::test]
async fn graceful_shutdown_keeps_inflight_during_grace() {
    let echo_port = spawn_echo().await;
    // 宽限期给足，但本测试会在宽限内主动关闭在途连接。
    let (srv, addr, sd) = start_server_grace(Duration::from_millis(800)).await;
    let remote = free_port();
    let run_id_file = unique_run_id_file();
    let cli = start_client(
        addr,
        vec![tcp_proxy("ssh", echo_port, remote)],
        run_id_file.clone(),
    )
    .await;
    wait_ready().await;

    // 建立一条保持打开的在途用户连接。
    let mut user = TcpStream::connect((addr.ip(), remote)).await.unwrap();
    user.write_all(b"ping").await.unwrap();
    let mut buf = [0u8; 4];
    user.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"ping");

    // 触发优雅退出。
    sd.cancel();

    // 宽限期内：在途连接仍可继续通信（服务端仍在桥接）。
    user.write_all(b"ping2").await.unwrap();
    let mut buf2 = [0u8; 5];
    user.read_exact(&mut buf2).await.unwrap();
    assert_eq!(&buf2, b"ping2");

    // 关闭在途连接后，run 应在宽限期内返回（不再无限等待）。
    drop(user);
    let r = tokio::time::timeout(Duration::from_secs(3), srv).await;
    assert!(
        r.is_ok(),
        "server.run() must return after in-flight drained within grace"
    );

    cli.abort();
    let _ = std::fs::remove_file(run_id_file);
}

#[tokio::test]
async fn proxy_listener_closed_after_control_disconnect() {
    let echo_port = spawn_echo().await;
    let (srv, addr, _sd) = start_server_grace(Duration::from_millis(300)).await;
    let remote = free_port();
    let run_id_file = unique_run_id_file();
    let cli = start_client(
        addr,
        vec![tcp_proxy("ssh", echo_port, remote)],
        run_id_file.clone(),
    )
    .await;
    wait_ready().await;

    // 代理可用。
    let mut u = TcpStream::connect((addr.ip(), remote)).await.unwrap();
    u.write_all(b"x").await.unwrap();
    let mut b = [0u8; 1];
    u.read_exact(&mut b).await.unwrap();

    // 模拟客户端控制连接崩溃（直接中止任务）。
    cli.abort();
    // 服务端应在会话清理时中止代理监听。
    let refused = wait_until_refused((addr.ip(), remote).into(), Duration::from_secs(5)).await;
    assert!(
        refused,
        "proxy listener should be closed after client control disconnect"
    );

    srv.abort();
    let _ = std::fs::remove_file(run_id_file);
}
