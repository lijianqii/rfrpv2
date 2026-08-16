//! 服务端健壮性/混沌集成测试（DESIGN §8.5 / §14.4 / M2d）。

mod common;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use common::*;
use rfrp_common::config::ClientProxy;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

static TMP: AtomicU64 = AtomicU64::new(0);

async fn start_server_grace(grace: Duration) -> (JoinHandle<()>, SocketAddr, CancellationToken) {
    start_server_with_grace(server_config(0), grace).await
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
    let cfg = client_config(
        server_addr,
        proxies,
        Some(run_id_file.to_string_lossy().to_string()),
    );
    common::start_client(cfg).await
}

/// 轮询直到目标端口不再接受连接（监听已回收），或超时返回 false。
async fn wait_until_refused(target: SocketAddr, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        match TcpStream::connect(target).await {
            Ok(mut s) => {
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
    let (srv, addr, sd) = start_server_grace(Duration::from_millis(800)).await;
    let remote = free_port();
    let run_id_file = unique_run_id_file();
    let cli = start_client(
        addr,
        vec![tcp_proxy("ssh", echo_port, remote)],
        run_id_file.clone(),
    )
    .await;
    assert!(
        wait_for_proxy(addr, remote, Duration::from_secs(5)).await,
        "proxy should become ready"
    );

    let mut user = TcpStream::connect((addr.ip(), remote)).await.unwrap();
    user.write_all(b"ping").await.unwrap();
    let mut buf = [0u8; 4];
    user.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"ping");

    sd.cancel();

    user.write_all(b"ping2").await.unwrap();
    let mut buf2 = [0u8; 5];
    user.read_exact(&mut buf2).await.unwrap();
    assert_eq!(&buf2, b"ping2");

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
    assert!(
        wait_for_proxy(addr, remote, Duration::from_secs(5)).await,
        "proxy should become ready"
    );

    let mut u = TcpStream::connect((addr.ip(), remote)).await.unwrap();
    u.write_all(b"x").await.unwrap();
    let mut b = [0u8; 1];
    u.read_exact(&mut b).await.unwrap();

    cli.abort();
    let refused = wait_until_refused((addr.ip(), remote).into(), Duration::from_secs(5)).await;
    assert!(
        refused,
        "proxy listener should be closed after client control disconnect"
    );

    srv.abort();
    let _ = std::fs::remove_file(run_id_file);
}
