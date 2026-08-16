//! 客户端断线重连与代理恢复集成测试（DESIGN §8.1 / §8.3 / M2b）。

mod common;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use common::*;
use rfrp_common::config::ClientProxy;
use rfrps::server::Server;
use tokio::task::JoinHandle;
use tokio::time::sleep;

static TMP: AtomicU64 = AtomicU64::new(0);

async fn start_server_on(port: u16) -> (JoinHandle<()>, SocketAddr) {
    let mut last_err = None;
    for _ in 0..40 {
        let cfg = server_config(port);
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
                sleep(Duration::from_millis(50)).await;
            }
        }
    }
    panic!("failed to rebind server on port {port}: {last_err:?}");
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
    let cfg = client_config(
        server_addr,
        proxies,
        Some(run_id_file.to_string_lossy().to_string()),
    );
    common::start_client(cfg).await
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
        sleep(Duration::from_millis(150)).await;
    }
}

#[tokio::test]
async fn client_reconnects_and_recovers_proxy_after_server_restart() {
    let echo_port = spawn_echo().await;
    let (srv1, addr) = start_server(server_config(0)).await;
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
        "initial proxy should become ready"
    );

    try_echo(addr, remote, b"before")
        .await
        .expect("proxy works before server crash");

    srv1.abort();
    let (srv2, _) = start_server_on(addr.port()).await;

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
    let (srv1, addr) = start_server(server_config(0)).await;
    let r1 = free_port();
    let r2 = free_port();
    let run_id_file = unique_run_id_file();
    let cli = start_client(
        addr,
        vec![tcp_proxy("ssh", echo1, r1), tcp_proxy("web", echo2, r2)],
        run_id_file.clone(),
    )
    .await;
    assert!(
        wait_for_proxy(addr, r1, Duration::from_secs(5)).await,
        "initial proxy should become ready"
    );

    try_echo(addr, r1, b"to-ssh").await.unwrap();
    try_echo(addr, r2, b"to-web").await.unwrap();

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
