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

/// 回归：控制连接**静默失联**（无 FIN/RST，如对端进程挂起、NAT/防火墙静默丢弃）
/// 时，客户端必须通过心跳超时感知并重连。
///
/// 修复前 rfrpc 只被动响应服务端心跳、依赖 TCP EOF 感知断开；半开连接下
/// reader 永久阻塞 → 客户端卡死、永不重连（Windows 未启用 keepalive 时尤甚）。
#[tokio::test]
async fn client_reconnects_after_silent_control_death() {
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;

    use futures::{SinkExt, StreamExt};
    use rfrp_common::protocol::frame::FrameCodec;
    use rfrp_common::protocol::msg::{LoginResp, Message};
    use rfrpc::client::Client;
    use tokio_util::codec::{FramedRead, FramedWrite};

    // 假服务端：回应 Login，但对 Heartbeat 一律不回应（TCP 连接保持不关）。
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let conns = Arc::new(AtomicUsize::new(0));
    let conns_srv = conns.clone();
    tokio::spawn(async move {
        while let Ok((s, _)) = listener.accept().await {
            conns_srv.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                let (r, w) = tokio::io::split(s);
                let mut r = FramedRead::new(r, FrameCodec);
                let mut w = FramedWrite::new(w, FrameCodec);
                while let Some(Ok(frame)) = r.next().await {
                    if let Ok(Message::Login(_)) = Message::from_frame(&frame) {
                        let resp = Message::LoginResp(LoginResp {
                            ok: true,
                            error: None,
                            session_id: Some("fake".into()),
                            work_conn_tls: Some(false),
                        });
                        let _ = w.send(resp.to_frame().unwrap()).await;
                    }
                    // 其余消息（Heartbeat）故意不回应，模拟静默失联。
                }
            });
        }
    });

    // 缩短心跳以便快速验证：200ms 间隔 + 200ms 超时。
    let cfg = client_config(
        addr,
        vec![],
        Some(unique_run_id_file().to_string_lossy().to_string()),
    );
    let client = Client::new(cfg)
        .unwrap()
        .with_heartbeat(Duration::from_millis(200), Duration::from_millis(200));
    let task = tokio::spawn(async move {
        let _ = client.run().await;
    });

    // 首次连接 + 心跳超时后重连 ⇒ 至少 2 次连接。
    let reconnected = retry_until(
        || async {
            if conns.load(Ordering::SeqCst) >= 2 {
                Ok(())
            } else {
                Err(std::io::Error::other("not yet"))
            }
        },
        Duration::from_secs(15),
    )
    .await;
    assert!(
        reconnected,
        "client must reconnect after silent control death (heartbeat timeout)"
    );
    task.abort();
}

/// 回归：注册返回可重试错误码（`port occupied`，如旧会话尚未释放端口）时，
/// 客户端应在后台退避重试并最终注册成功，而不是"连上但代理不可用"。
#[tokio::test]
async fn client_retries_retryable_registration_failure() {
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;

    use futures::{SinkExt, StreamExt};
    use rfrp_common::protocol::frame::FrameCodec;
    use rfrp_common::protocol::msg::{LoginResp, Message, NewProxyResp};
    use rfrpc::client::Client;
    use tokio_util::codec::{FramedRead, FramedWrite};

    // 假服务端：第一次 NewProxy 回 "port occupied"，之后回 ok。
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let attempts = Arc::new(AtomicUsize::new(0));
    let registered = Arc::new(AtomicUsize::new(0));
    let (attempts_srv, registered_srv) = (attempts.clone(), registered.clone());
    tokio::spawn(async move {
        while let Ok((s, _)) = listener.accept().await {
            let (attempts, registered) = (attempts_srv.clone(), registered_srv.clone());
            tokio::spawn(async move {
                let (r, w) = tokio::io::split(s);
                let mut r = FramedRead::new(r, FrameCodec);
                let mut w = FramedWrite::new(w, FrameCodec);
                while let Some(Ok(frame)) = r.next().await {
                    match Message::from_frame(&frame) {
                        Ok(Message::Login(_)) => {
                            let resp = Message::LoginResp(LoginResp {
                                ok: true,
                                error: None,
                                session_id: Some("fake".into()),
                                work_conn_tls: Some(false),
                            });
                            let _ = w.send(resp.to_frame().unwrap()).await;
                        }
                        Ok(Message::NewProxy(np)) => {
                            let n = attempts.fetch_add(1, Ordering::SeqCst);
                            let ok = n >= 1; // 首次拒绝，模拟端口被旧会话占用
                            if ok {
                                registered.fetch_add(1, Ordering::SeqCst);
                            }
                            let resp = Message::NewProxyResp(NewProxyResp {
                                proxy_name: np.proxy_name,
                                ok,
                                error: if ok {
                                    None
                                } else {
                                    Some("port occupied".into())
                                },
                            });
                            let _ = w.send(resp.to_frame().unwrap()).await;
                        }
                        _ => {}
                    }
                }
            });
        }
    });

    let echo_port = spawn_echo().await;
    let mut proxy = tcp_proxy("p1", echo_port, free_port());
    proxy.pool_size = 0;
    let cfg = client_config(
        addr,
        vec![proxy],
        Some(unique_run_id_file().to_string_lossy().to_string()),
    );
    let client = Client::new(cfg).unwrap();
    let task = tokio::spawn(async move {
        let _ = client.run().await;
    });

    // 首次注册被拒 + 后台重试成功 ⇒ 至少 2 次尝试且 1 次成功。
    let ok = retry_until(
        || async {
            if attempts.load(Ordering::SeqCst) >= 2 && registered.load(Ordering::SeqCst) >= 1 {
                Ok(())
            } else {
                Err(std::io::Error::other("not yet"))
            }
        },
        Duration::from_secs(15),
    )
    .await;
    assert!(
        ok,
        "client must retry retryable registration failure (attempts={}, registered={})",
        attempts.load(Ordering::SeqCst),
        registered.load(Ordering::SeqCst)
    );
    task.abort();
}
