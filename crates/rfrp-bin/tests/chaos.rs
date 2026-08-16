//! 真实信号路径混沌测试（DESIGN §14.4）。
//!
//! 对真实 `rfrp` 子进程发送 SIGTERM / SIGINT / SIGKILL，验证：
//! - SIGTERM / SIGINT 经信号 watcher 触发优雅退出（进程以 0 退出，而非被强杀或挂死）；
//! - SIGKILL 强制终止（进程确实退出，用于验证”强杀无残留“的终止面）；
//! - 配置通过 `--grace-secs` 缩短宽限，避免测试等待默认 30s。

use std::path::PathBuf;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;

const BIN: &str = env!("CARGO_BIN_EXE_rfrp");

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

fn write_server_config(port: u16) -> PathBuf {
    let cfg = format!(
        "[server]\n\
         bind_addr = \"127.0.0.1\"\n\
         bind_port = {port}\n\
         token = \"x\"\n\
         tls_enable = false\n\
         work_conn_tls = false\n\
         \n\
         [log]\n\
         level = \"info\"\n\
         output = \"stderr\"\n\
         format = \"text\"\n"
    );
    let path = std::env::temp_dir().join(format!(
        "rfrp-chaos-srv-{}-{}.toml",
        std::process::id(),
        port
    ));
    std::fs::write(&path, cfg).unwrap();
    path
}

fn write_client_config(server_port: u16) -> PathBuf {
    let cfg = format!(
        "[client]\n\
         server_addr = \"127.0.0.1\"\n\
         server_port = {server_port}\n\
         token = \"x\"\n\
         tls_enable = false\n\
         work_conn_tls = false\n\
         run_id_file = \"\"\n\
         \n\
         [[proxy]]\n\
         name = \"noop\"\n\
         type = \"tcp\"\n\
         local_ip = \"127.0.0.1\"\n\
         local_port = 9\n\
         remote_port = 9\n\
         pool_size = 0\n\
         \n\
         [log]\n\
         level = \"info\"\n\
         output = \"stderr\"\n\
         format = \"text\"\n"
    );
    let path = std::env::temp_dir().join(format!(
        "rfrp-chaos-cli-{}-{}.toml",
        std::process::id(),
        server_port
    ));
    std::fs::write(&path, cfg).unwrap();
    path
}

async fn wait_listening(port: u16) {
    for _ in 0..100 {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("server did not start listening on {port}");
}

fn send_signal(pid: u32, sig: libc::c_int) {
    unsafe {
        libc::kill(pid as libc::pid_t, sig);
    }
}

#[tokio::test]
async fn sigterm_triggers_graceful_exit() {
    let port = free_port();
    let cfg = write_server_config(port);
    let mut child = Command::new(BIN)
        .args(["server", "-c", cfg.to_str().unwrap(), "--grace-secs", "1"])
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn rfrp server");
    let stderr = child.stderr.take().expect("server stderr must be piped");
    let mut lines = BufReader::new(stderr).lines();
    let ready = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(line) = lines.next_line().await.expect("read server stderr") {
            if line.contains("OS signal handler installed") {
                return;
            }
        }
        panic!("server stderr closed before signal handler was installed");
    })
    .await;
    assert!(
        ready.is_ok(),
        "server should install signal handler before SIGTERM"
    );

    send_signal(child.id().expect("pid"), libc::SIGTERM);

    let status = tokio::time::timeout(Duration::from_secs(10), child.wait())
        .await
        .expect("child should exit after SIGTERM")
        .unwrap();
    assert!(
        status.success(),
        "SIGTERM should lead to clean exit (code 0)"
    );
}

#[tokio::test]
async fn sigint_triggers_graceful_exit() {
    let port = free_port();
    let cfg = write_server_config(port);
    let mut child = Command::new(BIN)
        .args(["server", "-c", cfg.to_str().unwrap(), "--grace-secs", "1"])
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn rfrp server");
    let stderr = child.stderr.take().expect("server stderr must be piped");
    let mut lines = BufReader::new(stderr).lines();
    let ready = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(line) = lines.next_line().await.expect("read server stderr") {
            if line.contains("OS signal handler installed") {
                return;
            }
        }
        panic!("server stderr closed before signal handler was installed");
    })
    .await;
    assert!(
        ready.is_ok(),
        "server should install signal handler before SIGINT"
    );

    send_signal(child.id().expect("pid"), libc::SIGINT);

    let status = tokio::time::timeout(Duration::from_secs(10), child.wait())
        .await
        .expect("child should exit after SIGINT")
        .unwrap();
    assert!(
        status.success(),
        "SIGINT should lead to clean exit (code 0)"
    );
}

#[tokio::test]
async fn sigkill_forces_termination() {
    let port = free_port();
    let cfg = write_server_config(port);
    let mut child = Command::new(BIN)
        .args(["server", "-c", cfg.to_str().unwrap(), "--grace-secs", "30"])
        .spawn()
        .expect("spawn rfrp server");
    wait_listening(port).await;

    send_signal(child.id().expect("pid"), libc::SIGKILL);

    let status = tokio::time::timeout(Duration::from_secs(10), child.wait())
        .await
        .expect("child should be terminated by SIGKILL")
        .unwrap();
    assert!(
        !status.success(),
        "SIGKILL must terminate the process (non-zero)"
    );
}

#[tokio::test]
async fn client_sigterm_stops_reconnect_loop() {
    // 客户端连不上服务端时持续重连；收到 SIGTERM 应停止重连并干净退出。
    // 不用固定 sleep，而是等待子进程日志出现“OS signal handler installed”，
    // 确保 tokio 信号处理器已经注册后再发 SIGTERM，避免高负载下信号未就绪导致误杀。
    let cfg = write_client_config(9); // 端口 9 无人监听
    let mut child = Command::new(BIN)
        .args(["client", "-c", cfg.to_str().unwrap()])
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn rfrp client");
    let stderr = child.stderr.take().expect("client stderr must be piped");
    let mut lines = BufReader::new(stderr).lines();
    let ready = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(line) = lines.next_line().await.expect("read client stderr") {
            if line.contains("OS signal handler installed") {
                return;
            }
        }
        panic!("client stderr closed before signal handler was installed");
    })
    .await;
    assert!(
        ready.is_ok(),
        "client should install signal handler before SIGTERM"
    );

    send_signal(child.id().expect("pid"), libc::SIGTERM);

    let status = tokio::time::timeout(Duration::from_secs(10), child.wait())
        .await
        .expect("client should exit after SIGTERM")
        .unwrap();
    assert!(
        status.success(),
        "client SIGTERM should lead to clean exit (code 0)"
    );
}
