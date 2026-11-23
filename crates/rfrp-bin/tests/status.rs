//! `rfrp client status` 查询本地状态端点的集成测试。

use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::Command;

const BIN: &str = env!("CARGO_BIN_EXE_rfrp");

fn tmp_config(name: &str, body: &str) -> PathBuf {
    // 同一测试二进制内用例并行执行：文件名必须唯一，否则一个用例 truncate
    // 会让另一个用例读到空配置（CI 上已实际触发）。
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    let path = std::env::temp_dir().join(format!(
        "rfrp-status-{}-{name}-{stamp}.toml",
        std::process::id()
    ));
    std::fs::write(&path, body).unwrap();
    path
}

/// 起一个最小 HTTP 服务，对任意请求返回固定 JSON。
fn spawn_stub_endpoint(body: &'static str) -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        if let Ok((mut s, _)) = listener.accept() {
            let mut buf = [0u8; 1024];
            let _ = s.read(&mut buf);
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = s.write_all(resp.as_bytes());
        }
    });
    addr
}

fn client_config() -> PathBuf {
    tmp_config(
        "client",
        r#"
        [client]
        server_addr = "127.0.0.1"
        server_port = 7000
        token = "x"
        tls_enable = false
        work_conn_tls = false
    "#,
    )
}

#[test]
fn client_status_prints_endpoint_body() {
    let addr = spawn_stub_endpoint(r#"{"connected":false,"proxies":[]}"#);
    let cfg = client_config();
    let out = Command::new(BIN)
        .args([
            "client",
            "status",
            "-c",
            cfg.to_str().unwrap(),
            "--addr",
            &addr.to_string(),
        ])
        .output()
        .unwrap();
    assert!(out.status.success(), "{out:?}");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("\"connected\""), "{stdout}");
}

#[test]
fn client_status_without_endpoint_hints_config() {
    // 配置里没有 status_addr、也未用 --addr：应给出可操作提示并非零退出。
    let cfg = client_config();
    let out = Command::new(BIN)
        .args(["client", "status", "-c", cfg.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(!out.status.success(), "{out:?}");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("status_addr"), "{stderr}");
}

#[test]
fn client_status_check_does_not_connect() {
    // `--check` 只校验配置与目标地址，不建立连接：指向一个必然拒绝的端口也应成功。
    let cfg = client_config();
    let started = std::time::Instant::now();
    let out = Command::new(BIN)
        .args([
            "client",
            "status",
            "-c",
            cfg.to_str().unwrap(),
            "--addr",
            "127.0.0.1:1",
            "--check",
        ])
        .output()
        .unwrap();
    assert!(out.status.success(), "{out:?}");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("client config OK"), "{stdout}");
    assert!(stdout.contains("127.0.0.1:1"), "{stdout}");
    assert!(
        started.elapsed() < std::time::Duration::from_secs(2),
        "status --check must not attempt a network connection"
    );
}
