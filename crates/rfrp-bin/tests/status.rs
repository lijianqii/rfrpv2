//! `rfrp client status` 查询本地状态端点的集成测试。

use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::Command;

const BIN: &str = env!("CARGO_BIN_EXE_rfrp");

fn tmp_config(name: &str, body: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("rfrp-status-{}-{name}.toml", std::process::id()));
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
