//! `--check` 配置校验模式与"缺 `-c` 退出码"的集成测试。

use std::path::PathBuf;
use std::process::{Command, Output};

const BIN: &str = env!("CARGO_BIN_EXE_rfrp");

/// 写一个临时配置文件（同一进程内各用例用不同名字，避免并行冲突）。
fn tmp_config(name: &str, body: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("rfrp-check-{}-{name}.toml", std::process::id()));
    std::fs::write(&path, body).unwrap();
    path
}

fn run(args: &[&str]) -> Output {
    Command::new(BIN).args(args).output().unwrap()
}

#[test]
fn check_server_config_prints_summary_without_token() {
    let cfg = tmp_config(
        "server",
        r#"
        [server]
        bind_addr = "127.0.0.1"
        bind_port = 7000
        token = "super-secret-token"
        tls_enable = false
        work_conn_tls = false
    "#,
    );
    let out = run(&["server", "-c", cfg.to_str().unwrap(), "--check"]);
    assert!(out.status.success(), "check must succeed: {out:?}");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("server config OK"), "{stdout}");
    assert!(stdout.contains("127.0.0.1:7000"), "{stdout}");
    assert!(
        !stdout.contains("super-secret-token"),
        "token must never be printed: {stdout}"
    );
}

#[test]
fn check_client_config_prints_summary() {
    let cfg = tmp_config(
        "client",
        r#"
        [client]
        server_addr = "frp.example.com"
        server_port = 7000
        token = "super-secret-token"
        tls_enable = false
        work_conn_tls = false

        [[proxy]]
        name = "ssh"
        type = "tcp"
        local_port = 22
        remote_port = 6000
    "#,
    );
    let out = run(&["client", "-c", cfg.to_str().unwrap(), "--check"]);
    assert!(out.status.success(), "check must succeed: {out:?}");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("client config OK"), "{stdout}");
    assert!(stdout.contains("frp.example.com:7000"), "{stdout}");
    assert!(stdout.contains("remote_port=6000"), "{stdout}");
    assert!(
        !stdout.contains("super-secret-token"),
        "token must never be printed: {stdout}"
    );
}

#[test]
fn missing_config_exits_nonzero() {
    let out = run(&["server"]);
    assert!(!out.status.success(), "no -c must be a failure: {out:?}");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("-c"), "hint must mention -c: {stderr}");
}

#[test]
fn invalid_config_exits_nonzero() {
    // 缺少必填 token → 校验失败、非零退出。
    let cfg = tmp_config(
        "bad",
        r#"
        [server]
        tls_enable = false
        work_conn_tls = false
    "#,
    );
    let out = run(&["server", "-c", cfg.to_str().unwrap(), "--check"]);
    assert!(!out.status.success(), "invalid config must fail: {out:?}");
}
