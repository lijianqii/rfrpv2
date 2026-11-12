//! 日志初始化集成测试：用子进程覆盖全局 subscriber 只能初始化一次的分支。

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const BIN: &str = env!("CARGO_BIN_EXE_rfrp");

fn temp_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "rfrp-logging-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn write_server_config(dir: &Path, port: u16, output: &str, format: &str) -> PathBuf {
    let path = dir.join("server.toml");
    let cfg = format!(
        "[server]\n\
         bind_addr = \"127.0.0.1\"\n\
         bind_port = {port}\n\
         token = \"test-token\"\n\
         tls_enable = false\n\
         work_conn_tls = false\n\
         \n\
         [log]\n\
         level = \"info\"\n\
         output = \"{output}\"\n\
         format = \"{format}\"\n"
    );
    std::fs::write(&path, cfg).unwrap();
    path
}

fn run_server(config: &Path) -> Output {
    Command::new(BIN)
        .arg("server")
        .arg("-c")
        .arg(config)
        .output()
        .expect("run rfrp server")
}

#[test]
fn json_format_emits_structured_log_lines() {
    let dir = temp_dir("json");
    // 占住端口，让 Server::new 失败；日志初始化已在此前完成。
    let blocker = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = blocker.local_addr().unwrap().port();
    let cfg = write_server_config(&dir, port, "stderr", "json");

    let out = run_server(&cfg);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success(), "occupied port must fail: {stderr}");
    assert!(
        stderr.lines().any(|line| {
            line.starts_with('{') && line.contains("\"message\":\"failed to start server\"")
        }),
        "expected a JSON error log line, got:\n{stderr}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn missing_log_file_falls_back_to_stderr() {
    let dir = temp_dir("fallback");
    let blocker = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = blocker.local_addr().unwrap().port();
    // 父目录不存在，File::create/append 必然失败。
    let bad_log = dir.join("missing-subdir").join("rfrp.log");
    let output = format!("file:{}", bad_log.display());
    let cfg = write_server_config(&dir, port, &output, "text");

    let out = run_server(&cfg);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success(), "occupied port must fail: {stderr}");
    assert!(
        stderr.contains("failed to open log file"),
        "fallback warning missing:\n{stderr}"
    );
    assert!(
        stderr.contains("failed to start server"),
        "fallback logger must still emit the startup error:\n{stderr}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
