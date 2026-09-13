//! 真实信号路径混沌测试（DESIGN §14.4）。
//!
//! 对真实 `rfrp` 子进程发送退出/终止事件，验证终止面语义：
//! - **Unix**：SIGTERM / SIGINT 经信号 watcher 触发优雅退出（进程以 0 退出，
//!   而非被强杀或挂死）；SIGKILL 强制终止（验证"强杀无残留"的终止面）。
//! - **Windows**：没有 POSIX 信号，用等价机制覆盖同一语义：
//!   - 优雅退出：`GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT)`（子进程以
//!     `CREATE_NEW_PROCESS_GROUP` 创建，Ctrl-Break 可定向投递到该进程组，
//!     不会影响运行测试的进程）。服务端信号 watcher 同时监听 Ctrl-C/Ctrl-Break；
//!   - 强制终止：`TerminateProcess`（tokio `Child::kill`，等价 SIGKILL），
//!     并验证端口随进程退出被释放；
//!   - 无控制台环境（服务/CI）下无法投递 Ctrl-Break，优雅退出用例自动跳过，
//!     强制终止用例不依赖控制台、始终运行。
//!
//! 配置通过 `--grace-secs` 缩短宽限，避免测试等待默认 30s。

use std::path::PathBuf;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, BufReader, Lines};
use tokio::process::{Child, ChildStderr, Command};

const BIN: &str = env!("CARGO_BIN_EXE_rfrp");

/// 构造指向 rfrp 二进制的命令。
///
/// Windows 下额外设置 `CREATE_NEW_PROCESS_GROUP`：Ctrl-Break 可按进程组定向投递，
/// 同时避免测试终端自身的 Ctrl-C 事件波及子进程。
fn rfrp_command() -> Command {
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        let mut cmd = Command::new(BIN);
        cmd.as_std_mut()
            .creation_flags(windows_sys::Win32::System::Threading::CREATE_NEW_PROCESS_GROUP);
        cmd
    }
    #[cfg(not(windows))]
    {
        Command::new(BIN)
    }
}

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

/// 等待子进程 stderr 出现信号处理器安装完成的日志，确保事件投递前处理器已就绪。
///
/// 返回 stderr 行读取器，调用方需继续持有，避免提前关闭管道导致子进程日志写入失败。
async fn wait_signal_handler(child: &mut Child, what: &str) -> Lines<BufReader<ChildStderr>> {
    let stderr = child.stderr.take().expect("stderr must be piped");
    let mut lines = BufReader::new(stderr).lines();
    let ready = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(line) = lines.next_line().await.expect("read stderr") {
            if line.contains("OS signal handler installed") {
                return;
            }
        }
        panic!("{what} stderr closed before signal handler was installed");
    })
    .await;
    assert!(
        ready.is_ok(),
        "{what} should install signal handler before the test signal"
    );
    lines
}

// ---- Unix：POSIX 信号路径 ----

#[cfg(unix)]
fn send_signal(pid: u32, sig: libc::c_int) {
    unsafe {
        libc::kill(pid as libc::pid_t, sig);
    }
}

#[cfg(unix)]
#[tokio::test]
async fn sigterm_triggers_graceful_exit() {
    let port = free_port();
    let cfg = write_server_config(port);
    let mut child = rfrp_command()
        .args(["server", "-c", cfg.to_str().unwrap(), "--grace-secs", "1"])
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn rfrp server");
    let _lines = wait_signal_handler(&mut child, "server").await;

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

#[cfg(unix)]
#[tokio::test]
async fn sigint_triggers_graceful_exit() {
    let port = free_port();
    let cfg = write_server_config(port);
    let mut child = rfrp_command()
        .args(["server", "-c", cfg.to_str().unwrap(), "--grace-secs", "1"])
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn rfrp server");
    let _lines = wait_signal_handler(&mut child, "server").await;

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

#[cfg(unix)]
#[tokio::test]
async fn sigkill_forces_termination() {
    let port = free_port();
    let cfg = write_server_config(port);
    let mut child = rfrp_command()
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

#[cfg(unix)]
#[tokio::test]
async fn client_sigterm_stops_reconnect_loop() {
    // 客户端连不上服务端时持续重连；收到 SIGTERM 应停止重连并干净退出。
    // 不用固定 sleep，而是等待子进程日志出现“OS signal handler installed”，
    // 确保 tokio 信号处理器已经注册后再发 SIGTERM，避免高负载下信号未就绪导致误杀。
    let cfg = write_client_config(9); // 端口 9 无人监听
    let mut child = rfrp_command()
        .args(["client", "-c", cfg.to_str().unwrap()])
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn rfrp client");
    let _lines = wait_signal_handler(&mut child, "client").await;

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

// ---- Windows：控制台事件路径 ----

/// 当前进程是否有控制台：无控制台（服务/CI/分离启动）时 Ctrl-Break 事件无法投递。
#[cfg(windows)]
fn console_available() -> bool {
    !unsafe { windows_sys::Win32::System::Console::GetConsoleWindow() }.is_null()
}

/// 向 `pid` 所在的进程组投递 CTRL_BREAK_EVENT（等价 Ctrl+Break）。
#[cfg(windows)]
fn send_ctrl_break(pid: u32) -> bool {
    unsafe {
        windows_sys::Win32::System::Console::GenerateConsoleCtrlEvent(
            windows_sys::Win32::System::Console::CTRL_BREAK_EVENT,
            pid,
        ) != 0
    }
}

#[cfg(windows)]
#[tokio::test]
async fn ctrl_break_triggers_graceful_exit() {
    if !console_available() {
        eprintln!("skip ctrl_break test: no console available");
        return;
    }
    let port = free_port();
    let cfg = write_server_config(port);
    let mut child = rfrp_command()
        .args(["server", "-c", cfg.to_str().unwrap(), "--grace-secs", "1"])
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn rfrp server");
    let _lines = wait_signal_handler(&mut child, "server").await;

    assert!(
        send_ctrl_break(child.id().expect("pid")),
        "GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT) must succeed"
    );

    let status = tokio::time::timeout(Duration::from_secs(10), child.wait())
        .await
        .expect("child should exit after CTRL_BREAK")
        .unwrap();
    assert!(
        status.success(),
        "CTRL_BREAK should lead to clean exit (code 0), got {status:?}"
    );
}

#[cfg(windows)]
#[tokio::test]
async fn client_ctrl_break_stops_reconnect_loop() {
    if !console_available() {
        eprintln!("skip client ctrl_break test: no console available");
        return;
    }
    let cfg = write_client_config(9); // 端口 9 无人监听
    let mut child = rfrp_command()
        .args(["client", "-c", cfg.to_str().unwrap()])
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn rfrp client");
    let _lines = wait_signal_handler(&mut child, "client").await;

    assert!(
        send_ctrl_break(child.id().expect("pid")),
        "GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT) must succeed"
    );

    let status = tokio::time::timeout(Duration::from_secs(10), child.wait())
        .await
        .expect("client should exit after CTRL_BREAK")
        .unwrap();
    assert!(
        status.success(),
        "client CTRL_BREAK should lead to clean exit (code 0), got {status:?}"
    );
}

#[cfg(windows)]
#[tokio::test]
async fn force_kill_terminates_and_releases_port() {
    // TerminateProcess 等价 SIGKILL：进程必须退出（非 0），且监听端口随进程被 OS 回收。
    // 该用例不依赖控制台，任何环境都运行。
    let port = free_port();
    let cfg = write_server_config(port);
    let mut child = rfrp_command()
        .args(["server", "-c", cfg.to_str().unwrap(), "--grace-secs", "30"])
        .spawn()
        .expect("spawn rfrp server");
    wait_listening(port).await;

    child.kill().await.expect("force kill");

    let status = tokio::time::timeout(Duration::from_secs(10), child.wait())
        .await
        .expect("child should exit after force kill")
        .unwrap();
    assert!(
        !status.success(),
        "force kill must terminate the process (non-zero)"
    );

    // 进程退出后端口应可立即重新绑定（无残留监听）。
    let rebind = std::net::TcpListener::bind(("127.0.0.1", port));
    assert!(
        rebind.is_ok(),
        "port {port} must be released after force kill: {:?}",
        rebind.err()
    );
}
