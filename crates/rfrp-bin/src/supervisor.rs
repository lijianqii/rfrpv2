//! `--watch-config=restart`：父进程监督子进程，配置变化时优雅重启。
//!
//! 父进程不监听业务端口，也不解析配置；它用同一套 CLI/`--check` 校验新配置，
//! 然后通过 stdin 管道让子进程优雅退出，再重新启动子进程。这样每次重启都走完整
//! 启动路径，不存在进程内配置迁移。

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::{ExitCode, Stdio};
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::process::{Child, Command};
use tokio::sync::mpsc;
use tokio::time::{interval, sleep, timeout};
use tokio_util::sync::CancellationToken;

use rfrp_common::util::signal::spawn_signal_watcher;

use crate::watch::watch_config_file;

const DEBOUNCE: Duration = Duration::from_millis(300);
const STOP_TIMEOUT: Duration = Duration::from_secs(10);
const BACKOFF_INITIAL: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

/// 启动 supervisor。`config` 用于文件监听，`args` 是完整 argv（含程序名）。
pub async fn run(config: PathBuf, args: Vec<OsString>) -> ExitCode {
    let exe = match std::env::current_exe() {
        Ok(exe) => exe,
        Err(e) => {
            eprintln!("rfrp supervisor: cannot resolve current executable: {e}");
            return ExitCode::FAILURE;
        }
    };
    let child_args: Vec<OsString> = args.into_iter().skip(1).collect();

    // 初始配置不合法时不要进入“启动失败→退避→再启动”的死循环。
    if !validate(&exe, &child_args).await {
        eprintln!("rfrp supervisor: initial config validation failed; not starting");
        return ExitCode::FAILURE;
    }

    let (event_tx, mut event_rx) = mpsc::unbounded_channel();
    let _watcher = match watch_config_file(&config, event_tx) {
        Ok(watcher) => watcher,
        Err(e) => {
            eprintln!("rfrp supervisor: cannot watch config: {e}");
            return ExitCode::FAILURE;
        }
    };

    let shutdown = CancellationToken::new();
    let sig = spawn_signal_watcher(shutdown.clone());

    let mut child = match spawn_child(&exe, &child_args).await {
        Ok(child) => child,
        Err(e) => {
            eprintln!("rfrp supervisor: cannot start child: {e}");
            sig.abort();
            return ExitCode::FAILURE;
        }
    };
    eprintln!(
        "rfrp supervisor: started child pid={}",
        child.id().unwrap_or(0)
    );

    let mut backoff = BACKOFF_INITIAL;
    let mut tick = interval(Duration::from_millis(500));
    loop {
        let mut reload_requested = false;
        let mut child_status = None;
        tokio::select! {
            _ = shutdown.cancelled() => {
                eprintln!("rfrp supervisor: shutdown requested");
                break;
            }
            _ = tick.tick() => {
                // 不能用 child.wait() 的 future 常驻在 select! 里：std/tokio 的
                // wait 会在等待前关闭 stdin，导致子进程的 supervisor 管道立即 EOF。
                match child.try_wait() {
                    Ok(Some(status)) => child_status = Some(status),
                    Ok(None) => {}
                    Err(e) => eprintln!("rfrp supervisor: child try_wait failed: {e}"),
                }
            }
            event = event_rx.recv() => {
                match event {
                    Some(()) => reload_requested = true,
                    None => {
                        eprintln!("rfrp supervisor: config watcher closed");
                        break;
                    }
                }
            }
        }

        if let Some(status) = child_status {
            eprintln!(
                "rfrp supervisor: child exited unexpectedly ({}); restarting",
                status
            );
            if !wait_or_shutdown(backoff, &shutdown).await {
                break;
            }
            match spawn_child(&exe, &child_args).await {
                Ok(new_child) => {
                    child = new_child;
                    eprintln!(
                        "rfrp supervisor: started child pid={}",
                        child.id().unwrap_or(0)
                    );
                    backoff = (backoff * 2).min(BACKOFF_MAX);
                }
                Err(e) => {
                    eprintln!("rfrp supervisor: cannot restart child: {e}");
                    backoff = (backoff * 2).min(BACKOFF_MAX);
                }
            }
            continue;
        }

        if !reload_requested {
            continue;
        }
        // 合并编辑器保存产生的连续事件。
        sleep(DEBOUNCE).await;
        while event_rx.try_recv().is_ok() {}

        if !validate(&exe, &child_args).await {
            eprintln!("rfrp supervisor: config invalid; keeping current child");
            continue;
        }
        eprintln!("rfrp supervisor: config valid; restarting child");
        stop_child(&mut child).await;
        match spawn_child(&exe, &child_args).await {
            Ok(new_child) => {
                child = new_child;
                eprintln!(
                    "rfrp supervisor: started child pid={}",
                    child.id().unwrap_or(0)
                );
                backoff = BACKOFF_INITIAL;
            }
            Err(e) => {
                eprintln!("rfrp supervisor: cannot start child after config change: {e}");
                // 下一轮意外退出路径会继续退避重试。
            }
        }
    }

    stop_child(&mut child).await;
    sig.abort();
    ExitCode::SUCCESS
}

/// 使用同一 argv 运行 `--check`，确保 CLI 覆盖和校验逻辑与子进程启动完全一致。
async fn validate(exe: &Path, child_args: &[OsString]) -> bool {
    match Command::new(exe)
        .args(child_args)
        .arg("--check")
        .env("RFRP_WATCH_CHILD", "1")
        .env("RFRP_SUPERVISOR_PIPE", "1")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .status()
        .await
    {
        Ok(status) => status.success(),
        Err(e) => {
            eprintln!("rfrp supervisor: config validation failed to run: {e}");
            false
        }
    }
}

async fn spawn_child(exe: &Path, child_args: &[OsString]) -> std::io::Result<Child> {
    Command::new(exe)
        .args(child_args)
        .env("RFRP_WATCH_CHILD", "1")
        .env("RFRP_SUPERVISOR_PIPE", "1")
        .stdin(Stdio::piped())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
}

async fn wait_or_shutdown(backoff: Duration, shutdown: &CancellationToken) -> bool {
    tokio::select! {
        _ = sleep(backoff) => true,
        _ = shutdown.cancelled() => false,
    }
}

async fn stop_child(child: &mut Child) {
    if let Some(stdin) = child.stdin.as_mut() {
        let _ = stdin.write_all(b"shutdown\n").await;
        let _ = stdin.flush().await;
    }
    match timeout(STOP_TIMEOUT, child.wait()).await {
        Ok(_) => {}
        Err(_) => {
            eprintln!("rfrp supervisor: child did not stop in time; killing");
            let _ = child.kill().await;
            let _ = child.wait().await;
        }
    }
}
