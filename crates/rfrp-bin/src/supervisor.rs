//! `--watch-config=restart`：父进程监督子进程，配置变化时优雅重启。
//!
//! 父进程不监听业务端口，也不解析配置；它用同一套 CLI/`--check` 校验新配置，
//! 然后通过 stdin 管道让子进程优雅退出，再重新启动子进程。这样每次重启都走完整
//! 启动路径，不存在进程内配置迁移。
//!
//! 新配置的子进程在启动窗口内退出时，父进程会用最后一份正常配置启动回退子进程，
//! 并保留原配置文件不覆盖；服务先恢复到旧配置，等待下一次配置事件。

use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::process::{ExitCode, Stdio};
use std::time::{Duration, Instant};

use tokio::io::AsyncWriteExt;
use tokio::process::{Child, Command};
use tokio::sync::mpsc;
use tokio::time::{interval, sleep, timeout};
use tokio_util::sync::CancellationToken;

use rfrp_common::util::signal::spawn_signal_watcher;

use crate::watch::watch_config_file;

const DEBOUNCE: Duration = Duration::from_millis(300);
const STARTUP_GRACE: Duration = Duration::from_secs(3);
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

    let rollback_path = rollback_path(&config);
    let _ = std::fs::remove_file(&rollback_path);
    let mut last_good = std::fs::read(&config).ok();

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

    let mut active_args = child_args.clone();
    let mut child = match spawn_child(&exe, &active_args).await {
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
            match spawn_child(&exe, &active_args).await {
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

        match spawn_and_check(&exe, &child_args).await {
            Ok(new_child) => {
                child = new_child;
                active_args = child_args.clone();
                last_good = std::fs::read(&config).ok().or(last_good);
                let _ = std::fs::remove_file(&rollback_path);
                eprintln!(
                    "rfrp supervisor: started child pid={}",
                    child.id().unwrap_or(0)
                );
                backoff = BACKOFF_INITIAL;
            }
            Err(e) => {
                eprintln!("rfrp supervisor: new config child failed to start: {e}; rolling back");
                let Some(bytes) = last_good.as_ref() else {
                    eprintln!("rfrp supervisor: no last-good config available");
                    stop_child(&mut child).await;
                    sig.abort();
                    let _ = std::fs::remove_file(&rollback_path);
                    return ExitCode::FAILURE;
                };
                if let Err(write_err) = std::fs::write(&rollback_path, bytes) {
                    eprintln!("rfrp supervisor: cannot write rollback config: {write_err}");
                    sig.abort();
                    let _ = std::fs::remove_file(&rollback_path);
                    return ExitCode::FAILURE;
                }
                let rollback_args = match with_config_arg(&child_args, &rollback_path) {
                    Ok(args) => args,
                    Err(e) => {
                        eprintln!("rfrp supervisor: cannot build rollback args: {e}");
                        sig.abort();
                        let _ = std::fs::remove_file(&rollback_path);
                        return ExitCode::FAILURE;
                    }
                };
                match spawn_and_check(&exe, &rollback_args).await {
                    Ok(rollback_child) => {
                        child = rollback_child;
                        active_args = rollback_args;
                        eprintln!(
                            "rfrp supervisor: rollback child pid={} (config file unchanged)",
                            child.id().unwrap_or(0)
                        );
                        backoff = BACKOFF_INITIAL;
                    }
                    Err(e) => {
                        eprintln!("rfrp supervisor: rollback child failed to start: {e}");
                        sig.abort();
                        let _ = std::fs::remove_file(&rollback_path);
                        return ExitCode::FAILURE;
                    }
                }
            }
        }
    }

    stop_child(&mut child).await;
    let _ = std::fs::remove_file(&rollback_path);
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

async fn spawn_and_check(
    exe: &Path,
    child_args: &[OsString],
) -> std::result::Result<Child, String> {
    let mut child = spawn_child(exe, child_args)
        .await
        .map_err(|e| format!("spawn failed: {e}"))?;
    let deadline = Instant::now() + STARTUP_GRACE;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                return Err(format!("child exited during startup: {status}"));
            }
            Ok(None) => {}
            Err(e) => return Err(format!("try_wait failed: {e}")),
        }
        if Instant::now() >= deadline {
            return Ok(child);
        }
        sleep(Duration::from_millis(100)).await;
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

fn rollback_path(config: &Path) -> PathBuf {
    let name = config
        .file_name()
        .and_then(OsStr::to_str)
        .unwrap_or("config.toml");
    config.with_file_name(format!(".{name}.rollback.{}.toml", std::process::id()))
}

fn with_config_arg(args: &[OsString], config: &Path) -> std::result::Result<Vec<OsString>, String> {
    let mut out = Vec::with_capacity(args.len());
    let mut replaced = false;
    let mut i = 0;
    while i < args.len() {
        let text = args[i].to_string_lossy();
        if (text == "-c" || text == "--config") && i + 1 < args.len() {
            out.push(args[i].clone());
            out.push(config.as_os_str().to_os_string());
            i += 2;
            replaced = true;
            continue;
        }
        if text.starts_with("--config=") {
            let mut value = OsString::from("--config=");
            value.push(config);
            out.push(value);
            i += 1;
            replaced = true;
            continue;
        }
        if text.starts_with("-c=") {
            let mut value = OsString::from("-c=");
            value.push(config);
            out.push(value);
            i += 1;
            replaced = true;
            continue;
        }
        out.push(args[i].clone());
        i += 1;
    }
    if replaced {
        Ok(out)
    } else {
        Err("cannot locate -c/--config in supervisor argv".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replaces_separate_and_attached_config_args() {
        let path = Path::new("/tmp/new-config.toml");
        let args = vec![
            OsString::from("server"),
            OsString::from("-c"),
            OsString::from("old.toml"),
            OsString::from("--watch-config=restart"),
        ];
        let replaced = with_config_arg(&args, path).unwrap();
        assert_eq!(replaced[2], path.as_os_str());

        let args = vec![
            OsString::from("client"),
            OsString::from("--config=old.toml"),
        ];
        let replaced = with_config_arg(&args, path).unwrap();
        assert_eq!(replaced[1], "--config=/tmp/new-config.toml");

        let args = vec![OsString::from("client"), OsString::from("-c=old.toml")];
        let replaced = with_config_arg(&args, path).unwrap();
        assert_eq!(replaced[1], "-c=/tmp/new-config.toml");

        assert!(with_config_arg(&[OsString::from("server")], path).is_err());
    }

    #[test]
    fn rollback_path_stays_in_config_directory() {
        let config = Path::new("/etc/rfrp/server.toml");
        let path = rollback_path(config);
        assert_eq!(path.parent(), config.parent());
        assert!(path.to_string_lossy().contains("rollback"));
    }
}
