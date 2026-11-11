//! `--watch-config=restart`：父进程监督子进程，配置变化时优雅重启。
//!
//! 父进程不监听业务端口，也不解析配置；它用同一套 CLI/`--check` 校验新配置，
//! 然后通过 stdin 管道让子进程优雅退出，再重新启动子进程。每次子进程启动都使用
//! 一个私有 stage 配置文件（0600），旧 stage 保留到新子进程确认启动成功后才删除；
//! 新子进程启动失败时直接回到旧 stage。这样避免了“校验的是 A、子进程读到 B”的
//! 配置快照竞态，也不会用新配置覆盖用户原文件。
//!
//! 父进程被强杀时，子进程 stdin 会 EOF 并优雅退出；stage 文件可能残留，但权限为
//! 0600，正常退出时会删除当前 active stage。

use std::ffi::{OsStr, OsString};
use std::fs;
#[cfg(unix)]
use std::fs::OpenOptions;
#[cfg(unix)]
use std::io::Write as _;
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

enum RollbackOutcome {
    StartedOld {
        child: Child,
        config_changed: bool,
    },
    StartedNew {
        child: Child,
        stage: PathBuf,
        args: Vec<OsString>,
    },
    Shutdown,
}

/// 启动 supervisor。`config` 用于文件监听，`args` 是完整 argv（含程序名）。
pub async fn run(config: PathBuf, args: Vec<OsString>) -> ExitCode {
    let exe = match std::env::current_exe() {
        Ok(exe) => exe,
        Err(e) => {
            eprintln!("rfrp supervisor: cannot resolve current executable: {e}");
            return ExitCode::FAILURE;
        }
    };
    let base_args: Vec<OsString> = args.into_iter().skip(1).collect();

    // 监听必须在读取/校验初始配置之前建立，避免启动窗口内的变更被漏掉。
    let (event_tx, mut event_rx) = mpsc::channel(1);
    let _watcher = match watch_config_file(&config, event_tx) {
        Ok(watcher) => watcher,
        Err(e) => {
            eprintln!("rfrp supervisor: cannot watch config: {e}");
            return ExitCode::FAILURE;
        }
    };

    let mut generation = 0u64;
    let mut active_stage = match stage_config(&config, generation) {
        Ok(path) => path,
        Err(e) => {
            eprintln!("rfrp supervisor: cannot create initial stage config: {e}");
            return ExitCode::FAILURE;
        }
    };
    let mut active_args = match with_config_arg(&base_args, &active_stage) {
        Ok(args) => args,
        Err(e) => {
            eprintln!("rfrp supervisor: cannot build child args: {e}");
            let _ = fs::remove_file(&active_stage);
            return ExitCode::FAILURE;
        }
    };
    if !validate(&exe, &active_args).await {
        eprintln!("rfrp supervisor: initial config validation failed; not starting");
        let _ = fs::remove_file(&active_stage);
        return ExitCode::FAILURE;
    }

    let shutdown = CancellationToken::new();
    let sig = spawn_signal_watcher(shutdown.clone());

    let mut child = match spawn_child(&exe, &active_args).await {
        Ok(child) => child,
        Err(e) => {
            eprintln!("rfrp supervisor: cannot start child: {e}");
            sig.abort();
            let _ = fs::remove_file(&active_stage);
            return ExitCode::FAILURE;
        }
    };
    eprintln!(
        "rfrp supervisor: started child pid={}",
        child.id().unwrap_or(0)
    );
    let mut child_started_at = Instant::now();

    let mut backoff = BACKOFF_INITIAL;
    let mut tick = interval(Duration::from_millis(500));
    // 回滚期间收到配置事件并只恢复了旧配置时，外层需要再处理一次当前配置。
    let mut pending_reload = false;
    loop {
        let mut reload_requested = pending_reload;
        pending_reload = false;
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
            // 稳定运行过的子进程崩溃不应继承历史退避（最多 30s）。
            if child_started_at.elapsed()
                >= Duration::from_secs(rfrp_common::constants::MIN_STABLE_CONNECTION_SECS)
            {
                backoff = BACKOFF_INITIAL;
            }
            if !wait_or_shutdown(backoff, &shutdown).await {
                break;
            }
            match spawn_child(&exe, &active_args).await {
                Ok(new_child) => {
                    child = new_child;
                    child_started_at = Instant::now();
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

        // 先把新配置复制到私有 stage，再校验 stage；子进程只读取这个快照，
        // 用户随后再次保存也不会让“校验的内容”和“实际启动的内容”不一致。
        generation += 1;
        let (new_stage, new_args) =
            match stage_and_validate(&exe, &config, &base_args, generation).await {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("rfrp supervisor: {e}; keeping current child");
                    continue;
                }
            };
        eprintln!("rfrp supervisor: config valid; restarting child");

        let old_stage = active_stage.clone();
        let old_args = active_args.clone();
        stop_child(&mut child).await;

        match spawn_and_check(&exe, &new_args).await {
            Ok(new_child) => {
                child = new_child;
                child_started_at = Instant::now();
                active_args = new_args;
                active_stage = new_stage;
                let _ = fs::remove_file(&old_stage);
                eprintln!(
                    "rfrp supervisor: started child pid={}",
                    child.id().unwrap_or(0)
                );
                backoff = BACKOFF_INITIAL;
            }
            Err(e) => {
                eprintln!("rfrp supervisor: new config child failed to start: {e}; rolling back");
                let _ = fs::remove_file(&new_stage);
                // 旧 stage 一直保留到新子进程成功后才删除，因此回滚是确定可用的。
                // 回滚重试期间继续消费配置事件：用户改出第三个合法配置时应尝试
                // 新配置，而不是永远卡在旧配置上。
                match spawn_rollback_with_retry(
                    &exe,
                    &old_args,
                    &shutdown,
                    &mut event_rx,
                    &config,
                    &base_args,
                    &mut generation,
                )
                .await
                {
                    RollbackOutcome::StartedOld {
                        child: rollback_child,
                        config_changed,
                    } => {
                        child = rollback_child;
                        child_started_at = Instant::now();
                        active_args = old_args;
                        active_stage = old_stage;
                        eprintln!(
                            "rfrp supervisor: rollback child pid={} (config file unchanged)",
                            child.id().unwrap_or(0)
                        );
                        backoff = BACKOFF_INITIAL;
                        pending_reload = config_changed;
                    }
                    RollbackOutcome::StartedNew {
                        child: new_child,
                        stage,
                        args,
                    } => {
                        child = new_child;
                        child_started_at = Instant::now();
                        active_args = args;
                        active_stage = stage;
                        let _ = fs::remove_file(&old_stage);
                        eprintln!(
                            "rfrp supervisor: started child pid={} with updated config",
                            child.id().unwrap_or(0)
                        );
                        backoff = BACKOFF_INITIAL;
                    }
                    RollbackOutcome::Shutdown => {
                        // 回滚重试期间收到退出信号。
                        sig.abort();
                        let _ = fs::remove_file(&old_stage);
                        return ExitCode::SUCCESS;
                    }
                }
            }
        }
    }

    stop_child(&mut child).await;
    let _ = fs::remove_file(&active_stage);
    sig.abort();
    ExitCode::SUCCESS
}

/// 将配置复制到同目录的私有 stage 文件；同目录保证相对证书路径仍能解析。
fn stage_config(config: &Path, generation: u64) -> std::result::Result<PathBuf, String> {
    let bytes = fs::read(config).map_err(|e| format!("read {}: {e}", config.display()))?;
    let path = stage_path(config, generation);
    write_private(&path, &bytes).map_err(|e| format!("write {}: {e}", path.display()))?;
    Ok(path)
}

fn stage_path(config: &Path, generation: u64) -> PathBuf {
    let name = config
        .file_name()
        .and_then(OsStr::to_str)
        .unwrap_or("config.toml");
    config.with_file_name(format!(
        ".{name}.stage.{}.{generation}.toml",
        std::process::id()
    ))
}

#[cfg(unix)]
fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

#[cfg(not(unix))]
fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    // Windows 下文件继承配置目录的 ACL；不额外引入平台 unsafe API。
    fs::write(path, bytes)
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

/// 新配置启动失败后，持续用旧 stage 尝试恢复；避免父进程直接退出导致长期离线。
///
/// 等待退避期间同时消费配置事件：旧配置也起不来时，用户改出的新配置仍有机会
/// 被立即尝试，而不是被回滚循环永久忽略。
async fn spawn_rollback_with_retry(
    exe: &Path,
    old_args: &[OsString],
    shutdown: &CancellationToken,
    event_rx: &mut mpsc::Receiver<()>,
    config: &Path,
    base_args: &[OsString],
    generation: &mut u64,
) -> RollbackOutcome {
    let mut delay = BACKOFF_INITIAL;
    let mut event_open = true;
    let mut config_changed = false;
    loop {
        match spawn_and_check(exe, old_args).await {
            Ok(child) => {
                return RollbackOutcome::StartedOld {
                    child,
                    config_changed,
                }
            }
            Err(e) => {
                eprintln!("rfrp supervisor: rollback child failed to start: {e}; retrying");
            }
        }

        let mut event_seen = false;
        tokio::select! {
            _ = sleep(delay) => {}
            _ = shutdown.cancelled() => return RollbackOutcome::Shutdown,
            event = event_rx.recv(), if event_open => {
                match event {
                    Some(()) => {
                        event_seen = true;
                        config_changed = true;
                    }
                    None => {
                        eprintln!("rfrp supervisor: config watcher closed during rollback");
                        event_open = false;
                    }
                }
            }
        }

        if event_seen {
            // 合并编辑器保存产生的连续事件，再尝试当前配置。
            sleep(DEBOUNCE).await;
            while event_rx.try_recv().is_ok() {}
            *generation += 1;
            match stage_and_validate(exe, config, base_args, *generation).await {
                Ok((stage, args)) => match spawn_and_check(exe, &args).await {
                    Ok(child) => {
                        return RollbackOutcome::StartedNew { child, stage, args };
                    }
                    Err(e) => {
                        eprintln!(
                            "rfrp supervisor: updated config child failed to start: {e}; continuing rollback"
                        );
                        let _ = fs::remove_file(&stage);
                    }
                },
                Err(e) => {
                    eprintln!("rfrp supervisor: updated config invalid: {e}; continuing rollback");
                }
            }
            // 事件处理后立即重试旧配置，不额外等待。
            continue;
        }
        delay = (delay * 2).min(BACKOFF_MAX);
    }
}

/// 复制当前配置到 stage 并校验；失败时清理 stage。
async fn stage_and_validate(
    exe: &Path,
    config: &Path,
    base_args: &[OsString],
    generation: u64,
) -> std::result::Result<(PathBuf, Vec<OsString>), String> {
    let stage = stage_config(config, generation)?;
    let args = match with_config_arg(base_args, &stage) {
        Ok(args) => args,
        Err(e) => {
            let _ = fs::remove_file(&stage);
            return Err(e);
        }
    };
    if !validate(exe, &args).await {
        let _ = fs::remove_file(&stage);
        return Err("config invalid".into());
    }
    Ok((stage, args))
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
        // Clap 也接受短选项粘连写法 `-cPATH`。
        if text.starts_with("-c") && text.len() > 2 && !text.starts_with("-c=") {
            let mut value = OsString::from("-c");
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

        let args = vec![OsString::from("server"), OsString::from("-cold.toml")];
        let replaced = with_config_arg(&args, path).unwrap();
        assert_eq!(replaced[1], "-c/tmp/new-config.toml");

        assert!(with_config_arg(&[OsString::from("server")], path).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn stage_file_is_private() {
        use std::os::unix::fs::PermissionsExt;
        let dir = std::env::temp_dir().join(format!(
            "rfrp-stage-mode-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or_default()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let config = dir.join("server.toml");
        std::fs::write(&config, "token = \"secret\"\n").unwrap();
        let stage = stage_config(&config, 0).unwrap();
        let mode = std::fs::metadata(&stage).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn stage_path_stays_in_config_directory() {
        let config = Path::new("/etc/rfrp/server.toml");
        let path = stage_path(config, 7);
        assert_eq!(path.parent(), config.parent());
        assert!(path.to_string_lossy().contains("stage"));
        assert!(path.to_string_lossy().contains(".7."));
    }
}
