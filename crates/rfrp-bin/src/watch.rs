//! 配置文件变更通知（notify 文件系统事件）。
//!
//! 监听配置所在目录而不是文件本身：编辑器通常写临时文件后 `rename` 覆盖，
//! 只监听文件会收到 `Remove` 后永久失联。事件按目标文件名过滤，避免同目录日志/
//! 证书变化触发无意义重启。

use std::ffi::OsString;
use std::path::{Path, PathBuf};

use notify::{Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::mpsc;

/// 持有 watcher 的生命周期；drop 后停止监听。
pub struct ConfigWatcher {
    _watcher: RecommendedWatcher,
}

/// 比较事件文件名与目标文件名；Windows 文件系统大小写不敏感。
fn same_file_name(a: &std::ffi::OsStr, b: &std::ffi::OsStr) -> bool {
    #[cfg(windows)]
    {
        a.to_string_lossy()
            .eq_ignore_ascii_case(&b.to_string_lossy())
    }
    #[cfg(not(windows))]
    {
        a == b
    }
}

/// 启动配置文件监听。事件通过容量为 1 的通道发送（无界通道在事件风暴下会增长）。
pub fn watch_config_file(path: &Path, events: mpsc::Sender<()>) -> notify::Result<ConfigWatcher> {
    let absolute = absolute_path(path);
    // 配置可能是符号链接：同时监听链接所在目录和目标文件所在目录。
    let target = std::fs::canonicalize(&absolute).unwrap_or_else(|_| absolute.clone());
    let parent = absolute
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    let target_parent = target
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    let mut names: Vec<OsString> = Vec::with_capacity(2);
    if let Some(name) = absolute.file_name() {
        names.push(name.to_os_string());
    }
    if let Some(name) = target.file_name() {
        if !names.iter().any(|n| n.as_os_str() == name) {
            names.push(name.to_os_string());
        }
    }

    let mut watcher = RecommendedWatcher::new(
        move |result: notify::Result<Event>| {
            let event = match result {
                Ok(event) => event,
                Err(e) => {
                    // supervisor 父进程没有 tracing subscriber，这里直接写 stderr，
                    // exit 模式也能看到；同时触发一次重新校验，避免静默丢事件。
                    eprintln!("rfrp config watcher error: {e}; scheduling reload");
                    let _ = events.try_send(());
                    return;
                }
            };
            // inotify/Windows 缓冲溢出会标记 rescan：必须按“可能有任意变更”处理。
            if event.need_rescan() {
                eprintln!("rfrp config watcher requested rescan; scheduling reload");
                let _ = events.try_send(());
            }
            if matches!(event.kind, EventKind::Access(_)) {
                return;
            }
            let relevant = if names.is_empty() {
                true
            } else {
                event.paths.iter().any(|p| {
                    p.file_name()
                        .is_some_and(|name| names.iter().any(|n| same_file_name(name, n)))
                })
            };
            if relevant {
                let _ = events.try_send(());
            }
        },
        Config::default(),
    )?;
    watcher.watch(&parent, RecursiveMode::NonRecursive)?;
    if target_parent != parent {
        watcher.watch(&target_parent, RecursiveMode::NonRecursive)?;
    }
    tracing::info!(
        path = %absolute.display(),
        target = %target.display(),
        dir = %parent.display(),
        "config file watcher started"
    );
    Ok(ConfigWatcher { _watcher: watcher })
}

fn absolute_path(path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map(|cwd| cwd.join(path))
            .unwrap_or_else(|_| path.to_path_buf())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn absolute_path_keeps_absolute_and_joins_relative() {
        let abs = Path::new("/tmp/config.toml");
        assert_eq!(absolute_path(abs), abs);
        let rel = absolute_path(Path::new("config.toml"));
        assert!(rel.is_absolute());
        assert!(rel.ends_with("config.toml"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn watcher_follows_symlink_target() {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default();
        let dir =
            std::env::temp_dir().join(format!("rfrp-watch-link-{}-{stamp}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let real = dir.join("real.toml");
        let link = dir.join("server.toml");
        std::fs::write(&real, "a").unwrap();
        std::os::unix::fs::symlink(&real, &link).unwrap();

        let (tx, mut rx) = mpsc::channel(1);
        let _watcher = watch_config_file(&link, tx).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        std::fs::write(&real, "b").unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
                .await
                .is_ok(),
            "editing the symlink target must trigger reload"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn watcher_notifies_on_target_write_and_ignores_other_files() {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default();
        let dir =
            std::env::temp_dir().join(format!("rfrp-watch-test-{}-{stamp}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let config = dir.join("server.toml");
        std::fs::write(&config, "x").unwrap();

        let (tx, mut rx) = mpsc::channel(1);
        let _watcher = watch_config_file(&config, tx).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let other = dir.join("other.log");
        std::fs::write(&other, "noise").unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(200), rx.recv())
                .await
                .is_err(),
            "unrelated file must not trigger config reload"
        );

        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&config)
            .unwrap();
        f.write_all(b"y").unwrap();
        drop(f);
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
                .await
                .is_ok(),
            "target config write must trigger reload"
        );
        while rx.try_recv().is_ok() {}

        let tmp = dir.join("server.toml.new");
        std::fs::write(&tmp, "z").unwrap();
        std::fs::rename(&tmp, &config).unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
                .await
                .is_ok(),
            "atomic rename of target config must trigger reload"
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}
