//! 配置文件变更通知（notify 文件系统事件）。
//!
//! 监听配置所在目录而不是文件本身：编辑器通常写临时文件后 `rename` 覆盖，
//! 只监听文件会收到 `Remove` 后永久失联。事件按目标文件名过滤，避免同目录日志/
//! 证书变化触发无意义重启。

use std::path::{Path, PathBuf};

use notify::{Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::mpsc;

/// 持有 watcher 的生命周期；drop 后停止监听。
pub struct ConfigWatcher {
    _watcher: RecommendedWatcher,
}

/// 启动配置文件监听。事件通过 `events` 发送（无界通道，回调内不得阻塞）。
pub fn watch_config_file(
    path: &Path,
    events: mpsc::UnboundedSender<()>,
) -> notify::Result<ConfigWatcher> {
    let absolute = absolute_path(path);
    let parent = absolute
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    let file_name = absolute.file_name().map(|n| n.to_os_string());

    let mut watcher = RecommendedWatcher::new(
        move |result: notify::Result<Event>| {
            let Ok(event) = result else {
                return;
            };
            if matches!(event.kind, EventKind::Access(_)) {
                return;
            }
            let relevant = match file_name.as_ref() {
                Some(name) => event
                    .paths
                    .iter()
                    .any(|p| p.file_name() == Some(name.as_os_str())),
                None => true,
            };
            if relevant {
                let _ = events.send(());
            }
        },
        Config::default(),
    )?;
    watcher.watch(&parent, RecursiveMode::NonRecursive)?;
    tracing::info!(
        path = %absolute.display(),
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

        let (tx, mut rx) = mpsc::unbounded_channel();
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
