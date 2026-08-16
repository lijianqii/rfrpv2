//! 统一信号处理：监听 SIGINT/SIGTERM/Ctrl-C 并触发 `CancellationToken`。

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// 启动 OS 信号监听任务，收到信号后取消 `shutdown`。
///
/// 在 Unix 下显式注册 SIGINT 和 SIGTERM，避免 `tokio::signal::ctrl_c()` 在
/// `select!` 内才注册造成竞态。注册完成后输出 `OS signal handler installed` 日志，
/// 便于集成测试等待信号处理就绪。
pub fn spawn_signal_watcher(shutdown: CancellationToken) -> JoinHandle<()> {
    tokio::spawn(async move {
        if shutdown.is_cancelled() {
            return;
        }
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigint = match signal(SignalKind::interrupt()) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!("install SIGINT handler failed: {e}");
                    let _ = tokio::signal::ctrl_c().await;
                    shutdown.cancel();
                    return;
                }
            };
            let mut sigterm = match signal(SignalKind::terminate()) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!("install SIGTERM handler failed: {e}");
                    let _ = tokio::signal::ctrl_c().await;
                    shutdown.cancel();
                    return;
                }
            };
            tracing::info!("OS signal handler installed (SIGINT/SIGTERM)");
            tokio::select! {
                _ = sigint.recv() => {}
                _ = sigterm.recv() => {}
            }
        }
        #[cfg(not(unix))]
        {
            tracing::info!("OS signal handler installed (Ctrl-C)");
            let _ = tokio::signal::ctrl_c().await;
        }
        shutdown.cancel();
    })
}
