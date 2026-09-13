//! 统一信号处理：Unix 监听 SIGINT/SIGTERM，Windows 监听 Ctrl-C/Ctrl-Break，
//! 收到后触发 `CancellationToken` 优雅退出。

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
        #[cfg(windows)]
        {
            // Windows 没有 SIGTERM/SIGINT：同时监听 Ctrl-C 与 Ctrl-Break。
            // - 交互式终端用户使用 Ctrl-C；
            // - 服务/脚本/测试可用 GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT) 定向触发
            //   （CTRL_C_EVENT 无法限定到单个进程组，故不适用于自动化）。
            use tokio::signal::windows::{ctrl_break, ctrl_c};
            let mut c = match ctrl_c() {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!("install Ctrl-C handler failed: {e}");
                    let _ = tokio::signal::ctrl_c().await;
                    shutdown.cancel();
                    return;
                }
            };
            let mut b = match ctrl_break() {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!("install Ctrl-Break handler failed: {e}");
                    let _ = tokio::signal::ctrl_c().await;
                    shutdown.cancel();
                    return;
                }
            };
            tracing::info!("OS signal handler installed (Ctrl-C/Ctrl-Break)");
            tokio::select! {
                _ = c.recv() => {}
                _ = b.recv() => {}
            }
        }
        #[cfg(not(any(unix, windows)))]
        {
            tracing::info!("OS signal handler installed (Ctrl-C)");
            let _ = tokio::signal::ctrl_c().await;
        }
        shutdown.cancel();
    })
}
