//! 轻量任务跟踪器：登记后台任务，优雅退出时等待其结束。
//!
//! 只依赖 `tokio::sync::Notify`，避免为了 `tokio-util` 的 `TaskTracker`
//! 额外启用 feature。`TaskGroup` 可克隆，供 `ServerState` 与各监听循环共享。

use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::sync::Notify;
use tokio::task::JoinHandle;

#[derive(Clone)]
pub struct TaskGroup {
    inner: Arc<Inner>,
}

struct Inner {
    active: AtomicUsize,
    closed: AtomicBool,
    notify: Notify,
}

struct ActiveGuard(Arc<Inner>);

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        if self.0.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            // 唤醒等待者；等待端在检查前后都重新读 active，不依赖许可存储。
            self.0.notify.notify_waiters();
        }
    }
}

impl TaskGroup {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                active: AtomicUsize::new(0),
                closed: AtomicBool::new(false),
                notify: Notify::new(),
            }),
        }
    }

    /// 登记并 spawn 一个任务；任务 panic 时 guard 仍会递减计数。
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.inner.active.fetch_add(1, Ordering::AcqRel);
        let guard = ActiveGuard(self.inner.clone());
        tokio::spawn(async move {
            let _guard = guard;
            future.await
        })
    }

    /// 标记不再期望新任务。调用后仍应避免继续 spawn；等待请使用
    /// [`TaskGroup::close_and_wait`]（它会先 close 再等待）。
    pub fn close(&self) {
        self.inner.closed.store(true, Ordering::Release);
        // 若 wait 在 close 之前进入等待，此时 active 可能已经为 0，
        // 不会再有 ActiveGuard drop 来触发通知；必须在这里主动唤醒。
        self.inner.notify.notify_waiters();
    }

    pub fn is_empty(&self) -> bool {
        self.inner.active.load(Ordering::Acquire) == 0
    }

    /// 关闭任务组并等待所有已登记任务结束。
    ///
    /// 这是公开的等待入口：内部先 `close` 再 `wait`，避免调用方忘记 close
    /// 导致 active 已为 0 时仍永久等待。退出路径应先停止 accept，再调用本方法。
    pub async fn close_and_wait(&self) {
        self.close();
        self.wait().await;
    }

    /// 等待所有已登记任务结束。仅由 [`TaskGroup::close_and_wait`] 调用；
    /// 单独调用时若未 close，会一直等到 close 或最后一个任务结束。
    async fn wait(&self) {
        loop {
            let notified = self.inner.notify.notified();
            if self.inner.closed.load(Ordering::Acquire)
                && self.inner.active.load(Ordering::Acquire) == 0
            {
                return;
            }
            notified.await;
        }
    }
}

impl Default for TaskGroup {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn waits_for_spawned_tasks() {
        let group = TaskGroup::new();
        group.spawn(async {
            tokio::time::sleep(Duration::from_millis(50)).await;
        });
        group.close();
        tokio::time::timeout(Duration::from_secs(1), group.close_and_wait())
            .await
            .expect("task group must finish");
        assert!(group.is_empty());
    }

    #[tokio::test]
    async fn panic_still_releases_slot() {
        let group = TaskGroup::new();
        let task = group.spawn(async {
            panic!("expected panic in test task");
        });
        let _ = task.await;
        group.close();
        tokio::time::timeout(Duration::from_secs(1), group.close_and_wait())
            .await
            .expect("panicked task must release its slot");
        assert!(group.is_empty());
    }

    #[tokio::test]
    async fn close_wakes_wait_started_before_close() {
        let group = TaskGroup::new();
        let waiter = {
            let group = group.clone();
            tokio::spawn(async move {
                group.wait().await;
            })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        group.close();
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("wait must be woken by close even with no active tasks")
            .unwrap();
    }
}
