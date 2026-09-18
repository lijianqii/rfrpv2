//! accept 循环的瞬时错误退避策略（服务端主监听、代理监听、vhost / Dashboard /
//! 客户端状态端点共用）。
//!
//! `accept()` 出错（`EMFILE`、握手期对端重置、fd 耗尽等）绝大多数是**瞬时**的。
//! 这时若直接 `break` 结束循环，监听会永久失效而进程仍然健在——控制连接还在、
//! 客户端以为自己在线，用户侧却只能看到 connection refused，属于最难排查的
//! "静默故障"。本策略把退避与日志抑制集中在一处，避免各监听循环各写一套。

use std::time::Duration;

use crate::constants::MAX_CONSECUTIVE_ACCEPT_ERRORS;

/// 退避上限：1s（持续失败时以每秒一次的节奏重试，fd 释放后自动恢复）。
const BACKOFF_MAX_MS: u64 = 1000;
/// 日志抑制周期：连续失败每增加这么多次才再记一条（配合 1s 退避约每分钟一条）。
const LOG_EVERY: u32 = 60;

/// accept 错误退避计数器。
#[derive(Debug, Default)]
pub struct AcceptRetry {
    consecutive: u32,
}

impl AcceptRetry {
    pub fn new() -> Self {
        Self::default()
    }

    /// 记录一次成功 accept（清零计数）。
    pub fn record_ok(&mut self) {
        self.consecutive = 0;
    }

    /// 记录一次失败，返回本次应退避的时长。
    pub fn record_err(&mut self) -> Duration {
        self.consecutive = self.consecutive.saturating_add(1);
        Duration::from_millis((self.consecutive as u64 * 100).min(BACKOFF_MAX_MS))
    }

    /// 连续失败次数。
    pub fn consecutive(&self) -> u32 {
        self.consecutive
    }

    /// 是否建议输出一条日志（首次、每 [`LOG_EVERY`] 次、以及达到阈值时），
    /// 避免持续失败时每秒一条刷屏。
    pub fn should_log(&self) -> bool {
        let n = self.consecutive;
        n == 1 || n % LOG_EVERY == 0 || n == MAX_CONSECUTIVE_ACCEPT_ERRORS
    }

    /// 是否已达到"监听判定不可恢复"的连续失败次数。
    ///
    /// 仅**主监听**用它决定退出进程（交服务管理器重启）；代理/辅助监听的生命周期
    /// 绑定在会话或进程上，应持续重试而不是自行退出。
    pub fn is_fatal(&self) -> bool {
        self.consecutive >= MAX_CONSECUTIVE_ACCEPT_ERRORS
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_then_caps() {
        let mut r = AcceptRetry::new();
        assert_eq!(r.record_err(), Duration::from_millis(100));
        assert_eq!(r.record_err(), Duration::from_millis(200));
        for _ in 0..8 {
            r.record_err();
        }
        assert_eq!(r.consecutive(), 10);
        assert_eq!(r.record_err(), Duration::from_millis(BACKOFF_MAX_MS));
        for _ in 0..100 {
            assert_eq!(r.record_err(), Duration::from_millis(BACKOFF_MAX_MS));
        }
    }

    #[test]
    fn success_resets() {
        let mut r = AcceptRetry::new();
        r.record_err();
        r.record_err();
        r.record_ok();
        assert_eq!(r.consecutive(), 0);
        assert_eq!(r.record_err(), Duration::from_millis(100));
    }

    #[test]
    fn logs_are_rate_limited() {
        let mut r = AcceptRetry::new();
        assert!(r.should_log(), "首次失败应记录");
        r.record_err();
        assert!(r.should_log());
        for _ in 1..LOG_EVERY {
            r.record_err();
            if r.consecutive() != LOG_EVERY {
                assert!(!r.should_log(), "中间失败不应刷屏: {}", r.consecutive());
            }
        }
        assert_eq!(r.consecutive(), LOG_EVERY);
        assert!(r.should_log(), "每 LOG_EVERY 次应记录一次");
    }

    #[test]
    fn fatal_at_threshold() {
        let mut r = AcceptRetry::new();
        for _ in 1..MAX_CONSECUTIVE_ACCEPT_ERRORS {
            r.record_err();
        }
        assert!(!r.is_fatal());
        r.record_err();
        assert!(r.is_fatal());
        assert!(r.should_log());
        r.record_ok();
        assert!(!r.is_fatal());
    }
}
