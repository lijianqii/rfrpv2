//! 简单的每 IP 请求限频（滑动窗口计数），Dashboard 与客户端状态端点共用。

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// 限频表条目上限：达到后先清理过期项再插入（防不同源 IP 导致内存增长）。
const MAX_ENTRIES: usize = 4096;

/// 每 IP 滑动窗口限频器。
pub struct RateLimiter {
    inner: Mutex<HashMap<IpAddr, (u32, Instant)>>,
    max: u32,
    window: Duration,
}

impl RateLimiter {
    /// `max` 为窗口内允许的请求数，`window` 为窗口长度。
    pub fn new(max: u32, window: Duration) -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            max,
            window,
        }
    }

    /// 是否允许该 IP 的本次请求（`now` 由调用方提供，便于测试）。
    pub fn allow(&self, ip: IpAddr, now: Instant) -> bool {
        let mut m = self.inner.lock().unwrap();
        if let Some((count, start)) = m.get_mut(&ip) {
            if now.duration_since(*start) >= self.window {
                *count = 1;
                *start = now;
                true
            } else if *count < self.max {
                *count += 1;
                true
            } else {
                false
            }
        } else {
            if m.len() >= MAX_ENTRIES {
                m.retain(|_, (_, start)| now.duration_since(*start) < self.window);
            }
            m.insert(ip, (1, now));
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocks_after_limit_and_resets_after_window() {
        let limiter = RateLimiter::new(3, Duration::from_secs(60));
        let ip: IpAddr = "127.0.0.1".parse().unwrap();
        let now = Instant::now();
        assert!(limiter.allow(ip, now));
        assert!(limiter.allow(ip, now));
        assert!(limiter.allow(ip, now));
        assert!(!limiter.allow(ip, now), "4th request in window blocked");

        // 窗口过后重置。
        assert!(limiter.allow(ip, now + Duration::from_secs(61)));

        // 不同 IP 不受影响。
        assert!(limiter.allow("127.0.0.2".parse().unwrap(), now));
    }

    #[test]
    fn evicts_expired_entries_when_table_grows() {
        let limiter = RateLimiter::new(1, Duration::from_millis(10));
        let now = Instant::now();
        for i in 0..(MAX_ENTRIES + 1) {
            let ip = IpAddr::from(std::net::Ipv4Addr::from(i as u32 + 1));
            let _ = limiter.allow(ip, now - Duration::from_secs(1));
        }
        let _ = limiter.allow("9.9.9.9".parse().unwrap(), now);
        assert!(limiter.inner.lock().unwrap().len() < MAX_ENTRIES);
    }
}
