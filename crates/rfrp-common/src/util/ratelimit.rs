//! 简单的每 IP 请求限频（滑动窗口计数），Dashboard 与客户端状态端点共用。

use parking_lot::Mutex;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv6Addr};
use std::time::{Duration, Instant};

/// 限流键：IPv4 保持原样；IPv6 聚合到 /64，IPv4-mapped IPv6 归一为 IPv4。
///
/// 单个 IPv6 用户通常控制整个 /64；若按单地址限流，攻击者可轮换地址
/// 绕过 Dashboard/登录失败/请求频率限制。按 /64 聚合会牺牲少量共享同一
/// 前缀的客户端独立性，换取不可轻易绕过的限速。
pub fn rate_limit_key(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V4(v4) => IpAddr::V4(v4),
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return IpAddr::V4(v4);
            }
            let mut octets = v6.octets();
            octets[8..].fill(0);
            IpAddr::V6(Ipv6Addr::from(octets))
        }
    }
}

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
        let mut m = self.inner.lock();
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
                // 若仍满（窗口内活跃 IP 过多），淘汰最早条目；否则表会无界增长。
                if m.len() >= MAX_ENTRIES {
                    if let Some(oldest) = m
                        .iter()
                        .min_by_key(|(_, (_, start))| *start)
                        .map(|(ip, _)| *ip)
                    {
                        m.remove(&oldest);
                    }
                }
            }
            m.insert(ip, (1, now));
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

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
    fn evicts_oldest_when_all_entries_are_active() {
        // 窗口内活跃 IP 达到上限时，新 IP 仍可插入，但表大小不得继续增长。
        let limiter = RateLimiter::new(1, Duration::from_secs(60));
        let now = Instant::now();
        for i in 0..=MAX_ENTRIES {
            let ip = IpAddr::from(Ipv4Addr::from(i as u32 + 1));
            assert!(limiter.allow(ip, now));
        }
        assert!(limiter.inner.lock().len() <= MAX_ENTRIES);
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
        assert!(limiter.inner.lock().len() < MAX_ENTRIES);
    }

    #[test]
    fn rate_limit_key_aggregates_ipv6_by_64() {
        let a: IpAddr = "2001:db8:1:2::1".parse().unwrap();
        let b: IpAddr = "2001:db8:1:2::ffff".parse().unwrap();
        let c: IpAddr = "2001:db8:1:3::1".parse().unwrap();
        assert_eq!(rate_limit_key(a), rate_limit_key(b));
        assert_ne!(rate_limit_key(a), rate_limit_key(c));
    }

    #[test]
    fn rate_limit_key_maps_ipv4_mapped_ipv6_to_ipv4() {
        let mapped: IpAddr = "::ffff:192.0.2.1".parse().unwrap();
        let expected: IpAddr = "192.0.2.1".parse().unwrap();
        assert_eq!(rate_limit_key(mapped), expected);
    }
}
