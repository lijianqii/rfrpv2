//! 通用工具模块。
pub mod bridge;
pub mod platform;
pub mod signal;
pub mod stream;

use std::time::{SystemTime, UNIX_EPOCH};

/// 当前 Unix 毫秒时间戳（DESIGN §6.2：心跳 `ts` 用 `SystemTime`，不引第三方时间库）。
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
