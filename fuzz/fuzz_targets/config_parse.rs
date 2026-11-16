#![no_main]

use libfuzzer_sys::fuzz_target;
use rfrp_common::config::{ClientConfig, ServerConfig};

fuzz_target!(|data: &[u8]| {
    // 配置解析必须对任意 UTF-8 文本返回 Ok/Err，而不是 panic。
    if let Ok(text) = std::str::from_utf8(data) {
        let _ = toml::from_str::<ServerConfig>(text);
        let _ = toml::from_str::<ClientConfig>(text);
    }
});
