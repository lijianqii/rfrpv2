//! 日志初始化（tracing + tracing-subscriber）。
//!
//! M0 支持 stderr / env-filter；file 输出（DESIGN §9.1 `output = "file:/path"`）
//! 将在 M5 接入。CLI 的 `--log-level` 覆盖配置或环境变量。

use tracing_subscriber::{fmt, EnvFilter};

/// 初始化全局 subscriber。
///
/// `level_override` 为非空时优先作为过滤指令（如 `debug`、`info,rfrp_common=debug`）。
pub fn init_logging(level_override: Option<&str>) {
    let filter = match level_override {
        Some(lvl) => EnvFilter::try_new(lvl).unwrap_or_else(|_| EnvFilter::new("info")),
        None => EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
    };

    // M2 及之前仅 stderr 文本输出；file/json 输出在 M5 完善。
    // 显式指定 stderr，避免 tracing-subscriber 默认写到 stdout 与 DESIGN §9.1 不符。
    let _ = fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(filter)
        .try_init();
}
