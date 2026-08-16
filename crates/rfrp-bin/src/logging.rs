//! 日志初始化（tracing + tracing-subscriber）。
//!
//! 支持 stderr 与 `file:/path/to.log` 输出，支持 text/json 格式。
//! 优先级：CLI 参数 > 配置文件 `[log]` > 默认值。

use std::fs::File;
use std::sync::Mutex;

use tracing_subscriber::fmt;
use tracing_subscriber::fmt::writer::BoxMakeWriter;
use tracing_subscriber::EnvFilter;

/// 初始化全局 subscriber。
///
/// `level_override` / `output_override` / `format_override` 已经由调用方按
/// “CLI 参数 > 配置文件 > 默认值”合并完成；这里只负责真正安装。
pub fn init_logging(
    level_override: Option<&str>,
    output_override: Option<&str>,
    format_override: Option<&str>,
) {
    let level = level_override.unwrap_or("info");
    let output = output_override.unwrap_or("stderr");
    let format = format_override.unwrap_or("text");

    let filter = EnvFilter::try_new(level).unwrap_or_else(|_| EnvFilter::new("info"));
    let is_json = format.eq_ignore_ascii_case("json");

    let install = |writer: tracing_subscriber::fmt::writer::BoxMakeWriter| {
        if is_json {
            fmt()
                .json()
                .with_env_filter(filter.clone())
                .with_writer(writer)
                .try_init()
        } else {
            fmt()
                .with_env_filter(filter.clone())
                .with_writer(writer)
                .try_init()
        }
    };

    if let Some(path) = output.strip_prefix("file:") {
        match File::options().create(true).append(true).open(path) {
            Ok(file) => {
                let _ = install(BoxMakeWriter::new(Mutex::new(file)));
            }
            Err(e) => {
                eprintln!("failed to open log file {path}: {e}; falling back to stderr");
                let _ = install(BoxMakeWriter::new(std::io::stderr));
            }
        }
    } else {
        let _ = install(BoxMakeWriter::new(std::io::stderr));
    }
}
