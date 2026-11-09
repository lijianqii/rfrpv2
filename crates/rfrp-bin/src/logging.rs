//! 日志初始化（tracing + tracing-subscriber）。
//!
//! 支持 stderr 与 `file:/path/to.log` 输出，支持 text/json 格式。
//! 优先级：CLI 参数 > 配置文件 `[log]` > 默认值。
//!
//! 日志**级别**支持运行时热重载（`[log].level`）；输出位置与格式涉及 writer /
//! layer 结构，变更时保留旧配置并提示需要重启。

use std::fs::File;
use std::io::IsTerminal;
// 这里保持 `std::sync::Mutex`：`tracing-subscriber` 的 `MakeWriter` 针对标准库
// `Mutex` 实现，且日志写者与运行期共享状态无关（其余共享锁统一用 parking_lot）。
use std::sync::Mutex;

use rfrp_common::config::LogSection;
use tracing_subscriber::fmt;
use tracing_subscriber::fmt::writer::BoxMakeWriter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::reload;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Registry;

/// 日志热重载句柄；仅在全局 subscriber 安装成功时返回。
pub struct LogReloadHandle {
    handle: reload::Handle<EnvFilter, Registry>,
    /// CLI `--log-level` 优先，配置文件中的 level 变更不得覆盖它。
    level_locked: bool,
    initial_output: String,
    initial_format: String,
}

impl LogReloadHandle {
    /// 应用新的 `[log]` 配置。
    ///
    /// 返回 `Err` 表示本次变更未应用（旧日志配置继续生效），调用方记录一次告警即可。
    pub fn reload(&self, log: &LogSection) -> Result<(), String> {
        if self.level_locked {
            return Err("log.level is controlled by --log-level; config change ignored".into());
        }
        let output = log.output.as_deref().unwrap_or("stderr");
        let format = log.format.as_deref().unwrap_or("text");
        if output != self.initial_output || format != self.initial_format {
            return Err("log.output/log.format change requires restart".into());
        }
        let level = log.level.as_deref().unwrap_or("info");
        let filter =
            EnvFilter::try_new(level).map_err(|e| format!("invalid log.level {level:?}: {e}"))?;
        self.handle
            .reload(filter)
            .map_err(|e| format!("failed to reload log filter: {e}"))
    }
}

/// 初始化全局 subscriber。
///
/// `level_override` / `output_override` / `format_override` 已经由调用方按
/// “CLI 参数 > 配置文件 > 默认值”合并完成；`level_locked` 表示 level 来自
/// CLI（配置热重载不得覆盖）。
///
/// 返回值用于后续热重载；如果全局 subscriber 已安装（测试或重复初始化），
/// 返回 `None`，原有 subscriber 保持不变。
pub fn init_logging(
    level_override: Option<&str>,
    output_override: Option<&str>,
    format_override: Option<&str>,
    level_locked: bool,
) -> Option<LogReloadHandle> {
    let level = level_override.unwrap_or("info");
    let output = output_override.unwrap_or("stderr").to_string();
    let format = format_override.unwrap_or("text").to_string();

    let filter = EnvFilter::try_new(level).unwrap_or_else(|_| EnvFilter::new("info"));
    let (filter_layer, reload_handle) = reload::Layer::new(filter);
    let is_json = format.eq_ignore_ascii_case("json");

    let (writer, ansi): (BoxMakeWriter, bool) = if let Some(path) = output.strip_prefix("file:") {
        match File::options().create(true).append(true).open(path) {
            Ok(file) => (BoxMakeWriter::new(Mutex::new(file)), false),
            Err(e) => {
                eprintln!("failed to open log file {path}: {e}; falling back to stderr");
                (BoxMakeWriter::new(std::io::stderr), ansi_enabled())
            }
        }
    } else {
        (BoxMakeWriter::new(std::io::stderr), ansi_enabled())
    };

    // 不同日志格式产生不同的 fmt layer 类型；用 `Box<dyn Subscriber>` 统一后
    // 交给 `tracing` 安装，避免为 text/json 写两套 subscriber 类型。
    let subscriber: Box<dyn tracing::Subscriber + Send + Sync> = if is_json {
        Box::new(
            tracing_subscriber::registry()
                .with(filter_layer)
                .with(fmt::layer().json().with_ansi(false).with_writer(writer)),
        )
    } else {
        Box::new(
            tracing_subscriber::registry()
                .with(filter_layer)
                .with(fmt::layer().with_ansi(ansi).with_writer(writer)),
        )
    };
    let installed = tracing::subscriber::set_global_default(subscriber).is_ok();
    if !installed {
        return None;
    }
    Some(LogReloadHandle {
        handle: reload_handle,
        level_locked,
        initial_output: output,
        initial_format: format,
    })
}

/// 判断 stderr 日志是否应启用 ANSI 颜色。
///
/// Windows 下 PowerShell/旧终端对 ANSI 支持不稳定，默认关闭；
/// 非终端（管道/重定向/服务）也关闭；设置 `NO_COLOR` 时关闭。
fn ansi_enabled() -> bool {
    if cfg!(windows) {
        return false;
    }
    if !std::io::stderr().is_terminal() {
        return false;
    }
    if std::env::var_os("NO_COLOR").is_some() {
        return false;
    }
    true
}
