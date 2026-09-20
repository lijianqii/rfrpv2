//! 统一错误类型。
//!
//! 库内部错误用 `Error`，上层（如 rfrp-bin）可用 `anyhow` 承接。所有底层错误
//! 通过 `#[from]` 收敛到本类型，保持错误链完整。

use std::io::ErrorKind;
use thiserror::Error;

/// 将 `std::io::Error` 渲染为稳定的 rfrp 错误描述。
///
/// 直接用 `io::Error` 的 `Display` 会带上操作系统本地化文案（例如 Windows 中文
/// “远程主机强迫关闭了一个现有的连接。”），既随系统语言变化、不利于日志检索与告警
/// 匹配，也不属于 rfrp 自己的错误语义。这里按 [`ErrorKind`] 映射为固定的英文短语，
/// 并保留原始 OS 错误码（若有）用于排查。
pub fn describe_io_error(e: &std::io::Error) -> String {
    let kind = match e.kind() {
        ErrorKind::ConnectionReset => "connection reset by peer",
        ErrorKind::ConnectionAborted => "connection aborted",
        ErrorKind::ConnectionRefused => "connection refused",
        ErrorKind::BrokenPipe => "broken pipe",
        ErrorKind::UnexpectedEof => "unexpected eof",
        ErrorKind::TimedOut => "operation timed out",
        ErrorKind::NotConnected => "not connected",
        ErrorKind::AddrInUse => "address already in use",
        ErrorKind::AddrNotAvailable => "address not available",
        ErrorKind::PermissionDenied => "permission denied",
        ErrorKind::WouldBlock => "operation would block",
        ErrorKind::InvalidData => "invalid data",
        ErrorKind::InvalidInput => "invalid input",
        ErrorKind::NotFound => "not found",
        ErrorKind::AlreadyExists => "already exists",
        ErrorKind::Interrupted => "interrupted",
        ErrorKind::WriteZero => "write zero",
        _ => "io error",
    };
    match e.raw_os_error() {
        Some(code) => format!("{kind} (os error {code})"),
        None => kind.to_string(),
    }
}

/// rfrp 统一错误类型。
#[derive(Debug, Error)]
pub enum Error {
    /// 底层 I/O 错误（网络、文件等）。
    ///
    /// 显示为稳定的 rfrp 描述而非 OS 本地化文案（见 [`describe_io_error`]）。
    #[error("{}", describe_io_error(.0))]
    Io(#[from] std::io::Error),

    /// JSON 编解码错误（控制消息 Payload）。
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    /// TOML 配置解析错误。
    #[error("toml error: {0}")]
    Toml(#[from] toml::de::Error),

    /// 协议层错误（版本不匹配、长度超限、未知消息类型、畸形帧等）。
    #[error("protocol error: {0}")]
    Protocol(String),

    /// 配置校验错误（端口范围、字段一致性、格式等）。
    #[error("config error: {0}")]
    Config(String),

    /// 鉴权错误（token 不匹配等）。
    #[error("auth error: {0}")]
    Auth(String),

    /// 其他未归类错误。
    #[error("{0}")]
    Other(String),
}

/// 统一 `Result` 别名。
pub type Result<T> = std::result::Result<T, Error>;

/// 便捷构造 `Error::Protocol`。
pub(crate) fn protocol(msg: impl Into<String>) -> Error {
    Error::Protocol(msg.into())
}

/// 便捷构造 `Error::Config`。
pub(crate) fn config(msg: impl Into<String>) -> Error {
    Error::Config(msg.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Error as IoError;

    #[test]
    fn io_error_display_is_stable_and_not_localized() {
        // 构造带本地化文案的 io 错误：Display 必须只反映 ErrorKind，不含 OS 原文。
        let e = Error::Io(IoError::new(
            ErrorKind::ConnectionReset,
            "远程主机强迫关闭了一个现有的连接。",
        ));
        assert_eq!(e.to_string(), "connection reset by peer");
    }

    #[test]
    fn io_error_display_keeps_raw_os_code() {
        // 保留原始 OS 错误码，便于排查；但不回显本地化文案。
        let e = Error::Io(IoError::from_raw_os_error(10054));
        let s = e.to_string();
        assert!(s.contains("os error 10054"), "{s}");
        assert!(!s.contains("远程主机"), "{s}");
    }

    #[test]
    fn describe_io_error_maps_common_kinds() {
        for (kind, expect) in [
            (ErrorKind::ConnectionReset, "connection reset by peer"),
            (ErrorKind::ConnectionAborted, "connection aborted"),
            (ErrorKind::ConnectionRefused, "connection refused"),
            (ErrorKind::BrokenPipe, "broken pipe"),
            (ErrorKind::UnexpectedEof, "unexpected eof"),
            (ErrorKind::TimedOut, "operation timed out"),
        ] {
            let e = IoError::new(kind, "ignored");
            assert_eq!(describe_io_error(&e), expect);
        }
    }
}
