//! 统一错误类型。
//!
//! 库内部错误用 `Error`，上层（如 rfrp-bin）可用 `anyhow` 承接。所有底层错误
//! 通过 `#[from]` 收敛到本类型，保持错误链完整。

use thiserror::Error;

/// rfrp 统一错误类型。
#[derive(Debug, Error)]
pub enum Error {
    /// 底层 I/O 错误（网络、文件等）。
    #[error("io error: {0}")]
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
