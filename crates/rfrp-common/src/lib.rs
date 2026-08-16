//! rfrp-common：rfrp 的共享库。
//!
//! 包含协议编解码、配置结构、TLS/鉴权原语、统一错误类型、常量与平台工具。
//! 本 crate **不**包含任何网络 I/O 编排逻辑，便于独立单测与两端复用。

pub mod auth;
pub mod config;
pub mod constants;
pub mod crypto;
pub mod error;
pub mod protocol;
pub mod util;

// 常用类型再导出，调用方无需深入子模块。
pub use error::{Error, Result};
pub use protocol::frame::{Frame, FrameCodec};
pub use protocol::msg::{
    Close, Heartbeat, HeartbeatResp, Login, LoginResp, Message, NewProxy, NewProxyResp, ProxyType,
    ReqWorkConn, StartWorkConn, MSG_CLOSE, MSG_HEARTBEAT, MSG_HEARTBEAT_RESP, MSG_LOGIN,
    MSG_LOGIN_RESP, MSG_NEW_PROXY, MSG_NEW_PROXY_RESP, MSG_REQ_WORK_CONN, MSG_START_WORK_CONN,
};
