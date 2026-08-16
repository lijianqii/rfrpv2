//! rfrpc：客户端库（client 子命令逻辑）。
//!
//! 负责：控制连接、登录、代理注册、工作连接建立、本地服务回连、双向桥接。
//! 当前仅实现 TCP 代理；TLS 与 token 鉴权已在 M3 完成，UDP/HTTP/HTTPS 在 M4 扩展。

pub mod client;
pub mod control;
pub mod workconn;

pub use client::Client;
