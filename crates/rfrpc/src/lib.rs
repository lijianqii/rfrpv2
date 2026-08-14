//! rfrpc：客户端库（client 子命令逻辑）。
//!
//! 负责：控制连接、登录、代理注册、工作连接建立、本地服务回连、双向桥接。
//! M1 仅实现 TCP 代理，无 TLS、无 token 校验（见 DESIGN §12 M1）。

pub mod bridge;
pub mod client;
pub mod control;
pub mod workconn;

pub use client::Client;
