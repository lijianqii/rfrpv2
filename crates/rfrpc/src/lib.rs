//! rfrpc：客户端库（client 子命令逻辑）。
//!
//! 负责：控制连接（登录/心跳/重连）、代理注册（TCP/UDP/HTTP/HTTPS）、
//! 工作连接建立与池化、本地服务回连、双向桥接。
//!
//! 对外入口：[`Client`]（长驻运行、指数退避重连），CLI 覆盖见 `cli` 模块。

pub mod cli;
pub mod client;
pub mod control;
pub mod workconn;

pub use client::Client;
