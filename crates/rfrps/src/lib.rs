//! rfrps：服务端库（server 子命令逻辑）。
//!
//! 负责：控制连接处理、按代理类型监听公网端口、工作连接路由、双向桥接。
//! 当前仅实现 TCP 代理；TLS 与 token 鉴权已在 M3 完成，UDP/HTTP/HTTPS 在 M4 扩展。

pub mod control;
pub mod listener;
pub mod server;
pub mod vhost;
pub mod work;

pub use server::Server;
