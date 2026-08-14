//! rfrps：服务端库（server 子命令逻辑）。
//!
//! 负责：控制连接处理、按代理类型监听公网端口、工作连接路由、双向桥接。
//! M1 仅实现 TCP 代理，无 TLS、无 token 校验（见 DESIGN §12 M1）。

pub mod bridge;
pub mod control;
pub mod listener;
pub mod server;
pub mod work;

pub use server::Server;
