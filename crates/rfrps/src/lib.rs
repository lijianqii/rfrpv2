//! rfrps：服务端库（server 子命令逻辑）。
//!
//! 负责：控制连接处理（登录/心跳/会话清理）、按代理类型监听公网端口
//! （TCP/UDP/HTTP/HTTPS vhost）、工作连接路由与池化、双向桥接、
//! Dashboard 与 Prometheus 指标。
//!
//! 对外入口：[`Server`]（accept 循环 + 优雅退出），CLI 覆盖见 `cli` 模块。

pub mod cli;
pub mod control;
pub mod dashboard;
pub mod listener;
pub mod metrics;
pub mod server;
pub mod state;
pub mod udp;
pub mod vhost;
pub mod work;

pub use server::Server;
