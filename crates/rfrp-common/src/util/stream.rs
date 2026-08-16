//! 类型擦除的异步字节流，用于同时持有明文 TCP 与 TLS 流。

use tokio::io::{AsyncRead, AsyncWrite};

/// 同时满足桥接/编解码所需 trait 的异步流。
pub trait AsyncStream: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T: AsyncRead + AsyncWrite + Unpin + Send> AsyncStream for T {}

/// 可跨任务传递的异步字节流。
pub type BoxedStream = Box<dyn AsyncStream>;
