//! 类型擦除的异步字节流，用于同时持有明文 TCP 与 TLS 流。

use bytes::{Buf, BytesMut};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// 同时满足桥接/编解码所需 trait 的异步流。
pub trait AsyncStream: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T: AsyncRead + AsyncWrite + Unpin + Send> AsyncStream for T {}

/// 可跨任务传递的异步字节流。
pub type BoxedStream = Box<dyn AsyncStream>;

/// 先回放已读缓冲、再透传底层流的包装器（vhost 读取请求头后使用）。
pub struct PrependStream {
    buf: BytesMut,
    inner: BoxedStream,
}

impl PrependStream {
    pub fn new(buf: Vec<u8>, inner: BoxedStream) -> Self {
        Self {
            buf: BytesMut::from(&buf[..]),
            inner,
        }
    }
}

impl AsyncRead for PrependStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        dst: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if !self.buf.is_empty() {
            let n = std::cmp::min(self.buf.len(), dst.remaining());
            dst.put_slice(&self.buf[..n]);
            self.buf.advance(n);
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut self.inner).poll_read(cx, dst)
    }
}

impl AsyncWrite for PrependStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
