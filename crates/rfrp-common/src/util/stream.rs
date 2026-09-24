//! 类型擦除的异步字节流，用于同时持有明文 TCP 与 TLS 流。

use bytes::{Buf, Bytes};
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
    /// 已读缓冲。`Bytes::from(Vec)` 零拷贝接管调用方的分配，避免再复制一份
    /// （vhost 请求头最长可达 64 KiB）。
    buf: Bytes,
    inner: BoxedStream,
}

impl PrependStream {
    /// 先回放 `buf`，再透传 `inner`；`buf` 的所有权被接管，不做拷贝。
    pub fn new(buf: Vec<u8>, inner: BoxedStream) -> Self {
        Self {
            buf: Bytes::from(buf),
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn prepend_stream_serves_buffer_then_inner() {
        let (mut inner_w, inner_r) = duplex(64);
        inner_w.write_all(b"world").await.unwrap();

        let mut s = PrependStream::new(b"hello ".to_vec(), Box::new(inner_r));
        let mut buf = [0u8; 11];
        s.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello world");
    }
}
