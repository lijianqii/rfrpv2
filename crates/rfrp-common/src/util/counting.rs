//! 带流量计数的流包装器，用于服务端统计转发字节数。

use std::pin::Pin;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use super::stream::BoxedStream;

/// 统计读/写字节数，并在 drop 时递减活跃连接数。
pub struct CountingStream {
    inner: BoxedStream,
    read: Arc<AtomicU64>,
    write: Arc<AtomicU64>,
    active: Arc<AtomicI64>,
}

impl CountingStream {
    pub fn new(
        inner: BoxedStream,
        read: Arc<AtomicU64>,
        write: Arc<AtomicU64>,
        active: Arc<AtomicI64>,
    ) -> Self {
        Self {
            inner,
            read,
            write,
            active,
        }
    }
}

impl Drop for CountingStream {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::Relaxed);
    }
}

impl AsyncRead for CountingStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        dst: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = dst.filled().len();
        let r = Pin::new(&mut self.inner).poll_read(cx, dst);
        if let Poll::Ready(Ok(())) = &r {
            let n = dst.filled().len() - before;
            self.read.fetch_add(n as u64, Ordering::Relaxed);
        }
        r
    }
}

impl AsyncWrite for CountingStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        match Pin::new(&mut self.inner).poll_write(cx, buf) {
            Poll::Ready(Ok(n)) => {
                self.write.fetch_add(n as u64, Ordering::Relaxed);
                Poll::Ready(Ok(n))
            }
            other => other,
        }
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
    async fn counts_bytes_and_active() {
        let (mut a, b) = duplex(1024);
        let read = Arc::new(AtomicU64::new(0));
        let write = Arc::new(AtomicU64::new(0));
        let active = Arc::new(AtomicI64::new(0));

        let mut counted =
            CountingStream::new(Box::new(b), read.clone(), write.clone(), active.clone());
        active.fetch_add(1, Ordering::Relaxed);

        a.write_all(b"hello").await.unwrap();
        let mut buf = [0u8; 5];
        counted.read_exact(&mut buf).await.unwrap();
        counted.write_all(b"pong").await.unwrap();
        let mut rbuf = [0u8; 4];
        a.read_exact(&mut rbuf).await.unwrap();

        assert_eq!(read.load(Ordering::Relaxed), 5);
        assert_eq!(write.load(Ordering::Relaxed), 4);

        drop(counted);
        assert_eq!(active.load(Ordering::Relaxed), 0);
    }
}
