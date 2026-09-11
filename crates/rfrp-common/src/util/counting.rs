//! 带流量计数的流包装器，用于服务端统计转发字节数。

use std::pin::Pin;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use super::stream::BoxedStream;

/// 本地累计达到该字节数后写入全局计数器。
///
/// `bytes_up` / `bytes_down` 是所有连接共享的原子变量；原实现每个读写块都
/// `fetch_add`，高并发大流量下多个核心争用同一 cache line 成为热点。
const FLUSH_THRESHOLD: u64 = 256 * 1024;

/// 距上次写入全局计数器的最大时间：保证低速长连接（如 SSH）在传输中也能
/// 及时反映到监控，而不是等到连接关闭（监控滞后 ≤ 1s）。
const FLUSH_INTERVAL: Duration = Duration::from_secs(1);

/// 一组额外计数目标（与主计数器同时累加），用于每代理统计等。
#[derive(Clone)]
pub struct ExtraCounters {
    pub read: Arc<AtomicU64>,
    pub write: Arc<AtomicU64>,
    pub active: Arc<AtomicI64>,
}

/// 统计读/写字节数，并在 drop 时递减活跃连接数。
///
/// 计数在本地累计，达到 `FLUSH_THRESHOLD` 或超过 `FLUSH_INTERVAL` 时批量
/// 写入全局原子计数器；`Drop` 时做最后一次 flush。
pub struct CountingStream {
    inner: BoxedStream,
    read: Arc<AtomicU64>,
    write: Arc<AtomicU64>,
    active: Arc<AtomicI64>,
    /// 可选的额外计数目标（如每代理统计）。
    extra: Option<ExtraCounters>,
    pending_read: u64,
    pending_write: u64,
    last_flush: Instant,
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
            extra: None,
            pending_read: 0,
            pending_write: 0,
            last_flush: Instant::now(),
        }
    }

    /// 附加一组计数目标（与主计数器同时累加），用于每代理统计。
    pub fn with_extra(mut self, extra: ExtraCounters) -> Self {
        extra.active.fetch_add(1, Ordering::Relaxed);
        self.extra = Some(extra);
        self
    }

    /// 把本地累计的读写字节数写入全局计数器（含可选的额外目标）。
    fn flush(&mut self) {
        if self.pending_read > 0 {
            self.read.fetch_add(self.pending_read, Ordering::Relaxed);
            if let Some(e) = &self.extra {
                e.read.fetch_add(self.pending_read, Ordering::Relaxed);
            }
            self.pending_read = 0;
        }
        if self.pending_write > 0 {
            self.write.fetch_add(self.pending_write, Ordering::Relaxed);
            if let Some(e) = &self.extra {
                e.write.fetch_add(self.pending_write, Ordering::Relaxed);
            }
            self.pending_write = 0;
        }
        self.last_flush = Instant::now();
    }

    /// 达到字节阈值或时间阈值时刷新全局计数器。
    fn maybe_flush(&mut self) {
        if self.pending_read == 0 && self.pending_write == 0 {
            return;
        }
        if self.pending_read >= FLUSH_THRESHOLD
            || self.pending_write >= FLUSH_THRESHOLD
            || self.last_flush.elapsed() >= FLUSH_INTERVAL
        {
            self.flush();
        }
    }
}

impl Drop for CountingStream {
    fn drop(&mut self) {
        self.flush();
        self.active.fetch_sub(1, Ordering::Relaxed);
        if let Some(e) = &self.extra {
            e.active.fetch_sub(1, Ordering::Relaxed);
        }
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
            let n = (dst.filled().len() - before) as u64;
            self.pending_read += n;
            self.maybe_flush();
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
                self.pending_write += n as u64;
                self.maybe_flush();
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

        // 未达批量阈值/时间阈值：全局计数器暂不更新（drop 时统一 flush）。
        assert_eq!(read.load(Ordering::Relaxed), 0);
        assert_eq!(write.load(Ordering::Relaxed), 0);

        drop(counted);
        assert_eq!(read.load(Ordering::Relaxed), 5);
        assert_eq!(write.load(Ordering::Relaxed), 4);
        assert_eq!(active.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn flushes_globally_after_interval_for_slow_stream() {
        // 低速长连接：即使未达字节阈值，超过刷新间隔后的下一次读写也应写入全局
        // 计数器（保证监控实时性，而非等连接关闭）。
        let (mut a, b) = duplex(1024);
        let read = Arc::new(AtomicU64::new(0));
        let write = Arc::new(AtomicU64::new(0));
        let active = Arc::new(AtomicI64::new(0));
        let mut counted =
            CountingStream::new(Box::new(b), read.clone(), write.clone(), active.clone());

        a.write_all(b"hi").await.unwrap();
        let mut buf = [0u8; 2];
        counted.read_exact(&mut buf).await.unwrap();
        assert_eq!(read.load(Ordering::Relaxed), 0);

        tokio::time::sleep(Duration::from_millis(1100)).await;
        a.write_all(b"x").await.unwrap();
        let mut one = [0u8; 1];
        counted.read_exact(&mut one).await.unwrap();
        assert_eq!(read.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn extra_counters_accumulate_and_decrement() {
        let (mut a, b) = duplex(1024);
        let g_read = Arc::new(AtomicU64::new(0));
        let g_write = Arc::new(AtomicU64::new(0));
        let g_active = Arc::new(AtomicI64::new(0));
        let e_read = Arc::new(AtomicU64::new(0));
        let e_write = Arc::new(AtomicU64::new(0));
        let e_active = Arc::new(AtomicI64::new(0));
        let mut counted = CountingStream::new(
            Box::new(b),
            g_read.clone(),
            g_write.clone(),
            g_active.clone(),
        )
        .with_extra(ExtraCounters {
            read: e_read.clone(),
            write: e_write.clone(),
            active: e_active.clone(),
        });
        g_active.fetch_add(1, Ordering::Relaxed);
        assert_eq!(
            e_active.load(Ordering::Relaxed),
            1,
            "extra active incremented"
        );

        a.write_all(b"hello").await.unwrap();
        let mut buf = [0u8; 5];
        counted.read_exact(&mut buf).await.unwrap();
        drop(counted);

        assert_eq!(g_read.load(Ordering::Relaxed), 5);
        assert_eq!(
            e_read.load(Ordering::Relaxed),
            5,
            "extra read mirrors global"
        );
        assert_eq!(e_active.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn flushes_globally_at_threshold_before_drop() {
        // 累计达到阈值即写入全局计数器（无需等连接关闭），保证监控实时性。
        let cap = FLUSH_THRESHOLD as usize;
        let (_a, b) = duplex(cap + 4096);
        let read = Arc::new(AtomicU64::new(0));
        let write = Arc::new(AtomicU64::new(0));
        let active = Arc::new(AtomicI64::new(0));

        let mut counted =
            CountingStream::new(Box::new(b), read.clone(), write.clone(), active.clone());
        counted.write_all(&vec![0u8; cap]).await.unwrap();

        assert!(
            write.load(Ordering::Relaxed) >= FLUSH_THRESHOLD,
            "global counter must be flushed at threshold, got {}",
            write.load(Ordering::Relaxed)
        );
    }
}
