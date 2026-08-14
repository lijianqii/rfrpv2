//! 双向字节泵（零拷贝转发路径）。
//!
//! 一对实现 `AsyncRead + AsyncWrite` 的流（用户 ↔ 工作连接 / 工作连接 ↔ 本地服务）
//! 之间的透明桥接。`copy_bidirectional` 自动处理半关闭与背压。泛型化以便用
//! `tokio::io::duplex` 在测试/基准中复用同一逻辑（见 DESIGN §13）。

use rfrp_common::error::Result;
use tokio::io::{copy_bidirectional, AsyncRead, AsyncWrite};

/// 将 `a` 与 `b` 双向桥接，直到任一侧关闭。
pub async fn bridge<A, B>(mut a: A, mut b: B) -> Result<()>
where
    A: AsyncRead + AsyncWrite + Unpin,
    B: AsyncRead + AsyncWrite + Unpin,
{
    copy_bidirectional(&mut a, &mut b).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn bridge_forwards_both_directions() {
        // 两条独立管道 (c1<->c2) 与 (d1<->d2)；bridge(c1, d1) 后：
        // 写 c2 应出现在 d2，写 d2 应出现在 c2。
        let (c1, c2) = duplex(4096);
        let (d1, d2) = duplex(4096);

        tokio::spawn(async move {
            let _ = bridge(c1, d1).await;
        });

        // c2 -> d2
        let mut c2_w = c2;
        let mut d2_r = d2;
        c2_w.write_all(b"ping").await.unwrap();
        let mut buf = [0u8; 4];
        d2_r.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ping");

        // 反向 d2 -> c2（用新句柄：duplex 两端各自可读写，需重新获取写端）
        // 注意：c2_w 已持有 c2 的读写端，d2_r 已持有 d2 的读写端。
        // 这里直接复用同一句柄完成反向验证。
        d2_r.write_all(b"pong").await.unwrap();
        let mut rbuf = [0u8; 4];
        c2_w.read_exact(&mut rbuf).await.unwrap();
        assert_eq!(&rbuf, b"pong");
    }
}
