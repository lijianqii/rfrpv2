//! 双向字节泵，服务端与客户端共用。

use crate::constants::BRIDGE_BUF_SIZE;
use crate::error::Result;
use tokio::io::{copy_bidirectional_with_sizes, AsyncRead, AsyncWrite};

/// 将 `a` 与 `b` 双向桥接，直到任一侧关闭。
///
/// 使用 `BRIDGE_BUF_SIZE` 缓冲（默认 32 KiB），比 tokio 默认 8 KiB 减少
/// 大流量下的系统调用次数；小包交互不受影响（读到多少转发多少）。
pub async fn bridge<A, B>(a: A, b: B) -> Result<()>
where
    A: AsyncRead + AsyncWrite + Unpin,
    B: AsyncRead + AsyncWrite + Unpin,
{
    bridge_with_buf_size(a, b, BRIDGE_BUF_SIZE).await
}

/// 同 [`bridge`]，但可指定每方向缓冲区大小（供基准测试对比不同尺寸）。
pub async fn bridge_with_buf_size<A, B>(mut a: A, mut b: B, buf_size: usize) -> Result<()>
where
    A: AsyncRead + AsyncWrite + Unpin,
    B: AsyncRead + AsyncWrite + Unpin,
{
    copy_bidirectional_with_sizes(&mut a, &mut b, buf_size, buf_size).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn bridge_forwards_both_directions() {
        let (c1, c2) = duplex(4096);
        let (d1, d2) = duplex(4096);

        tokio::spawn(async move {
            let _ = bridge(c1, d1).await;
        });

        let mut c2_w = c2;
        let mut d2_r = d2;
        c2_w.write_all(b"ping").await.unwrap();
        let mut buf = [0u8; 4];
        d2_r.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ping");

        d2_r.write_all(b"pong").await.unwrap();
        let mut rbuf = [0u8; 4];
        c2_w.read_exact(&mut rbuf).await.unwrap();
        assert_eq!(&rbuf, b"pong");
    }

    #[tokio::test]
    async fn bridge_forwards_payload_larger_than_buf_size() {
        // 大于缓冲区的数据必须完整透传（分多次 copy），且一侧 EOF 后正常结束。
        let (c1, mut c2) = duplex(64 * 1024);
        let (d1, mut d2) = duplex(64 * 1024);
        let task = tokio::spawn(async move { bridge_with_buf_size(c1, d1, 1024).await });

        let data = vec![0x5Au8; 8 * 1024]; // 8× 缓冲区
        c2.write_all(&data).await.unwrap();
        let mut got = vec![0u8; data.len()];
        d2.read_exact(&mut got).await.unwrap();
        assert_eq!(got, data);

        drop(c2); // 外部写端 EOF → 桥接 shutdown 隧道侧写端
        drop(d2); // 隧道写端 EOF → 两个方向都结束，桥接返回
        assert!(task.await.unwrap().is_ok());
    }
}
