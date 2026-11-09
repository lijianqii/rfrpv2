//! 双向字节泵，服务端与客户端共用。
//!
//! 直接使用 tokio 的 `copy_bidirectional`（两个方向并发复制；一侧 EOF 后对另一侧做
//! 半关闭并继续复制反方向；任一侧 I/O 出错立即返回）。
//!
//! **为什么不用"自实现 + 复用缓冲"**：tokio 的实现每次调用都会 `vec![0; buf_size]`
//! （零初始化），一条用户连接过两次桥接等于每连接 128 KiB 额外的 memset，看起来值得
//! 优化。但实测（2026-10，macOS，见 docs/BENCHMARKS.md）：
//! - 收益端：改用 `BytesMut::with_capacity`（不零初始化）约省 0.4 µs/桥接、~2 µs/连接
//!   （缓冲真正被数据填满时，零初始化的开销会被吸收，所以收益有限）；
//! - 代价端：为了两个方向并发读写同一对 socket，必须先把流 `split`，而 split 让**同一个
//!   socket 的读半端与写半端共享一把锁**——真实用户连接是全双工（RDP/SSH/echo），
//!   实测全双工单流吞吐 **1770 → 1560 MiB/s（−11%）**。
//!
//! 因此结论是：这条优化只有在自己手写 poll 状态机（需要 `unsafe` 操作未初始化缓冲，
//! 与 `#![forbid(unsafe_code)]` 冲突）时才划算，当前收益不足以抵消全双工吞吐回归。

use crate::constants::BRIDGE_BUF_SIZE;
use crate::error::Result;
use tokio::io::{copy_bidirectional_with_sizes, AsyncRead, AsyncWrite};

/// 将 `a` 与 `b` 双向桥接，直到任一侧关闭。
///
/// 使用 `BRIDGE_BUF_SIZE` 缓冲（默认 64 KiB），比 tokio 默认 8 KiB 减少
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

    #[tokio::test]
    async fn bridge_propagates_half_close_and_keeps_reverse_direction() {
        // 半关闭语义（HTTP 请求体发完、SSH 客户端 shutdown 写端等）：
        // 一侧 EOF 只应关闭另一侧的**写**方向，反方向必须继续传数据。
        let (mut client, server) = duplex(64 * 1024);
        let (mut backend, backend_peer) = duplex(64 * 1024);
        let task = tokio::spawn(async move { bridge(server, backend_peer).await });

        client.write_all(b"request").await.unwrap();
        client.shutdown().await.unwrap(); // 客户端半关闭

        let mut got = [0u8; 7];
        backend.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, b"request");

        // 半关闭之后，后端仍应能把响应送回来。
        backend.write_all(b"response").await.unwrap();
        backend.flush().await.unwrap();
        let mut resp = [0u8; 8];
        client.read_exact(&mut resp).await.unwrap();
        assert_eq!(&resp, b"response");

        // 关掉后端 → 反方向也结束 → 桥接返回。
        drop(backend);
        drop(client);
        assert!(task.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn bridge_reuses_buffer_for_many_small_bursts() {
        // 小包频繁往返（RDP/SSH 交互形态）：缓冲在循环内复用、不需要每次分配新内存。
        let (mut a, a_peer) = duplex(8 * 1024);
        let (mut b, b_peer) = duplex(8 * 1024);
        let task = tokio::spawn(async move { bridge(a_peer, b_peer).await });

        for i in 0..500u32 {
            let msg = i.to_be_bytes();
            a.write_all(&msg).await.unwrap();
            a.flush().await.unwrap();
            let mut got = [0u8; 4];
            b.read_exact(&mut got).await.unwrap();
            assert_eq!(got, msg);
        }

        drop(a);
        drop(b);
        assert!(task.await.unwrap().is_ok());
    }
}
