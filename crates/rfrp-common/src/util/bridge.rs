//! 双向字节泵，服务端与客户端共用。

use crate::error::Result;
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
}
