//! 双向字节泵（客户端侧，与 rfrps::bridge 同语义，见 DESIGN §13）。

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
