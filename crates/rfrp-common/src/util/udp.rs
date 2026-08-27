//! UDP 代理工作连接上的分帧工具。
//!
//! UDP 无连接，工作连接上按「4 字节大端长度前缀 + 数据」分帧（DESIGN §8.6）。

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::constants::MAX_UDP_PACKET_SIZE;

/// 从流上读取一个 UDP 帧，返回数据；EOF 返回 `None`。
pub async fn read_udp_frame<R>(r: &mut R) -> std::io::Result<Option<Vec<u8>>>
where
    R: AsyncRead + Unpin,
{
    let mut len_buf = [0u8; 4];
    let mut filled = 0;
    while filled < 4 {
        let n = r.read(&mut len_buf[filled..]).await?;
        if n == 0 {
            if filled == 0 {
                return Ok(None); // 干净 EOF
            }
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "udp frame header truncated",
            ));
        }
        filled += n;
    }
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_UDP_PACKET_SIZE {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("udp frame too large: {len}"),
        ));
    }
    let mut data = vec![0u8; len];
    r.read_exact(&mut data).await?;
    Ok(Some(data))
}

/// 向流上写入一个 UDP 帧（4 字节大端长度前缀 + 数据）。
pub async fn write_udp_frame<W>(w: &mut W, data: &[u8]) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    if data.len() > MAX_UDP_PACKET_SIZE {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("udp frame too large: {}", data.len()),
        ));
    }
    w.write_all(&(data.len() as u32).to_be_bytes()).await?;
    w.write_all(data).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    #[tokio::test]
    async fn frame_roundtrip() {
        let (mut a, mut b) = duplex(1024);
        write_udp_frame(&mut a, b"hello").await.unwrap();
        let data = read_udp_frame(&mut b).await.unwrap().unwrap();
        assert_eq!(data, b"hello");
    }

    #[tokio::test]
    async fn eof_returns_none() {
        let (a, mut b) = duplex(1024);
        drop(a);
        assert!(read_udp_frame(&mut b).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn oversized_frame_rejected() {
        let (mut a, mut b) = duplex(1024);
        a.write_all(&(MAX_UDP_PACKET_SIZE as u32 + 1).to_be_bytes())
            .await
            .unwrap();
        a.write_all(&[0u8; 1]).await.unwrap();
        assert!(read_udp_frame(&mut b).await.is_err());
    }
}
