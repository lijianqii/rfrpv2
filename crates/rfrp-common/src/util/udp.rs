//! UDP 代理工作连接上的分帧工具。
//!
//! UDP 无连接，工作连接上按「4 字节大端长度前缀 + 数据」分帧（DESIGN §8.6）。

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::constants::{MAX_UDP_PACKET_SIZE, UDP_SOCKET_RECV_BUF_BYTES};
use bytes::Buf;

/// best-effort 调大 UDP socket 的接收缓冲（返回内核实际生效的值）。
///
/// 突发（刷屏/视频）时先由内核缓冲吸收，避免"应用层队列还没腾出空间"就直接丢包。
/// 内核会按 `rmem_max` 截断（Linux 默认约 208 KB，需要时用 sysctl 调大），
/// 因此调用方应只当它是尽力而为，失败时记 debug 日志继续。
pub fn enlarge_recv_buffer(sock: &tokio::net::UdpSocket) -> std::io::Result<usize> {
    let s = socket2::SockRef::from(sock);
    s.set_recv_buffer_size(UDP_SOCKET_RECV_BUF_BYTES)?;
    s.recv_buffer_size()
}

/// 从流上读取一个 UDP 帧，返回数据；EOF 返回 `None`。
pub async fn read_udp_frame<R>(r: &mut R) -> std::io::Result<Option<Vec<u8>>>
where
    R: AsyncRead + Unpin,
{
    let mut data = Vec::new();
    match read_udp_frame_into(r, &mut data).await? {
        Some(()) => Ok(Some(data)),
        None => Ok(None),
    }
}

/// 同 [`read_udp_frame`]，但把数据写入复用的 `buf`（清空后追加），
/// 避免高频 UDP 转发下每包一次分配。EOF 返回 `Ok(None)`。
pub async fn read_udp_frame_into<R>(r: &mut R, buf: &mut Vec<u8>) -> std::io::Result<Option<()>>
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
    buf.clear();
    buf.resize(len, 0);
    r.read_exact(buf).await?;
    Ok(Some(()))
}

/// 向流上写入一个 UDP 帧（4 字节大端长度前缀 + 数据）。
pub async fn write_udp_frame<W>(w: &mut W, data: &[u8]) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut scratch = Vec::with_capacity(data.len() + 4);
    write_udp_frame_buffered(w, &mut scratch, data).await
}

/// 同 [`write_udp_frame`]，但使用调用方提供的复用缓冲，并且**只发起一次 write**。
///
/// 分两次 `write_all`（先 4 字节前缀、再载荷）在高频转发下等于每包两次系统调用；
/// 走 TLS 时更糟——两次写会产生两个 TLS record（各自带 5 字节头与加密开销）。
/// 合成一个缓冲后：明文 TCP 一次写，TLS 一个 record。
pub async fn write_udp_frame_buffered<W>(
    w: &mut W,
    scratch: &mut Vec<u8>,
    data: &[u8],
) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    if data.len() > MAX_UDP_PACKET_SIZE {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("udp frame too large: {}", data.len()),
        ));
    }
    scratch.clear();
    scratch.reserve(data.len() + 4);
    scratch.extend_from_slice(&(data.len() as u32).to_be_bytes());
    scratch.extend_from_slice(data);
    w.write_all(scratch).await
}

/// 把多个 UDP 数据报批量写入工作连接：每包仍保留独立的 4 字节长度前缀（**线格式不变**），
/// 但只发起一次 `write_all`。接收端逐帧读取即可，因此不需要协议版本协商。
///
/// 这让"一个视频/刷屏突发"从 N 次写系统调用（TLS 下 N 个 record）降为 1 次。
pub async fn write_udp_frames_buffered<W>(
    w: &mut W,
    scratch: &mut Vec<u8>,
    data: &[Vec<u8>],
) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let total: usize = data.iter().map(|d| d.len() + 4).sum();
    scratch.clear();
    scratch.reserve(total);
    for d in data {
        if d.len() > MAX_UDP_PACKET_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("udp frame too large: {}", d.len()),
            ));
        }
        scratch.extend_from_slice(&(d.len() as u32).to_be_bytes());
        scratch.extend_from_slice(d);
    }
    w.write_all(scratch).await
}

/// UDP 帧的批量读缓冲：把"每包一次 read 唤醒"变成"一次 read 解析多帧"。
///
/// 工作连接是全双工字节流，突发时内核里往往已经排好了多个完整帧；逐帧 `read_exact`
/// 会让每包都经历一次任务唤醒（实测突发下这是客户端侧的瓶颈之一）。
#[derive(Default)]
pub struct UdpFrameBuf {
    buf: bytes::BytesMut,
}

impl UdpFrameBuf {
    pub fn new() -> Self {
        Self::default()
    }

    /// 读取一批数据报（最多 `limit` 个）追加到 `out`（会先清空 `out`）。
    ///
    /// - `Ok(0)`：流已干净关闭（EOF 且无残留半帧）；
    /// - `Ok(n>0)`：本次解析出 n 个数据报；已有至少一个完整帧时**不再等待更多**，
    ///   避免把"批量"变成额外延迟；
    /// - `Err(UnexpectedEof)`：EOF 时残留半帧；`Err(InvalidData)`：长度超限。
    pub async fn read_batch<R>(
        &mut self,
        r: &mut R,
        out: &mut Vec<Vec<u8>>,
        limit: usize,
    ) -> std::io::Result<usize>
    where
        R: AsyncRead + Unpin,
    {
        out.clear();
        loop {
            while out.len() < limit && self.buf.len() >= 4 {
                let len = u32::from_be_bytes([self.buf[0], self.buf[1], self.buf[2], self.buf[3]])
                    as usize;
                if len > MAX_UDP_PACKET_SIZE {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("udp frame too large: {len}"),
                    ));
                }
                if self.buf.len() < 4 + len {
                    break; // 半帧：等更多数据
                }
                self.buf.advance(4);
                out.push(self.buf.split_to(len).to_vec());
            }
            if !out.is_empty() {
                return Ok(out.len());
            }
            // 一个完整帧都没有：继续读（阻塞等待首批数据）
            self.buf.reserve(64 * 1024);
            let n = r.read_buf(&mut self.buf).await?;
            if n == 0 {
                return if self.buf.is_empty() {
                    Ok(0) // 干净 EOF
                } else {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "udp frame header truncated",
                    ))
                };
            }
        }
    }
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
    async fn read_into_reuses_buffer() {
        // 复用缓冲：第二次读取覆盖第一次内容，不残留旧数据。
        let (mut a, mut b) = duplex(1024);
        let mut buf = Vec::new();
        write_udp_frame(&mut a, b"first").await.unwrap();
        read_udp_frame_into(&mut b, &mut buf)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(buf, b"first");
        write_udp_frame(&mut a, b"xy").await.unwrap();
        read_udp_frame_into(&mut b, &mut buf)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(buf, b"xy");
        // EOF 返回 None。
        drop(a);
        assert!(read_udp_frame_into(&mut b, &mut buf)
            .await
            .unwrap()
            .is_none());
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

    #[tokio::test]
    async fn buffered_write_keeps_wire_format() {
        // 缓冲写：线格式与 write_udp_frame 完全一致（接收端无需感知差异），且复用缓冲不残留。
        let (mut a, mut b) = duplex(4096);
        let mut scratch = Vec::new();
        write_udp_frame_buffered(&mut a, &mut scratch, b"hello")
            .await
            .unwrap();
        assert_eq!(scratch.len(), 9, "4 字节前缀 + 5 字节载荷");
        assert_eq!(read_udp_frame(&mut b).await.unwrap().unwrap(), b"hello");

        write_udp_frame_buffered(&mut a, &mut scratch, b"xy")
            .await
            .unwrap();
        assert_eq!(read_udp_frame(&mut b).await.unwrap().unwrap(), b"xy");
    }

    #[tokio::test]
    async fn batch_write_is_read_as_individual_frames() {
        // 批量写把 N 个数据报合并成一次 write，但对端仍逐帧读取 —— 线格式不变，
        // 因此不需要协议版本协商。
        let (mut a, mut b) = duplex(64 * 1024);
        let batch = vec![b"one".to_vec(), b"twoo".to_vec(), vec![0xAB; 1500]];
        let mut scratch = Vec::new();
        write_udp_frames_buffered(&mut a, &mut scratch, &batch)
            .await
            .unwrap();
        let mut buf = Vec::new();
        for expect in &batch {
            read_udp_frame_into(&mut b, &mut buf)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(&buf, expect);
        }
    }

    #[tokio::test]
    async fn batch_write_rejects_oversized_datagram() {
        let (mut a, _b) = duplex(1024);
        let batch = vec![vec![0u8; MAX_UDP_PACKET_SIZE + 1]];
        let mut scratch = Vec::new();
        assert!(write_udp_frames_buffered(&mut a, &mut scratch, &batch)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn batch_reader_parses_multiple_frames_per_read() {
        let (mut a, mut b) = duplex(64 * 1024);
        let batch = vec![b"aa".to_vec(), b"bbb".to_vec(), b"c".to_vec()];
        write_udp_frames_buffered(&mut a, &mut Vec::new(), &batch)
            .await
            .unwrap();

        let mut reader = UdpFrameBuf::new();
        let mut out = Vec::new();
        assert_eq!(reader.read_batch(&mut b, &mut out, 32).await.unwrap(), 3);
        assert_eq!(out, batch);
    }

    #[tokio::test]
    async fn batch_reader_respects_limit_and_keeps_remainder() {
        let (mut a, mut b) = duplex(64 * 1024);
        let batch: Vec<Vec<u8>> = (0..5u8).map(|i| vec![i; 3]).collect();
        write_udp_frames_buffered(&mut a, &mut Vec::new(), &batch)
            .await
            .unwrap();

        let mut reader = UdpFrameBuf::new();
        let mut out = Vec::new();
        assert_eq!(reader.read_batch(&mut b, &mut out, 2).await.unwrap(), 2);
        assert_eq!(out, batch[..2]);
        // 剩余帧仍留在内部缓冲，无需再次 read。
        assert_eq!(reader.read_batch(&mut b, &mut out, 32).await.unwrap(), 3);
        assert_eq!(out, batch[2..]);
    }

    #[tokio::test]
    async fn batch_reader_waits_for_partial_frame() {
        // 半截长度前缀 + 分两次到达：read_batch 必须等到完整帧再返回。
        let (mut a, mut b) = duplex(64 * 1024);
        let writer = tokio::spawn(async move {
            a.write_all(&3u32.to_be_bytes()[..2]).await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            a.write_all(&3u32.to_be_bytes()[2..]).await.unwrap();
            a.write_all(b"abc").await.unwrap();
        });

        let mut reader = UdpFrameBuf::new();
        let mut out = Vec::new();
        assert_eq!(reader.read_batch(&mut b, &mut out, 32).await.unwrap(), 1);
        assert_eq!(out[0], b"abc");
        writer.await.unwrap();
    }

    #[tokio::test]
    async fn batch_reader_distinguishes_clean_eof_from_truncated() {
        // 干净 EOF。
        let (a, mut b) = duplex(1024);
        drop(a);
        let mut reader = UdpFrameBuf::new();
        let mut out = Vec::new();
        assert_eq!(reader.read_batch(&mut b, &mut out, 32).await.unwrap(), 0);

        // 半帧 EOF 必须报错（否则会静默吞掉数据）。
        let (mut a, mut b) = duplex(1024);
        a.write_all(&5u32.to_be_bytes()).await.unwrap();
        a.write_all(b"ab").await.unwrap();
        drop(a);
        let mut reader = UdpFrameBuf::new();
        assert!(reader.read_batch(&mut b, &mut out, 32).await.is_err());
    }

    #[tokio::test]
    async fn batch_reader_rejects_oversized_length() {
        let (mut a, mut b) = duplex(1024);
        a.write_all(&(MAX_UDP_PACKET_SIZE as u32 + 1).to_be_bytes())
            .await
            .unwrap();
        let mut reader = UdpFrameBuf::new();
        let mut out = Vec::new();
        assert!(reader.read_batch(&mut b, &mut out, 32).await.is_err());
    }
}
