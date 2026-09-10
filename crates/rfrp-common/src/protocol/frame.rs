//! 帧编解码：`FrameCodec` 实现 `tokio_util::codec` 的 `Encoder`/`Decoder`，
//! 以及 `read_one_frame` 用于在 accept 后从一条 `TcpStream` 上读取恰好一个帧
//! （用于服务端区分控制连接 Login 与工作连接 StartWorkConn）。
//!
//! 帧格式见 DESIGN §6.1：
//! ```text
//! +----------+----------+----------+----------------+
//! | Version  | MsgType  |  Length  |    Payload     |
//! |  1 byte  |  1 byte  | 4 bytes  |  Length bytes  |
//! +----------+----------+----------+----------------+
//! ```

use crate::constants::{FRAME_HEADER_LEN, FRAME_MAX_PAYLOAD, PROTOCOL_VERSION};
use crate::error::{protocol, Error, Result};
use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_util::codec::{Decoder, Encoder};

pub use tokio_util::codec::{Framed, FramedRead, FramedWrite};

/// 一个协议帧：版本、消息类型、Payload（原始字节）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub version: u8,
    pub msg_type: u8,
    pub payload: Vec<u8>,
}

impl Frame {
    /// 构造帧。调用方应确保 payload 长度 ≤ `FRAME_MAX_PAYLOAD`。
    pub fn new(version: u8, msg_type: u8, payload: Vec<u8>) -> Self {
        Self {
            version,
            msg_type,
            payload,
        }
    }
}

/// 帧编解码器（无状态，可 `Default`）。
#[derive(Default)]
pub struct FrameCodec;

impl Decoder for FrameCodec {
    type Item = Frame;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Frame>> {
        if src.len() < FRAME_HEADER_LEN {
            return Ok(None); // 头未到齐
        }
        let version = src[0];
        let msg_type = src[1];
        let length = u32::from_be_bytes([src[2], src[3], src[4], src[5]]) as usize;

        // 先校验版本与长度上限，再等待 payload：否则恶意头声称超大 length 时
        // 解码器会一直等数据，缓冲区随读取无界增长（DoS）。
        if version != PROTOCOL_VERSION {
            return Err(protocol(format!(
                "unsupported protocol version {version}, expected {PROTOCOL_VERSION}"
            )));
        }
        if length > FRAME_MAX_PAYLOAD as usize {
            return Err(protocol(format!("frame payload too large: {length} bytes")));
        }

        if src.len() < FRAME_HEADER_LEN + length {
            return Ok(None); // payload 未到齐
        }

        src.advance(FRAME_HEADER_LEN);
        let payload = src[..length].to_vec();
        src.advance(length);

        Ok(Some(Frame {
            version,
            msg_type,
            payload,
        }))
    }
}

impl Encoder<Frame> for FrameCodec {
    type Error = Error;

    fn encode(&mut self, frame: Frame, dst: &mut BytesMut) -> Result<()> {
        if frame.payload.len() > FRAME_MAX_PAYLOAD as usize {
            return Err(protocol(format!(
                "frame payload too large: {} bytes",
                frame.payload.len()
            )));
        }
        dst.put_u8(frame.version);
        dst.put_u8(frame.msg_type);
        dst.put_u32(frame.payload.len() as u32);
        dst.put_slice(&frame.payload);
        Ok(())
    }
}

/// 从一条异步字节流上读取恰好一个帧，返回 `(Frame, 剩余流)`。
///
/// 用于服务端在 accept 后区分控制连接（首帧 Login）与工作连接（首帧 StartWorkConn），
/// 而不丢失首帧之后的原始字节（工作连接首帧后即透传）。
pub async fn read_one_frame<S>(mut stream: S) -> Result<(Frame, S)>
where
    S: AsyncRead + Unpin,
{
    let mut header = [0u8; FRAME_HEADER_LEN];
    stream.read_exact(&mut header).await?;
    let version = header[0];
    let msg_type = header[1];
    let length = u32::from_be_bytes([header[2], header[3], header[4], header[5]]) as usize;
    if version != PROTOCOL_VERSION {
        return Err(protocol(format!(
            "unsupported protocol version {version}, expected {PROTOCOL_VERSION}"
        )));
    }
    if length > FRAME_MAX_PAYLOAD as usize {
        return Err(protocol(format!("frame payload too large: {length} bytes")));
    }
    let mut payload = vec![0u8; length];
    if length > 0 {
        stream.read_exact(&mut payload).await?;
    }
    Ok((Frame::new(version, msg_type, payload), stream))
}

#[cfg(test)]
mod tests;
