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
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
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

        if src.len() < FRAME_HEADER_LEN + length {
            return Ok(None); // payload 未到齐
        }

        if version != PROTOCOL_VERSION {
            return Err(protocol(format!(
                "unsupported protocol version {version}, expected {PROTOCOL_VERSION}"
            )));
        }
        if length > FRAME_MAX_PAYLOAD as usize {
            return Err(protocol(format!("frame payload too large: {length} bytes")));
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

/// 从一条 `TcpStream` 上读取恰好一个帧，返回 `(Frame, 剩余 TcpStream)`。
///
/// 用于服务端在 accept 后区分控制连接（首帧 Login）与工作连接（首帧 StartWorkConn），
/// 而不丢失首帧之后的原始字节（工作连接首帧后即透传）。
pub async fn read_one_frame(mut stream: TcpStream) -> Result<(Frame, TcpStream)> {
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
mod tests {
    use super::*;
    use crate::protocol::msg::{Login, Message, MSG_LOGIN};
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    fn roundtrip(f: Frame) -> Frame {
        let mut buf = BytesMut::new();
        FrameCodec.encode(f.clone(), &mut buf).unwrap();
        FrameCodec.decode(&mut buf).unwrap().unwrap()
    }

    #[test]
    fn encode_decode_roundtrip() {
        let f = Frame::new(PROTOCOL_VERSION, 0x01, b"hello".to_vec());
        assert_eq!(
            roundtrip(f),
            Frame::new(PROTOCOL_VERSION, 0x01, b"hello".to_vec())
        );
    }

    #[test]
    fn empty_payload_allowed() {
        let f = Frame::new(PROTOCOL_VERSION, 0x09, Vec::new());
        assert_eq!(roundtrip(f).payload, Vec::<u8>::new());
    }

    #[test]
    fn partial_header_returns_none() {
        let mut buf = BytesMut::new();
        buf.put_slice(&[PROTOCOL_VERSION, 0x01, 0x00, 0x00]);
        assert!(FrameCodec.decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn partial_payload_returns_none() {
        let mut buf = BytesMut::new();
        buf.put_slice(&[PROTOCOL_VERSION, 0x01, 0x00, 0x00, 0x00, 0x05, b'h', b'e']);
        assert!(FrameCodec.decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn version_mismatch_rejected() {
        let mut buf = BytesMut::new();
        buf.put_slice(&[0x02, 0x01, 0x00, 0x00, 0x00, 0x00]);
        let err = FrameCodec.decode(&mut buf).unwrap_err();
        assert!(matches!(err, Error::Protocol(_)));
    }

    #[test]
    fn oversize_payload_rejected() {
        let f = Frame::new(
            PROTOCOL_VERSION,
            0x01,
            vec![0u8; (FRAME_MAX_PAYLOAD as usize) + 1],
        );
        let mut buf = BytesMut::new();
        let err = FrameCodec.encode(f, &mut buf).unwrap_err();
        assert!(matches!(err, Error::Protocol(_)));
    }

    #[tokio::test]
    async fn read_one_frame_roundtrip() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut client = TcpStream::connect(addr).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();

        let msg = Message::Login(Login {
            run_id: "x".into(),
            token: "".into(),
            version: PROTOCOL_VERSION,
        });
        let frame = msg.to_frame().unwrap();
        let mut buf = BytesMut::new();
        FrameCodec.encode(frame.clone(), &mut buf).unwrap();
        client.write_all(&buf).await.unwrap();

        let (got, _rest) = read_one_frame(server).await.unwrap();
        assert_eq!(got, frame);
    }

    #[test]
    fn msg_type_const_valid() {
        assert_eq!(MSG_LOGIN, 0x01);
    }

    #[tokio::test]
    async fn read_one_frame_truncated_errors() {
        // 客户端连接后立即关闭：首帧头被截断，read_one_frame 应报错（§6.1）。
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).await.unwrap();
        drop(client);
        let (server, _peer) = listener.accept().await.unwrap();
        assert!(read_one_frame(server).await.is_err());
    }
}
