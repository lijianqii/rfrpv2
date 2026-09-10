//! 帧编解码单元测试。

use super::*;
use crate::protocol::msg::{Login, Message, MSG_LOGIN};
use tokio::io::{duplex, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

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

#[test]
fn oversize_length_in_header_rejected_without_payload() {
    // 长度校验必须先于 payload 等待：恶意头声称超大 length 时，
    // 即使 body 未到齐也应立即报错，避免缓冲区无界增长（DoS 防护）。
    let mut buf = BytesMut::new();
    buf.put_u8(PROTOCOL_VERSION);
    buf.put_u8(0x01);
    buf.put_u32(FRAME_MAX_PAYLOAD + 1);
    // 仅头部，无任何 payload 字节。
    let err = FrameCodec.decode(&mut buf).unwrap_err();
    assert!(matches!(err, Error::Protocol(_)));
}

#[test]
fn oversize_length_header_is_not_oversized_by_arithmetic() {
    // 长度字段是 u32 全宽（~4 GiB）：不会因 FRAME_HEADER_LEN + length 溢出 usize
    // 而误判为"未到齐"，必须走长度上限错误分支。
    let mut buf = BytesMut::new();
    buf.put_u8(PROTOCOL_VERSION);
    buf.put_u8(0x01);
    buf.put_u32(u32::MAX);
    assert!(matches!(
        FrameCodec.decode(&mut buf).unwrap_err(),
        Error::Protocol(_)
    ));
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
async fn read_one_frame_works_with_duplex() {
    // 验证 read_one_frame 已泛型化，可用于 TcpStream 以外的异步流。
    let (mut a, b) = duplex(64);
    let msg = Message::Login(Login {
        run_id: "duplex".into(),
        token: "".into(),
        version: PROTOCOL_VERSION,
    });
    let frame = msg.to_frame().unwrap();
    let mut buf = BytesMut::new();
    FrameCodec.encode(frame.clone(), &mut buf).unwrap();
    a.write_all(&buf).await.unwrap();
    let (got, _rest) = read_one_frame(b).await.unwrap();
    assert_eq!(got, frame);
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

#[tokio::test]
async fn read_one_frame_truncated_payload_errors() {
    // 头声明 payload 长度，但连接提前关闭（payload 被截断）：read_one_frame 应报错（§6.1）。
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mut client = TcpStream::connect(addr).await.unwrap();
    // 头：version + msg_type + length=5，但只发 2 字节 payload 即关闭。
    let header = [PROTOCOL_VERSION, 0x01, 0x00, 0x00, 0x00, 0x05];
    client.write_all(&header).await.unwrap();
    client.write_all(b"ab").await.unwrap();
    drop(client);
    let (server, _peer) = listener.accept().await.unwrap();
    assert!(read_one_frame(server).await.is_err());
}

#[tokio::test]
async fn read_one_frame_version_mismatch_errors() {
    // 首帧版本不符：read_one_frame 应报错（§6.1 / §6.6）。
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mut client = TcpStream::connect(addr).await.unwrap();
    let header = [0x02, 0x01, 0x00, 0x00, 0x00, 0x00]; // 版本 0x02 不符
    client.write_all(&header).await.unwrap();
    drop(client);
    let (server, _peer) = listener.accept().await.unwrap();
    assert!(read_one_frame(server).await.is_err());
}
