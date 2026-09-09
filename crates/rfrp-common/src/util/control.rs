//! 控制连接关闭与消息发送辅助（优雅关闭 + 防背压死锁）。

use std::time::Duration;

use futures::SinkExt;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::protocol::frame::{FrameCodec, FramedWrite};
use crate::protocol::msg::{Close, Message};

/// 控制消息发送超时（秒）：socket 写阻塞超过该时间视为控制连接异常。
pub const CONTROL_SEND_TIMEOUT: u64 = 5;

/// 发送 Close 帧并关闭底层写端（发 close_notify）。
pub async fn graceful_close<W>(writer: &mut FramedWrite<W, FrameCodec>, reason: &str)
where
    W: AsyncWrite + Unpin,
{
    if let Ok(frame) = Message::Close(Close {
        reason: Some(reason.into()),
    })
    .to_frame()
    {
        let _ = writer.send(frame).await;
    }
    let _ = writer.get_mut().shutdown().await;
}

/// 尝试立即发送（通道满时返回 false，不阻塞）。
pub fn try_send(tx: &mpsc::Sender<Message>, msg: Message) -> bool {
    tx.try_send(msg).is_ok()
}

/// 带超时发送：避免有界通道满时永久阻塞（背压死锁）。
pub async fn send_with_timeout(tx: &mpsc::Sender<Message>, msg: Message) -> bool {
    matches!(
        tokio::time::timeout(Duration::from_secs(CONTROL_SEND_TIMEOUT), tx.send(msg)).await,
        Ok(Ok(()))
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::msg::{Heartbeat, Message};
    use tokio::io::duplex;
    use tokio::io::AsyncReadExt;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn try_send_succeeds_when_capacity_available() {
        let (tx, _rx) = mpsc::channel::<Message>(1);
        assert!(try_send(&tx, Message::Heartbeat(Heartbeat { ts: 1 })));
    }

    #[tokio::test]
    async fn try_send_returns_false_when_channel_full() {
        // 通道满时不阻塞、立即返回 false（防背压死锁的核心行为）。
        let (tx, _rx) = mpsc::channel::<Message>(1);
        tx.try_send(Message::Heartbeat(Heartbeat { ts: 1 }))
            .unwrap();
        assert!(!try_send(&tx, Message::Heartbeat(Heartbeat { ts: 2 })));
    }

    #[tokio::test]
    async fn send_with_timeout_succeeds_with_consumer() {
        let (tx, mut rx) = mpsc::channel::<Message>(1);
        assert!(send_with_timeout(&tx, Message::Heartbeat(Heartbeat { ts: 42 })).await);
        match rx.recv().await {
            Some(Message::Heartbeat(h)) => assert_eq!(h.ts, 42),
            other => panic!("unexpected message: {other:?}"),
        }
    }

    #[tokio::test]
    async fn send_with_timeout_fails_fast_when_channel_closed() {
        // 接收端已关闭：不应等满 CONTROL_SEND_TIMEOUT，应快速返回 false。
        let (tx, rx) = mpsc::channel::<Message>(1);
        drop(rx);
        let started = tokio::time::Instant::now();
        assert!(!send_with_timeout(&tx, Message::Heartbeat(Heartbeat { ts: 1 })).await);
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[tokio::test]
    async fn graceful_close_writes_close_frame_then_shutdown() {
        // graceful_close 应写出一帧 Close，随后 shutdown 写端（对端读到 EOF）。
        let (a, b) = duplex(1024);
        let mut framed = FramedWrite::new(a, FrameCodec);
        graceful_close(&mut framed, "test reason").await;

        let mut reader = tokio::io::BufReader::new(b);
        let mut buf = Vec::new();
        let n = reader.read_to_end(&mut buf).await.unwrap();
        assert!(n > 0, "close frame bytes must be written");

        // 首字节应为协议版本，随后 msg_type 应为 MSG_CLOSE（见 protocol::frame）。
        assert_eq!(buf[0], crate::constants::PROTOCOL_VERSION);
        assert_eq!(buf[1], crate::protocol::msg::MSG_CLOSE);
    }
}
