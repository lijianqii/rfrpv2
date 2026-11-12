//! 控制连接关闭与消息发送辅助（优雅关闭 + 防背压死锁）。

use std::time::Duration;

use futures::SinkExt;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::protocol::frame::{FrameCodec, FramedWrite};
use crate::protocol::msg::{Close, Message};

/// 控制消息发送超时（秒）：socket 写阻塞超过该时间视为控制连接异常。
pub const CONTROL_SEND_TIMEOUT: u64 = 5;
/// 为心跳响应预留的控制通道槽位。批量消息不得用满通道，否则高峰时
/// `HeartbeatResp` 的 `try_send` 会静默失败并触发客户端误超时。
pub const CONTROL_HEARTBEAT_RESERVE: usize = 16;

/// 发送 Close 帧并关闭底层写端（发 close_notify）。
///
/// `version` 是本次会话协商出的协议版本（DESIGN §6.4）：Close 帧必须与会话内
/// 其余帧用同一版本，否则对端的帧解码器会因版本不符而拒收。
pub async fn graceful_close<W>(writer: &mut FramedWrite<W, FrameCodec>, reason: &str, version: u8)
where
    W: AsyncWrite + Unpin,
{
    if let Ok(frame) = Message::Close(Close {
        reason: Some(reason.into()),
    })
    .to_frame_with_version(version)
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

/// 批量控制消息发送：通道容量不足 `reserve` 时等待，给心跳响应留出槽位。
pub async fn send_with_timeout_reserved(
    tx: &mpsc::Sender<Message>,
    msg: Message,
    reserve: usize,
) -> bool {
    // 小容量测试通道/自定义通道不能被 reserve 卡死：保留至多 max_capacity-1 个槽位。
    let reserve = reserve.min(tx.max_capacity().saturating_sub(1));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(CONTROL_SEND_TIMEOUT);
    loop {
        if tx.capacity() > reserve {
            return send_with_timeout(tx, msg).await;
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return false;
        }
        let wait = remaining.min(Duration::from_millis(10));
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            _ = tx.closed() => return false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constants::PROTOCOL_VERSION;
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
        graceful_close(&mut framed, "test reason", PROTOCOL_VERSION).await;

        let mut reader = tokio::io::BufReader::new(b);
        let mut buf = Vec::new();
        let n = reader.read_to_end(&mut buf).await.unwrap();
        assert!(n > 0, "close frame bytes must be written");

        // 首字节应为协议版本，随后 msg_type 应为 MSG_CLOSE（见 protocol::frame）。
        assert_eq!(buf[0], crate::constants::PROTOCOL_VERSION);
        assert_eq!(buf[1], crate::protocol::msg::MSG_CLOSE);
    }
}

#[cfg(test)]
mod reserved_tests {
    use super::*;
    use crate::protocol::msg::Heartbeat;

    #[tokio::test]
    async fn reserved_send_waits_for_headroom() {
        let (tx, mut rx) = mpsc::channel::<Message>(4);
        tx.send(Message::Heartbeat(Heartbeat { ts: 1 }))
            .await
            .unwrap();
        tx.send(Message::Heartbeat(Heartbeat { ts: 2 }))
            .await
            .unwrap();
        let tx2 = tx.clone();
        let task = tokio::spawn(async move {
            send_with_timeout_reserved(&tx2, Message::Heartbeat(Heartbeat { ts: 3 }), 2).await
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!task.is_finished(), "sender must leave reserved capacity");
        let _ = rx.recv().await;
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(1), task)
                .await
                .unwrap()
                .unwrap()
        );
    }
}
