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
