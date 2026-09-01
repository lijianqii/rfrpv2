//! 控制连接关闭辅助（优雅关闭：发 Close 帧 + TLS close_notify）。

use futures::SinkExt;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::protocol::frame::{FrameCodec, FramedWrite};
use crate::protocol::msg::{Close, Message};

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
