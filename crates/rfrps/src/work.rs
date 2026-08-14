//! 工作连接处理（服务端侧）。
//!
//! 服务端 accept 到一条工作连接，首帧为 `StartWorkConn`，其后转透传字节流。
//! 按 `work_id` 取出对应的待处理用户连接，双向桥接（见 DESIGN §8.2）。

use std::sync::Arc;

use rfrp_common::error::Result;
use rfrp_common::protocol::frame::Frame;
use rfrp_common::protocol::msg::*;
use tokio::net::TcpStream;

use crate::bridge;
use crate::server::ServerState;

pub async fn handle_work_connection(
    start_frame: Frame,
    stream: TcpStream,
    state: Arc<ServerState>,
) -> Result<()> {
    let msg = Message::from_frame(&start_frame)?;
    let (proxy_name, work_id) = match msg {
        Message::StartWorkConn(s) => (s.proxy_name, s.work_id),
        _ => {
            return Err(rfrp_common::Error::Protocol(
                "expected StartWorkConn on work connection".into(),
            ))
        }
    };

    let pending = state.pending.lock().unwrap().remove(&work_id);
    let pending = match pending {
        Some(p) => p,
        None => {
            tracing::warn!(work_id, "no pending work connection for work_id");
            return Ok(());
        }
    };

    let user = match pending.user {
        Some(u) => u,
        None => {
            tracing::warn!(work_id, "pending work user socket missing");
            return Ok(());
        }
    };

    tracing::debug!(%proxy_name, work_id, "bridging work connection");
    // stream 已越过 StartWorkConn 首帧，剩余为透传字节；直接桥接。
    let _ = bridge::bridge(user, stream).await;
    Ok(())
}
