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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::{TcpListener, TcpStream};

    #[tokio::test]
    async fn unknown_work_id_closes_without_panic() {
        // work_id 不在 pending 中：应 Ok 返回，不桥接、不 panic（§8.2 负路径）。
        let state = ServerState::new();
        let frame = Message::StartWorkConn(StartWorkConn {
            proxy_name: "ssh".into(),
            work_id: 999,
        })
        .to_frame()
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _client = TcpStream::connect(addr).await.unwrap();
        let (server, _peer) = listener.accept().await.unwrap();
        assert!(handle_work_connection(frame, server, state).await.is_ok());
    }
}
