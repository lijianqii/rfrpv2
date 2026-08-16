//! 工作连接处理（服务端侧）。
//!
//! 服务端 accept 到一条工作连接，首帧为 `StartWorkConn`，其后转透传字节流。
//! 按 `work_id` 取出对应的待处理用户连接，双向桥接（见 DESIGN §8.2）。

use std::sync::Arc;

use rfrp_common::constants::WORK_ID_POOL_RESERVED;
use rfrp_common::error::Result;
use rfrp_common::protocol::frame::Frame;
use rfrp_common::protocol::msg::*;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::bridge;
use crate::server::ServerState;

pub async fn handle_work_connection<S>(
    start_frame: Frame,
    stream: S,
    state: Arc<ServerState>,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let msg = Message::from_frame(&start_frame)?;
    let (proxy_name, work_id) = match msg {
        Message::StartWorkConn(s) => (s.proxy_name, s.work_id),
        _ => {
            return Err(rfrp_common::Error::Protocol(
                "expected StartWorkConn on work connection".into(),
            ))
        }
    };

    // work_id=0：预热池连接，归入所属会话的池，等待用户连接命中（§8.2）。
    if work_id == WORK_ID_POOL_RESERVED {
        match find_session_by_proxy(&state, &proxy_name) {
            Some(s) => {
                s.pools
                    .lock()
                    .unwrap()
                    .entry(proxy_name.clone())
                    .or_default()
                    .push(Box::new(stream));
                tracing::debug!(%proxy_name, "work connection pooled");
            }
            None => {
                tracing::warn!(%proxy_name, "no session owns proxy for pooled work connection");
            }
        }
        return Ok(());
    }

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

/// 按 proxy_name 查找其所属控制会话（用于把预热工作连接归入对应池，§8.2）。
fn find_session_by_proxy(
    state: &ServerState,
    proxy_name: &str,
) -> Option<Arc<crate::control::Session>> {
    let sessions = state.sessions.lock().unwrap();
    for s in sessions.values() {
        if s.proxies.lock().unwrap().contains_key(proxy_name) {
            return Some(s.clone());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use rfrp_common::constants::PROTOCOL_VERSION;
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

    #[tokio::test]
    async fn pooled_work_conn_no_session_is_safe() {
        // work_id=0 但没有任何会话拥有该 proxy：应 Ok 返回（§8.2 负路径）。
        let state = ServerState::new(); // sessions 为空
        let frame = Message::StartWorkConn(StartWorkConn {
            proxy_name: "ghost".into(),
            work_id: WORK_ID_POOL_RESERVED,
        })
        .to_frame()
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _client = TcpStream::connect(addr).await.unwrap();
        let (server, _peer) = listener.accept().await.unwrap();
        assert!(handle_work_connection(frame, server, state).await.is_ok());
    }

    #[tokio::test]
    async fn non_startworkconn_frame_errors() {
        // 工作连接首帧不是 StartWorkConn（此处用 Login 模拟）：应报错（§8.2）。
        let state = ServerState::new();
        let frame = Message::Login(Login {
            run_id: "x".into(),
            token: "".into(),
            version: PROTOCOL_VERSION,
        })
        .to_frame()
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _client = TcpStream::connect(addr).await.unwrap();
        let (server, _peer) = listener.accept().await.unwrap();
        assert!(handle_work_connection(frame, server, state).await.is_err());
    }
}
