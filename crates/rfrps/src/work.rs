//! 工作连接处理（服务端侧）。
//!
//! 服务端 accept 到一条工作连接，首帧为 `StartWorkConn`，其后转透传字节流。
//! 按 `work_id` 取出对应的待处理用户连接，双向桥接（见 DESIGN §8.2）。

use std::sync::Arc;

use rfrp_common::constants::{MAX_POOLED_WORK_CONNS_PER_PROXY, WORK_ID_POOL_RESERVED};
use rfrp_common::error::Result;
use rfrp_common::protocol::frame::Frame;
use rfrp_common::protocol::msg::*;
use rfrp_common::util::bridge::bridge;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::control::Session;
use crate::state::ServerState;

/// 校验工作连接令牌（常量时间比对，防时序侧信道）。
fn verify_work_conn_token(session: &Session, provided: Option<&str>) -> bool {
    match provided {
        Some(t) => rfrp_common::auth::verify_token(&session.work_conn_token, t),
        None => false,
    }
}

pub async fn handle_work_connection<S>(
    start_frame: Frame,
    stream: S,
    state: Arc<ServerState>,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let msg = Message::from_frame(&start_frame)?;
    let (proxy_name, work_id, token) = match msg {
        Message::StartWorkConn(s) => (s.proxy_name, s.work_id, s.work_conn_token),
        _ => {
            return Err(rfrp_common::Error::Protocol(
                "expected StartWorkConn on work connection".into(),
            ))
        }
    };

    // 工作连接鉴权（DESIGN §8.2）：工作连接是独立 TCP 连接、不携带登录信息，
    // 必须凭 LoginResp 下发的 per-session token 证明归属。否则任何能访问控制
    // 端口的人都能：① 注入预热池 → 下一个用户连接被桥接到攻击者（中间人）；
    // ② 凭顺序 work_id 认领他人的待处理用户连接。
    let session = match state.session_for_proxy(&proxy_name) {
        Some(s) => s,
        None => {
            tracing::warn!(%proxy_name, "work connection for unknown proxy, closing");
            return Ok(());
        }
    };
    if !verify_work_conn_token(&session, token.as_deref()) {
        tracing::warn!(%proxy_name, "work connection rejected: invalid or missing token");
        return Ok(());
    }

    // UDP 代理：工作连接走分帧协议，不走 TCP 桥接（DESIGN §8.6）。
    if crate::udp::is_udp_proxy(&state, &proxy_name) {
        if work_id == WORK_ID_POOL_RESERVED {
            tracing::warn!(%proxy_name, "udp proxy does not support pooled work conns");
            return Ok(());
        }
        if let Some(proxy) = crate::udp::get_udp_proxy(&state, &proxy_name) {
            return crate::udp::handle_udp_work_conn(proxy, work_id, Box::new(stream)).await;
        }
        return Ok(());
    }

    // work_id=0：预热池连接，归入所属会话的池，等待用户连接命中（§8.2）。
    if work_id == WORK_ID_POOL_RESERVED {
        let mut pools = session.pools.lock();
        let pool = pools.entry(proxy_name.clone()).or_default();
        // 池上限：`pool_size` 是客户端本地配置、不上送协议，服务端必须自设上限，
        // 否则持有合法 token 的连接可以把池子灌满（每条都是常驻 socket + 内存）。
        if pool.len() >= MAX_POOLED_WORK_CONNS_PER_PROXY {
            tracing::warn!(
                %proxy_name,
                limit = MAX_POOLED_WORK_CONNS_PER_PROXY,
                "pooled work connection rejected: pool full"
            );
            return Ok(()); // 关闭该连接（stream 在此 drop）
        }
        pool.push(Box::new(stream));
        drop(pools);
        tracing::debug!(%proxy_name, "work connection pooled");
        return Ok(());
    }

    // 待处理用户连接：必须属于同一会话且 proxy_name 一致（防止跨会话/跨代理认领）。
    let pending = {
        let mut map = state.pending.lock();
        let owned = map
            .get(&work_id)
            .is_some_and(|e| e.session_id == session.session_id && e.proxy_name == proxy_name);
        if !owned {
            tracing::warn!(
                work_id, %proxy_name,
                "work connection does not match a pending user connection of this session"
            );
            return Ok(());
        }
        map.remove(&work_id).expect("checked above")
    };

    let user = match pending.user {
        Some(u) => u,
        None => {
            tracing::warn!(work_id, "pending work user socket missing");
            return Ok(());
        }
    };

    tracing::info!(%proxy_name, work_id, "work connection established");
    // stream 已越过 StartWorkConn 首帧，剩余为透传字节；直接桥接。
    let _ = bridge(user, stream).await;
    tracing::debug!(%proxy_name, work_id, "work bridge finished");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use rfrp_common::constants::{MAX_POOLED_WORK_CONNS_PER_PROXY, PROTOCOL_VERSION};

    use std::sync::Arc;
    use std::time::Duration;
    use tokio::io::AsyncReadExt;
    use tokio::net::{TcpListener, TcpStream};

    fn start_frame(proxy_name: &str, work_id: u64, token: Option<&str>) -> Frame {
        Message::StartWorkConn(StartWorkConn {
            proxy_name: proxy_name.into(),
            work_id,
            work_conn_token: token.map(String::from),
        })
        .to_frame()
        .unwrap()
    }

    /// 构造"已登录且已注册 ssh 代理"的状态：`session_for_proxy` 需要它才能校验工作连接。
    fn state_with_session(token: &str) -> (Arc<ServerState>, Arc<Session>) {
        let state = ServerState::new();
        let session = crate::control::test_session_with_token("r1", token).0;
        state.sessions.lock().insert("r1".into(), session.clone());
        state.index_proxy("ssh", "r1");
        (state, session)
    }

    #[tokio::test]
    async fn unknown_work_id_closes_without_panic() {
        // work_id 不在 pending 中：应 Ok 返回，不桥接、不 panic（§8.2 负路径）。
        let state = ServerState::new();
        let frame = Message::StartWorkConn(StartWorkConn {
            proxy_name: "ssh".into(),
            work_id: 999,
            work_conn_token: None,
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
            work_conn_token: None,
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

    #[tokio::test]
    async fn pooled_work_conn_rejected_when_pool_full() {
        // 池上限：`pool_size` 是客户端本地配置、不上送协议，服务端必须自己设限，
        // 否则持有合法 token 的连接可以无限灌 work_id=0 撑爆服务端内存/句柄。
        let (state, session) = state_with_session("tok");
        let mut keep_alive = Vec::new();

        // 池未满：正常入池。
        let (client, server) = tokio::io::duplex(8);
        keep_alive.push(client);
        assert!(handle_work_connection(
            start_frame("ssh", WORK_ID_POOL_RESERVED, Some("tok")),
            server,
            state.clone()
        )
        .await
        .is_ok());
        assert_eq!(session.pools.lock()["ssh"].len(), 1);

        // 灌满到上限。
        {
            let mut pools = session.pools.lock();
            let pool = pools.entry("ssh".into()).or_default();
            while pool.len() < MAX_POOLED_WORK_CONNS_PER_PROXY {
                pool.push(Box::new(tokio::io::duplex(8).0));
            }
        }

        // 超限连接必须被关闭且不入池。
        let (mut rejected, server) = tokio::io::duplex(8);
        assert!(handle_work_connection(
            start_frame("ssh", WORK_ID_POOL_RESERVED, Some("tok")),
            server,
            state.clone()
        )
        .await
        .is_ok());
        assert_eq!(
            session.pools.lock()["ssh"].len(),
            MAX_POOLED_WORK_CONNS_PER_PROXY,
            "rejected connection must not be pooled"
        );
        let mut buf = [0u8; 1];
        let n = tokio::time::timeout(Duration::from_secs(2), rejected.read(&mut buf))
            .await
            .expect("rejected work connection should be closed promptly")
            .expect("read should succeed");
        assert_eq!(n, 0, "rejected pooled connection must be closed");
    }

    #[tokio::test]
    async fn pooled_work_conn_without_token_not_pooled() {
        // 无 token 的预热连接不得入池（防止未认证连接注入池做中间人，§8.2）。
        let (state, session) = state_with_session("tok");
        let (_client, server) = tokio::io::duplex(8);
        assert!(handle_work_connection(
            start_frame("ssh", WORK_ID_POOL_RESERVED, None),
            server,
            state
        )
        .await
        .is_ok());
        assert!(session.pools.lock().is_empty());
    }
}
