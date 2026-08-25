//! 工作连接建立（客户端侧）。
//!
//! 收到 `ReqWorkConn` 后：新建一条到服务端的 TCP 工作连接，首帧发
//! `StartWorkConn`（回传 work_id），再连本地服务，双向桥接（见 DESIGN §8.2）。

use std::sync::Arc;

use futures::SinkExt;
use rfrp_common::constants::WORK_CONN_TIMEOUT_RFRPC;
use rfrp_common::error::{Error, Result};
use rfrp_common::protocol::frame::FrameCodec;
use rfrp_common::protocol::msg::*;
use rfrp_common::util::bridge::bridge;
use rfrp_common::util::stream::BoxedStream;
use rfrp_common::util::tcp::configure_tcp_stream;
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};
use tokio_util::codec::Framed;

use crate::client::ClientState;

pub async fn handle_work_conn(req: ReqWorkConn, state: Arc<ClientState>) -> Result<()> {
    let proxy = state.proxies.iter().find(|p| p.name == req.proxy_name);
    let proxy = match proxy {
        Some(p) => p,
        None => {
            tracing::warn!(proxy = %req.proxy_name, "unknown proxy for work connection");
            return Ok(());
        }
    };

    // 工作连接到服务端；根据 LoginResp 下发的偏好决定是否 TLS（DESIGN §6.5）。
    // 本地总截止时间由 WORK_CONN_TIMEOUT_RFRPC 控制，避免悬挂。
    let work = timeout(
        Duration::from_secs(WORK_CONN_TIMEOUT_RFRPC),
        TcpStream::connect(state.server_addr),
    )
    .await
    .map_err(|_| Error::Other("work connection connect timeout".into()))??;
    if let Err(e) = configure_tcp_stream(&work) {
        tracing::warn!(proxy = %req.proxy_name, error = %e, "failed to configure work TCP stream");
    }
    let use_tls = *state.work_conn_tls.lock().unwrap();
    let work: BoxedStream = if use_tls {
        let tls = state.tls.as_ref().ok_or_else(|| {
            Error::Other("work_conn_tls enabled but client TLS not initialized".into())
        })?;
        let tls_work = timeout(
            Duration::from_secs(WORK_CONN_TIMEOUT_RFRPC),
            tls.connect(work),
        )
        .await
        .map_err(|_| Error::Other("work connection TLS handshake timeout".into()))??;
        Box::new(tls_work)
    } else {
        Box::new(work)
    };
    let mut framed = Framed::new(work, FrameCodec);

    // 先回连本地服务，成功后再发 StartWorkConn。这样本地连接失败时不会让服务端把
    // 这条工作连接放入预热池，避免池中出现“死连接”（DESIGN §8.2 预建场景）。
    let local_addr = format!("{}:{}", proxy.local_ip, proxy.local_port);
    let local = match timeout(
        Duration::from_secs(WORK_CONN_TIMEOUT_RFRPC),
        TcpStream::connect(&local_addr),
    )
    .await
    {
        Ok(Ok(l)) => {
            if let Err(e) = configure_tcp_stream(&l) {
                tracing::warn!(proxy = %req.proxy_name, error = %e, "failed to configure local TCP stream");
            }
            l
        }
        Ok(Err(e)) => {
            // 本地连不上：直接关闭工作连接（TCP FIN），服务端不会入池。
            tracing::warn!(proxy = %req.proxy_name, error = %e, "local connect failed; closing work connection");
            return Ok(());
        }
        Err(_) => {
            tracing::warn!(proxy = %req.proxy_name, "local connect timeout; closing work connection");
            return Ok(());
        }
    };

    framed
        .send(
            Message::StartWorkConn(StartWorkConn {
                proxy_name: req.proxy_name.clone(),
                work_id: req.work_id,
            })
            .to_frame()?,
        )
        .await?;
    // 首帧之后为透传字节，取回原始流（明文或 TLS）。
    let work_stream = framed.into_inner();

    tracing::info!(proxy = %req.proxy_name, work_id = req.work_id, tls = use_tls, "work connection established");
    let _ = bridge(work_stream, local).await;
    tracing::debug!(proxy = %req.proxy_name, work_id = req.work_id, "work bridge finished");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rfrp_common::config::ClientProxy;
    use rfrp_common::protocol::msg::ProxyType;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn unknown_proxy_returns_ok() {
        // 未知 proxy_name：不应连接、不应 panic，直接 Ok 返回（§8.2 负路径）。
        let state = Arc::new(ClientState {
            server_addr: "127.0.0.1:9".parse().unwrap(),
            run_id: "r".into(),
            proxies: vec![],
            resps: Mutex::new(HashMap::new()),
            login_tx: Mutex::new(None),
            tls: None,
            work_conn_tls: Mutex::new(false),
        });
        let req = ReqWorkConn {
            proxy_name: "nope".into(),
            work_id: 1,
        };
        assert!(handle_work_conn(req, state).await.is_ok());
    }

    #[tokio::test]
    async fn local_service_unreachable_closes_gracefully() {
        // 服务端可达，但本地服务不可达：仍应 Ok 返回（关闭工作连接），不 panic（§8.2/§8.5）。
        let server = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = server.local_addr().unwrap();
        let state = Arc::new(ClientState {
            server_addr,
            run_id: "r".into(),
            proxies: vec![ClientProxy {
                name: "web".into(),
                r#type: ProxyType::Tcp,
                local_ip: "127.0.0.1".into(),
                local_port: 1, // 无人监听
                remote_port: Some(8080),
                custom_domains: None,
                pool_size: 0,
            }],
            resps: Mutex::new(HashMap::new()),
            login_tx: Mutex::new(None),
            tls: None,
            work_conn_tls: Mutex::new(false),
        });
        let req = ReqWorkConn {
            proxy_name: "web".into(),
            work_id: 1,
        };
        assert!(handle_work_conn(req, state).await.is_ok());
    }
}
