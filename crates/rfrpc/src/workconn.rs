//! 工作连接建立（客户端侧）。
//!
//! 收到 `ReqWorkConn` 后：新建一条到服务端的 TCP 工作连接，首帧发
//! `StartWorkConn`（回传 work_id），再连本地服务，双向桥接（见 DESIGN §8.2）。

use std::sync::Arc;

use futures::SinkExt;
use rfrp_common::config::ClientConfig;
use rfrp_common::error::Result;
use rfrp_common::protocol::frame::FrameCodec;
use rfrp_common::protocol::msg::*;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::bridge;
use crate::client::ClientState;

pub async fn handle_work_conn(
    req: ReqWorkConn,
    state: Arc<ClientState>,
    _config: ClientConfig,
) -> Result<()> {
    let proxy = state.proxies.iter().find(|p| p.name == req.proxy_name);
    let proxy = match proxy {
        Some(p) => p,
        None => {
            tracing::warn!(proxy = %req.proxy_name, "unknown proxy for work connection");
            return Ok(());
        }
    };

    // 工作连接到服务端（M1 不加密）。
    let work = TcpStream::connect(state.server_addr).await?;
    let mut framed = Framed::new(work, FrameCodec);
    framed
        .send(
            Message::StartWorkConn(StartWorkConn {
                proxy_name: req.proxy_name.clone(),
                work_id: req.work_id,
            })
            .to_frame()?,
        )
        .await?;
    // 首帧之后为透传字节，取回原始 TcpStream。
    let work_stream = framed.into_inner();

    // 回连本地服务。
    let local_addr = format!("{}:{}", proxy.local_ip, proxy.local_port);
    let local = match TcpStream::connect(&local_addr).await {
        Ok(l) => l,
        Err(e) => {
            // 本地连不上：关闭工作连接（TCP FIN），服务端用户侧同步断开（见 DESIGN §8.2/§8.5）。
            tracing::warn!(proxy = %req.proxy_name, error = %e, "local connect failed; closing work connection");
            return Ok(());
        }
    };

    tracing::debug!(proxy = %req.proxy_name, work_id = req.work_id, "bridging work <-> local");
    let _ = bridge::bridge(work_stream, local).await;
    Ok(())
}
