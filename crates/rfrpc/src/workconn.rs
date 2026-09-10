//! 工作连接建立（客户端侧）。
//!
//! 收到 `ReqWorkConn` 后：新建一条到服务端的 TCP 工作连接，首帧发
//! `StartWorkConn`（回传 work_id），再连本地服务，双向桥接（见 DESIGN §8.2）。

use std::sync::Arc;

use futures::SinkExt;
use rfrp_common::constants::{MAX_UDP_PACKET_SIZE, WORK_CONN_TIMEOUT_RFRPC};
use rfrp_common::error::{Error, Result};
use rfrp_common::protocol::frame::FrameCodec;
use rfrp_common::protocol::msg::*;
use rfrp_common::util::bridge::bridge;
use rfrp_common::util::stream::BoxedStream;
use rfrp_common::util::tcp::configure_tcp_stream;
use rfrp_common::util::udp::{read_udp_frame_into, write_udp_frame};
use tokio::net::{TcpStream, UdpSocket};
use tokio::time::{timeout, Duration};
use tokio_util::codec::Framed;

use crate::client::ClientState;

pub async fn handle_work_conn(req: ReqWorkConn, state: Arc<ClientState>) -> Result<()> {
    let proxy = match state.proxies.get(&req.proxy_name) {
        Some(p) => p,
        None => {
            state.metrics.inc_work_conn_failure();
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
    .map_err(|_| {
        state.metrics.inc_work_conn_failure();
        Error::Other("work connection connect timeout".into())
    })??;
    if let Err(e) = configure_tcp_stream(&work) {
        tracing::warn!(proxy = %req.proxy_name, error = %e, "failed to configure work TCP stream");
    }
    let use_tls = *state.work_conn_tls.lock().unwrap();
    let work: BoxedStream = if use_tls {
        let tls = state.tls.as_ref().ok_or_else(|| {
            state.metrics.inc_work_conn_failure();
            Error::Other("work_conn_tls enabled but client TLS not initialized".into())
        })?;
        let tls_work = timeout(
            Duration::from_secs(WORK_CONN_TIMEOUT_RFRPC),
            tls.connect(work),
        )
        .await
        .map_err(|_| {
            state.metrics.inc_work_conn_failure();
            Error::Other("work connection TLS handshake timeout".into())
        })??;
        Box::new(tls_work)
    } else {
        Box::new(work)
    };
    let mut framed = Framed::new(work, FrameCodec);

    let local_addr = format!("{}:{}", proxy.local_ip, proxy.local_port);

    if proxy.r#type == ProxyType::Udp {
        // UDP：本地用 UDP socket，工作连接上按长度前缀分帧（DESIGN §8.6）。
        let local = UdpSocket::bind("0.0.0.0:0").await?;
        if let Err(e) = local.connect(&local_addr).await {
            state.metrics.inc_work_conn_failure();
            tracing::warn!(proxy = %req.proxy_name, error = %e, "local udp connect failed; closing work connection");
            return Ok(());
        }
        framed
            .send(
                Message::StartWorkConn(StartWorkConn {
                    proxy_name: req.proxy_name.clone(),
                    work_id: req.work_id,
                })
                .to_frame()?,
            )
            .await?;
        let work_stream = framed.into_inner();
        state.metrics.inc_work_conn();
        tracing::info!(proxy = %req.proxy_name, work_id = req.work_id, tls = use_tls, "udp work connection established");
        return udp_bridge(work_stream, local, &req).await;
    }

    // 先回连本地服务，成功后再发 StartWorkConn。这样本地连接失败时不会让服务端把
    // 这条工作连接放入预热池，避免池中出现“死连接”（DESIGN §8.2 预建场景）。
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
            state.metrics.inc_work_conn_failure();
            tracing::warn!(proxy = %req.proxy_name, error = %e, "local connect failed; closing work connection");
            return Ok(());
        }
        Err(_) => {
            state.metrics.inc_work_conn_failure();
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

    state.metrics.inc_work_conn();
    tracing::info!(proxy = %req.proxy_name, work_id = req.work_id, tls = use_tls, "work connection established");
    let _ = bridge(work_stream, local).await;
    tracing::debug!(proxy = %req.proxy_name, work_id = req.work_id, "work bridge finished");
    Ok(())
}

/// UDP 分帧桥接：工作连接 <-> 本地 UDP socket。
async fn udp_bridge(
    mut work_stream: BoxedStream,
    local: UdpSocket,
    req: &ReqWorkConn,
) -> Result<()> {
    let mut buf = vec![0u8; MAX_UDP_PACKET_SIZE];
    // 复用下行帧缓冲，避免每包一次分配。
    let mut frame_buf = Vec::with_capacity(MAX_UDP_PACKET_SIZE);
    loop {
        tokio::select! {
            r = read_udp_frame_into(&mut work_stream, &mut frame_buf) => {
                match r {
                    Ok(Some(())) => {
                        if let Err(e) = local.send(&frame_buf).await {
                            tracing::warn!(proxy = %req.proxy_name, error = %e, "udp send to local failed");
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        tracing::warn!(proxy = %req.proxy_name, error = %e, "udp read frame error");
                        break;
                    }
                }
            }
            r = local.recv(&mut buf) => {
                match r {
                    Ok(n) => {
                        if let Err(e) = write_udp_frame(&mut work_stream, &buf[..n]).await {
                            tracing::warn!(proxy = %req.proxy_name, error = %e, "udp write frame error");
                            break;
                        }
                    }
                    Err(e) => {
                        tracing::warn!(proxy = %req.proxy_name, error = %e, "local udp recv error");
                        break;
                    }
                }
            }
        }
    }
    tracing::debug!(proxy = %req.proxy_name, work_id = req.work_id, "udp bridge finished");
    Ok(())
}

#[cfg(test)]
mod tests;
