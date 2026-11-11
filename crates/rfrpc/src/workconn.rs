//! 工作连接建立（客户端侧）。
//!
//! 收到 `ReqWorkConn` 后：新建一条到服务端的 TCP 工作连接，首帧发
//! `StartWorkConn`（回传 work_id），再连本地服务，双向桥接（见 DESIGN §8.2）。

use std::sync::Arc;

use bytes::Bytes;
use futures::SinkExt;
use rfrp_common::constants::{
    MAX_UDP_PACKET_SIZE, UDP_READ_BATCH, UDP_RECV_BATCH, WORK_CONN_TIMEOUT_RFRPC,
};
use rfrp_common::error::{Error, Result};
use rfrp_common::protocol::frame::FrameCodec;
use rfrp_common::protocol::msg::*;
use rfrp_common::util::bridge::bridge;
use rfrp_common::util::stream::BoxedStream;
use rfrp_common::util::tcp::configure_tcp_stream;
use rfrp_common::util::udp::{append_udp_frame, enlarge_recv_buffer, UdpFrameBuf};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpStream, UdpSocket};
use tokio::time::{timeout_at, Duration};
use tokio_util::codec::Framed;

use crate::client::ClientState;

/// 根据 local_ip 选择 UDP socket 的绑定地址。
///
/// 只识别 IP 字面量；域名仍由 `UdpSocket::connect` 解析，若域名解析到 IPv6
/// 则仍可能失败（当前配置校验允许域名，但 UDP 本地 socket 未做多地址回退）。
fn udp_bind_addr(local_ip: &str) -> &'static str {
    let bare = local_ip.trim_start_matches('[').trim_end_matches(']');
    match bare.parse::<std::net::IpAddr>() {
        Ok(std::net::IpAddr::V6(_)) => "[::]:0",
        _ => "0.0.0.0:0",
    }
}

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
    // 整个建连流程共用一个总截止时间，避免“每段都 8s”累计超过服务端 10s pending 窗口。
    let deadline = tokio::time::Instant::now() + Duration::from_secs(WORK_CONN_TIMEOUT_RFRPC);
    let work = timeout_at(deadline, TcpStream::connect(state.server_addr))
        .await
        .map_err(|_| {
            state.metrics.inc_work_conn_failure();
            Error::Other("work connection connect timeout".into())
        })??;
    if let Err(e) = configure_tcp_stream(&work) {
        tracing::warn!(proxy = %req.proxy_name, error = %e, "failed to configure work TCP stream");
    }
    let use_tls = *state.work_conn_tls.lock();
    let work: BoxedStream = if use_tls {
        let tls = state.tls.as_ref().ok_or_else(|| {
            state.metrics.inc_work_conn_failure();
            Error::Other("work_conn_tls enabled but client TLS not initialized".into())
        })?;
        let tls_work = timeout_at(deadline, tls.connect(work))
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

    if proxy.r#type == ProxyType::Udp {
        // UDP：本地用 UDP socket，工作连接上按长度前缀分帧（DESIGN §8.6）。
        // 按 local_ip 的地址族选择绑定：IPv4 用 0.0.0.0，IPv6 字面量用 [::]，
        // 否则 IPv6 本地服务会因地址族不匹配而 connect 失败。
        let local = UdpSocket::bind(udp_bind_addr(&proxy.local_ip)).await?;
        // 用 `(host, port)` 元组形式：允许域名，且 IPv6 字面量无需手工加方括号。
        match timeout_at(
            deadline,
            local.connect((proxy.local_ip.as_str(), proxy.local_port)),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                state.metrics.inc_work_conn_failure();
                tracing::warn!(
                    proxy = %req.proxy_name, local_ip = %proxy.local_ip, local_port = proxy.local_port, error = %e,
                    "local udp connect failed; closing work connection"
                );
                return Ok(());
            }
            Err(_) => {
                state.metrics.inc_work_conn_failure();
                tracing::warn!(
                    proxy = %req.proxy_name, local_ip = %proxy.local_ip, local_port = proxy.local_port,
                    "local udp connect timeout; closing work connection"
                );
                return Ok(());
            }
        }
        let work_conn_token = state.work_conn_token.lock().clone();
        // 本地服务回包（RDP 服务端 → 用户方向）也走这个 socket：突发时先由内核缓冲吸收。
        if let Err(e) = enlarge_recv_buffer(&local) {
            tracing::debug!(proxy = %req.proxy_name, error = %e, "failed to enlarge local udp recv buffer");
        }
        let frame = Message::StartWorkConn(StartWorkConn {
            proxy_name: req.proxy_name.clone(),
            work_id: req.work_id,
            work_conn_token,
        })
        .to_frame()?;
        match timeout_at(deadline, framed.send(frame)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                state.metrics.inc_work_conn_failure();
                tracing::warn!(proxy = %req.proxy_name, error = %e, "udp start frame send failed");
                return Ok(());
            }
            Err(_) => {
                state.metrics.inc_work_conn_failure();
                tracing::warn!(proxy = %req.proxy_name, "udp start frame send timeout");
                return Ok(());
            }
        }
        let work_stream = framed.into_inner();
        state.metrics.inc_work_conn();
        tracing::debug!(proxy = %req.proxy_name, work_id = req.work_id, tls = use_tls, "udp work connection established");
        return udp_bridge(work_stream, local, &req).await;
    }

    // 先回连本地服务，成功后再发 StartWorkConn。这样本地连接失败时不会让服务端把
    // 这条工作连接放入预热池，避免池中出现“死连接”（DESIGN §8.2 预建场景）。
    let local = match timeout_at(
        deadline,
        TcpStream::connect((proxy.local_ip.as_str(), proxy.local_port)),
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
            tracing::warn!(
                proxy = %req.proxy_name, local_ip = %proxy.local_ip, local_port = proxy.local_port, error = %e,
                "local connect failed; closing work connection"
            );
            return Ok(());
        }
        Err(_) => {
            state.metrics.inc_work_conn_failure();
            tracing::warn!(
                proxy = %req.proxy_name, local_ip = %proxy.local_ip, local_port = proxy.local_port,
                "local connect timeout; closing work connection"
            );
            return Ok(());
        }
    };

    let work_conn_token = state.work_conn_token.lock().clone();
    let frame = Message::StartWorkConn(StartWorkConn {
        proxy_name: req.proxy_name.clone(),
        work_id: req.work_id,
        work_conn_token,
    })
    .to_frame()?;
    match timeout_at(deadline, framed.send(frame)).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            state.metrics.inc_work_conn_failure();
            tracing::warn!(proxy = %req.proxy_name, error = %e, "start frame send failed");
            return Ok(());
        }
        Err(_) => {
            state.metrics.inc_work_conn_failure();
            tracing::warn!(proxy = %req.proxy_name, "start frame send timeout");
            return Ok(());
        }
    }
    // 首帧之后为透传字节，取回原始流（明文或 TLS）。
    let work_stream = framed.into_inner();

    state.metrics.inc_work_conn();
    tracing::debug!(proxy = %req.proxy_name, work_id = req.work_id, tls = use_tls, "work connection established");
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
    // 本地 UDP 收包缓冲：只在本任务内复用，不再每包分配。
    let mut buf = vec![0u8; MAX_UDP_PACKET_SIZE];
    // 上行（本地 → 工作连接）写缓冲：一批数据报合成一次 write，TLS 下即一个 record。
    // 从小容量起步，按实际突发增长，避免每个会话固定占 64 KiB。
    let mut write_buf = Vec::with_capacity(rfrp_common::constants::UDP_BUF_INITIAL_BYTES);
    // 下行：一次 read 解析多帧（突发时内核里通常已排好多个完整帧）。
    let mut reader = UdpFrameBuf::new();
    let mut down: Vec<Bytes> = Vec::with_capacity(UDP_READ_BATCH);
    let mut send_error_logged = false;
    loop {
        tokio::select! {
            r = reader.read_batch(&mut work_stream, &mut down, UDP_READ_BATCH) => {
                match r {
                    Ok(n) if n > 0 => {
                        for d in &down {
                            // 与服务端 forward_to_client 的策略一致：单个数据报发送失败
                            // 不应丢掉同批其余数据报（UDP 本身允许逐包丢失）。
                            // 连续失败只记一条日志，避免刷屏。
                            match local.send(d).await {
                                Ok(_) => send_error_logged = false,
                                Err(e) => {
                                    if !send_error_logged {
                                        tracing::warn!(
                                            proxy = %req.proxy_name, error = %e,
                                            "udp send to local failed"
                                        );
                                        send_error_logged = true;
                                    }
                                }
                            }
                        }
                    }
                    Ok(_) => break, // EOF
                    Err(e) => {
                        tracing::warn!(proxy = %req.proxy_name, error = %e, "udp read frame error");
                        break;
                    }
                }
            }
            r = local.recv(&mut buf) => {
                match r {
                    Ok(n) => {
                        write_buf.clear();
                        if let Err(e) = append_udp_frame(&mut write_buf, &buf[..n]) {
                            tracing::warn!(proxy = %req.proxy_name, error = %e, "udp frame encode error");
                            break;
                        }
                        // 同一唤醒内继续 drain 本地 socket：RDP-UDP 突发时把多个数据报
                        // 合并成一次工作连接写入；无更多数据时立即停止，不引入额外延迟。
                        let mut batch_err = None;
                        for _ in 1..UDP_RECV_BATCH {
                            match local.try_recv(&mut buf) {
                                Ok(n) => {
                                    if let Err(e) = append_udp_frame(&mut write_buf, &buf[..n]) {
                                        batch_err = Some(e);
                                        break;
                                    }
                                }
                                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                                Err(e) => {
                                    batch_err = Some(e);
                                    break;
                                }
                            }
                        }
                        if let Some(e) = batch_err {
                            tracing::warn!(proxy = %req.proxy_name, error = %e, "udp local recv/encode error");
                            break;
                        }
                        if let Err(e) = work_stream.write_all(&write_buf).await {
                            tracing::warn!(proxy = %req.proxy_name, error = %e, "udp write frames error");
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
