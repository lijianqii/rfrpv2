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

    let local_addr = format!("{}:{}", proxy.local_ip, proxy.local_port);

    if proxy.r#type == ProxyType::Udp {
        // UDP：本地用 UDP socket，工作连接上按长度前缀分帧（DESIGN §8.6）。
        let local = UdpSocket::bind("0.0.0.0:0").await?;
        if let Err(e) = local.connect(&local_addr).await {
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
mod tests {
    use super::*;
    use futures::StreamExt;
    use rfrp_common::config::ClientProxy;
    use rfrp_common::protocol::msg::ProxyType;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn tcp_proxy(local_port: u16) -> ClientProxy {
        ClientProxy {
            name: "web".into(),
            r#type: ProxyType::Tcp,
            local_ip: "127.0.0.1".into(),
            local_port,
            remote_port: Some(8080),
            custom_domains: None,
            pool_size: 0,
        }
    }

    #[tokio::test]
    async fn unknown_proxy_returns_ok() {
        // 未知 proxy_name：不应连接、不应 panic，直接 Ok 返回（§8.2 负路径）。
        let state = Arc::new(ClientState {
            server_addr: "127.0.0.1:9".parse().unwrap(),
            run_id: "r".into(),
            proxies: HashMap::new(),
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
            proxies: HashMap::from([(
                "web".to_string(),
                ClientProxy {
                    name: "web".into(),
                    r#type: ProxyType::Tcp,
                    local_ip: "127.0.0.1".into(),
                    local_port: 1, // 无人监听
                    remote_port: Some(8080),
                    custom_domains: None,
                    pool_size: 0,
                },
            )]),
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

    #[tokio::test]
    async fn work_conn_tls_enabled_without_tls_errors() {
        // work_conn_tls=true 但客户端 TLS 未初始化：应返回 Err 且不 panic（§6.5 负路径）。
        let server = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = server.local_addr().unwrap();
        let state = Arc::new(ClientState {
            server_addr,
            run_id: "r".into(),
            proxies: HashMap::from([("web".to_string(), tcp_proxy(1))]),
            resps: Mutex::new(HashMap::new()),
            login_tx: Mutex::new(None),
            tls: None,
            work_conn_tls: Mutex::new(true),
        });
        let req = ReqWorkConn {
            proxy_name: "web".into(),
            work_id: 7,
        };
        let err = handle_work_conn(req, state).await.unwrap_err();
        assert!(err.to_string().contains("TLS not initialized"), "{err}");
    }

    #[tokio::test]
    async fn tcp_work_conn_sends_start_frame_and_bridges() {
        // 正常 TCP 路径：StartWorkConn 帧回传正确 work_id，随后桥接数据可回环（§8.2）。
        let server = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = server.local_addr().unwrap();
        let local = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local_port = local.local_addr().unwrap().port();
        let state = Arc::new(ClientState {
            server_addr,
            run_id: "r".into(),
            proxies: HashMap::from([("web".to_string(), tcp_proxy(local_port))]),
            resps: Mutex::new(HashMap::new()),
            login_tx: Mutex::new(None),
            tls: None,
            work_conn_tls: Mutex::new(false),
        });
        let req = ReqWorkConn {
            proxy_name: "web".into(),
            work_id: 42,
        };

        let task = tokio::spawn(async move { handle_work_conn(req, state).await });

        // 服务端侧接受工作连接与客户端本地连接。
        let (work, _) = server.accept().await.unwrap();
        let (mut local_conn, _) = local.accept().await.unwrap();

        // 读 StartWorkConn 帧，验证 work_id 回传。
        let mut framed = Framed::new(work, FrameCodec);
        let frame = framed.next().await.unwrap().expect("start frame");
        let msg = Message::from_frame(&frame).unwrap();
        match msg {
            Message::StartWorkConn(s) => assert_eq!(s.work_id, 42),
            other => panic!("expected StartWorkConn, got {other:?}"),
        }

        // 本地 echo：从 work 侧写入，经客户端桥接 + echo 后应原路返回。
        let echo = tokio::spawn(async move {
            let mut buf = [0u8; 16];
            loop {
                match local_conn.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        if local_conn.write_all(&buf[..n]).await.is_err() {
                            break;
                        }
                    }
                }
            }
        });
        let mut work = framed.into_inner();
        work.write_all(b"ping").await.unwrap();
        work.flush().await.unwrap();
        let mut reply = [0u8; 8];
        let n = work.read(&mut reply).await.unwrap();
        assert_eq!(&reply[..n], b"ping");

        // 关闭工作连接 → 桥接退出 → 任务正常 Ok 返回。
        drop(work);
        assert!(task.await.unwrap().is_ok());
        echo.await.unwrap();
    }

    #[tokio::test]
    async fn udp_work_conn_sends_start_frame_and_ends_on_eof() {
        // UDP 代理：工作连接发送 StartWorkConn 帧后进入分帧桥接；服务端关闭即退出（§8.6）。
        let server = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = server.local_addr().unwrap();
        let state = Arc::new(ClientState {
            server_addr,
            run_id: "r".into(),
            proxies: HashMap::from([(
                "udp-x".to_string(),
                ClientProxy {
                    name: "udp-x".into(),
                    r#type: ProxyType::Udp,
                    local_ip: "127.0.0.1".into(),
                    local_port: 9, // UDP connect 不校验可达性，任意端口均可
                    remote_port: Some(9000),
                    custom_domains: None,
                    pool_size: 0,
                },
            )]),
            resps: Mutex::new(HashMap::new()),
            login_tx: Mutex::new(None),
            tls: None,
            work_conn_tls: Mutex::new(false),
        });
        let req = ReqWorkConn {
            proxy_name: "udp-x".into(),
            work_id: 9,
        };

        let task = tokio::spawn(async move { handle_work_conn(req, state).await });

        let (work, _) = server.accept().await.unwrap();
        let mut framed = Framed::new(work, FrameCodec);
        let frame = framed.next().await.unwrap().expect("udp start frame");
        let msg = Message::from_frame(&frame).unwrap();
        match msg {
            Message::StartWorkConn(s) => assert_eq!(s.work_id, 9),
            other => panic!("expected StartWorkConn, got {other:?}"),
        }
        // 关闭工作连接 → 客户端 udp_bridge 读到 EOF 退出 → Ok。
        drop(framed);
        assert!(task.await.unwrap().is_ok());
    }
}
