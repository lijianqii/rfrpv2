//! HTTP vhost 代理：按 Host 头路由到对应代理，把已读请求头连同剩余流一起桥接。

use std::sync::Arc;

use rfrp_common::config::ServerConfig;
use rfrp_common::error::Result;
use rfrp_common::protocol::msg::ProxyType;
use rfrp_common::util::stream::{BoxedStream, PrependStream};
use rfrp_common::util::tcp::configure_tcp_stream;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

use crate::control::Session;
use crate::listener::dispatch_user_connection;
use crate::server::ServerState;

/// HTTP vhost accept 循环：读取请求头取 Host，路由到对应代理。
pub async fn run_http_vhost(
    listener: TcpListener,
    state: Arc<ServerState>,
    _config: ServerConfig,
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, peer)) => {
                        if let Err(e) = configure_tcp_stream(&stream) {
                            tracing::warn!(%peer, error = %e, "failed to configure vhost TCP stream");
                        }
                        let state = state.clone();
                        tokio::spawn(async move {
                            let _ = handle_http_connection(stream, state).await;
                        });
                    }
                    Err(e) => {
                        tracing::warn!("vhost accept error: {e}");
                        break;
                    }
                }
            }
            _ = shutdown.cancelled() => {
                tracing::info!("vhost http listener shutting down");
                break;
            }
        }
    }
}

async fn handle_http_connection(stream: TcpStream, state: Arc<ServerState>) -> Result<()> {
    let (host, stream) = match read_request_head(stream).await? {
        Some(x) => x,
        None => return Ok(()), // 客户端未发完整请求头
    };

    let (session, proxy_name) = match find_proxy_by_domain(&state, &host) {
        Some(x) => x,
        None => {
            tracing::warn!(host = %host, "no vhost proxy matched, closing");
            return Ok(());
        }
    };

    // 确认该代理确实是 HTTP 类型（HTTPS vhost 走独立监听）。
    let is_http = session
        .proxies
        .lock()
        .unwrap()
        .get(&proxy_name)
        .map(|e| e.kind == ProxyType::Http)
        .unwrap_or(false);
    if !is_http {
        tracing::warn!(host = %host, proxy = %proxy_name, "proxy is not http type, closing");
        return Ok(());
    }

    dispatch_user_connection(proxy_name, stream, session, state);
    Ok(())
}

/// 读取 HTTP 请求头，返回 `(Host, 带已读缓冲的流)`。
async fn read_request_head(mut stream: TcpStream) -> Result<Option<(String, BoxedStream)>> {
    let mut buf = Vec::with_capacity(4096);
    let mut tmp = [0u8; 8192];
    loop {
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            return Ok(None); // 对端关闭
        }
        buf.extend_from_slice(&tmp[..n]);

        let mut headers = [httparse::EMPTY_HEADER; 32];
        let mut req = httparse::Request::new(&mut headers);
        match req.parse(&buf) {
            Ok(httparse::Status::Complete(_)) => {
                let host = req
                    .headers
                    .iter()
                    .find(|h| h.name.eq_ignore_ascii_case("host"))
                    .and_then(|h| std::str::from_utf8(h.value).ok())
                    .map(|h| strip_port(h).to_string());
                let stream: BoxedStream = Box::new(PrependStream::new(buf, Box::new(stream)));
                return Ok(host.map(|h| (h, stream)));
            }
            Ok(httparse::Status::Partial) => {
                if buf.len() > 64 * 1024 {
                    tracing::warn!("vhost request head too large, closing");
                    return Ok(None);
                }
            }
            Err(e) => {
                tracing::warn!("invalid vhost request head: {e}");
                return Ok(None);
            }
        }
    }
}

/// 去掉 Host 头中的端口部分（忽略 IPv6 的 `[...]:port` 场景）。
fn strip_port(host: &str) -> &str {
    if host.starts_with('[') {
        return host;
    }
    host.split(':').next().unwrap_or(host)
}

/// 按域名查找所属会话与代理名。
fn find_proxy_by_domain(state: &ServerState, host: &str) -> Option<(Arc<Session>, String)> {
    let sessions = state.sessions.lock().unwrap();
    for s in sessions.values() {
        if let Some(proxy) = s.proxy_domains.lock().unwrap().get(host) {
            return Some((s.clone(), proxy.clone()));
        }
    }
    None
}
