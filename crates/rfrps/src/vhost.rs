//! HTTP/HTTPS vhost 代理：按 Host/SNI 路由到对应代理，
//! 把已读请求头连同剩余流一起桥接。

use std::sync::Arc;

use rfrp_common::crypto::ServerTls;
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
    route_and_dispatch(host, ProxyType::Http, stream, state).await
}

/// HTTPS vhost accept 循环：TLS 终止后按 SNI/Host 路由。
pub async fn run_https_vhost(
    listener: TcpListener,
    tls: ServerTls,
    state: Arc<ServerState>,
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, peer)) => {
                        if let Err(e) = configure_tcp_stream(&stream) {
                            tracing::warn!(%peer, error = %e, "failed to configure vhost TLS TCP stream");
                        }
                        let tls = tls.clone();
                        let state = state.clone();
                        tokio::spawn(async move {
                            match tls.accept(stream).await {
                                Ok(tls_stream) => {
                                    let sni = tls_stream
                                        .get_ref()
                                        .1
                                        .server_name()
                                        .map(|s| s.to_string());
                                    let _ = handle_https_connection(sni, tls_stream, state).await;
                                }
                                Err(e) => {
                                    tracing::warn!(%peer, error = %e, "vhost TLS accept failed");
                                }
                            }
                        });
                    }
                    Err(e) => {
                        tracing::warn!("vhost https accept error: {e}");
                        break;
                    }
                }
            }
            _ = shutdown.cancelled() => {
                tracing::info!("vhost https listener shutting down");
                break;
            }
        }
    }
}

async fn handle_https_connection<S>(
    sni: Option<String>,
    stream: S,
    state: Arc<ServerState>,
) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (host, stream) = match read_request_head(stream).await? {
        Some(x) => x,
        None => return Ok(()),
    };
    // SNI 优先（仅当 SNI 能命中代理时使用），否则回退到 Host 头。
    let host = match &sni {
        Some(s) if find_proxy_by_domain(&state, s).is_some() => s.clone(),
        _ => host,
    };
    route_and_dispatch(host, ProxyType::Https, stream, state).await
}

/// 按域名找到代理后做类型校验并分发用户连接。
async fn route_and_dispatch(
    host: String,
    expected_kind: ProxyType,
    stream: BoxedStream,
    state: Arc<ServerState>,
) -> Result<()> {
    // 域名大小写不敏感，统一小写后路由。
    let host = host.to_lowercase();
    let (session, proxy_name) = match find_proxy_by_domain(&state, &host) {
        Some(x) => x,
        None => {
            tracing::warn!(host = %host, "no vhost proxy matched, closing");
            return Ok(());
        }
    };

    let kind_ok = session
        .proxies
        .lock()
        .unwrap()
        .get(&proxy_name)
        .map(|e| e.kind == expected_kind)
        .unwrap_or(false);
    if !kind_ok {
        tracing::warn!(host = %host, proxy = %proxy_name, "proxy type mismatch, closing");
        return Ok(());
    }

    dispatch_user_connection(proxy_name, stream, session, state);
    Ok(())
}

/// 读取 HTTP 请求头，返回 `(Host, 带已读缓冲的流)`。
async fn read_request_head<S>(mut stream: S) -> Result<Option<(String, BoxedStream)>>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
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
pub(crate) fn find_proxy_by_domain(
    state: &ServerState,
    host: &str,
) -> Option<(Arc<Session>, String)> {
    let sessions = state.sessions.lock().unwrap();
    for s in sessions.values() {
        if let Some(proxy) = s.proxy_domains.lock().unwrap().get(host) {
            return Some((s.clone(), proxy.clone()));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::ServerState;
    use tokio::sync::mpsc;

    fn test_session(domains: &[&str]) -> Arc<Session> {
        let (tx, _rx) = mpsc::channel::<rfrp_common::protocol::msg::Message>(8);
        let session = Arc::new(Session {
            run_id: "r".into(),
            session_id: "s".into(),
            tx,
            proxies: std::sync::Mutex::new(std::collections::HashMap::new()),
            proxy_domains: std::sync::Mutex::new(std::collections::HashMap::new()),
            stop: Arc::new(tokio::sync::Notify::new()),
            pools: std::sync::Mutex::new(std::collections::HashMap::new()),
        });
        {
            let mut map = session.proxy_domains.lock().unwrap();
            for d in domains {
                map.insert(d.to_string(), "web".to_string());
            }
        }
        session
    }

    fn test_state() -> Arc<ServerState> {
        ServerState::new()
    }

    #[test]
    fn strip_port_handles_variants() {
        assert_eq!(strip_port("dev.example.com"), "dev.example.com");
        assert_eq!(strip_port("dev.example.com:8080"), "dev.example.com");
        assert_eq!(strip_port("[::1]:8080"), "[::1]:8080");
    }

    #[test]
    fn find_proxy_by_domain_finds_and_skips() {
        let state = test_state();
        {
            let mut sessions = state.sessions.lock().unwrap();
            sessions.insert("s1".into(), test_session(&["dev.example.com"]));
            sessions.insert("s2".into(), test_session(&["other.example.com"]));
        }

        let hit = find_proxy_by_domain(&state, "dev.example.com");
        assert!(hit.is_some());
        assert_eq!(hit.unwrap().1, "web");

        assert!(find_proxy_by_domain(&state, "missing.example.com").is_none());
    }
}

#[cfg(test)]
mod head_tests {
    use super::*;
    use crate::control::ProxyEntry;
    use crate::server::ServerState;
    use tokio::io::{duplex, AsyncWriteExt};
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn read_request_head_partial_eof_returns_none() {
        let (mut a, b) = duplex(1024);
        a.write_all(b"GET / HTTP/1.1\r\nHost: dev.example.com")
            .await
            .unwrap();
        drop(a);
        assert!(read_request_head(b).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn read_request_head_malformed_returns_none() {
        let (mut a, b) = duplex(1024);
        a.write_all(b"garbage\r\n\r\n").await.unwrap();
        assert!(read_request_head(b).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn route_and_dispatch_type_mismatch_ok() {
        // 会话里代理是 Tcp 类型，但 vhost 期望 Http：应返回 Ok 且不建立连接。
        let state = ServerState::new();
        let (tx, _rx) = mpsc::channel::<rfrp_common::protocol::msg::Message>(8);
        let session = Arc::new(Session {
            run_id: "r".into(),
            session_id: "s".into(),
            tx,
            proxies: std::sync::Mutex::new(std::collections::HashMap::new()),
            proxy_domains: std::sync::Mutex::new(std::collections::HashMap::new()),
            stop: Arc::new(tokio::sync::Notify::new()),
            pools: std::sync::Mutex::new(std::collections::HashMap::new()),
        });
        {
            let mut m = session.proxy_domains.lock().unwrap();
            m.insert("dev.example.com".into(), "web".into());
        }
        session.proxies.lock().unwrap().insert(
            "web".into(),
            ProxyEntry {
                handle: tokio::spawn(async {}),
                kind: ProxyType::Tcp,
            },
        );
        {
            let mut sessions = state.sessions.lock().unwrap();
            sessions.insert("r".into(), session);
        }

        let (mut a, b) = duplex(1024);
        a.write_all(b"x").await.unwrap();
        let stream: BoxedStream = Box::new(b);
        let r = route_and_dispatch("dev.example.com".into(), ProxyType::Http, stream, state).await;
        assert!(r.is_ok(), "type mismatch should not error");
    }
}
