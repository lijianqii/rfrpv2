//! HTTP/HTTPS vhost 代理：按 Host/SNI 路由到对应代理，
//! 把已读请求头连同剩余流一起桥接。

use std::sync::Arc;
use std::time::Duration;

use rfrp_common::constants::{HTTP_HEAD_TIMEOUT, TLS_HANDSHAKE_TIMEOUT};
use rfrp_common::crypto::ServerTls;
use rfrp_common::error::Result;
use rfrp_common::protocol::msg::ProxyType;
use rfrp_common::util::accept::AcceptRetry;
use rfrp_common::util::stream::{BoxedStream, PrependStream};
use rfrp_common::util::tcp::configure_tcp_stream;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

use crate::control::Session;
use crate::listener::dispatch_user_connection;
use crate::state::ServerState;

/// HTTP vhost accept 循环：读取请求头取 Host，路由到对应代理。
pub async fn run_http_vhost(
    listener: TcpListener,
    state: Arc<ServerState>,
    shutdown: CancellationToken,
) {
    // accept 出错不得结束循环：vhost 监听是进程级的，自行退出会让所有 vhost 代理
    // 静默失效（详见 util::accept 模块说明）。
    let mut retry = AcceptRetry::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, peer)) => {
                        retry.record_ok();
                        if let Err(e) = configure_tcp_stream(&stream) {
                            tracing::warn!(%peer, error = %e, "failed to configure vhost TCP stream");
                        }
                        let state = state.clone();
                        tokio::spawn(async move {
                            let _ = handle_http_connection(stream, state).await;
                        });
                    }
                    Err(e) => {
                        let backoff = retry.record_err();
                        if retry.should_log() {
                            tracing::warn!(
                                consecutive = retry.consecutive(),
                                error = %e,
                                "vhost http accept error; retrying"
                            );
                        }
                        tokio::time::sleep(backoff).await;
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
    let mut retry = AcceptRetry::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, peer)) => {
                        retry.record_ok();
                        if let Err(e) = configure_tcp_stream(&stream) {
                            tracing::warn!(%peer, error = %e, "failed to configure vhost TLS TCP stream");
                        }
                        let tls = tls.clone();
                        let state = state.clone();
                        tokio::spawn(async move {
                            let accepted = tokio::time::timeout(
                                Duration::from_secs(TLS_HANDSHAKE_TIMEOUT),
                                tls.accept(stream),
                            )
                            .await;
                            match accepted {
                                Err(_) => {
                                    tracing::debug!(%peer, "vhost TLS handshake timeout; closing");
                                }
                                Ok(Err(e)) => {
                                    tracing::warn!(%peer, error = %e, "vhost TLS accept failed");
                                }
                                Ok(Ok(tls_stream)) => {
                                    let sni = tls_stream
                                        .get_ref()
                                        .1
                                        .server_name()
                                        .map(|s| s.to_string());
                                    let _ = handle_https_connection(sni, tls_stream, state).await;
                                }
                            }
                        });
                    }
                    Err(e) => {
                        let backoff = retry.record_err();
                        if retry.should_log() {
                            tracing::warn!(
                                consecutive = retry.consecutive(),
                                error = %e,
                                "vhost https accept error; retrying"
                            );
                        }
                        tokio::time::sleep(backoff).await;
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
    let mut stream = stream;
    // 域名大小写不敏感，统一小写后路由。
    let host = host.to_lowercase();
    let (session, proxy_name) = match find_proxy_by_domain(&state, &host) {
        Some(x) => x,
        None => {
            tracing::warn!(host = %host, "no vhost proxy matched, returning 404");
            // 返回 404 而不是静默断连：用户/上游 LB 能明确区分"没有这个 vhost"与
            // "链路故障"，也避免浏览器显示连接重置。
            return respond_not_found(&mut stream).await;
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
        tracing::warn!(host = %host, proxy = %proxy_name, "proxy type mismatch, returning 404");
        return respond_not_found(&mut stream).await;
    }

    dispatch_user_connection(proxy_name, stream, session, state);
    Ok(())
}

/// 路由失败时回一个最小 404（HTTP/1.1 + Connection: close），随后关闭连接。
///
/// HTTPS vhost 的 TLS 已在 rfrps 终止，这里写出的明文响应会经 TLS 加密回给用户。
async fn respond_not_found(stream: &mut BoxedStream) -> Result<()> {
    rfrp_common::util::http::write_response(
        stream,
        404,
        "text/plain; charset=utf-8",
        "404 Not Found: no rfrp proxy matched this vhost\n",
        None,
    )
    .await?;
    Ok(())
}

/// 读取 HTTP 请求头，返回 `(Host, 带已读缓冲的流)`。
async fn read_request_head<S>(stream: S) -> Result<Option<(String, BoxedStream)>>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    read_request_head_with_timeout(stream, Duration::from_secs(HTTP_HEAD_TIMEOUT)).await
}

/// 同 [`read_request_head`]，但可指定整体超时（测试用，避免等待 10s 常量）。
async fn read_request_head_with_timeout<S>(
    mut stream: S,
    timeout: Duration,
) -> Result<Option<(String, BoxedStream)>>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let mut buf = Vec::with_capacity(4096);
    let mut tmp = [0u8; 8192];
    // 整体截止时间：慢速请求（slowloris）不得长期占用任务与套接字。
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            tracing::debug!("vhost request head timeout, closing");
            return Ok(None);
        }
        let n = match tokio::time::timeout(remaining, stream.read(&mut tmp)).await {
            Ok(r) => r?,
            Err(_) => {
                tracing::debug!("vhost request head timeout, closing");
                return Ok(None);
            }
        };
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
                // HTTP/1.1 要求 Host 头；缺失时用空 host 走路由，由路由层回 404，
                // 而不是静默断连（用户能看到明确原因）。
                return Ok(Some((host.unwrap_or_default(), stream)));
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

/// 按域名查找所属会话与代理名（O(1)，走全局域名索引）。
pub(crate) fn find_proxy_by_domain(
    state: &ServerState,
    host: &str,
) -> Option<(Arc<Session>, String)> {
    state.session_for_domain(host)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::ServerState;
    use tokio::sync::mpsc;

    fn test_session(run_id: &str, domains: &[&str]) -> Arc<Session> {
        let (tx, _rx) = mpsc::channel::<rfrp_common::protocol::msg::Message>(8);
        let session = Arc::new(Session {
            run_id: run_id.into(),
            session_id: "s".into(),
            work_conn_token: "tok".into(),
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
        let s1 = test_session("r1", &["dev.example.com"]);
        let s2 = test_session("r2", &["other.example.com"]);
        {
            let mut sessions = state.sessions.lock().unwrap();
            sessions.insert("r1".into(), s1.clone());
            sessions.insert("r2".into(), s2.clone());
        }
        // 域名索引由注册路径维护；这里按同样方式登记（避免测试绕过真实路径的语义）。
        state.index_domain("dev.example.com", "r1", "web");
        state.index_domain("other.example.com", "r2", "web");

        let (session, proxy) =
            find_proxy_by_domain(&state, "dev.example.com").expect("indexed domain");
        assert_eq!(proxy, "web");
        assert!(
            Arc::ptr_eq(&session, &s1),
            "must resolve to the owner session"
        );

        assert!(find_proxy_by_domain(&state, "missing.example.com").is_none());

        // 会话清理后索引必须失效（否则会命中已注销的会话）。
        state.unindex_domains(vec!["dev.example.com".to_string()]);
        assert!(find_proxy_by_domain(&state, "dev.example.com").is_none());
        assert!(find_proxy_by_domain(&state, "other.example.com").is_some());
    }
}

#[cfg(test)]
mod head_tests {
    use super::*;
    use crate::control::ProxyEntry;
    use crate::state::ServerState;
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
            work_conn_token: "tok".into(),
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
        state.index_domain("dev.example.com", "r", "web");

        let (mut a, b) = duplex(1024);
        a.write_all(b"x").await.unwrap();
        let stream: BoxedStream = Box::new(b);
        let r = route_and_dispatch("dev.example.com".into(), ProxyType::Http, stream, state).await;
        assert!(r.is_ok(), "type mismatch should not error");
    }

    #[tokio::test]
    async fn read_request_head_times_out_on_slow_client() {
        // 慢速请求（slowloris）：只发一半请求头后停住，整体超时后必须关闭（返回 None），
        // 不得长期占用任务与套接字。
        let (mut a, b) = duplex(4096);
        a.write_all(b"GET / HTTP/1.1\r\nHost: dev.example.com\r\n")
            .await
            .unwrap();
        let started = tokio::time::Instant::now();
        let out = read_request_head_with_timeout(b, Duration::from_millis(150))
            .await
            .unwrap();
        assert!(out.is_none(), "incomplete head must time out");
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "must not hang: {:?}",
            started.elapsed()
        );
        // 保持写端存活到断言之后，避免提前 EOF 掩盖超时路径。
        drop(a);
    }
}
