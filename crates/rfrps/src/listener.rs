//! 代理公网监听（服务端侧，当前仅 TCP；UDP/HTTP/HTTPS 在 M4 扩展）。
//!
//! 注册成功后为每个 `remote_port` 起一个 accept 循环：每来一个用户连接，
//! 分配 work_id、登记待处理项、向客户端发 `ReqWorkConn`（见 DESIGN §8.2）。

use std::sync::Arc;

use rfrp_common::config::ServerConfig;
use rfrp_common::protocol::msg::*;
use rfrp_common::util::bridge::bridge;
use rfrp_common::util::stream::BoxedStream;
use rfrp_common::util::tcp::configure_tcp_stream;
use rfrp_common::{constants::*, error::Result};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::time::{sleep, Duration};

use crate::control::{ProxyEntry, Session};
use crate::server::{PendingWork, ServerState};
use crate::vhost::find_proxy_by_domain;

/// 注册代理：TCP 在 `remote_port` 起监听；HTTP 走共享 vhost 监听，仅登记域名。
pub async fn register_proxy(
    np: &NewProxy,
    session: &Arc<Session>,
    state: &Arc<ServerState>,
    config: &ServerConfig,
) -> Result<()> {
    if matches!(np.r#type, ProxyType::Http | ProxyType::Https) {
        // vhost 代理：不绑定独立端口，仅登记域名与元信息（共享 vhost 监听已在 Server 启动）。
        let domains = np.custom_domains.as_ref().ok_or_else(|| {
            rfrp_common::Error::Config("http/https proxy requires custom_domains".into())
        })?;
        // 域名全局唯一：与其他代理冲突则拒绝（DESIGN §6.6）。
        for d in domains {
            if let Some((_, owner)) = find_proxy_by_domain(state, d) {
                return Err(rfrp_common::Error::Config(format!(
                    "domain conflict: {d} owned by {owner}"
                )));
            }
        }
        let mut map = session.proxy_domains.lock().unwrap();
        for d in domains {
            map.insert(d.clone(), np.proxy_name.clone());
        }
        let handle = tokio::spawn(async {});
        session.proxies.lock().unwrap().insert(
            np.proxy_name.clone(),
            ProxyEntry {
                handle,
                kind: np.r#type,
            },
        );
        tracing::info!(proxy = %np.proxy_name, typ = ?np.r#type, "proxy registered (vhost)");
        return Ok(());
    }
    if np.r#type != ProxyType::Tcp {
        // UDP 在后续 M4 子任务实现。
        return Err(rfrp_common::Error::Config(
            "only tcp/http/https proxy supported".into(),
        ));
    }
    let remote_port = np
        .remote_port
        .ok_or_else(|| rfrp_common::Error::Config("tcp proxy requires remote_port".into()))?;
    if !config.proxy.is_port_allowed(remote_port)? {
        return Err(rfrp_common::Error::Config("port not allowed".into()));
    }
    {
        let proxies = session.proxies.lock().unwrap();
        if proxies.contains_key(&np.proxy_name) {
            return Err(rfrp_common::Error::Config("proxy_name exists".into()));
        }
    }

    let listener = TcpListener::bind((config.server.bind_addr.as_str(), remote_port)).await;
    let listener = match listener {
        Ok(l) => l,
        // 端口占用/权限问题不回显具体原因（见 DESIGN §8.5）。
        Err(_) => return Err(rfrp_common::Error::Config("internal error".into())),
    };

    let proxy_name = np.proxy_name.clone();
    let session = session.clone();
    let session_for_insert = session.clone();
    let state = state.clone();
    let handle = tokio::spawn(async move {
        proxy_accept_loop(listener, proxy_name, session, state).await;
    });
    session_for_insert.proxies.lock().unwrap().insert(
        np.proxy_name.clone(),
        ProxyEntry {
            handle,
            kind: np.r#type,
        },
    );
    if let Some(domains) = &np.custom_domains {
        let mut map = session_for_insert.proxy_domains.lock().unwrap();
        for d in domains {
            map.insert(d.clone(), np.proxy_name.clone());
        }
    }
    tracing::info!(proxy = %np.proxy_name, remote_port = ?np.remote_port, "proxy registered (tcp)");
    Ok(())
}

async fn proxy_accept_loop(
    listener: TcpListener,
    proxy_name: String,
    session: Arc<Session>,
    state: Arc<ServerState>,
) {
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                match accepted {
                    Ok((user, peer)) => {
                        if let Err(e) = configure_tcp_stream(&user) {
                            tracing::warn!(%proxy_name, %peer, error = %e, "failed to configure user TCP stream");
                        }
                        tracing::debug!(%proxy_name, %peer, "user connected");
                        dispatch_user_connection(proxy_name.clone(), Box::new(user), session.clone(), state.clone());
                    }
                    Err(e) => {
                        tracing::warn!("proxy listener accept error: {e}");
                        break;
                    }
                }
            }
            _ = state.shutdown.cancelled() => {
                tracing::info!(%proxy_name, "shutdown requested, closing proxy listener");
                break;
            }
        }
    }
}

/// 统一处理一条用户连接：优先命中预热池，否则登记 pending 并按需请求工作连接。
/// 桥接与 ReqWorkConn 均放入独立任务，避免阻塞 accept 循环。
pub(crate) fn dispatch_user_connection(
    proxy_name: String,
    user: BoxedStream,
    session: Arc<Session>,
    state: Arc<ServerState>,
) {
    // 优先命中预热池（§8.2）。
    let pooled = {
        let mut pools = session.pools.lock().unwrap();
        pools.get_mut(&proxy_name).and_then(|v| v.pop())
    };
    if let Some(work) = pooled {
        tracing::debug!(%proxy_name, "user connected; pool hit, bridging");
        let pname = proxy_name.clone();
        tokio::spawn(async move {
            let _ = bridge(user, work).await;
            tracing::debug!(proxy = %pname, "pooled work bridge finished");
        });
        // 立即请求补充预热连接（无需等待本次用户断开）。
        let tx = session.tx.clone();
        tokio::spawn(async move {
            let _ = tx
                .send(Message::ReqWorkConn(ReqWorkConn {
                    proxy_name,
                    work_id: WORK_ID_POOL_RESERVED,
                }))
                .await;
        });
        return;
    }

    let work_id = state.next_work_id();
    tracing::debug!(%proxy_name, work_id, "user connected (on-demand)");
    state.pending.lock().unwrap().insert(
        work_id,
        PendingWork {
            proxy_name: proxy_name.clone(),
            session_id: session.session_id.clone(),
            user: Some(user),
        },
    );

    let tx = session.tx.clone();
    let state2 = state.clone();
    tokio::spawn(async move {
        if tx
            .send(Message::ReqWorkConn(ReqWorkConn {
                proxy_name,
                work_id,
            }))
            .await
            .is_err()
        {
            // 控制连接已断，清理待处理项。
            state2.pending.lock().unwrap().remove(&work_id);
            return;
        }
        // 超时兜底：用户连接长时间等不到工作连接则关闭（见 DESIGN §8.5）。
        spawn_pending_timeout(work_id, state2);
    });
}

/// 超时清理：WORK_CONN_TIMEOUT_RFRPS 后仍未消费则关闭用户连接。
fn spawn_pending_timeout(work_id: u64, state: Arc<ServerState>) {
    tokio::spawn(async move {
        sleep(Duration::from_secs(WORK_CONN_TIMEOUT_RFRPS)).await;
        let user = {
            let mut p = state.pending.lock().unwrap();
            p.remove(&work_id).and_then(|pw| pw.user)
        };
        if let Some(mut u) = user {
            let _ = u.shutdown().await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::PendingWork;
    use crate::work::handle_work_connection;
    use rfrp_common::config::{LogSection, ProxySection, ServerConfig, ServerSection};
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::time::Duration;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::mpsc;
    use tokio::sync::Notify;

    fn test_config(allow_ports: &str) -> ServerConfig {
        let proxy = ProxySection {
            allow_ports: allow_ports.into(),
            ..Default::default()
        };
        ServerConfig {
            server: ServerSection {
                bind_addr: "127.0.0.1".into(),
                bind_port: 0,
                token: "".into(),
                tls_enable: false,
                tls_cert: None,
                tls_key: None,
                work_conn_tls: false,
            },
            dashboard: None,
            proxy,
            log: LogSection::default(),
        }
    }

    fn test_session() -> Arc<Session> {
        let (tx, _rx) = mpsc::channel::<Message>(8);
        Arc::new(Session {
            run_id: "r".into(),
            session_id: "s".into(),
            tx,
            proxies: Mutex::new(HashMap::new()),
            proxy_domains: Mutex::new(HashMap::new()),
            stop: Arc::new(Notify::new()),
            pools: Mutex::new(HashMap::new()),
        })
    }

    fn free_port() -> u16 {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    }

    #[test]
    fn next_work_id_starts_at_one_and_increments() {
        let state = ServerState::new();
        assert_eq!(state.next_work_id(), 1);
        assert_eq!(state.next_work_id(), 2);
        assert_eq!(state.next_work_id(), 3);
    }

    #[tokio::test]
    async fn register_rejects_non_tcp() {
        let state = ServerState::new();
        let session = test_session();
        let cfg = test_config("");
        let np = NewProxy {
            proxy_name: "p".into(),
            r#type: ProxyType::Udp,
            remote_port: Some(18080),
            custom_domains: None,
        };
        let r = register_proxy(&np, &session, &state, &cfg).await;
        assert!(r.is_err());
    }

    #[tokio::test]
    async fn register_rejects_missing_remote_port() {
        let state = ServerState::new();
        let session = test_session();
        let cfg = test_config("");
        let np = NewProxy {
            proxy_name: "p".into(),
            r#type: ProxyType::Tcp,
            remote_port: None,
            custom_domains: None,
        };
        assert!(register_proxy(&np, &session, &state, &cfg).await.is_err());
    }

    #[tokio::test]
    async fn register_rejects_port_not_allowed() {
        let state = ServerState::new();
        let session = test_session();
        // 仅允许 5000-5001，注册 18080 应被拒。
        let cfg = test_config("5000-5001");
        let np = NewProxy {
            proxy_name: "p".into(),
            r#type: ProxyType::Tcp,
            remote_port: Some(18080),
            custom_domains: None,
        };
        assert!(register_proxy(&np, &session, &state, &cfg).await.is_err());
    }

    #[tokio::test]
    async fn register_ok_then_duplicate_name_rejected() {
        let state = ServerState::new();
        let session = test_session();
        let cfg = test_config("");
        let np1 = NewProxy {
            proxy_name: "p".into(),
            r#type: ProxyType::Tcp,
            remote_port: Some(free_port()),
            custom_domains: None,
        };
        assert!(register_proxy(&np1, &session, &state, &cfg).await.is_ok());
        // 同名再注册（不同端口）应被拒。
        let np2 = NewProxy {
            proxy_name: "p".into(),
            r#type: ProxyType::Tcp,
            remote_port: Some(free_port()),
            custom_domains: None,
        };
        assert!(register_proxy(&np2, &session, &state, &cfg).await.is_err());
    }

    #[tokio::test]
    async fn register_rejects_occupied_port() {
        let state = ServerState::new();
        let session = test_session();
        let cfg = test_config("");
        // 先占用一个端口。
        let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = occupied.local_addr().unwrap().port();
        let np = NewProxy {
            proxy_name: "p".into(),
            r#type: ProxyType::Tcp,
            remote_port: Some(port),
            custom_domains: None,
        };
        // occupied 持有该端口直至 drop，注册应失败（内部错误）。
        assert!(register_proxy(&np, &session, &state, &cfg).await.is_err());
    }

    #[tokio::test]
    async fn pooled_work_connection_registered() {
        // work_id=0 的工作连接应归入会话池，供用户连接命中（§8.2）。
        let state = ServerState::new();
        let session = test_session();
        state
            .sessions
            .lock()
            .unwrap()
            .insert(session.run_id.clone(), session.clone());
        let cfg = test_config("");
        let np = NewProxy {
            proxy_name: "ssh".into(),
            r#type: ProxyType::Tcp,
            remote_port: Some(free_port()),
            custom_domains: None,
        };
        assert!(register_proxy(&np, &session, &state, &cfg).await.is_ok());

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _client = TcpStream::connect(addr).await.unwrap();
        let (server, _peer) = listener.accept().await.unwrap();
        let frame = Message::StartWorkConn(StartWorkConn {
            proxy_name: "ssh".into(),
            work_id: WORK_ID_POOL_RESERVED,
        })
        .to_frame()
        .unwrap();
        assert!(handle_work_connection(frame, server, state.clone())
            .await
            .is_ok());

        let pooled = session
            .pools
            .lock()
            .unwrap()
            .get("ssh")
            .map(|v| v.len())
            .unwrap_or(0);
        assert_eq!(pooled, 1);
    }

    #[tokio::test]
    async fn register_rejects_udp_proxy() {
        // UDP 尚未实现：应被拒。
        let state = ServerState::new();
        let session = test_session();
        let cfg = test_config("");
        let np = NewProxy {
            proxy_name: "p".into(),
            r#type: ProxyType::Udp,
            remote_port: Some(18080),
            custom_domains: None,
        };
        assert!(register_proxy(&np, &session, &state, &cfg).await.is_err());
    }

    #[tokio::test]
    async fn register_http_proxy_registers_domains() {
        let state = ServerState::new();
        let session = test_session();
        let cfg = test_config("");
        let np = NewProxy {
            proxy_name: "web".into(),
            r#type: ProxyType::Http,
            remote_port: None,
            custom_domains: Some(vec!["dev.example.com".into()]),
        };
        assert!(register_proxy(&np, &session, &state, &cfg).await.is_ok());
        assert_eq!(
            session
                .proxy_domains
                .lock()
                .unwrap()
                .get("dev.example.com")
                .map(|s| s.as_str()),
            Some("web")
        );
        let proxies = session.proxies.lock().unwrap();
        let entry = proxies.get("web").unwrap();
        assert_eq!(entry.kind, ProxyType::Http);
    }

    #[tokio::test]
    async fn register_https_proxy_registers_domains() {
        let state = ServerState::new();
        let session = test_session();
        let cfg = test_config("");
        let np = NewProxy {
            proxy_name: "web".into(),
            r#type: ProxyType::Https,
            remote_port: None,
            custom_domains: Some(vec!["secure.example.com".into()]),
        };
        assert!(register_proxy(&np, &session, &state, &cfg).await.is_ok());
        let proxies = session.proxies.lock().unwrap();
        let entry = proxies.get("web").unwrap();
        assert_eq!(entry.kind, ProxyType::Https);
    }

    #[tokio::test]
    async fn register_http_domain_conflict_rejected() {
        let state = ServerState::new();
        let session_a = test_session();
        state
            .sessions
            .lock()
            .unwrap()
            .insert(session_a.run_id.clone(), session_a.clone());
        let cfg = test_config("");
        let np1 = NewProxy {
            proxy_name: "a".into(),
            r#type: ProxyType::Http,
            remote_port: None,
            custom_domains: Some(vec!["dev.example.com".into()]),
        };
        assert!(register_proxy(&np1, &session_a, &state, &cfg).await.is_ok());

        let session_b = test_session();
        let np2 = NewProxy {
            proxy_name: "b".into(),
            r#type: ProxyType::Http,
            remote_port: None,
            custom_domains: Some(vec!["dev.example.com".into()]),
        };
        let r = register_proxy(&np2, &session_b, &state, &cfg).await;
        assert!(r.is_err(), "duplicate domain should be rejected");
    }

    #[tokio::test]
    async fn register_vhost_without_domains_rejected() {
        let state = ServerState::new();
        let session = test_session();
        let cfg = test_config("");
        let np = NewProxy {
            proxy_name: "web".into(),
            r#type: ProxyType::Http,
            remote_port: None,
            custom_domains: None,
        };
        assert!(register_proxy(&np, &session, &state, &cfg).await.is_err());
    }

    #[tokio::test]
    async fn pending_work_conn_cleaned_after_timeout() {
        // 待处理工作连接在 WORK_CONN_TIMEOUT_RFRPS 内未被消费，应被清理（§8.5）。
        let state = ServerState::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _client = TcpStream::connect(addr).await.unwrap();
        let (user, _peer) = listener.accept().await.unwrap();
        state.pending.lock().unwrap().insert(
            42,
            PendingWork {
                proxy_name: "ssh".into(),
                session_id: "s".into(),
                user: Some(Box::new(user)),
            },
        );
        spawn_pending_timeout(42, state.clone());
        // 超时后 pending 项被移除（用户侧连接被关闭）。
        tokio::time::sleep(Duration::from_secs(WORK_CONN_TIMEOUT_RFRPS + 2)).await;
        assert!(!state.pending.lock().unwrap().contains_key(&42));
    }
}
