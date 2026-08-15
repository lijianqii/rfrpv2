//! 代理公网监听（服务端侧，M1 仅 TCP）。
//!
//! 注册成功后为每个 `remote_port` 起一个 accept 循环：每来一个用户连接，
//! 分配 work_id、登记待处理项、向客户端发 `ReqWorkConn`（见 DESIGN §8.2）。

use std::sync::Arc;

use rfrp_common::config::ServerConfig;
use rfrp_common::protocol::msg::*;
use rfrp_common::{constants::*, error::Result};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::time::{sleep, Duration};

use crate::control::Session;
use crate::server::{PendingWork, ServerState};

/// 注册一个 TCP 代理：校验后在 `remote_port` 起监听，返回 Ok 表示注册成功。
pub async fn register_proxy(
    np: &NewProxy,
    session: &Arc<Session>,
    state: &Arc<ServerState>,
    config: &ServerConfig,
) -> Result<()> {
    if np.r#type != ProxyType::Tcp {
        // M2 仅 TCP；其余类型在 M4 实现。
        return Err(rfrp_common::Error::Config(
            "only tcp proxy supported".into(),
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

    let listener = TcpListener::bind(("0.0.0.0", remote_port)).await;
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
    session_for_insert
        .proxies
        .lock()
        .unwrap()
        .insert(np.proxy_name.clone(), handle);
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
        match listener.accept().await {
            Ok((user, peer)) => {
                let work_id = state.next_work_id();
                tracing::debug!(%proxy_name, work_id, %peer, "user connected");
                {
                    let mut p = state.pending.lock().unwrap();
                    p.insert(
                        work_id,
                        PendingWork {
                            proxy_name: proxy_name.clone(),
                            session_id: session.session_id.clone(),
                            user: Some(user),
                        },
                    );
                }
                // 经控制连接请求客户端建立工作连接。
                if session
                    .tx
                    .send(Message::ReqWorkConn(ReqWorkConn {
                        proxy_name: proxy_name.clone(),
                        work_id,
                    }))
                    .await
                    .is_err()
                {
                    // 控制连接已断，清理待处理项。
                    state.pending.lock().unwrap().remove(&work_id);
                    continue;
                }
                // 超时兜底：用户连接长时间等不到工作连接则关闭（见 DESIGN §8.5）。
                spawn_pending_timeout(work_id, state.clone());
            }
            Err(e) => {
                tracing::warn!("proxy listener accept error: {e}");
                break;
            }
        }
    }
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
    use rfrp_common::config::{LogSection, ProxySection, ServerConfig, ServerSection};
    use std::collections::HashMap;
    use std::sync::Mutex;
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
            stop: Arc::new(Notify::new()),
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
}
