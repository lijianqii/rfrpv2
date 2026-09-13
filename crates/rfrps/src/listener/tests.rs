//! 代理注册与用户连接分发单元测试。

use super::*;
use crate::state::PendingWork;
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
            tcp_keepalive_secs: None,
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
        work_conn_token: "tok".into(),
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
async fn register_udp_port_not_allowed_rejected() {
    let state = ServerState::new();
    let session = test_session();
    let cfg = test_config("5000-5001");
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
    let err = register_proxy(&np, &session, &state, &cfg)
        .await
        .unwrap_err();
    assert_eq!(err, ProxyError::InvalidField);
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
    let err = register_proxy(&np, &session, &state, &cfg)
        .await
        .unwrap_err();
    assert_eq!(err, ProxyError::PortNotAllowed);
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
    let err = register_proxy(&np2, &session, &state, &cfg)
        .await
        .unwrap_err();
    assert_eq!(err, ProxyError::NameExists);
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
    // occupied 持有该端口直至 drop，注册应失败并返回可重试的 port occupied（§6.6）。
    let err = register_proxy(&np, &session, &state, &cfg)
        .await
        .unwrap_err();
    assert_eq!(err, ProxyError::PortOccupied);
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
        work_conn_token: Some(session.work_conn_token.clone()),
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
async fn register_udp_missing_remote_port_rejected() {
    let state = ServerState::new();
    let session = test_session();
    let cfg = test_config("");
    let np = NewProxy {
        proxy_name: "p".into(),
        r#type: ProxyType::Udp,
        remote_port: None,
        custom_domains: None,
    };
    assert!(register_proxy(&np, &session, &state, &cfg).await.is_err());
}

#[tokio::test]
async fn register_udp_proxy_ok() {
    let state = ServerState::new();
    let session = test_session();
    let cfg = test_config("");
    let np = NewProxy {
        proxy_name: "dns".into(),
        r#type: ProxyType::Udp,
        remote_port: Some(free_port()),
        custom_domains: None,
    };
    assert!(register_proxy(&np, &session, &state, &cfg).await.is_ok());
    let proxies = session.proxies.lock().unwrap();
    let entry = proxies.get("dns").unwrap();
    assert_eq!(entry.kind, ProxyType::Udp);
    assert!(state.udp.lock().unwrap().contains_key("dns"));
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
async fn register_http_duplicate_name_rejected() {
    // 与 TCP/UDP 一致：同名 vhost 代理拒绝，不静默覆盖旧条目（§6.6）。
    let state = ServerState::new();
    let session = test_session();
    let cfg = test_config("");
    let np1 = NewProxy {
        proxy_name: "web".into(),
        r#type: ProxyType::Http,
        remote_port: None,
        custom_domains: Some(vec!["a.example.com".into()]),
    };
    assert!(register_proxy(&np1, &session, &state, &cfg).await.is_ok());

    // 同名、不同域名：应返回 proxy_name exists。
    let np2 = NewProxy {
        proxy_name: "web".into(),
        r#type: ProxyType::Http,
        remote_port: None,
        custom_domains: Some(vec!["b.example.com".into()]),
    };
    let err = register_proxy(&np2, &session, &state, &cfg)
        .await
        .unwrap_err();
    assert_eq!(err, ProxyError::NameExists);

    // 原域名映射保持不变，新域名未被登记。
    let domains = session.proxy_domains.lock().unwrap();
    assert!(domains.contains_key("a.example.com"));
    assert!(!domains.contains_key("b.example.com"));
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
    let err = register_proxy(&np2, &session_b, &state, &cfg)
        .await
        .unwrap_err();
    assert_eq!(err, ProxyError::DomainConflict);
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

#[tokio::test]
async fn pooled_work_conn_without_token_rejected() {
    // 未携带 work_conn_token 的工作连接不得进入预热池（防池注入/中间人）。
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

    for token in [None, Some("wrong-token".to_string())] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _client = TcpStream::connect(addr).await.unwrap();
        let (server, _peer) = listener.accept().await.unwrap();
        let frame = Message::StartWorkConn(StartWorkConn {
            proxy_name: "ssh".into(),
            work_id: WORK_ID_POOL_RESERVED,
            work_conn_token: token,
        })
        .to_frame()
        .unwrap();
        assert!(handle_work_connection(frame, server, state.clone())
            .await
            .is_ok());
    }

    let pooled = session
        .pools
        .lock()
        .unwrap()
        .get("ssh")
        .map(|v| v.len())
        .unwrap_or(0);
    assert_eq!(pooled, 0, "unauthenticated work conns must not be pooled");
}

#[tokio::test]
async fn pending_work_conn_proxy_name_mismatch_rejected() {
    // 合法 token 但 proxy_name 与 pending 项不一致：不得认领该用户连接。
    let state = ServerState::new();
    let session = test_session();
    state
        .sessions
        .lock()
        .unwrap()
        .insert(session.run_id.clone(), session.clone());
    let cfg = test_config("");
    for name in ["p1", "p2"] {
        let np = NewProxy {
            proxy_name: name.into(),
            r#type: ProxyType::Tcp,
            remote_port: Some(free_port()),
            custom_domains: None,
        };
        assert!(register_proxy(&np, &session, &state, &cfg).await.is_ok());
    }

    // 伪造一个属于 p1 的待处理用户连接。
    let (user, _other) = tokio::io::duplex(64);
    state.pending.lock().unwrap().insert(
        7,
        PendingWork {
            proxy_name: "p1".into(),
            session_id: session.session_id.clone(),
            user: Some(Box::new(user)),
        },
    );

    // 用 p2 的名字 + 合法 token 认领 work_id=7 → 应被拒绝且 pending 保留。
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let _client = TcpStream::connect(addr).await.unwrap();
    let (server, _peer) = listener.accept().await.unwrap();
    let frame = Message::StartWorkConn(StartWorkConn {
        proxy_name: "p2".into(),
        work_id: 7,
        work_conn_token: Some(session.work_conn_token.clone()),
    })
    .to_frame()
    .unwrap();
    assert!(handle_work_connection(frame, server, state.clone())
        .await
        .is_ok());
    assert!(
        state.pending.lock().unwrap().contains_key(&7),
        "pending entry must be preserved on mismatch"
    );
}

#[tokio::test]
async fn register_rejects_beyond_proxy_limit() {
    // 单会话代理数上限：认证客户端也不得无限占用端口/内存。
    let state = ServerState::new();
    let session = test_session();
    let cfg = test_config("");
    for i in 0..MAX_PROXIES_PER_SESSION {
        session.proxies.lock().unwrap().insert(
            format!("p{i}"),
            ProxyEntry {
                handle: tokio::spawn(async {}),
                kind: ProxyType::Tcp,
            },
        );
    }
    let np = NewProxy {
        proxy_name: "extra".into(),
        r#type: ProxyType::Tcp,
        remote_port: Some(free_port()),
        custom_domains: None,
    };
    let err = register_proxy(&np, &session, &state, &cfg)
        .await
        .unwrap_err();
    assert_eq!(err, ProxyError::TooManyProxies);
}
