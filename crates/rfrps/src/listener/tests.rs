//! 代理注册与用户连接分发单元测试。

use super::*;
use crate::state::PendingWork;
use crate::work::handle_work_connection;
use rfrp_common::config::{LogSection, ProxySection, ServerConfig, ServerSection};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};

fn test_config(allow_ports: &str) -> ServerConfig {
    let proxy = ProxySection {
        allow_ports: allow_ports.into(),
        ..Default::default()
    };
    ServerConfig {
        server: ServerSection {
            bind_addr: "127.0.0.1".into(),
            bind_port: 0,
            work_conn_tls: false,
            ..Default::default()
        },
        dashboard: None,
        proxy,
        log: LogSection::default(),
    }
}

use crate::control::{test_entry, test_session};

use rfrp_common::testutil::free_port;

/// 把测试会话登记为当前会话：`register_proxy` 会校验会话仍有效。
fn register_test_session(state: &Arc<ServerState>, session: &Arc<Session>) {
    state
        .sessions
        .lock()
        .insert(session.run_id.clone(), session.clone());
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
    let session = test_session("r");
    register_test_session(&state, &session);
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
async fn register_rejects_remote_port_zero() {
    let state = ServerState::new();
    let session = test_session("r");
    register_test_session(&state, &session);
    let cfg = test_config("");
    let np = NewProxy {
        proxy_name: "p".into(),
        r#type: ProxyType::Tcp,
        remote_port: Some(0),
        custom_domains: None,
    };
    assert_eq!(
        register_proxy(&np, &session, &state, &cfg)
            .await
            .unwrap_err(),
        ProxyError::InvalidField
    );
}

#[tokio::test]
async fn register_rejects_invalid_domain() {
    let state = ServerState::new();
    let session = test_session("r");
    register_test_session(&state, &session);
    let cfg = test_config("");
    let np = NewProxy {
        proxy_name: "web".into(),
        r#type: ProxyType::Http,
        remote_port: None,
        custom_domains: Some(vec!["bad_domain".into()]),
    };
    assert_eq!(
        register_proxy(&np, &session, &state, &cfg)
            .await
            .unwrap_err(),
        ProxyError::InvalidField
    );
}

#[tokio::test]
async fn register_rejects_missing_remote_port() {
    let state = ServerState::new();
    let session = test_session("r");
    register_test_session(&state, &session);
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
    let session = test_session("r");
    register_test_session(&state, &session);
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
    let session = test_session("r");
    register_test_session(&state, &session);
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
async fn register_rejects_same_name_in_another_session() {
    // DESIGN §6.6：proxy_name 全局唯一，第二个会话不能用同名顶掉第一个。
    let state = ServerState::new();
    let a = test_session("ra");
    let b = test_session("rb");
    register_test_session(&state, &a);
    register_test_session(&state, &b);
    let cfg = test_config("");

    let np_a = NewProxy {
        proxy_name: "shared".into(),
        r#type: ProxyType::Tcp,
        remote_port: Some(free_port()),
        custom_domains: None,
    };
    assert!(register_proxy(&np_a, &a, &state, &cfg).await.is_ok());

    let np_b = NewProxy {
        proxy_name: "shared".into(),
        r#type: ProxyType::Tcp,
        remote_port: Some(free_port()),
        custom_domains: None,
    };
    let err = register_proxy(&np_b, &b, &state, &cfg).await.unwrap_err();
    assert_eq!(err, ProxyError::NameExists);
    assert!(state.session_for_proxy("shared").is_some());
}

#[tokio::test]
async fn bind_failure_releases_name_and_domain_reservations() {
    // 预留发生在 bind 之前；bind 失败必须回滚，否则同名/同域名永远无法重试。
    let state = ServerState::new();
    let session = test_session("r");
    register_test_session(&state, &session);
    let cfg = test_config("");

    let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let occupied_port = occupied.local_addr().unwrap().port();
    let np = NewProxy {
        proxy_name: "p".into(),
        r#type: ProxyType::Tcp,
        remote_port: Some(occupied_port),
        custom_domains: None,
    };
    assert_eq!(
        register_proxy(&np, &session, &state, &cfg)
            .await
            .unwrap_err(),
        ProxyError::PortOccupied
    );

    // 同名 + 新端口应能成功（说明 proxy_index 已回滚）。
    let retry = NewProxy {
        proxy_name: "p".into(),
        r#type: ProxyType::Tcp,
        remote_port: Some(free_port()),
        custom_domains: None,
    };
    assert!(register_proxy(&retry, &session, &state, &cfg).await.is_ok());
}

#[tokio::test]
async fn register_rejects_occupied_port() {
    let state = ServerState::new();
    let session = test_session("r");
    register_test_session(&state, &session);
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
    let session = test_session("r");
    register_test_session(&state, &session);
    state
        .sessions
        .lock()
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
        .get("ssh")
        .map(|v| v.len())
        .unwrap_or(0);
    assert_eq!(pooled, 1);
}

#[tokio::test]
async fn register_udp_missing_remote_port_rejected() {
    let state = ServerState::new();
    let session = test_session("r");
    register_test_session(&state, &session);
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
    let session = test_session("r");
    register_test_session(&state, &session);
    let cfg = test_config("");
    let np = NewProxy {
        proxy_name: "dns".into(),
        r#type: ProxyType::Udp,
        remote_port: Some(free_port()),
        custom_domains: None,
    };
    assert!(register_proxy(&np, &session, &state, &cfg).await.is_ok());
    let proxies = session.proxies.lock();
    let entry = proxies.get("dns").unwrap();
    assert_eq!(entry.kind, ProxyType::Udp);
    assert!(state.udp.lock().contains_key("dns"));
}

#[tokio::test]
async fn register_http_proxy_registers_domains() {
    let state = ServerState::new();
    let session = test_session("r");
    register_test_session(&state, &session);
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
            .get("dev.example.com")
            .map(|s| s.as_str()),
        Some("web")
    );
    let proxies = session.proxies.lock();
    let entry = proxies.get("web").unwrap();
    assert_eq!(entry.kind, ProxyType::Http);
}

#[tokio::test]
async fn register_http_proxy_normalizes_domain_case() {
    // 域名统一小写登记：含大写字母的域名也必须能被小写 Host 路由命中；
    // 大小写不同的同名域名必须按冲突处理，不能绕过 §6.6 的全局唯一约束。
    let state = ServerState::new();
    let session = test_session("r");
    register_test_session(&state, &session);
    state
        .sessions
        .lock()
        .insert(session.run_id.clone(), session.clone());
    let cfg = test_config("");
    let web = NewProxy {
        proxy_name: "web".into(),
        r#type: ProxyType::Http,
        remote_port: None,
        custom_domains: Some(vec!["Dev.Example.COM".into()]),
    };
    assert!(register_proxy(&web, &session, &state, &cfg).await.is_ok());

    // 路由按小写查表（vhost::route_and_dispatch），必须命中。
    assert!(state.session_for_domain("dev.example.com").is_some());
    // 会话内登记的键也已归一化。
    assert_eq!(
        session
            .proxy_domains
            .lock()
            .get("dev.example.com")
            .map(|s| s.as_str()),
        Some("web")
    );

    // 另一代理用不同大小写注册同一域名：必须判定冲突。
    let tcp = NewProxy {
        proxy_name: "raw".into(),
        r#type: ProxyType::Tcp,
        remote_port: Some(free_port()),
        custom_domains: Some(vec!["DEV.example.com".into()]),
    };
    let err = register_proxy(&tcp, &session, &state, &cfg)
        .await
        .unwrap_err();
    assert_eq!(err, ProxyError::DomainConflict);
    assert!(!session.proxies.lock().contains_key("raw"));
}

#[tokio::test]
async fn register_tcp_with_conflicting_domain_rejected() {
    // 域名为全局唯一资源：TCP/UDP 代理即使不走 vhost 路由，也不得抢占已被
    // vhost 代理登记的域名（否则同名 vhost 请求会路由到类型不匹配的代理，
    // 用户只看到 404，且注册方毫无察觉）。
    let state = ServerState::new();
    let session = test_session("r");
    register_test_session(&state, &session);
    // 域名查重走全局会话表（生产路径由登录时登记），此处按同样方式登记。
    state
        .sessions
        .lock()
        .insert(session.run_id.clone(), session.clone());
    let cfg = test_config("");
    let web = NewProxy {
        proxy_name: "web".into(),
        r#type: ProxyType::Http,
        remote_port: None,
        custom_domains: Some(vec!["dev.example.com".into()]),
    };
    assert!(register_proxy(&web, &session, &state, &cfg).await.is_ok());

    let tcp = NewProxy {
        proxy_name: "raw".into(),
        r#type: ProxyType::Tcp,
        remote_port: Some(free_port()),
        custom_domains: Some(vec!["dev.example.com".into()]),
    };
    let err = register_proxy(&tcp, &session, &state, &cfg)
        .await
        .unwrap_err();
    assert_eq!(err, ProxyError::DomainConflict);
    // 域名仍归属原 vhost 代理，失败注册不留下任何痕迹。
    assert_eq!(
        session
            .proxy_domains
            .lock()
            .get("dev.example.com")
            .map(|s| s.as_str()),
        Some("web")
    );
    assert!(!session.proxies.lock().contains_key("raw"));
}

#[tokio::test]
async fn tcp_proxy_does_not_reserve_new_domains() {
    let state = ServerState::new();
    let session = test_session("r");
    register_test_session(&state, &session);
    state
        .sessions
        .lock()
        .insert(session.run_id.clone(), session.clone());
    let cfg = test_config("");
    let tcp = NewProxy {
        proxy_name: "raw".into(),
        r#type: ProxyType::Tcp,
        remote_port: Some(free_port()),
        custom_domains: Some(vec!["new.example.com".into()]),
    };
    assert!(register_proxy(&tcp, &session, &state, &cfg).await.is_ok());
    assert!(
        state.session_for_domain("new.example.com").is_none(),
        "TCP proxy must not reserve a new vhost domain"
    );

    let web = NewProxy {
        proxy_name: "web".into(),
        r#type: ProxyType::Http,
        remote_port: None,
        custom_domains: Some(vec!["new.example.com".into()]),
    };
    assert!(register_proxy(&web, &session, &state, &cfg).await.is_ok());
    assert!(state.session_for_domain("new.example.com").is_some());
}

#[tokio::test]
async fn register_http_duplicate_name_rejected() {
    // 与 TCP/UDP 一致：同名 vhost 代理拒绝，不静默覆盖旧条目（§6.6）。
    let state = ServerState::new();
    let session = test_session("r");
    register_test_session(&state, &session);
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
    let domains = session.proxy_domains.lock();
    assert!(domains.contains_key("a.example.com"));
    assert!(!domains.contains_key("b.example.com"));
}

#[tokio::test]
async fn register_https_proxy_registers_domains() {
    let state = ServerState::new();
    let session = test_session("r");
    register_test_session(&state, &session);
    let cfg = test_config("");
    let np = NewProxy {
        proxy_name: "web".into(),
        r#type: ProxyType::Https,
        remote_port: None,
        custom_domains: Some(vec!["secure.example.com".into()]),
    };
    assert!(register_proxy(&np, &session, &state, &cfg).await.is_ok());
    let proxies = session.proxies.lock();
    let entry = proxies.get("web").unwrap();
    assert_eq!(entry.kind, ProxyType::Https);
}

#[tokio::test]
async fn register_http_domain_conflict_rejected() {
    let state = ServerState::new();
    let session_a = test_session("r");
    state
        .sessions
        .lock()
        .insert(session_a.run_id.clone(), session_a.clone());
    let cfg = test_config("");
    let np1 = NewProxy {
        proxy_name: "a".into(),
        r#type: ProxyType::Http,
        remote_port: None,
        custom_domains: Some(vec!["dev.example.com".into()]),
    };
    assert!(register_proxy(&np1, &session_a, &state, &cfg).await.is_ok());

    let session_b = test_session("r");
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
    let session = test_session("r");
    register_test_session(&state, &session);
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
    state.pending.lock().insert(
        42,
        PendingWork {
            proxy_name: "ssh".into(),
            session_id: "s".into(),
            user: Some(Box::new(user)),
            created: std::time::Instant::now() - Duration::from_secs(1),
        },
    );
    // 单周期扫描：直接把 created 设为过去，调用一次 sweep 即可，无需等待真实超时。
    let removed = sweep_expired_pending(&state, Duration::from_millis(100)).await;
    assert_eq!(removed, 1);
    assert!(!state.pending.lock().contains_key(&42));
}

#[tokio::test]
async fn work_conn_request_is_retried_until_consumed() {
    // 工作连接迟迟未建立时，服务端应在超时窗口内重发 ReqWorkConn（给客户端重试机会），
    // 而不是只发一次就等超时把用户连接关掉。
    let state = ServerState::new();
    state
        .pending_request_interval_ms
        .store(30, std::sync::atomic::Ordering::Relaxed);
    let (session, mut rx) = crate::control::test_session_with_token("r", "tok");
    let (user, _peer) = tokio::io::duplex(64);
    dispatch_user_connection("ssh".to_string(), user, session, state);

    let mut work_ids = Vec::new();
    for _ in 0..2 {
        match tokio::time::timeout(Duration::from_secs(2), rx.recv()).await {
            Ok(Some(Message::ReqWorkConn(r))) => work_ids.push(r.work_id),
            other => panic!("expected ReqWorkConn, got {other:?}"),
        }
    }
    assert_eq!(
        work_ids[0], work_ids[1],
        "the same work_id must be re-requested"
    );
}

#[tokio::test]
async fn pooled_work_conn_without_token_rejected() {
    // 未携带 work_conn_token 的工作连接不得进入预热池（防池注入/中间人）。
    let state = ServerState::new();
    let session = test_session("r");
    register_test_session(&state, &session);
    state
        .sessions
        .lock()
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
        .get("ssh")
        .map(|v| v.len())
        .unwrap_or(0);
    assert_eq!(pooled, 0, "unauthenticated work conns must not be pooled");
}

#[tokio::test]
async fn pending_work_conn_proxy_name_mismatch_rejected() {
    // 合法 token 但 proxy_name 与 pending 项不一致：不得认领该用户连接。
    let state = ServerState::new();
    let session = test_session("r");
    register_test_session(&state, &session);
    state
        .sessions
        .lock()
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
    state.pending.lock().insert(
        7,
        PendingWork {
            proxy_name: "p1".into(),
            session_id: session.session_id.clone(),
            user: Some(Box::new(user)),
            created: std::time::Instant::now(),
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
        state.pending.lock().contains_key(&7),
        "pending entry must be preserved on mismatch"
    );
}

#[tokio::test]
async fn register_rejects_beyond_proxy_limit() {
    // 单会话代理数上限：认证客户端也不得无限占用端口/内存。
    let state = ServerState::new();
    let session = test_session("r");
    register_test_session(&state, &session);
    let cfg = test_config("");
    for i in 0..MAX_PROXIES_PER_SESSION {
        session
            .proxies
            .lock()
            .insert(format!("p{i}"), test_entry(ProxyType::Tcp));
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

#[tokio::test]
async fn register_rejects_global_proxy_limit() {
    // 全局代理总数达到配置上限后，新名字必须返回 TooManyProxies，
    // 而不是只报 NameExists 或继续占用端口。
    let state = ServerState::new();
    state
        .max_total_proxies
        .store(1, std::sync::atomic::Ordering::Relaxed);
    let session = test_session("r");
    register_test_session(&state, &session);
    let cfg = test_config("");

    let first = NewProxy {
        proxy_name: "global-a".into(),
        r#type: ProxyType::Tcp,
        remote_port: Some(free_port()),
        custom_domains: None,
    };
    assert!(register_proxy(&first, &session, &state, &cfg).await.is_ok());

    let second = NewProxy {
        proxy_name: "global-b".into(),
        r#type: ProxyType::Tcp,
        remote_port: Some(free_port()),
        custom_domains: None,
    };
    assert_eq!(
        register_proxy(&second, &session, &state, &cfg)
            .await
            .unwrap_err(),
        ProxyError::TooManyProxies
    );
}

#[tokio::test]
async fn register_rejects_global_domain_limit() {
    // 全局域名索引达到配置上限后，新域名必须返回 TooManyDomains。
    let state = ServerState::new();
    state
        .max_total_domains
        .store(1, std::sync::atomic::Ordering::Relaxed);
    let session = test_session("r");
    register_test_session(&state, &session);
    let cfg = test_config("");

    let first = NewProxy {
        proxy_name: "web-a".into(),
        r#type: ProxyType::Http,
        remote_port: None,
        custom_domains: Some(vec!["a.example.com".into()]),
    };
    assert!(register_proxy(&first, &session, &state, &cfg).await.is_ok());

    let second = NewProxy {
        proxy_name: "web-b".into(),
        r#type: ProxyType::Http,
        remote_port: None,
        custom_domains: Some(vec!["b.example.com".into()]),
    };
    assert_eq!(
        register_proxy(&second, &session, &state, &cfg)
            .await
            .unwrap_err(),
        ProxyError::TooManyDomains
    );
}

#[tokio::test]
async fn register_udp_occupied_port_returns_port_occupied() {
    // UDP 端口占用必须映射为可重试的 PortOccupied，而不是笼统的 Internal。
    let blocker = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let occupied = blocker.local_addr().unwrap().port();
    let state = ServerState::new();
    let session = test_session("r");
    register_test_session(&state, &session);
    let cfg = test_config("");
    let np = NewProxy {
        proxy_name: "udp-occupied".into(),
        r#type: ProxyType::Udp,
        remote_port: Some(occupied),
        custom_domains: None,
    };
    assert_eq!(
        register_proxy(&np, &session, &state, &cfg)
            .await
            .unwrap_err(),
        ProxyError::PortOccupied
    );
}
