//! UDP 代理单元测试。

use super::*;
use crate::metrics::Metrics;
use tokio::sync::Notify;

fn test_session() -> (Arc<Session>, mpsc::Receiver<Message>) {
    let (tx, rx) = mpsc::channel::<Message>(16);
    let session = Arc::new(Session {
        run_id: "r".into(),
        session_id: "s".into(),
        tx,
        proxies: Mutex::new(HashMap::new()),
        proxy_domains: Mutex::new(HashMap::new()),
        stop: Arc::new(Notify::new()),
        pools: Mutex::new(HashMap::new()),
    });
    (session, rx)
}

async fn test_proxy(session_timeout: Duration, pending_timeout: Duration) -> Arc<UdpProxy> {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    Arc::new(UdpProxy {
        socket: Arc::new(socket),
        sessions: Mutex::new(HashMap::new()),
        pending_by_id: Mutex::new(HashMap::new()),
        pending_client: Mutex::new(HashMap::new()),
        metrics: Arc::new(Metrics::new()),
        session_timeout,
        pending_timeout,
    })
}

#[tokio::test]
async fn sweep_removes_expired_session_and_pending() {
    let proxy = test_proxy(Duration::from_millis(50), Duration::from_millis(50)).await;
    let (tx, _rx) = mpsc::channel::<Vec<u8>>(4);
    let old = Instant::now() - Duration::from_secs(1);

    proxy.sessions.lock().unwrap().insert(
        "127.0.0.1:1".parse().unwrap(),
        UdpSession {
            tx: tx.clone(),
            last_active: old,
        },
    );
    proxy.pending_by_id.lock().unwrap().insert(
        1,
        PendingUdp {
            client: "127.0.0.1:2".parse().unwrap(),
            tx: tx.clone(),
            rx: mpsc::channel(4).1,
            created: old,
        },
    );
    proxy
        .pending_client
        .lock()
        .unwrap()
        .insert("127.0.0.1:2".parse().unwrap(), 1);
    // 新鲜会话应保留。
    proxy.sessions.lock().unwrap().insert(
        "127.0.0.1:3".parse().unwrap(),
        UdpSession {
            tx: tx.clone(),
            last_active: Instant::now(),
        },
    );

    sweep(&proxy);

    assert!(!proxy
        .sessions
        .lock()
        .unwrap()
        .contains_key(&"127.0.0.1:1".parse().unwrap()));
    assert!(proxy
        .sessions
        .lock()
        .unwrap()
        .contains_key(&"127.0.0.1:3".parse().unwrap()));
    assert!(proxy.pending_by_id.lock().unwrap().is_empty());
    assert!(proxy.pending_client.lock().unwrap().is_empty());
}

#[tokio::test]
async fn datagram_forwarded_to_paired_session() {
    // 已配对会话：数据直接转发到会话通道，不触发新 ReqWorkConn（§8.6）。
    let proxy = test_proxy(Duration::from_secs(60), Duration::from_secs(60)).await;
    let (session, mut ctl_rx) = test_session();
    let state = ServerState::new();
    let peer: SocketAddr = "127.0.0.1:1001".parse().unwrap();

    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(4);
    proxy.sessions.lock().unwrap().insert(
        peer,
        UdpSession {
            tx,
            last_active: Instant::now(),
        },
    );

    handle_datagram(&proxy, "udp-x", &session, &state, peer, b"hello").await;

    assert_eq!(rx.recv().await.unwrap(), b"hello");
    // 控制通道不应收到 ReqWorkConn。
    assert!(ctl_rx.try_recv().is_err());
}

#[tokio::test]
async fn datagram_delivered_to_pending_session() {
    // 已有待配对项：数据继续投递到暂存通道（§8.6 首包后窗口期）。
    let proxy = test_proxy(Duration::from_secs(60), Duration::from_secs(60)).await;
    let (session, mut ctl_rx) = test_session();
    let state = ServerState::new();
    let peer: SocketAddr = "127.0.0.1:1002".parse().unwrap();

    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(4);
    proxy.pending_by_id.lock().unwrap().insert(
        5,
        PendingUdp {
            client: peer,
            tx: tx.clone(),
            rx: mpsc::channel(4).1,
            created: Instant::now(),
        },
    );
    proxy.pending_client.lock().unwrap().insert(peer, 5);

    handle_datagram(&proxy, "udp-x", &session, &state, peer, b"again").await;

    assert_eq!(rx.recv().await.unwrap(), b"again");
    assert!(ctl_rx.try_recv().is_err());
}

#[tokio::test]
async fn first_datagram_creates_pending_and_requests_work_conn() {
    // 首包：建立待配对项（含首包入队）、登记 client→work_id、并触发 ReqWorkConn（§8.6）。
    let proxy = test_proxy(Duration::from_secs(60), Duration::from_secs(60)).await;
    let (session, mut ctl_rx) = test_session();
    let state = ServerState::new();
    let peer: SocketAddr = "127.0.0.1:1003".parse().unwrap();

    handle_datagram(&proxy, "udp-x", &session, &state, peer, b"first").await;

    // 触发 ReqWorkConn，携带正确 proxy_name 与 work_id。
    let msg = ctl_rx.recv().await.expect("ReqWorkConn sent");
    match msg {
        Message::ReqWorkConn(req) => {
            assert_eq!(req.proxy_name, "udp-x");
            assert_eq!(req.work_id, 1);
        }
        other => panic!("expected ReqWorkConn, got {other:?}"),
    }

    // 首包已入待配对通道（取出后验证）。
    let id = proxy
        .pending_client
        .lock()
        .unwrap()
        .get(&peer)
        .copied()
        .expect("client mapped to work_id");
    let mut p = proxy
        .pending_by_id
        .lock()
        .unwrap()
        .remove(&id)
        .expect("pending exists");
    let first = tokio::time::timeout(Duration::from_secs(1), p.rx.recv())
        .await
        .expect("first datagram queued")
        .expect("channel alive");
    assert_eq!(first, b"first");
}
