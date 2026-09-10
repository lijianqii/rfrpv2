//! 客户端工作连接单元测试。

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
