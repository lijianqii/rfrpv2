//! TCP 全链路集成测试（DESIGN §14.2 M1）。

mod common;

use std::time::Duration;

use common::*;
use rfrp_common::config::ClientProxy;
use rfrp_common::protocol::msg::ProxyType;
use rfrps::server::Server;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::test]
async fn tcp_proxy_roundtrip() {
    let echo_port = spawn_echo().await;
    let (srv, addr) = start_server(server_config(0)).await;
    let remote = free_port();
    let cli = start_client(client_config(
        addr,
        vec![tcp_proxy("p1", echo_port, remote)],
        None,
    ))
    .await;
    assert!(
        wait_for_proxy(addr, remote, Duration::from_secs(5)).await,
        "proxy should become ready"
    );

    // 小包
    expect_echo(remote, addr, b"hello world").await;

    // 大包（超过常见 MTU，验证分段桥接）
    let big = vec![0xABu8; 65536];
    expect_echo(remote, addr, &big).await;

    // 多轮往返
    let pkt = [0x5u8; 200];
    for _ in 0..5 {
        expect_echo(remote, addr, &pkt).await;
    }

    srv.abort();
    cli.abort();
}

#[tokio::test]
async fn tcp_proxy_concurrent_users() {
    let echo_port = spawn_echo().await;
    let (srv, addr) = start_server(server_config(0)).await;
    let remote = free_port();
    let cli = start_client(client_config(
        addr,
        vec![tcp_proxy("p1", echo_port, remote)],
        None,
    ))
    .await;
    assert!(
        wait_for_proxy(addr, remote, Duration::from_secs(5)).await,
        "proxy should become ready"
    );

    let n = 20u16;
    let mut handles = Vec::new();
    for i in 0..n {
        handles.push(tokio::spawn(async move {
            let data = format!("user-{i}").into_bytes();
            expect_echo(remote, addr, &data).await;
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    srv.abort();
    cli.abort();
}

#[tokio::test]
async fn tcp_proxy_multiple_proxies() {
    let echo1 = spawn_echo().await;
    let echo2 = spawn_echo().await;
    let (srv, addr) = start_server(server_config(0)).await;
    let r1 = free_port();
    let r2 = free_port();
    let cli = start_client(client_config(
        addr,
        vec![tcp_proxy("ssh", echo1, r1), tcp_proxy("web", echo2, r2)],
        None,
    ))
    .await;
    assert!(
        wait_for_proxy(addr, r1, Duration::from_secs(5)).await,
        "first proxy should become ready"
    );

    expect_echo(r1, addr, b"to-ssh").await;
    expect_echo(r2, addr, b"to-web").await;
    expect_echo(r2, addr, b"web-again").await;
    expect_echo(r1, addr, b"ssh-again").await;

    srv.abort();
    cli.abort();
}

#[tokio::test]
async fn tcp_proxy_pool_size_two() {
    let echo_port = spawn_echo().await;
    let (srv, addr) = start_server(server_config(0)).await;
    let remote = free_port();
    let proxy = ClientProxy {
        name: "p1".into(),
        r#type: ProxyType::Tcp,
        local_ip: "127.0.0.1".into(),
        local_port: echo_port,
        remote_port: Some(remote),
        custom_domains: None,
        pool_size: 2,
    };
    let cli = start_client(client_config(addr, vec![proxy], None)).await;
    assert!(
        wait_for_proxy(addr, remote, Duration::from_secs(5)).await,
        "proxy should become ready"
    );

    for i in 0..3 {
        expect_echo(remote, addr, format!("pool-{i}").as_bytes()).await;
    }

    srv.abort();
    cli.abort();
}

#[tokio::test]
async fn tcp_proxy_concurrent_same_proxy_keep_open() {
    let echo_port = spawn_echo().await;
    let (srv, addr) = start_server(server_config(0)).await;
    let remote = free_port();
    let cli = start_client(client_config(
        addr,
        vec![tcp_proxy("p1", echo_port, remote)],
        None,
    ))
    .await;
    assert!(
        wait_for_proxy(addr, remote, Duration::from_secs(5)).await,
        "proxy should become ready"
    );

    let n = 5u16;
    let mut users = Vec::new();
    for i in 0..n {
        let mut u = TcpStream::connect((addr.ip(), remote))
            .await
            .expect("connect to server remote_port");
        u.write_all(format!("u{i}").as_bytes()).await.unwrap();
        users.push(u);
    }
    let verify = async {
        for (i, u) in users.iter_mut().enumerate() {
            let mut buf = vec![0u8; format!("u{i}").len()];
            u.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, format!("u{i}").as_bytes());
        }
    };
    tokio::time::timeout(Duration::from_secs(10), verify)
        .await
        .expect("all concurrent connections should be served");

    srv.abort();
    cli.abort();
}

#[tokio::test]
async fn tcp_proxy_rejects_unregistered_port() {
    let (srv, addr) = start_server(server_config(0)).await;
    let unregistered = free_port();
    match TcpStream::connect((addr.ip(), unregistered)).await {
        Ok(mut user) => {
            let mut buf = [0u8; 1];
            let n = user.read(&mut buf).await.unwrap();
            assert_eq!(n, 0, "unregistered port should close immediately");
        }
        Err(_) => { /* 端口无人监听，连接失败同样说明无代理 */ }
    }
    srv.abort();
}

#[tokio::test]
async fn first_user_connection_receives_pushed_data_from_local() {
    // 模拟 SSH：本地服务在 accept 后立即推送 banner，再回显。
    // 验证预热池连接在第一次被用户使用时能正确透传已推送的数据。
    let local = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_port = local.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((mut s, _)) = local.accept().await {
            tokio::spawn(async move {
                let _ = s.write_all(b"BANNER\n").await;
                let (mut r, mut w) = tokio::io::split(s);
                let _ = tokio::io::copy(&mut r, &mut w).await;
            });
        }
    });

    let (srv, addr) = start_server(server_config(0)).await;
    let remote = free_port();
    let proxy = ClientProxy {
        name: "ssh".into(),
        r#type: ProxyType::Tcp,
        local_ip: "127.0.0.1".into(),
        local_port,
        remote_port: Some(remote),
        custom_domains: None,
        pool_size: 1,
    };
    let cli = start_client(client_config(addr, vec![proxy], None)).await;

    // 等待预热完成（不消费池内连接）。
    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut user = TcpStream::connect((addr.ip(), remote)).await.unwrap();
    let mut buf = [0u8; 7];
    let r = tokio::time::timeout(Duration::from_secs(3), user.read_exact(&mut buf)).await;
    assert!(
        r.is_ok(),
        "first user connection must receive pushed banner"
    );
    assert_eq!(&buf, b"BANNER\n");

    drop(user);
    srv.abort();
    cli.abort();
}

#[tokio::test]
async fn tcp_proxy_connection_cap_enforced() {
    let echo_port = spawn_echo().await;
    let server = Server::new(server_config(0))
        .await
        .unwrap()
        .with_max_active(2);
    let addr = server.local_addr();
    let srv = tokio::spawn(async move {
        let _ = server.run().await;
    });
    let remote = free_port();
    let cli = start_client(client_config(
        addr,
        vec![tcp_proxy("p1", echo_port, remote)],
        None,
    ))
    .await;
    assert!(
        wait_for_proxy(addr, remote, Duration::from_secs(5)).await,
        "proxy should become ready"
    );

    // 占用两个活跃连接。
    let mut u1 = TcpStream::connect((addr.ip(), remote)).await.unwrap();
    let mut u2 = TcpStream::connect((addr.ip(), remote)).await.unwrap();
    let _ = &mut u1;
    let _ = &mut u2;

    // 第三个连接应被连接上限拒绝（立即关闭）。
    let mut u3 = TcpStream::connect((addr.ip(), remote)).await.unwrap();
    let mut buf = [0u8; 1];
    let r = tokio::time::timeout(Duration::from_secs(2), u3.read(&mut buf))
        .await
        .expect("rejected connection should be closed");
    match r {
        Ok(0) => {}
        Ok(_) => panic!("third connection should be rejected by connection cap"),
        Err(_) => {} // RST 同样视为被拒绝
    }

    drop(u1);
    drop(u2);
    srv.abort();
    cli.abort();
}

#[tokio::test]
async fn tcp_proxy_rapid_connect_disconnect_stays_usable() {
    // 回归：频繁建立/断开同一代理连接后，控制面不应被背压死锁卡住。
    let echo_port = spawn_echo().await;
    let (srv, addr) = start_server(server_config(0)).await;
    let remote = free_port();
    let cli = start_client(client_config(
        addr,
        vec![tcp_proxy("p1", echo_port, remote)],
        None,
    ))
    .await;
    assert!(
        wait_for_proxy(addr, remote, Duration::from_secs(5)).await,
        "proxy should become ready"
    );

    // 高频短连接：连上→回显→断开，重复 50 次。
    let loop_result = tokio::time::timeout(Duration::from_secs(30), async {
        for i in 0..50 {
            let mut s = TcpStream::connect((addr.ip(), remote)).await.unwrap();
            let data = format!("seq-{i}");
            s.write_all(data.as_bytes()).await.unwrap();
            let mut buf = vec![0u8; data.len()];
            s.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, data.as_bytes());
            drop(s);
        }
    })
    .await;
    assert!(
        loop_result.is_ok(),
        "rapid connect/disconnect should not hang the proxy"
    );

    // 结束后仍可正常使用。
    expect_echo(remote, addr, b"still-works").await;

    srv.abort();
    cli.abort();
}

/// 回归：本地服务踢掉空闲预连接后，池中死连接必须被跳过。
///
/// 场景对应 sshd / RDP 服务对空闲预连接的超时关闭（README 中 `pool_size`
/// 建议的根因）。修复前 `pool_size=1` 时用户连接会命中死连接并被立即重置；
/// 修复后服务端出池前探活，跳过死连接并回退按需建立。
#[tokio::test]
async fn pooled_work_conn_skips_local_idle_closed_conn() {
    // 本地服务：接受后 300ms 无数据即关闭（模拟 sshd/RDP 空闲超时）。
    let echo = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let echo_port = echo.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((s, _)) = echo.accept().await {
            tokio::spawn(async move {
                let (mut r, mut w) = tokio::io::split(s);
                let mut buf = [0u8; 4096];
                loop {
                    match tokio::time::timeout(Duration::from_millis(300), r.read(&mut buf)).await {
                        Ok(Ok(0)) | Ok(Err(_)) => break,
                        Ok(Ok(n)) => {
                            if w.write_all(&buf[..n]).await.is_err() {
                                break;
                            }
                        }
                        Err(_) => break, // 空闲超时：关闭本地连接
                    }
                }
            });
        }
    });

    let (srv, addr) = start_server(server_config(0)).await;
    let remote = free_port();
    // pool_size=1：预连接会被本地服务关闭，池中留下死连接。
    let mut proxy = tcp_proxy("p1", echo_port, remote);
    proxy.pool_size = 1;
    let cli = start_client(client_config(addr, vec![proxy], None)).await;
    assert!(
        wait_for_proxy(addr, remote, Duration::from_secs(5)).await,
        "proxy should become ready"
    );

    // 等池中预连接被本地服务踢掉，再发起用户连接。
    for i in 0..3 {
        tokio::time::sleep(Duration::from_millis(600)).await;
        expect_echo(remote, addr, format!("after-idle-{i}").as_bytes()).await;
    }

    srv.abort();
    cli.abort();
}
