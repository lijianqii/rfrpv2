//! TCP 全链路集成测试（DESIGN §14.2 M1）。
//!
//! 启本地 echo 服务，启动 rfrps + rfrpc，经服务端 `remote_port` 访问，
//! 断言用户数据完整往返（小包、大包、多轮、并发多用户、多代理）。

use std::net::SocketAddr;
use std::time::Duration;

use rfrp_common::config::{
    ClientConfig, ClientLogSection, ClientProxy, ClientSection, LogSection, ProxySection,
    ServerConfig, ServerSection,
};
use rfrp_common::protocol::msg::ProxyType;
use rfrpc::client::Client;
use rfrps::server::Server;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// 启动一个本地 echo 服务，返回监听端口。
async fn spawn_echo() -> u16 {
    let echo = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = echo.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((s, _)) = echo.accept().await {
            tokio::spawn(async move {
                let (mut r, mut w) = tokio::io::split(s);
                let _ = tokio::io::copy(&mut r, &mut w).await;
            });
        }
    });
    port
}

/// 启动 rfrps（绑定 127.0.0.1:0），返回任务句柄与监听地址。
async fn start_server() -> (JoinHandle<()>, SocketAddr) {
    let cfg = ServerConfig {
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
        proxy: ProxySection::default(),
        log: LogSection::default(),
    };
    let server = Server::new(cfg).await.unwrap();
    let addr = server.local_addr();
    let task = tokio::spawn(async move {
        let _ = server.run().await;
    });
    (task, addr)
}

/// 启动 rfrpc（指向给定服务端地址与一组代理），返回任务句柄。
async fn start_client(server_addr: SocketAddr, proxies: Vec<ClientProxy>) -> JoinHandle<()> {
    let cfg = ClientConfig {
        client: ClientSection {
            server_addr: server_addr.ip().to_string(),
            server_port: server_addr.port(),
            token: "".into(),
            tls_enable: false,
            tls_server_name: None,
            tls_ca: None,
            work_conn_tls: false,
            run_id_file: None,
        },
        proxies,
        log: ClientLogSection::default(),
    };
    tokio::spawn(async move {
        let _ = Client::new(cfg).unwrap().run().await;
    })
}

/// 构造一个 tcp 代理条目。
fn tcp_proxy(name: &str, local_port: u16, remote_port: u16) -> ClientProxy {
    ClientProxy {
        name: name.into(),
        r#type: ProxyType::Tcp,
        local_ip: "127.0.0.1".into(),
        local_port,
        remote_port: Some(remote_port),
        custom_domains: None,
        pool_size: 1,
    }
}

/// 等待控制连接建立并完成代理注册。
async fn wait_ready() {
    tokio::time::sleep(Duration::from_millis(300)).await;
}

/// 连接服务端 `remote_port`，发送 `data`，断言收到相同回声。
async fn expect_echo(remote_port: u16, server_addr: SocketAddr, data: &[u8]) {
    let mut user = TcpStream::connect((server_addr.ip(), remote_port))
        .await
        .expect("connect to server remote_port");
    user.write_all(data).await.unwrap();
    let mut buf = vec![0u8; data.len()];
    user.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, data);
}

#[tokio::test]
async fn tcp_proxy_roundtrip() {
    let echo_port = spawn_echo().await;
    let (srv, addr) = start_server().await;
    let remote = free_port();
    let cli = start_client(addr, vec![tcp_proxy("p1", echo_port, remote)]).await;
    wait_ready().await;

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
    let (srv, addr) = start_server().await;
    let remote = free_port();
    let cli = start_client(addr, vec![tcp_proxy("p1", echo_port, remote)]).await;
    wait_ready().await;

    // 20 个并发用户，各自独立往返。
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
    let (srv, addr) = start_server().await;
    let r1 = free_port();
    let r2 = free_port();
    let cli = start_client(
        addr,
        vec![tcp_proxy("ssh", echo1, r1), tcp_proxy("web", echo2, r2)],
    )
    .await;
    wait_ready().await;

    expect_echo(r1, addr, b"to-ssh").await;
    expect_echo(r2, addr, b"to-web").await;
    // 交叉再验证一次
    expect_echo(r2, addr, b"web-again").await;
    expect_echo(r1, addr, b"ssh-again").await;

    srv.abort();
    cli.abort();
}

#[tokio::test]
async fn tcp_proxy_pool_size_two() {
    // 工作连接池预热（pool_size=2）：多次往返命中预热池并被服务端补充（§8.2）。
    let echo_port = spawn_echo().await;
    let (srv, addr) = start_server().await;
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
    let cli = start_client(addr, vec![proxy]).await;
    wait_ready().await;

    for i in 0..3 {
        expect_echo(remote, addr, format!("pool-{i}").as_bytes()).await;
    }

    srv.abort();
    cli.abort();
}

#[tokio::test]
async fn tcp_proxy_concurrent_same_proxy_keep_open() {
    // 多用户同时持有长连接时，首个命中预热池（pool_size=1）的连接若把桥接
    // 放到 accept 循环内 await，会阻塞后续 accept，导致其余连接卡死（§8.2）。
    // 本测试所有连接保持打开，验证并发可达（无超时即通过）。
    let echo_port = spawn_echo().await;
    let (srv, addr) = start_server().await;
    let remote = free_port();
    let cli = start_client(addr, vec![tcp_proxy("p1", echo_port, remote)]).await;
    wait_ready().await;

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
    let (srv, addr) = start_server().await;
    let unregistered = free_port();
    // 没有任何 rfrpc 注册代理时，连接未注册的端口应立即被拒（FIN）或连不上。
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
