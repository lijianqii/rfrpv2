//! rfrpc 集成测试共享工具。
#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::Mutex;
use std::time::Duration;

use base64::Engine;
use rfrp_common::config::{
    ClientConfig, ClientLogSection, ClientProxy, ClientSection, LogSection, ProxySection,
    ServerConfig, ServerSection,
};
use rfrp_common::protocol::msg::ProxyType;
use rfrpc::client::Client;
use rfrps::server::Server;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// 把进程的 fd 软上限提升到硬上限（best-effort，失败静默忽略）。
///
/// 同一测试二进制里的用例默认并行执行，每个用例都会拉起 server/client/echo 与
/// 若干连接；macOS 默认软上限仅 256，**先耗尽 fd 的用例会随机报
/// `TooManyOpenFiles` / `ConnectionReset` / 连接超时**，看起来像协议 bug。
/// 在首次分配端口/启动服务前把软上限提到硬上限（通常为 unlimited）。
fn raise_fd_limit() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        #[cfg(unix)]
        {
            // SAFETY: getrlimit/setrlimit 接收本栈上的合法指针，且不持有任何锁。
            unsafe {
                let mut lim = libc::rlimit {
                    rlim_cur: 0,
                    rlim_max: 0,
                };
                if libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) == 0
                    && lim.rlim_cur < lim.rlim_max
                {
                    lim.rlim_cur = lim.rlim_max;
                    let _ = libc::setrlimit(libc::RLIMIT_NOFILE, &lim);
                }
            }
        }
    });
}

/// 取一个测试用端口（实现见 [`rfrp_common::testutil::free_port`]）。
pub fn free_port() -> u16 {
    raise_fd_limit();
    rfrp_common::testutil::free_port()
}

/// 测试用服务端句柄。
///
/// 退出统一走**优雅关闭令牌**而不是 `abort()`：`Server::run` 里的代理监听、
/// 控制写任务等是 `tokio::spawn` 出来的独立任务，硬 abort 只会丢下持有端口与
/// 会话的僵尸任务（表现为"重启后旧端口仍被占用"或"旧会话仍能服务连接"），
/// 使重连类用例产生假通过/假失败。取消令牌则会走完整的会话清理路径。
pub struct TestServer {
    pub addr: SocketAddr,
    shutdown: CancellationToken,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl TestServer {
    /// 启动一个已构造好的 [`Server`]（供需要自定义绑定/重试的用例复用）。
    pub async fn spawn(server: Server) -> (TestServer, SocketAddr) {
        let addr = server.local_addr();
        let shutdown = server.shutdown_token();
        let task = tokio::spawn(async move {
            let _ = server.run().await;
        });
        (
            TestServer {
                addr,
                shutdown,
                task: Mutex::new(Some(task)),
            },
            addr,
        )
    }

    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// 触发优雅退出（等价收到 SIGTERM），不等待结束。
    pub fn abort(&self) {
        self.shutdown.cancel();
    }

    /// 触发优雅退出并等待 accept 循环返回（最多 5s）。
    pub async fn stop(&self) {
        self.stop_with_timeout(Duration::from_secs(5)).await;
    }

    /// 等待 accept 循环返回（不触发退出）。
    ///
    /// 用于测试自行取消退出令牌后等待收尾；若任务句柄已被 [`Self::stop`] 取走，
    /// 则立即返回。
    pub async fn wait(&self) {
        let task = self.task.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(task) = task {
            let _ = task.await;
        }
    }

    pub async fn stop_with_timeout(&self, timeout: Duration) {
        self.shutdown.cancel();
        let task = self.task.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(task) = task {
            let _ = tokio::time::timeout(timeout, task).await;
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

/// 测试用客户端句柄（语义同 [`TestServer`]：优雅退出优先）。
pub struct TestClient {
    shutdown: CancellationToken,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl TestClient {
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// 触发优雅退出（等价收到 SIGTERM），不等待结束。
    pub fn abort(&self) {
        self.shutdown.cancel();
    }

    /// 触发优雅退出并等待客户端任务结束（最多 5s）。
    pub async fn stop(&self) {
        self.shutdown.cancel();
        let task = self.task.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(task) = task {
            let _ = tokio::time::timeout(Duration::from_secs(5), task).await;
        }
    }
}

impl Drop for TestClient {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

pub async fn spawn_echo() -> u16 {
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

/// 本地 UDP echo 服务（模拟 RDP 的 UDP 传输 / 通用 UDP 后端）。
pub async fn spawn_udp_echo() -> u16 {
    let s = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let port = s.local_addr().unwrap().port();
    tokio::spawn(async move {
        let mut buf = vec![0u8; 65507];
        while let Ok((n, peer)) = s.recv_from(&mut buf).await {
            let _ = s.send_to(&buf[..n], peer).await;
        }
    });
    port
}

/// 测试内日志（多测试并行时 `try_init` 失败可忽略）。
pub fn init_logging() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_writer(std::io::stderr)
        .try_init();
}

pub fn server_config(bind_port: u16) -> ServerConfig {
    ServerConfig {
        server: ServerSection {
            bind_addr: "127.0.0.1".into(),
            bind_port,
            token: "".into(),
            tls_enable: false,
            tls_cert: None,
            tls_key: None,
            work_conn_tls: false,
            tcp_keepalive_secs: None,
            heartbeat_interval_secs: None,
            heartbeat_timeout_secs: None,
        },
        dashboard: None,
        proxy: ProxySection::default(),
        log: LogSection::default(),
    }
}

pub fn tcp_proxy(name: &str, local_port: u16, remote_port: u16) -> ClientProxy {
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

/// 构造一个 UDP 代理配置条目。
pub fn udp_proxy(name: &str, local_port: u16, remote_port: u16) -> ClientProxy {
    ClientProxy {
        name: name.into(),
        r#type: ProxyType::Udp,
        local_ip: "127.0.0.1".into(),
        local_port,
        remote_port: Some(remote_port),
        custom_domains: None,
        pool_size: 0,
    }
}

/// 通过 UDP 代理发一个数据报并等待回声；超时/内容不匹配返回 false。
pub async fn udp_echo(addr: SocketAddr, remote_port: u16, data: &[u8]) -> bool {
    let s = match UdpSocket::bind("127.0.0.1:0").await {
        Ok(s) => s,
        Err(_) => return false,
    };
    if s.send_to(data, (addr.ip(), remote_port)).await.is_err() {
        return false;
    }
    let mut buf = vec![0u8; data.len()];
    match tokio::time::timeout(Duration::from_secs(2), s.recv_from(&mut buf)).await {
        Ok(Ok((n, _))) => n == data.len() && buf[..n] == *data,
        _ => false,
    }
}

/// 轮询直到 UDP 代理可用（首个数据报会触发按需工作连接）。
pub async fn wait_udp_ready(server_addr: SocketAddr, remote_port: u16) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !udp_echo(server_addr, remote_port, b"ready").await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "udp proxy did not become ready"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// 拉取 Dashboard 的 `/metrics`（Basic Auth）。
pub async fn fetch_metrics(port: u16, user: &str, password: &str) -> String {
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let auth = base64::engine::general_purpose::STANDARD.encode(format!("{user}:{password}"));
    let req = format!(
        "GET /metrics HTTP/1.1\r\nHost: 127.0.0.1\r\nAuthorization: Basic {auth}\r\nConnection: close\r\n\r\n"
    );
    s.write_all(req.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).await.unwrap();
    String::from_utf8_lossy(&buf).into_owned()
}

/// 从 Prometheus 文本里取某个 counters 的值（缺失按 0 处理）。
pub fn metric_value(metrics: &str, name: &str) -> u64 {
    let prefix = format!("{name} ");
    metrics
        .lines()
        .find_map(|l| l.strip_prefix(&prefix)?.trim().parse().ok())
        .unwrap_or(0)
}

pub fn client_config(
    server_addr: SocketAddr,
    proxies: Vec<ClientProxy>,
    run_id_file: Option<String>,
) -> ClientConfig {
    ClientConfig {
        client: ClientSection {
            server_addr: server_addr.ip().to_string(),
            server_port: server_addr.port(),
            token: "".into(),
            tls_enable: false,
            tls_server_name: None,
            tls_ca: None,
            work_conn_tls: false,
            run_id_file,
            tcp_keepalive_secs: None,
            heartbeat_interval_secs: None,
            heartbeat_timeout_secs: None,
            status_addr: None,
        },
        proxies,
        log: ClientLogSection::default(),
    }
}

/// 启动测试服务端。宽限期取 500ms：退出路径不必让用例等满 30s 默认值。
pub async fn start_server(cfg: ServerConfig) -> (TestServer, SocketAddr) {
    raise_fd_limit();
    let server = Server::new(cfg)
        .await
        .unwrap()
        .with_grace(Duration::from_millis(500));
    TestServer::spawn(server).await
}

pub async fn start_server_with_grace(
    cfg: ServerConfig,
    grace: Duration,
) -> (TestServer, SocketAddr) {
    let server = Server::new(cfg).await.unwrap().with_grace(grace);
    TestServer::spawn(server).await
}

pub async fn start_client(cfg: ClientConfig) -> TestClient {
    raise_fd_limit();
    let client = Client::new(cfg).unwrap();
    let shutdown = client.shutdown_token();
    let task = tokio::spawn(async move {
        let _ = client.run().await;
    });
    TestClient {
        shutdown,
        task: Mutex::new(Some(task)),
    }
}

/// 轮询直到代理端口可正常 echo，用于替代固定 sleep。
pub async fn wait_for_proxy(server_addr: SocketAddr, remote_port: u16, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if try_echo(server_addr, remote_port, b"ready").await.is_ok() {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

pub async fn try_echo(
    server_addr: SocketAddr,
    remote_port: u16,
    data: &[u8],
) -> std::io::Result<()> {
    let mut user = TcpStream::connect((server_addr.ip(), remote_port)).await?;
    user.write_all(data).await?;
    let mut buf = vec![0u8; data.len()];
    user.read_exact(&mut buf).await?;
    if buf.as_slice() == data {
        Ok(())
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "echo mismatch",
        ))
    }
}

pub async fn expect_echo(remote_port: u16, server_addr: SocketAddr, data: &[u8]) {
    try_echo(server_addr, remote_port, data)
        .await
        .expect("echo through proxy");
}
