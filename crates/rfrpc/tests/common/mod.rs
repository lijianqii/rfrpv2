//! rfrpc 集成测试共享工具。
//!
//! 按主题拆分：本文件是核心（服务端/客户端句柄、配置构造、TCP 回环），
//! UDP 与 HTTP 相关辅助分别在 [`udp`] / [`http`] 子模块，并在此再导出，
//! 调用方统一 `use common::*;` 即可。
#![allow(dead_code)]

pub mod http;
pub mod udp;
#[allow(unused_imports)] // 各测试二进制按需使用；未使用时不应报错。
pub use http::*;
#[allow(unused_imports)]
pub use udp::*;

use std::net::SocketAddr;
use std::sync::Mutex;
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
            work_conn_tls: false,
            ..Default::default()
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

pub fn client_config(
    server_addr: SocketAddr,
    proxies: Vec<ClientProxy>,
    run_id_file: Option<String>,
) -> ClientConfig {
    ClientConfig {
        client: ClientSection {
            server_addr: server_addr.ip().to_string(),
            server_port: server_addr.port(),
            run_id_file,
            ..Default::default()
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
