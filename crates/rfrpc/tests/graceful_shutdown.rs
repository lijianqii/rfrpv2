//! rfrpc / rfrps 优雅退出集成测试（§14.4 / M2d）。
//!
//! 收到退出信号（令牌取消，等价于 SIGINT/SIGTERM）时：
//! - client.run() 应停止重连并干净退出（而非无限重连）；
//! - 端到端：服务端优雅退出后，客户端控制连接被关闭，取消客户端令牌后二者均干净退出。

use std::time::Duration;

use rfrp_common::config::{ClientConfig, LogSection, ProxySection, ServerConfig, ServerSection};
use rfrpc::client::Client;
use rfrps::server::Server;
use tokio::time::timeout;

fn client_config() -> ClientConfig {
    let mut c = ClientConfig::default();
    // 指向无监听的端口，连接必然失败（用于验证不会陷入无限重连）。
    c.client.server_addr = "127.0.0.1".into();
    c.client.server_port = 1;
    c
}

fn server_config() -> ServerConfig {
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
        proxy: ProxySection::default(),
        log: LogSection::default(),
    }
}

#[tokio::test]
async fn client_run_exits_on_shutdown_without_infinite_reconnect() {
    let client = Client::new(client_config()).unwrap();
    let sd = client.shutdown_token();
    let task = tokio::spawn(async move { client.run().await });

    // 触发退出（等价收到 SIGINT/SIGTERM）。
    sd.cancel();

    // run 应立即干净退出，而不是陷入无限重连。
    let res = timeout(Duration::from_secs(5), task).await;
    assert!(
        res.is_ok(),
        "client.run() must return after shutdown signal"
    );
    let r = res.unwrap();
    assert!(r.is_ok(), "client.run() should exit cleanly, got: {r:?}");
}

#[tokio::test]
async fn server_and_client_exit_cleanly_on_shutdown() {
    // 端到端：触发服务端优雅退出后，客户端控制连接被关闭；再取消客户端令牌，二者均干净退出。
    let server = Server::new(server_config())
        .await
        .unwrap()
        .with_grace(Duration::from_millis(50));
    let addr = server.local_addr();
    let sd = server.shutdown_token();
    let server_task = tokio::spawn(async move { server.run().await.unwrap() });

    let mut client_cfg = ClientConfig::default();
    client_cfg.client.server_addr = addr.ip().to_string();
    client_cfg.client.server_port = addr.port();
    let client = Client::new(client_cfg).unwrap();
    let client_sd = client.shutdown_token();
    let client_task = tokio::spawn(async move { client.run().await });

    // 等待客户端建立控制连接。
    tokio::time::sleep(Duration::from_millis(300)).await;

    // 触发服务端优雅退出。
    sd.cancel();
    let sres = timeout(Duration::from_secs(5), server_task).await;
    assert!(
        sres.is_ok(),
        "server.run() must return after shutdown signal"
    );

    // 服务端退出后，取消客户端令牌，客户端也应干净退出（而非无限重连）。
    client_sd.cancel();
    let cres = timeout(Duration::from_secs(5), client_task).await;
    assert!(
        cres.is_ok(),
        "client.run() must return after shutdown signal"
    );
    assert!(cres.unwrap().is_ok(), "client.run() should exit cleanly");
}
