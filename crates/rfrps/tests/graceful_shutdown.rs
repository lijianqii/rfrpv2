//! rfrps 优雅退出集成测试（§14.4 / M2d）。
//!
//! 当退出令牌被取消（等价于收到 SIGINT/SIGTERM）时，server.run() 应停止接收新连接，
//! 在宽限期内让在途连接自然结束，随后返回。

use std::time::Duration;

use rfrp_common::config::{LogSection, ProxySection, ServerConfig, ServerSection};
use rfrps::server::Server;
use tokio::time::timeout;

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
async fn server_run_returns_after_shutdown_token() {
    let server = Server::new(server_config())
        .await
        .unwrap()
        .with_grace(Duration::from_millis(50));
    let sd = server.shutdown_token();
    let task = tokio::spawn(async move { server.run().await.unwrap() });

    // 等价收到 SIGINT/SIGTERM：取消退出令牌。
    sd.cancel();

    // run 应在宽限期内返回，而非无限挂起在 accept。
    let res = timeout(Duration::from_secs(5), task).await;
    assert!(
        res.is_ok(),
        "server.run() must return after shutdown signal"
    );
}
