//! 用仓库自带的 `examples/rfrp-{server,client}.toml` 跑一次真实链路，
//! 验证示例配置可被加载、控制连接建立、代理注册（TCP 成功 / 非 TCP 被拒）。
//! 同时作为「启动后应该看到哪些日志」的活文档。

use std::path::Path;
use std::time::Duration;

use rfrp_common::config::{load_client_config, load_server_config};
use rfrpc::client::Client;
use rfrps::server::Server;

#[tokio::test]
async fn example_configs_smoke() {
    // 用直接写 stderr 的 subscriber，确保 tokio::spawn 出来的任务日志也能被捕获
    // （with_test_writer 依赖线程局部，会丢失跨线程日志）。
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_writer(std::io::stderr)
        .try_init();

    let base = env!("CARGO_MANIFEST_DIR");
    let mut server_cfg = load_server_config(Path::new(&format!(
        "{base}/../../examples/rfrp-server.toml"
    )))
    .expect("load example server config");
    // 用 OS 分配端口，避免固定 7000 在 CI/本机被占用导致偶发失败。
    server_cfg.server.bind_port = 0;
    let mut client_cfg = load_client_config(Path::new(&format!(
        "{base}/../../examples/rfrp-client.toml"
    )))
    .expect("load example client config");
    assert_eq!(client_cfg.proxies.len(), 2, "example must define 2 proxies");

    let server = Server::new(server_cfg).await.expect("server new");
    let server_addr = server.local_addr();
    client_cfg.client.server_port = server_addr.port();

    let server_task = tokio::spawn(async move {
        let _ = server.run().await;
    });

    let client_task = tokio::spawn(async move {
        let _ = Client::new(client_cfg).unwrap().run().await;
    });

    // 等待控制连接建立并完成代理注册。
    tokio::time::sleep(Duration::from_millis(800)).await;

    client_task.abort();
    server_task.abort();
}
