//! RDP 场景回归：TCP 与 UDP 代理共用同一个 `remote_port`。
//!
//! 真实 RDP 客户端（mstsc）启用 UDP 传输时，会把 UDP 数据发往与 TCP **相同**的端口，
//! 因此 3389 的 TCP 与 UDP 代理必须能配置成同一个 `remote_port`。客户端配置校验
//! 曾把 TCP/UDP 混在一个集合里判重，直接拒绝这种配置，导致 RDP-UDP 只能静默回退到
//! TCP（弱网/高丢包下体验变差）。
//!
//! 本用例同时覆盖三件事：
//! 1. 配置校验允许 TCP/UDP 共用端口；
//! 2. 服务端在同一端口号上同时提供 TCP 与 UDP 监听（内核层面两者独立）；
//! 3. 两条代理各自独立通流、互不干扰。

mod common;

use std::time::Duration;

use common::*;

#[tokio::test]
async fn rdp_tcp_and_udp_share_remote_port() {
    init_logging();
    let tcp_echo_port = spawn_echo().await;
    let udp_echo_port = spawn_udp_echo().await;
    let shared = free_port();

    // allow_ports 也要覆盖该端口（同一端口号同时用于 TCP 与 UDP）。
    let mut server_cfg = server_config(0);
    server_cfg.server.token = "test-token".into(); // 与客户端 token 一致（非空以便走真实校验）
    server_cfg.proxy.allow_ports = shared.to_string();
    let (srv, addr) = start_server(server_cfg).await;

    let mut cfg = client_config(
        addr,
        vec![
            tcp_proxy("rdp-tcp", tcp_echo_port, shared),
            udp_proxy("rdp-udp", udp_echo_port, shared),
        ],
        None,
    );
    cfg.client.token = "test-token".into(); // 走真实校验路径要求 token 非空
                                            // 回归点：同一 remote_port 用于 TCP 与 UDP 必须通过校验（此前会报 duplicate remote_port）。
    cfg.validate()
        .expect("RDP 场景（TCP+UDP 同端口）必须通过配置校验");
    let cli = start_client(cfg).await;

    assert!(
        wait_for_proxy(addr, shared, Duration::from_secs(5)).await,
        "RDP TCP 代理应就绪"
    );
    expect_echo(shared, addr, b"rdp-tcp").await;

    // UDP 侧：首个数据报会触发按需工作连接，轮询到回声为止。
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !udp_echo(addr, shared, b"rdp-udp").await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "RDP UDP 代理应就绪（同端口不能与 TCP 冲突）"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // 同端口下两条代理互不干扰：TCP 再走一次，UDP 再走大包 + 会话复用。
    expect_echo(shared, addr, b"rdp-tcp-again").await;
    let big = vec![0x5Au8; 1200]; // 接近常见 MTU，覆盖分片场景
    assert!(udp_echo(addr, shared, &big).await, "UDP 大包应能通过");
    assert!(
        udp_echo(addr, shared, b"rdp-udp-again").await,
        "复用已有 UDP 会话应可用"
    );

    srv.abort();
    cli.stop().await;
}
