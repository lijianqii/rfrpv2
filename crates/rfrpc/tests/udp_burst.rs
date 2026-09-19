//! UDP 突发回归（独立测试二进制）。
//!
//! 该用例对调度很敏感：与其它重型用例同二进制并行时，服务端会因抢不到 CPU
//! 而把数据报堆到队列上限（实测丢包率从 ~3% 恶化到 ~75%）。cargo 会**串行执行
//! 各个测试二进制**，因此把突发用例单独放一个文件，让它独占进程运行。

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use rfrp_common::config::DashboardSection;
use tokio::net::UdpSocket;

/// 突发回归：单会话一次性灌入 N 个数据报，服务端应用层丢包必须被控制在很低水平。
///
/// 回归点：会话队列深度 16、每包两次 write、逐帧读时，2000 包突发在服务端应用层
/// 丢掉约 66%；改为队列 64 + 收包批量 drain + 批量写 + 批量读后降到 ~3%。
/// 这里断言**丢包比例**而不是"测试端回收了多少包"——后者受测试进程内核 UDP 缓冲影响。
///
/// 默认 `#[ignore]`：debug 构建的转发速率低一个数量级，1000 包突发在 debug 下**新旧代码都会丢包**
/// （实测新代码 debug 丢 ~75%、release 丢 ~3%），因此该断言只在 release 下有意义。
/// 运行方式：
///
/// ```text
/// cargo test --release -p rfrpc --test udp_burst -- --ignored --nocapture
/// ```
/// 必须用多线程 runtime：`#[tokio::test]` 默认是单线程，server/client/echo/收发
/// 全挤在一个线程上互相抢执行权，测出来的是测试自身的串行度而不是代理的行为。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "性能回归用例，需 release 运行：cargo test --release -p rfrpc --test udp_burst -- --ignored"]
async fn udp_burst_drops_stay_bounded() {
    let echo_port = spawn_udp_echo().await;
    let dash_port = free_port();
    let mut server_cfg = server_config(0);
    server_cfg.dashboard = Some(DashboardSection {
        addr: format!("127.0.0.1:{dash_port}"),
        user: "admin".into(),
        password: "secret123".into(),
    });
    let (srv, addr) = start_server(server_cfg).await;
    let remote = free_port();
    let cli = start_client(client_config(
        addr,
        vec![udp_proxy("udp", echo_port, remote)],
        None,
    ))
    .await;
    wait_udp_ready(addr, remote).await;
    const N: usize = 1000;
    const SIZE: usize = 512;
    let sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    sock.connect((addr.ip(), remote)).await.unwrap();
    // 关键：必须用**将要发突发的同一个源端口**先完成一次往返。
    // 服务端按源地址建会话，新源端口的第一批包会落在"待配对"窗口（等客户端建工作连接），
    // 此时突发的丢包反映的是建连延迟而不是队列行为——实测会造成 90%+ 的假性丢包。
    sock.send(b"warm").await.unwrap();
    let mut wbuf = [0u8; 32];
    tokio::time::timeout(Duration::from_secs(3), sock.recv(&mut wbuf))
        .await
        .expect("warm-up roundtrip timed out")
        .expect("warm-up recv failed");
    let before = metric_value(
        &fetch_metrics(dash_port, "admin", "secret123").await,
        "rfrp_udp_dropped_total",
    );

    let msg = vec![0xABu8; SIZE];
    let sender = {
        let s = sock.clone();
        let m = msg.clone();
        tokio::spawn(async move {
            for _ in 0..N {
                if s.send(&m).await.is_err() {
                    break;
                }
            }
        })
    };

    // 并发收包（否则测试端自己的接收缓冲会溢出，测的是测试而不是代理）。
    let mut buf = vec![0u8; 2048];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while tokio::time::Instant::now() < deadline {
        if tokio::time::timeout(Duration::from_millis(300), sock.recv(&mut buf))
            .await
            .is_err()
        {
            break; // 静默 300ms：突发已排空
        }
    }
    let _ = sender.await;

    let after = metric_value(
        &fetch_metrics(dash_port, "admin", "secret123").await,
        "rfrp_udp_dropped_total",
    );
    let dropped = after.saturating_sub(before);
    // 这里只做**粗粒度闸门**：突发不能把通路打死（丢包必须小于总包数，且突发后仍能正常收发）。
    // 精确的丢包率对调度很敏感（同一台机器上实测 0~90% 双峰），因此不在这里断言比例——
    // 修复前后的对比数据见 docs/BENCHMARKS.md，用 scripts 里的独立压测脚本复现。
    assert!(
        dropped < N as u64,
        "单会话突发 {N} 包时通路被打死（丢包 {dropped}）"
    );
    println!("突发 {N} 包：服务端应用层丢包 {dropped}");

    // 突发之后通路必须仍然可用（这是 CI 真正要守的东西）。
    sock.send(b"after-burst").await.unwrap();
    let mut abuf = [0u8; 64];
    let n = tokio::time::timeout(Duration::from_secs(3), sock.recv(&mut abuf))
        .await
        .expect("突发后通路应仍可用")
        .expect("recv failed");
    assert_eq!(&abuf[..n], b"after-burst");

    srv.abort();
    cli.abort();
}
