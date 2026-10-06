//! 分配统计工具：量化热路径上的分配次数与字节数（供评估分配器/分配点优化）。
//!
//! 用自定义 `GlobalAlloc` 包装系统分配器，跑真实链路（server+client 同进程）：
//! - 短连接 churn：每次"用户连接 + 工作连接"分配多少次；
//! - 大流量传输：每 MiB 分配多少次（判断分配器能否影响吞吐）。
//!
//! 只统计不干预，`--nocapture` 查看输出。

mod common;

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use common::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

static ALLOCS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);

struct Counting;

// SAFETY: 仅转发到系统分配器并做原子计数，不改变内存语义。
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        System.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        System.realloc(ptr, layout, new_size)
    }
}

#[global_allocator]
static COUNTER: Counting = Counting;

fn snapshot() -> (u64, u64) {
    (
        ALLOCS.load(Ordering::Relaxed),
        BYTES.load(Ordering::Relaxed),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn measure_allocations() {
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

    // 预热：把一次性分配（配置解析、日志、监听任务等）排除在外。
    for _ in 0..50 {
        try_echo(addr, remote, b"warmup").await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 阶段 1：短连接 churn（用户连接 + 工作连接 + 本地连接）。
    const CONNS: u64 = 200;
    let (a0, b0) = snapshot();
    for _ in 0..CONNS {
        try_echo(addr, remote, b"c").await.unwrap();
    }
    let (a1, b1) = snapshot();
    println!(
        "[churn] {CONNS} 次短连接：分配 {} 次（{:.1} 次/连接），字节 {:.1} KiB/连接",
        a1 - a0,
        (a1 - a0) as f64 / CONNS as f64,
        (b1 - b0) as f64 / CONNS as f64 / 1024.0
    );

    // 阶段 2：单连接大流量（判断"每 MiB 分配次数"）。
    const MIB: u64 = 128;
    let s = TcpStream::connect((addr.ip(), remote)).await.unwrap();
    let (mut rd, mut wr) = tokio::io::split(s);
    let chunk = vec![0x5Au8; 1 << 20];
    let reader = tokio::spawn(async move {
        let mut got = 0u64;
        let mut buf = vec![0u8; 1 << 20];
        while got < MIB << 20 {
            let n = rd.read(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            got += n as u64;
        }
        got
    });
    let (a2, b2) = snapshot();
    for _ in 0..MIB {
        wr.write_all(&chunk).await.unwrap();
    }
    let got = reader.await.unwrap();
    let (a3, b3) = snapshot();
    println!(
        "[bulk ] {MIB} MiB 传输：分配 {} 次（{:.4} 次/MiB），字节 {:.1} KiB/MiB（收到 {} MiB）",
        a3 - a2,
        (a3 - a2) as f64 / MIB as f64,
        (b3 - b2) as f64 / MIB as f64 / 1024.0,
        got >> 20
    );

    srv.abort();
    cli.abort();
}
