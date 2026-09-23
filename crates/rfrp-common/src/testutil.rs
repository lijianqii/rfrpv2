//! 测试辅助工具（仅 `test-support` feature 下编译，不进入发布产物）。
//!
//! 各 crate 的集成/单元测试都需要"拿一个没人占用的端口"，放在这里是为了只有一份实现：
//! 不同副本很容易各自演化出细微差异，而端口分配一旦不一致，测试失败会变得难以解释。
//!
//! 注意：这里只放**安全代码**，因此本 crate 可以保持 `#![forbid(unsafe_code)]`。
//! 需要 `setrlimit` 之类的平台调用（例如提升 fd 上限）应留在各测试 crate 内。

use std::sync::atomic::{AtomicU16, Ordering};

/// 测试端口区间：低于 Linux(32768+)/macOS(49152+)/Windows(49152+) 的临时端口起点，
/// 避开系统随机分配，从而不与 `bind(0)` 抢号。
const PORT_RANGE_START: u16 = 21_000;
const PORT_RANGE_END: u16 = 31_000;

/// 进程内自增的端口游标（见 [`free_port`]）。
static NEXT_PORT: AtomicU16 = AtomicU16::new(PORT_RANGE_START);

/// 返回一个当前可用的端口。
///
/// 不用经典的 "bind(0) 后立刻关闭"：那样拿到的端口会被系统迅速复用，同一测试进程内
/// 其它用例的 `bind(0)`（server 控制端口、echo 服务等）可能正好拿到同一个号，造成用例
/// 互相串台（偶发 `port occupied` / 连到别的用例的代理上）。这里改为固定区间内自增取号
/// —— 保证进程内不重复 —— 再逐个探测可用性。
pub fn free_port() -> u16 {
    for _ in 0..(PORT_RANGE_END - PORT_RANGE_START) as usize {
        let port = NEXT_PORT.fetch_add(1, Ordering::Relaxed);
        if !(PORT_RANGE_START..PORT_RANGE_END).contains(&port) {
            NEXT_PORT.store(PORT_RANGE_START, Ordering::Relaxed);
            continue;
        }
        if std::net::TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
    // 兜底：区间内全部被占用（实际不会发生），退回系统分配。
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    l.local_addr().expect("local_addr").port()
}

/// 读端永远 `Pending`、写端立即报错的测试流：模拟"写侧已死但读侧静默"的半开连接。
///
/// 用于验证心跳看门狗在写路径失效时仍能判定断连（rfrps / rfrpc 两侧共用一份实现）。
pub struct DeadWriteStream;

impl tokio::io::AsyncRead for DeadWriteStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        _buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Pending
    }
}

impl tokio::io::AsyncWrite for DeadWriteStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        _buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::task::Poll::Ready(Err(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "dead write side",
        )))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}
