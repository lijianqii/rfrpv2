//! 性能基准（DESIGN §14.3）：帧编解码吞吐、双向桥接吞吐、配置解析耗时。

use std::hint::black_box;

use bytes::BytesMut;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rfrp_common::config::{ClientConfig, ServerConfig};
use rfrp_common::protocol::frame::{Frame, FrameCodec};
use rfrp_common::protocol::msg::{Message, NewProxy, ProxyType};
use rfrp_common::util::bridge::bridge_with_buf_size;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::{Decoder, Encoder};

/// 桥接基准的单向传输量。
const BRIDGE_BYTES: usize = 1024 * 1024;

fn bench_frame_encode_decode(c: &mut Criterion) {
    let mut codec = FrameCodec;
    let frame = Frame::new(1, 0x01, vec![0xABu8; 256]);
    c.bench_function("frame_encode_decode_256b", |b| {
        b.iter(|| {
            let mut buf = BytesMut::new();
            codec.encode(frame.clone(), &mut buf).unwrap();
            let decoded = codec.decode(&mut buf).unwrap().unwrap();
            black_box(decoded);
        })
    });
}

/// 真实桥接吞吐：loopback TCP 上 1 MiB 单向穿过 `bridge()`。
///
/// 用真实 socket（而非 duplex）才能反映缓冲大小对 read/write 系统调用
/// 次数的影响；对比 8 KiB（tokio 默认）与 `BRIDGE_BUF_SIZE`（32 KiB）。
async fn tcp_pair() -> std::io::Result<(TcpStream, TcpStream)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let client = TcpStream::connect(addr).await?;
    let (server, _) = listener.accept().await?;
    Ok((client, server))
}

fn bench_bridge_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("bridge_1mib");
    group.throughput(Throughput::Bytes(BRIDGE_BYTES as u64));
    for buf_size in [8 * 1024usize, 32 * 1024, 64 * 1024] {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("buf_{}k", buf_size / 1024)),
            &buf_size,
            |b, &buf_size| {
                b.to_async(&rt).iter(|| async move {
                    // ext_* 为外部用户侧，tunnel_* 为隧道侧；bridge 连接两个 server 端。
                    let (mut ext_client, ext_server) = tcp_pair().await.unwrap();
                    let (tunnel_client, tunnel_server) = tcp_pair().await.unwrap();
                    let bridge_task = tokio::spawn(async move {
                        let _ = bridge_with_buf_size(ext_server, tunnel_server, buf_size).await;
                    });

                    let data = vec![0x5Au8; BRIDGE_BYTES];
                    let writer = tokio::spawn(async move {
                        ext_client.write_all(&data).await.unwrap();
                        ext_client
                    });
                    let mut got = vec![0u8; BRIDGE_BYTES];
                    let mut tunnel_client = tunnel_client;
                    tunnel_client.read_exact(&mut got).await.unwrap();
                    let ext_client = writer.await.unwrap();
                    // 两个方向都关闭，桥接才能结束（copy_bidirectional 语义）。
                    drop(tunnel_client);
                    drop(ext_client);
                    let _ = bridge_task.await;
                    black_box(got);
                })
            },
        );
    }
    group.finish();
}

fn bench_config_parse(c: &mut Criterion) {
    let server_toml = r#"
        [server]
        bind_addr = "0.0.0.0"
        bind_port = 7000
        token = "secret"
        work_conn_tls = false

        [proxy]
        allow_ports = "6000-6100"
    "#;
    let client_toml = r#"
        [client]
        server_addr = "127.0.0.1"
        server_port = 7000
        token = "secret"
        work_conn_tls = false

        [[proxy]]
        name = "ssh"
        type = "tcp"
        local_port = 22
        remote_port = 6000
        pool_size = 0
    "#;
    c.bench_function("config_parse_server", |b| {
        b.iter(|| {
            let cfg: ServerConfig = toml::from_str(black_box(server_toml)).unwrap();
            let _ = black_box(cfg.validate());
        })
    });
    c.bench_function("config_parse_client", |b| {
        b.iter(|| {
            let cfg: ClientConfig = toml::from_str(black_box(client_toml)).unwrap();
            let _ = black_box(cfg.validate());
        })
    });
    // 顺带让 Message 相关类型参与编译，避免 dead code 告警。
    let _ = Message::NewProxy(NewProxy {
        proxy_name: "x".into(),
        r#type: ProxyType::Tcp,
        remote_port: Some(1),
        custom_domains: None,
    });
}

criterion_group!(
    benches,
    bench_frame_encode_decode,
    bench_bridge_throughput,
    bench_config_parse
);
criterion_main!(benches);
