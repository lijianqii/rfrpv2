//! 性能基准（DESIGN §14.3）：帧编解码吞吐、双向桥接吞吐、配置解析耗时。

use std::hint::black_box;

use bytes::BytesMut;
use criterion::{criterion_group, criterion_main, Criterion};
use rfrp_common::config::{ClientConfig, ServerConfig};
use rfrp_common::protocol::frame::{Frame, FrameCodec};
use rfrp_common::protocol::msg::{Message, NewProxy, ProxyType};
use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt};
use tokio_util::codec::{Decoder, Encoder};

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

fn bench_bridge_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    c.bench_function("bridge_duplex_64k", |b| {
        b.to_async(&rt).iter(|| async {
            let (mut a, mut b) = duplex(64 * 1024);
            let handle = tokio::spawn(async move {
                let data = vec![0x5Au8; 64 * 1024];
                a.write_all(&data).await.unwrap();
                let mut buf = vec![0u8; 64 * 1024];
                a.read_exact(&mut buf).await.unwrap();
            });
            let data = vec![0x5Au8; 64 * 1024];
            b.write_all(&data).await.unwrap();
            let mut buf = vec![0u8; 64 * 1024];
            b.read_exact(&mut buf).await.unwrap();
            handle.await.unwrap();
        })
    });
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
