# rfrp

Rust Fast Reverse Proxy —— 用 Rust + tokio 实现的轻量级反向代理工具，单二进制同时支持服务端与客户端。

## 当前进度

- ✅ M0：workspace / 协议 / 配置 / CLI 骨架
- ✅ M1：TCP 代理全链路
- ✅ M2：心跳、重连、run_id、优雅退出、连接池
- ✅ M3：TLS 控制链路、工作连接 TLS、token 鉴权
- ⬜ M4：UDP / HTTP / HTTPS vhost
- ⬜ M5：Dashboard / 可观测
- ⬜ M6：发布与打包

## 构建

```bash
cargo build --release
```

质量门：

```bash
make ci
```

## 快速启动

示例配置已附带自签证书，可直接本地测试：

```bash
# 终端 1
cargo run -- server -c examples/rfrp-server.toml

# 终端 2
cargo run -- client -c examples/rfrp-client.toml
```

服务端默认监听 `127.0.0.1:7000`，客户端通过 TLS 连接并注册 TCP 代理。

## 目录结构

```text
crates/
├── rfrp-common/   # 协议、配置、TLS、鉴权、工具
├── rfrps/         # 服务端库
├── rfrpc/         # 客户端库
└── rfrp-bin/      # 统一二进制入口
```

## 测试

```bash
cargo test --all
```

## License

[MIT](LICENSE)
