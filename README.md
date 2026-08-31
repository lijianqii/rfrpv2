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

## 发布产物

在 Debian 开发机上一条命令构建三平台产物：

```bash
make release
```

产物输出到 `dist/`：

- Linux x86_64（glibc）`rfrp-<version>-x86_64-linux-gnu.tar.gz`
- Linux x86_64（musl 静态）`rfrp-<version>-x86_64-linux-musl.tar.gz`
- Windows x86_64 `rfrp-<version>-x86_64-windows-gnu.zip`

同时生成 `SHA256SUMS`。

生成自签证书：

```bash
make gen-cert
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

## 注意事项

- **SSH 等有状态协议建议 `pool_size = 0`**：预热会在启动时建立一条空闲本地连接，有状态服务可能在首次使用前将其关闭，导致第一次连接 `Connection reset by peer`。RDP 等场景建议保留预热以降低首连延迟。
- **Windows 下 TCP keepalive 已禁用**：Windows 上通过 socket2 设置 keepalive 可能导致空闲连接约 30s 后被系统主动断开；当前 Windows 仅启用 `TCP_NODELAY`，Linux 保留 keepalive。

## License

[MIT](LICENSE)
