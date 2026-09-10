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

## 监控与运维

### 服务端 Dashboard

`[dashboard]` 段启用后提供 Basic Auth 保护的只读页面与接口：

```toml
[dashboard]
addr = "127.0.0.1:7500"     # 建议绑回环，或置于反向代理/防火墙之后
user = "admin"
password = "change-me"      # 至少 6 位
```

| 端点 | 内容 |
|---|---|
| `GET /` | 状态页（版本、uptime、会话/代理/池计数，5s 自动刷新） |
| `GET /api/status` | JSON：会话与代理清单、pending、UDP 会话、池、指标 |
| `GET /metrics` | Prometheus 文本 |

`/metrics` 指标：

```
rfrp_connections_total      累计接受的用户连接
rfrp_active_connections     当前活跃用户连接
rfrp_bytes_up_total         上行字节（外部 → 本地）
rfrp_bytes_down_total       下行字节（本地 → 外部）
rfrp_sessions               活跃控制会话
rfrp_proxies                已注册代理
rfrp_pending_work           等待工作连接的用户连接
rfrp_udp_sessions           活跃 UDP 会话
rfrp_pooled_work_conns      池中空闲工作连接
rfrp_uptime_seconds         进程运行时长
```

### 客户端状态端点（可选）

`[client] status_addr` 启用后提供只读状态端点（默认关闭）：

```toml
[client]
status_addr = "127.0.0.1:7400"   # 仅只读、无鉴权，务必绑回环
```

| 端点 | 内容 |
|---|---|
| `GET /` | 状态页（版本、连接状态、uptime、代理清单，5s 自动刷新） |
| `GET /api/status` | JSON：连接状态、服务端地址、代理清单、指标 |
| `GET /metrics` | Prometheus 文本 |

客户端指标：`rfrp_client_uptime_seconds`、`rfrp_client_connected`、`rfrp_client_reconnects_total`、
`rfrp_client_work_conns_total`、`rfrp_client_work_conn_failures_total`、
`rfrp_client_proxy_register_failures_total`、`rfrp_client_proxy_register_retry_success_total`。

### 日志

`[log]` 段（或 CLI `--log-level/--log-output/--log-format`）控制输出：

```toml
[log]
level = "info"                  # trace/debug/info/warn/error
output = "stderr"               # 或 "file:/var/log/rfrp/rfrp.log"
format = "text"                 # 或 "json"（结构化，便于采集）
```

启动时会打印版本与关键配置摘要（不含 token），便于排障。

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

- **Windows 杀毒软件误报**：rfrp 是内网穿透/反代工具，与 frp、nps、ngrok 等同类，Windows 安全软件可能将其归类为 `HackTool`/`RiskWare` 风险工具。二进制已嵌入版本信息/清单/图标以降低启发式误报，但无法消除功能特征归类；加入信任区或代码签名可解决，详见 [docs/WINDOWS_ANTIVIRUS.md](docs/WINDOWS_ANTIVIRUS.md)。
- **`pool_size` 与有状态服务**：预热会建立一条空闲本地连接，sshd/RDP 等服务可能将其超时踢除。服务端出池前会探活并跳过死连接（自动回退按需建立），因此 `pool_size = 1` 可安全使用；若日志频繁出现 `discarded dead pooled work connection`，说明本地服务踢除较快，预热收益有限但不影响功能。
- **Windows 下 TCP keepalive 已禁用**：Windows 上通过 socket2 设置 keepalive 可能导致空闲连接约 30s 后被系统主动断开；当前 Windows 仅启用 `TCP_NODELAY`，Linux 保留 keepalive。

## 排障

- **代理注册被拒**：客户端日志会给出稳定错误码（`invalid type` / `invalid field` / `proxy_name exists` /
  `port not allowed` / `port occupied` / `domain conflict` / `internal error`）。`port occupied` 与
  `domain conflict` 为运行时冲突，客户端会自动退避重试（2s→30s，约 2 分钟）；其余为配置问题，需修正配置。
  服务端日志同时记录详细原因（端口、占用者等）。
- **控制连接异常**：客户端每 30s 心跳、10s 未收到回应判定失联并重连（指数退避 1s→30s）；
  已建立的数据连接（SSH/RDP 会话）在控制面重连期间不受影响。
- **`remote_port` 无法绑定**：检查 `allow_ports` 是否放行、端口是否被其他进程占用、是否使用了特权端口（<1024）。
- **Windows 空闲会话掉线**：见 [docs/WINDOWS_ANTIVIRUS.md](docs/WINDOWS_ANTIVIRUS.md) 与下方 keepalive 说明；
  SSH 客户端建议配置 `ServerAliveInterval 60`。

## License

[MIT](LICENSE)
