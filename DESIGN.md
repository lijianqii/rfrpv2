# rfrp — 反向代理工具设计计划

> **rfrp**（Rust Fast Reverse Proxy）是一个用 Rust 实现的轻量级反向代理工具，用于将内网/本地服务暴露到公网。本文档为项目设计与实现计划。

---

## 1. 项目概述

### 1.1 背景

在内网穿透、本地开发预览、私有服务公网化等场景下，需要一个稳定、高性能、可自托管的反向代理工具。rfrp 使用 Rust + tokio 实现，目标是提供良好的内存安全、性能与可维护性，并以**单一二进制**同时承担服务端与客户端两种角色。

### 1.2 一句话定位

**单一 `rfrp` 二进制**，通过子命令 `rfrp server` / `rfrp client` 切换角色：客户端主动连接服务端建立隧道，外部用户访问服务端端口，流量经隧道转发到客户端背后的本地服务。

### 1.3 首版范围（核心功能集）

| 类别 | 内容 |
|------|------|
| 代理类型 | TCP、UDP、HTTP、HTTPS（vhost） |
| 安全 | Token 鉴权、TLS 控制链路与工作连接 |
| 管理 | Dashboard 监控、多客户端/多代理管理 |
| 运维 | 配置文件 + 命令行参数、日志 |
| 平台 | Linux、Windows（Windows 使用 MinGW-w64 / windows-gnu 工具链） |

---

## 2. 设计目标与非目标

### 2.1 设计目标

1. **高性能**：基于 tokio 异步运行时，单机支撑万级并发连接，零拷贝转发。
2. **模块清晰**：workspace 拆分 crate，职责单一，便于测试与演进。
3. **协议自洽**：自定义二进制协议，带版本号，向前兼容。
4. **安全可靠**：默认 TLS + 鉴权，连接异常自动重建，资源不泄漏。
5. **易用性**：单一二进制 + 子命令，配置文件与 CLI 双入口，Dashboard 可视化。
6. **可观测**：结构化日志 + 指标暴露（Prometheus 兼容）。

### 2.2 非目标（首版不做）

- STCP / XTCP（点对点穿透）
- 插件系统（plugin）
- 负载均衡与多客户端冗余
- GUI 客户端

---

## 3. 技术栈与依赖

### 3.1 语言与运行时

- **语言**：Rust（edition 2021）
- **异步运行时**：tokio（multi-threaded runtime）
- **最低支持版本**：MSRV = Rust 1.75

### 3.2 关键依赖

| 用途 | crate | 说明 |
|------|-------|------|
| 异步运行时 | `tokio` | net / io / sync / time / signal |
| 序列化 | `serde` / `serde_json` | 配置与部分协议消息 |
| 配置 | `toml` | 配置文件格式 |
| TLS | `rustls` + `tokio-rustls` | 控制链路、工作连接、HTTPS |
| 日志 | `tracing` + `tracing-subscriber` | 结构化日志 |
| HTTP | `hyper` + `http` | HTTP/HTTPS 代理与 Dashboard |
| CLI | `clap` | 命令行解析（含子命令） |
| 错误 | `thiserror` / `anyhow` | 库错误 / 应用错误 |
| UUID | `uuid` | run_id / session_id 生成（v4） |
| 指标 | `prometheus` | 指标暴露 |
| Dashboard | `axum` | Dashboard HTTP 服务（基于 hyper，复用 tokio） |
| 静态资源 | `rust-embed` | Dashboard 前端页面嵌入二进制 |
| 工具 | `bytes` / `tokio-util` | 缓冲区、codec |

> 心跳时间戳 `ts`（Unix 毫秒）用 `std::time::SystemTime` 获取，不引入 `chrono` 等外部时间库。

> 依赖版本在 M0 搭建时于各 crate 的 `Cargo.toml` 中锁定（用 `^` 兼容版本），所有依赖须满足 MSRV = Rust 1.75。若某依赖要求更高 MSRV，则降级到兼容版本或寻找替代。

### 3.3 平台与构建策略

rfrp 同时支持 **Linux** 与 **Windows** 运行，但**所有编译均在 Linux 开发机上完成**（Debian）。Windows 版本通过交叉编译产出，不在 Windows 上构建。

#### 3.3.1 目标平台与工具链

| 平台 | Target Triple | 产物 | 构建方式 | 备注 |
|------|---------------|------|----------|------|
| Linux x86_64 | `x86_64-unknown-linux-gnu` | `rfrp` | 原生编译 | 默认开发与运行平台，动态链接 glibc |
| Linux x86_64 (静态) | `x86_64-unknown-linux-musl` | `rfrp` | 原生编译（musl） | 静态二进制，便于部署到任意 Linux |
| Windows x86_64 | `x86_64-pc-windows-gnu` | `rfrp.exe` | **交叉编译**（mingw-w64） | 产出 `.exe`，静态链接 winpthread |

> 明确不支持 `x86_64-pc-windows-msvc`，避免引入 MSVC 构建工具与 Windows SDK 依赖。Windows 版本统一用 MinGW-w64 交叉编译，开发机无需安装 Windows 工具。

#### 3.3.2 Debian 开发机工具链安装

开发机为 Debian 13（trixie），一次性安装所有构建所需工具。以下命令也兼容 Debian 11/12 及 Ubuntu 20.04+。

**1) 系统基础包**

```bash
sudo apt update
sudo apt install -y build-essential pkg-config curl git

# musl 静态编译所需
sudo apt install -y musl-tools

# MinGW-w64 交叉编译器（用于 Windows 产物）
sudo apt install -y mingw-w64
```

安装后可获得：
- `gcc` / `g++` — Linux 原生编译
- `musl-gcc` — Linux 静态编译链接器
- `x86_64-w64-mingw32-gcc` — Windows 交叉编译链接器
- `x86_64-w64-mingw32-g++`、`x86_64-w64-mingw32-windres` 等

**2) Rust 工具链（rustup）**

假设开发机已通过 rustup 安装好 stable 工具链。仅需添加三个目标 target：

```bash
# x86_64-unknown-linux-gnu 通常是默认 host，无需额外添加，可跳过本行
rustup target add x86_64-unknown-linux-gnu
rustup target add x86_64-unknown-linux-musl
rustup target add x86_64-pc-windows-gnu
```

**3) 项目级 cargo 配置（`.cargo/config.toml`）**

放在仓库根目录，提交到 git，确保所有人 clone 后即可交叉编译：

```toml
[target.x86_64-unknown-linux-musl]
linker = "musl-gcc"
rustflags = ["-C", "target-feature=+crt-static"]

[target.x86_64-pc-windows-gnu]
linker = "x86_64-w64-mingw32-gcc"
# 静态链接 winpthread，产物在 Windows 上无需额外 DLL
rustflags = ["-C", "link-args=-static", "-C", "link-args=-lpthread"]
```

**4) 验证安装**

```bash
# Linux 原生
cargo build --release --target x86_64-unknown-linux-gnu

# Linux 静态
cargo build --release --target x86_64-unknown-linux-musl

# Windows 交叉
cargo build --release --target x86_64-pc-windows-gnu
ls target/x86_64-pc-windows-gnu/release/rfrp.exe
```

> 三条命令均能在 Debian 上成功，即工具链就绪。

#### 3.3.3 构建命令速查

| 目标 | 命令 | 产物路径 |
|------|------|----------|
| Linux 动态 | `cargo build --release` | `target/release/rfrp` |
| Linux 静态 | `cargo build --release --target x86_64-unknown-linux-musl` | `target/x86_64-unknown-linux-musl/release/rfrp` |
| Windows | `cargo build --release --target x86_64-pc-windows-gnu` | `target/x86_64-pc-windows-gnu/release/rfrp.exe` |
| 单测（Linux） | `cargo test` | — |
| Clippy | `cargo clippy --all-targets -- -D warnings` | — |

#### 3.3.4 交叉编译矩阵与产物

手动构建时产出以下产物，命名规范 `rfrp-{version}-{target}.{ext}`：

| 产物 | Target | 文件 |
|------|--------|------|
| Linux 动态 | `x86_64-unknown-linux-gnu` | `rfrp-0.1.0-x86_64-linux-gnu.tar.gz` |
| Linux 静态 | `x86_64-unknown-linux-musl` | `rfrp-0.1.0-x86_64-linux-musl.tar.gz` |
| Windows | `x86_64-pc-windows-gnu` | `rfrp-0.1.0-x86_64-windows-gnu.zip`（含 `rfrp.exe`） |

- 全部产物在 Debian 开发机上一次构建产出，无需 Windows 环境。
- Windows 产物通过 `file` 命令确认格式：`PE32+ executable for MS Windows (console) x86-64`。
- 可用 `scripts/release.sh` 一键编译并打包三产物。

#### 3.3.5 平台差异注意点

| 维度 | Linux | Windows (gnu) |
|------|-------|---------------|
| 异步 IO | epoll | IOCP（tokio 自动适配） |
| 信号 | SIGTERM/SIGINT → 优雅退出 | Console Ctrl-C 事件（`tokio::signal::ctrl_c`） |
| UDP | 标准 socket | 需注意 `SO_REUSEADDR` 语义差异 |
| 文件权限 | chmod 0600 配置/密钥 | ACL（首版不处理，仅警告） |
| 端口占用 | `EADDRINUSE` 明确 | 同样明确，但 TIME_WAIT 行为不同 |
| 线程数 | tokio 默认 = CPU 核数 | 同左，但 IOCP 线程开销略高 |

> 代码层一律使用 tokio 抽象，禁止直接调用平台特定 API；如必须分支，用 `#[cfg(target_os = "windows")]` / `#[cfg(unix)]` 隔离，并集中放在 `rfrp-common::util::platform` 模块。Windows 产物的运行时验证在 Windows 测试机/虚拟机上手动进行，不纳入自动化测试。

---

## 4. 整体架构

### 4.1 拓扑

```
        外部用户                公网服务器 (rfrps)                               内网 (rfrpc)
        ┌──────┐                ┌──────────────────┐                            ┌──────────────────┐
        │ User │──访问公网端口──▶│ Listener (proxy) │                            │ 本地服务         │
        └──────┘                │   ↓              │                            │ (127.0.0.1:xxxx) │
                                │ Control Plane    │◄──────控制连接(7000)────────│ rfrpc            │
                                │   ↑              │◄──────工作连接(7000)────────│   ↓              │
                                │ Router/Forwarder │────────────────────────────▶│ 本地服务         │
                                └──────────────────┘                            └──────────────────┘
                                         ▲
                                         │ HTTP（独立端口，如 7500，不走控制协议）
                                  ┌──────────────┐
                                  │  Dashboard   │  ← 运维侧访问（Basic Auth）
                                  └──────────────┘
```

> 控制连接与工作连接共用 `bind_port`（如 7000），但工作连接是独立的 TCP 流（见 4.2）。Dashboard 是独立的 HTTP 服务，监听 `[dashboard].addr`（如 7500），不经过控制协议，与 `bind_port` 无关。

### 4.2 两种连接

1. **控制连接（Control Connection）**
   - 由 rfrpc 主动发起，长连接，贯穿整个生命周期。
   - 承载：登录鉴权、代理注册、心跳、工作连接协商指令。
   - 断开后触发重连。

2. **工作连接（Work Connection）**
   - 由 rfrpc 预建池 / 按需建立，承载数据面流量。
   - 每个外部用户连接对应一条工作连接，rfrps ↔ rfrpc ↔ 本地服务。
   - 生命周期随用户连接结束而结束。

### 4.3 数据面 vs 控制面分离

- **控制面**：低频、需可靠、需加密鉴权。
- **数据面**：高频、需吞吐、可复用连接池。
- 二者共用同一协议帧，但在不同 TCP 流上传输。

---

## 5. 核心概念

| 概念 | 说明 |
|------|------|
| **Proxy** | 一个对外暴露的服务条目，由 `proxy_name + type + 公网端口 + 本地地址` 定义（配置文件中字段名为 `name`，协议消息中映射为 `proxy_name`，见 6.2） |
| **Session** | 一个 rfrpc 与 rfrps 之间的控制连接会话，承载多个 Proxy |
| **WorkConn** | 数据面连接，由 rfrps 按需请求或 rfrpc 预建池提供 |
| **RunID** | rfrpc 身份标识，重连时复用以恢复已注册的 Proxy |
| **Group/Route**（预留） | 后续负载均衡/路由扩展点，首版不实现 |

---

## 6. 协议设计

### 6.1 帧格式（Frame）

**控制连接**上的字节流统一封装为长度前缀帧：

```
+----------+----------+----------+----------------+
| Version  | MsgType  |  Length  |    Payload     |
|  1 byte  |  1 byte  | 4 bytes  |  Length bytes  |
+----------+----------+----------+----------------+
```

- `Version`：协议版本（当前 = 1），用于兼容性协商。
- `MsgType`：消息类型枚举。
- `Length`：Payload 字节数（u32，大端），单帧上限 16 MiB。**`Length=0` 合法**，表示无 Payload（如 `Close` 无 reason 时）；解码端据此跳过 Payload 读取。
- `Payload`：按 MsgType 不同，为 JSON 或二进制。Length=0 时 Payload 为空。

> 帧头含 Version + MsgType，`tokio_util::codec::LengthDelimitedCodec` 无法直接承载（它只处理 Length + Payload）。因此**自定义 Codec**，在 `rfrp-common::protocol::frame` 中实现 `tokio_util::codec::Encoder<Frame>` 与 `Decoder`：解码时先读 6 字节头（Version + MsgType + Length），校验 Version 与 Length，再读 Payload；编码时按序写入。底层 `Framed<TcpStream, FrameCodec>` 自动处理半包/粘包。

**工作连接**首帧为 `StartWorkConn` 控制帧（标识归属），其后**直接透传原始字节流**，不再加帧头。原因：一条工作连接独占一个用户连接，无需多路复用，省去每段 6 字节帧头开销。

**控制连接的消息交错语义：**

- 一条控制连接上可**并发交错**收发多条控制消息，按 `MsgType` 分发处理，不要求严格请求-响应配对。
- rfrps 可在等待 rfrpc 的 NewProxyResp 期间，并行下发多条 `ReqWorkConn`（不同 `work_id`）；rfrpc 也可在处理某条 ReqWorkConn 时，继续发送其他 NewProxy。
- 实现层用 `tokio::select!` / 多任务并发处理收发，**不得**用单线程串行 await 阻塞整条控制连接（否则一个慢工作连接会拖垮所有控制信令）。
- `NewProxyResp` 与对应 `NewProxy` 的关联靠 `proxy_name` 字段（NewProxyResp 必带 `proxy_name`），不靠顺序；`StartWorkConn.work_id` 与对应 `ReqWorkConn.work_id` 关联。
- 心跳消息（Heartbeat/HeartbeatResp）独立于业务消息流，可随时插入，不影响其他消息处理。

> **"并发交错"与"NewProxy 串行"的关系：**
> - **底层收发并发**：控制连接的 socket 读写不得被任何单个业务消息阻塞（如 rfrpc 等 NewProxyResp 时，仍须能收 rfrps 下发的 ReqWorkConn/Heartbeat 并响应）。
> - **应用层 NewProxy 串行**：rfrpc 业务逻辑上按 `[[proxy]]` 顺序逐个发 NewProxy 并等对应 NewProxyResp（见 8.1），这是**应用层语义约束**，不等于底层 socket 串行。
> - 即：rfrpc 发完 NewProxy A 后，在等 NewProxyResp A 期间，底层 select 仍可并发处理 rfrps 下发的 ReqWorkConn（为已注册的 Proxy B 建工作连接）、Heartbeat 等。NewProxy 的"串行"仅指"不发 NewProxy B 直到 NewProxyResp A 到达"。

### 6.2 消息类型

> 方向栏中 **C = rfrpc（客户端进程）**，**S = rfrps（服务端进程）**，与外部用户无关。

| MsgType | 名称 | 方向 | Payload | 说明 |
|---------|------|------|---------|------|
| 0x01 | `Login` | C→S | `{run_id, token, version}` | 登录鉴权，不携带 proxy |
| 0x02 | `LoginResp` | S→C | `{ok, error?, session_id?, work_conn_tls?}` | 登录结果。`ok=true` 时必带 `session_id` 与 `work_conn_tls`；`ok=false` 时三者均省略（见 6.2.3） |
| 0x03 | `NewProxy` | C→S | `{proxy_name, type, remote_port, custom_domains}` | 注册单个代理（`remote_port` 与 `custom_domains` 按类型取用，见下方说明） |
| 0x04 | `NewProxyResp` | S→C | `{proxy_name, ok, error?}` | 注册结果，`proxy_name` 与对应 NewProxy 一致（见 6.1 关联约定） |
| 0x05 | `Heartbeat` | 双向 | `{ts}` | 心跳保活，rfrps 与 rfrpc 均可主动发送 |
| 0x06 | `HeartbeatResp` | 双向 | `{ts}` | 心跳回应，收到 Heartbeat 的一方必须回复，`ts` 原样回传 |
| 0x07 | `ReqWorkConn` | S→C | `{proxy_name, work_id}` | rfrps 请求 rfrpc 建立工作连接。`work_id=0` 为补充池场景（无具体用户连接）；非零为按需建立（关联用户连接），见 8.2 |
| 0x08 | `StartWorkConn` | C→S | `{proxy_name, work_id}` | 工作连接首帧，其后转透传。`work_id=0` 为池化预备标识（见 8.2），非零为按需建立 |
| 0x09 | `Close` | 双向 | `{reason?}` | 主动关闭通知 |

> Login 不再携带 `proxies[]`，所有代理统一通过 `NewProxy` 逐个注册，便于运行时动态增删（首版仅启动期注册，但流程统一）。

> `NewProxy` 的 `remote_port` 字段：TCP/UDP 类型必填；HTTP/HTTPS 类型忽略（走 `vhost_http_port` / `vhost_https_port`，可不填或填 0）。`custom_domains` 反之：HTTP/HTTPS 必填，TCP/UDP 忽略。

> **配置字段映射**：客户端配置文件 `[[proxy]]` 段中的 `name` 字段，在协议消息中统一映射为 `proxy_name`（协议 JSON 字段名）。即配置写 `name = "ssh"`，发 `NewProxy` 时 JSON 为 `{"proxy_name":"ssh", ...}`。

### 6.2.1 字段语义

| 字段 | 类型 | 生成方 | 用途 |
|------|------|--------|------|
| `run_id` | string (UUID v4) | rfrpc 首次启动生成，持久化到本地文件 | 重连身份标识，rfrps 据此恢复原 Proxy。文件路径默认 `~/.rfrp/run_id`，可由配置 `run_id_file` 覆盖；systemd 服务建议 `/var/lib/rfrp/run_id`。Linux 下文件权限 0600，Windows 侧仅警告（见风险表） |
| `session_id` | string (UUID v4) | rfrps 登录时生成 | 仅用于日志关联，后续消息不携带 |
| `work_id` | u64 单调递增（0 为保留值） | rfrps 生成，随 ReqWorkConn 下发给 rfrpc | 工作连接内部索引与日志关联，rfrpc 在 StartWorkConn 中透传回传。**`work_id=0` 为池化预备标识**（见 8.2）：rfrpc 预建池化工作连接时填 0，rfrps 据此放入空闲池而非绑定用户连接；非零值由 rfrps 在 ReqWorkConn 中下发，用于按需建立时关联用户连接 |
| `ts` | u64 (Unix 毫秒) | 发送方 | 心跳时间戳，用于超时判断 |
| `version` | u8 | 双方 | 协议版本，当前 = 1 |
| `type` | string 枚举 | rfrpc | 代理类型，小写：`"tcp"` / `"udp"` / `"http"` / `"https"` |
| `proxy_name` | string | rfrpc 注册时指定 | Proxy 全局唯一标识，后续消息引用 |
| `remote_port` | u16 (JSON number) | rfrpc NewProxy 携带 | TCP/UDP 必填，HTTP/HTTPS 忽略（可省略） |
| `custom_domains` | array of string | rfrpc NewProxy 携带 | HTTP/HTTPS 必填（至少 1 个），TCP/UDP 忽略（可省略） |

### 6.2.2 Close 消息触发场景

`Close` 用于控制连接层面的主动关闭通知（不用于单个工作连接，工作连接关闭由 TCP FIN 直接处理）。触发场景：

- **rfrpc 主动退出**：收到 SIGTERM/Ctrl-C，发送 `Close{reason:"client shutdown"}` 后断开。
- **rfrps 主动退出**：收到信号，向所有 Session 发送 `Close{reason:"server shutdown"}` 后断开。
- **配置/协议错误**：如帧格式错误、不兼容的 NewProxy 类型（rfrps 发 `Close` 后断开）。
- **版本不匹配**：发生在 Login 阶段，rfrps 返回 `LoginResp{ok:false, error:"version mismatch"}` 后断开，不发 Close。
- **鉴权失败**：rfrps 不发 Close，直接断开（避免回显，见 10.2）。
- **rfrps 主动下线单 Proxy**：首版不支持运行期动态下线（无热重载）；但若 rfrps 在 NewProxy 后因 bind remote_port 失败需清理已注册的 Proxy 条目，rfrps **不发 Close**（控制连接仍存活，仅该 Proxy 失效），rfrpc 已通过 NewProxyResp{ok:false} 获知失败，无需额外通知。若未来支持动态下线，可扩展 `Close{reason:"proxy removed", proxy_name?}` 携带 proxy_name 字段（首版不实现，Close 不含 proxy_name）。
- `reason` 字段可选，用于日志诊断，不影响对端行为（对端收到 Close 后应清理资源并视情况重连）。
- **可靠性**：Close 是"尽力通知"，发送后立即断开 TCP，rfrps 可能因缓冲未刷新而收不到。收不到则靠心跳超时（10s）兜底清理 Session。
- **心跳超时触发断开**：一方发出 Heartbeat 后 10s 未收到 HeartbeatResp，判定对端已死，**直接断开 TCP，不发 Close**（对端可能已崩溃，发了也收不到；发 Close 反而增加延迟）。rfrps 侧断开后清理 Session 与所有 Proxy 占用；rfrpc 侧断开后触发指数退避重连。

### 6.2.3 序列化与字段约束

**JSON 序列化约定：**

- Payload 统一用 JSON（UTF-8 编码），字段顺序无要求。
- 可选字段（如 `error?`、`reason?`）缺失时**省略不传**（而非传 `null`）；反序列化端用 `Option<T>` 接收，缺失与 `null` 均视为 `None`。
- `LoginResp.ok = false` 时，`session_id` 与 `work_conn_tls` 均省略不传。
- 数字字段（`work_id`、`ts`、`version`）用 JSON number，不引号。

**字符串字段约束：**

| 字段 | 编码 | 最大长度 | 非法值 |
|------|------|----------|--------|
| `run_id` | UTF-8 | 64 字节 | 非 UUID v4 格式 |
| `session_id` | UTF-8 | 64 字节 | — |
| `token` | UTF-8 | 256 字节 | 空串 |
| `proxy_name` | UTF-8 | 64 字节 | 空串、含 `/`、含空白 |
| `custom_domains` 元素 | UTF-8 | 253 字节（域名上限） | 空串、非法域名格式 |
| `error` / `reason` | UTF-8 | 512 字节 | — |

> rfrps 在解析每个字段时按上表校验，超长或非法返回协议层错误（回显原因，见 10.2）。单帧 Payload 上限 16 MiB（见 6.1），但单个字符串字段不应超过上表限制。

**数组字段约束：**

| 字段 | 类型 | 元素上限 | 说明 |
|------|------|----------|------|
| `custom_domains` | array of string | **16 个** | 超过 16 个域名返回 `"invalid field"` 错误；防止恶意客户端发送超大数组耗尽内存 |

> 数组元素上限在校验单个元素格式之前先检查长度，超长直接拒绝，不逐个解析。

### 6.3 协议升级（TLS）

- 控制连接建立后，先进行 TLS 握手，再发送 `Login`。
- 采用 **先 TLS 握手再登录** 的模型，避免明文泄露 token。
- `tls_enable = false`（M1 测试期）时跳过 TLS 握手，建立 TCP 后直接发 Login；生产环境必须 `tls_enable = true`。

### 6.4 版本协商

- 帧头 `Version` 与 `Login.version` 字段**必须一致**，否则服务端拒绝并返回 `LoginResp{ok:false, error:"version mismatch"}`。
- 客户端发送的帧头 Version 即其声明版本；服务端支持的最高版本若 ≥ 客户端版本则接受，否则拒绝。
- 首版仅支持 Version=1，未来版本通过 `Login.version` 字段协商降级（首版不实现降级逻辑）。

### 6.5 工作连接加密

工作连接是独立的 TCP 流，不在控制连接的 TLS 隧道内。加密策略：

- **默认**：工作连接同样走 TLS（rfrpc 发起时即 TLS 握手），复用 rfrps 服务端证书。
- **可选禁用**：rfrpc 配置 `work_conn_tls = false` 时工作连接明文传输，仅适用于信任网络或性能敏感场景。
- **服务端优先**：rfrps 在 `LoginResp` 中下发自身 `work_conn_tls` 偏好，rfrpc 据此决策。

**工作连接 TLS 决策表（rfrps 偏好优先）：**

| rfrps `work_conn_tls` | rfrpc `work_conn_tls` | 实际行为 | 不匹配时的处理 |
|-----------------------|-----------------------|----------|----------------|
| true  | true  | 双方 TLS 握手 | —（一致） |
| true  | false | **rfrpc 升级为 TLS** | rfrpc 建立工作连接时主动 TLS 握手；若 rfrpc 仍发明文，rfrps 拒绝并关闭该工作连接 |
| false | true  | **rfrpc 降级为明文** | rfrpc 按 rfrps 偏好发明文；rfrpc 不得强制 TLS（rfrps 未配证书） |
| false | false | 双方明文 | —（一致） |

- 不引入应用层对称加密（如 AES-GCM）—— TLS 已足够，避免重复造轮子；依赖表中不列此类 crate。
- rfrpc 在 Login 收到 `LoginResp.work_conn_tls` 后，立即覆盖自身运行期决策值（仅运行期，不写回配置文件），后续所有工作连接按该值执行。

### 6.6 校验与冲突处理

#### NewProxy 校验流程

rfrps 收到 `NewProxy` 后按顺序校验，任一失败返回 `NewProxyResp{ok:false, error:<原因>}`：

1. **类型合法**：`type` ∈ {tcp, udp, http, https}。
2. **名字唯一**：`proxy_name` 在当前 rfrps 所有 Session 内全局唯一，重复拒绝。
3. **端口范围**：`remote_port`（TCP/UDP 类型）必须在 rfrps `allow_ports` 范围内。
4. **端口占用**：`remote_port` 未被其他 Proxy 或系统进程占用。
5. **vhost 域名**：HTTP/HTTPS 类型的 `custom_domains` 不与其他 Proxy 冲突。

**`NewProxyResp.error` 取值规范（rfrps 返回，rfrpc 据此决策）：**

| 校验失败项 | error 字符串（小写，固定） | rfrpc 建议处理 |
|------------|---------------------------|----------------|
| type 非法 | `"invalid type"` | 配置错误，记日志，不重试该 Proxy |
| proxy_name 重复 | `"proxy_name exists"` | 配置错误，记日志，不重试 |
| remote_port 超出 allow_ports | `"port not allowed"` | 配置错误，记日志，不重试 |
| remote_port 被占用 | `"port occupied"` | 运行时冲突，可重试（重连恢复场景）；rfrpc 记日志，不中断其他 Proxy |
| custom_domains 冲突 | `"domain conflict"` | 运行时冲突，可重试；rfrpc 记日志，不中断其他 Proxy |
| 字段缺失/格式错误 | `"invalid field"` | 配置错误，记日志，不重试 |
| 内部错误（如监听失败） | `"internal error"` | rfrpc 记日志，不重试 |

> error 字符串为**小写英文标识符**，不含变量（端口号、域名等不拼入字符串，便于 rfrpc 按精确匹配分类处理）。具体冲突值可通过日志关联，不回显给对端。rfrpc 收到 `ok=false` 时按上表分类：`"port occupied"` / `"domain conflict"` 在重连恢复场景保留 Proxy 条目待下次重试，其余视为配置错误跳过。

#### 重连恢复冲突

rfrpc 重连复用 `run_id`，rfrps 处理流程：

- rfrps 收到 `Login` 且 `run_id` 匹配已存在 Session 时，**先清理旧 Session**（释放其所有 proxy_name 与 port 占用），再走正常登录流程，随后 rfrpc 逐个 NewProxy 恢复。
- 若旧 Session 因断线未及时清理，靠本步骤兜底；若旧 Session 已被清理，则直接按新登录处理。
- rfrpc 逐个 NewProxy 恢复时，按 6.6 校验流程逐条判定：
  - 原 `remote_port` 仍空闲 → 恢复成功。
  - 原 `remote_port` 已被其他 rfrpc 占用 → 该 Proxy 恢复失败，返回 `NewProxyResp{ok:false, error:"port occupied"}`，rfrpc 记录日志但不中断其他 Proxy。
  - HTTP/HTTPS 类型的 `custom_domains` 被占用 → 同上处理。

#### 多 rfrpc 同名/同端口

- 同 `proxy_name` Proxy：第二个注册请求被拒绝（名字全局唯一）。
- 同 `remote_port`：第二个注册请求被拒绝（端口占用）。
- 不做负载均衡，不允许多个 rfrpc 共享同一 Proxy 条目。

---

## 7. 模块划分

采用 Cargo workspace，按"公共库 + 服务端库 + 客户端库 + 统一二进制"分层，每个 crate 内部再按职责拆 module。服务端与客户端逻辑分别放在 `rfrps` / `rfrpc` 库 crate 中，最终由单一 `rfrp` 二进制按子命令调用。

### 7.1 目录结构

```
rfrp/
├── Cargo.toml                 # workspace 根
├── DESIGN.md
├── crates/
│   ├── rfrp-common/           # 共享库（协议/配置/TLS 封装/错误/工具）
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── protocol/      # 帧编解码 + 消息类型
│   │       │   ├── mod.rs
│   │       │   ├── frame.rs
│   │       │   └── msg.rs
│   │       ├── config/        # 配置结构 + 解析
│   │       │   ├── mod.rs
│   │       │   ├── server.rs
│   │       │   └── client.rs
│   │       ├── crypto/        # TLS 封装（rustls）
│   │       ├── auth/          # token 校验
│   │       ├── constants.rs   # 集中定义协议/超时/上限等常量（避免 magic number）
│   │       ├── error.rs       # 统一错误类型
│   │       └── util/          # 通用工具
│   │           └── platform.rs  # 平台差异封装（cfg windows/unix）
│   ├── rfrps/                 # 服务端库（server 子命令逻辑）
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── server.rs      # Server 主控
│   │       ├── control/       # 控制连接处理
│   │       ├── listener/      # 公网监听管理（per-proxy）
│   │       ├── proxy/         # 代理实现
│   │       │   ├── tcp.rs
│   │       │   ├── udp.rs
│   │       │   ├── http.rs
│   │       │   └── https.rs
│   │       ├── pool/          # 工作连接池（per-Proxy，预建/取用/补充，见 8.2）
│   │       ├── session/       # 客户端会话表
│   │       ├── router/        # 用户连接 → 工作连接路由
│   │       ├── dashboard/     # 监控 API + Web
│   │       └── metrics.rs
│   ├── rfrpc/                 # 客户端库（client 子命令逻辑）
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── client.rs      # Client 主控
│   │       ├── control/       # 控制连接 + 重连
│   │       ├── heartbeat.rs
│   │       ├── workconn/      # 工作连接建立（含按 pool_size 预建，池管理在 rfrps 侧）
│   │       └── proxy/         # 本地回连（按代理类型分流）
│   │           ├── tcp.rs     # TCP 桥接（字节流双向透传）
│   │           ├── udp.rs     # UDP 会话 + 4字节长度前缀分帧（见 8.6）
│   │           ├── http.rs    # HTTP 回连（复用 TCP 桥接）
│   │           └── https.rs   # HTTPS 回连（复用 TCP 桥接）
│   └── rfrp-bin/              # 统一二进制入口
│       └── src/
│           ├── main.rs        # clap 子命令分发 → 调用 rfrps::run / rfrpc::run
│           ├── cli.rs         # 子命令与参数定义
│           └── logging.rs     # 日志初始化
```

> **`rfrp-common::constants` 集中常量定义**：以下散布于各章节的数值常量统一在 `constants.rs` 中定义并导出，避免 magic number：
> - 协议：`PROTOCOL_VERSION = 1`、`FRAME_HEADER_LEN = 6`、`FRAME_MAX_PAYLOAD: u32 = 16 * 1024 * 1024`、`WORK_ID_POOL_RESERVED = 0`
> - 超时（秒）：`HEARTBEAT_INTERVAL = 30`、`HEARTBEAT_TIMEOUT = 10`、`WORK_CONN_TIMEOUT_RFRPS = 10`、`WORK_CONN_TIMEOUT_RFRPC = 8`、`UDP_SESSION_TIMEOUT = 60`、`GRACEFUL_SHUTDOWN_TIMEOUT = 30`
> - 重连退避（秒）：`RECONNECT_BACKOFF_INITIAL = 1`、`RECONNECT_BACKOFF_MAX = 30`
> - 上限：`MAX_CUSTOM_DOMAINS = 16`、`POOL_SIZE_DEFAULT = 1`、`POOL_SIZE_WARN_THRESHOLD = 16`、`MAX_UDP_PACKET_SIZE: usize = 65507`
> - 字符串长度上限（字节）：`MAX_RUN_ID_LEN = 64`、`MAX_TOKEN_LEN = 256`、`MAX_PROXY_NAME_LEN = 64`、`MAX_DOMAIN_LEN = 253`、`MAX_ERROR_LEN = 512`

### 7.2 职责边界

| Crate | 职责 | 不负责 |
|-------|------|--------|
| `rfrp-common` | 协议编解码、配置、TLS 封装、错误、鉴权原语 | 任何网络 I/O 编排 |
| `rfrps` | 服务端：监听、会话管理、代理转发、Dashboard | CLI 解析、退出码 |
| `rfrpc` | 客户端：控制连接、重连、工作连接、本地回连 | CLI 解析、退出码 |
| `rfrp-bin` | CLI 子命令分发、配置加载、日志初始化、调用 rfrps/rfrpc 库 | 业务逻辑 |

### 7.3 CLI 形态

```
rfrp 0.1.0 — Rust Fast Reverse Proxy

USAGE:
    rfrp <SUBCOMMAND>

SUBCOMMANDS:
    server    以服务端模式运行（公网监听，接受客户端隧道）
    client    以客户端模式运行（主动连接服务端，暴露本地服务）
    help      显示帮助

示例:
    rfrp server -c rfrp-server.toml
    rfrp client -c rfrp-client.toml
    rfrp server --bind 0.0.0.0:7000 --token secret
```

> 同一产物既能当服务端也能当客户端，部署与分发只需一个二进制。

**CLI 参数表：**

通用参数（两个子命令均适用）：

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `-c, --config <FILE>` | 配置文件路径（必填，或与下方字段参数二选一） | — |
| `--log-level <LEVEL>` | 覆盖 `[log].level` | 取自配置 |
| `--log-output <OUTPUT>` | 覆盖 `[log].output` | 取自配置 |
| `--log-format <FORMAT>` | 覆盖 `[log].format` | 取自配置 |

`server` 子命令额外参数（覆盖 `[server]` / `[proxy]` 段，未提供则取配置文件）：

| 参数 | 对应配置字段 |
|------|--------------|
| `--bind <ADDR:PORT>` | `bind_addr` + `bind_port` |
| `--token <TOKEN>` | `token` |
| `--tls-enable <BOOL>` | `tls_enable` |
| `--work-conn-tls <BOOL>` | `work_conn_tls` |

`client` 子命令额外参数（覆盖 `[client]` 段）：

| 参数 | 对应配置字段 |
|------|--------------|
| `--server <ADDR:PORT>` | `server_addr` + `server_port` |
| `--token <TOKEN>` | `token` |
| `--tls-enable <BOOL>` | `tls_enable` |
| `--work-conn-tls <BOOL>` | `work_conn_tls` |

> 规则：CLI 参数覆盖配置文件同名字段；`-c` 与字段参数可组合（先加载配置文件，再用 CLI 参数覆盖）。`-c` 与全部字段参数同时缺省时报错退出。

---

## 8. 核心流程

### 8.1 客户端启动与登录

```
rfrpc 启动
  → 读取配置
  → 生成/复用 run_id
  → 建立 TCP 到 rfrps
  →（若 tls_enable=true）TLS 握手
  → 发送 Login(run_id, token, version)
  → 收到 LoginResp
      ok=true  → 按 [[proxy]] 声明顺序串行发送 NewProxy → 等待对应 NewProxyResp 确认
                → 全部注册完成后进入就绪态，等待 ReqWorkConn（M2 起同时预建工作连接池）
      ok=false → 按错误类型区分：
            鉴权失败（无具体回显）：立即退出（记日志），不重连
            版本不匹配（error="version mismatch"）：立即退出（记日志），不重连——重连不会改变版本兼容性
            网络错误（连接断开/超时，未收到 LoginResp）：按指数退避重连（见 8.3）
```

> NewProxy 注册为**串行**：逐个发送 NewProxy 并等待对应 NewProxyResp（按 `proxy_name` 关联），确认成功后再发下一个。单条失败不中断后续（按 6.6 error 分类处理）。串行便于错误定位与端口冲突逐条处理；并行优化留待后续版本。

### 8.2 外部用户访问（TCP 代理为例）

> M2 起 rfrps 收到用户连接时**优先取工作连接池**，池空才走以下按需 ReqWorkConn 流程。
>
> **pool_size=0（纯按需模式）**：rfrpc 不预建工作连接，空闲池恒为空，每个用户连接都走按需 ReqWorkConn 流程。适用于低频访问或资源受限场景，代价是首包延迟增加（需等 rfrpc 建立工作连接）。pool_size=0 对所有代理类型生效。

**按需建立流程（池空或 M1）：**

```
User → rfrps:remote_port  (Listener 接收)
  → rfrps 生成 work_id，在控制连接发送 ReqWorkConn(proxy_name, work_id)
  → rfrpc 收到，新建 work TCP 到 rfrps
      →（若 work_conn_tls=true）TLS 握手
      → 首帧 StartWorkConn(proxy_name, work_id)  # 回传 work_id 供 rfrps 关联
  → rfrpc 发完 StartWorkConn 后，立即建立到 local_ip:local_port 的 TCP 连接
  → rfrps 将 User 与 WorkConn 双向桥接（双向 copy）
  → rfrpc 将 WorkConn 与本地连接双向桥接（双向 copy）
  → 任一端关闭，回收两端资源
```

**本地连接失败处理（rfrpc 侧）：**

若 rfrpc 建立 `local_ip:local_port` TCP 连接失败（如本地服务未启动/拒绝连接）：

- rfrpc **立即关闭工作连接**（TCP FIN，不发协议帧——工作连接首帧后即透传，无错误帧定义）。
- rfrps 侧工作连接收到 EOF → 同步关闭用户连接（用户侧表现为连接立即断开）。
- rfrpc 记 `warn` 日志（含 proxy_name、本地地址、错误原因），不触发重连、不影响其他 Proxy。
- 池化场景同理：预建或补充时若本地连接失败，关闭该工作连接，rfrps 收到 work_id=0 的 StartWorkConn 后未入池即断（rfrps 侧记日志，池计数不增）；rfrpc 不重试预建，等下次 ReqWorkConn 或下个预建周期再试。
- 首版不向 rfrps 上报本地连接失败的语义化错误（无对应协议帧）；用户侧表现为"连接被重置"，由上层应用重试。

**池命中流程（M2 起，池非空）：**

池中的工作连接由 rfrpc 在 NewProxy 成功后预建，但**预建时尚无 work_id**（尚未绑定用户连接）。协商方式：

```
[预建阶段] rfrpc NewProxy 成功后
  → rfrpc 新建 work TCP 到 rfrps（按 pool_size 预建 N 条）
      → 首帧 StartWorkConn(proxy_name, work_id=0)  # work_id=0 表示"池化预备，尚未绑定"
  → rfrpc 发完 StartWorkConn 后，立即建立到 local_ip:local_port 的 TCP 连接并桥接
  → rfrps 收到 work_id=0 的 StartWorkConn → 放入该 Proxy 的空闲池，不绑定用户连接

[取用阶段] User → rfrps:remote_port
  → rfrps 从空闲池取一条预备工作连接
  → rfrps 内部分配 work_id（仅用于 rfrps 侧日志关联，不下发给 rfrpc）
  → rfrps 将 User 与取出的 WorkConn 双向桥接  # 不再发 ReqWorkConn
  → rfrpc 侧工作连接已与本地服务桥接，直接透传即可
  → rfrps 同时要求 rfrpc 补充一条预备工作连接：
      发 ReqWorkConn(proxy_name, work_id=0)  # work_id=0 表示"补充池，无具体用户连接"
  → rfrpc 收到 work_id=0 的 ReqWorkConn → 走预建流程（建 work TCP + StartWorkConn.work_id=0 + 本地桥接）
```

> **work_id 语义统一约定：**
> - `ReqWorkConn.work_id = 0` → 补充池场景（无具体用户连接），rfrpc 回 `StartWorkConn.work_id = 0`。
> - `ReqWorkConn.work_id ≠ 0` → 按需建立场景（关联具体用户连接），rfrpc 回 `StartWorkConn.work_id` 透传原值。
> - rfrpc 主动预建（NewProxy 成功后，无 ReqWorkConn 触发）→ 直接发 `StartWorkConn.work_id = 0`。
> - rfrps 收到 `StartWorkConn.work_id = 0` → 放入该 `proxy_name` 的空闲池；收到非零 → 与待处理的用户连接关联。
>
> 池命中时 rfrps 不再通过控制连接通知 rfrpc 数据面已就绪（工作连接已桥接，直接透传），rfrpc 透明透传。补充池的 ReqWorkConn 是控制面消息，与数据面透传独立。

### 8.3 心跳与重连

- 心跳由**独立定时器驱动**，每 30s 固定发一次 `Heartbeat`，与业务消息流并行，**不依赖"控制连接空闲"**——即使持续有 NewProxy/ReqWorkConn 在传，心跳仍按 30s 周期发送。
- rfrps 与 rfrpc 均主动发送心跳，对端必须回 `HeartbeatResp`（`ts` 原样回传）。
- 发出 Heartbeat 后等待 HeartbeatResp，**10s 未收到 Resp 则判定断开**（触发清理与重连）。
- 断开后 rfrpc 按指数退避重连（1s → 2s → 4s → … → 30s 上限）。**此为全局唯一重连退避策略**（常量见 §7.1 `RECONNECT_BACKOFF_INITIAL` / `RECONNECT_BACKOFF_MAX`），§8.1 网络错误重连与 §8.5 rfrpc 离线后恢复均复用本策略。
- 重连复用 `run_id`，rfrps 恢复原 Proxy 监听（若端口仍可用，冲突处理见 6.6）。

### 8.4 HTTP/HTTPS（vhost）

- rfrps 共用 80/443 监听，按 `Host` / `SNI` 路由到对应 Proxy。
- **HTTP**：rfrps 用 `hyper` 读取请求头判定 Host，命中后将**已读缓冲区连同后续流**一起桥接到工作连接（不丢失已读字节）。
- **HTTPS**：TLS 终止在 rfrps（使用 `vhost_tls_cert` / `vhost_tls_key`），rfrps 完成 TLS 握手拿到 SNI 后，按明文 HTTP 处理取 Host，再将**解密后的明文流**桥接到工作连接。工作连接承载明文，本地服务收到的是明文 HTTP。
- HTTPS 所需证书由 rfrps 服务端配置提供（详见 9.1 `vhost_tls_cert` / `vhost_tls_key`）。**证书在 rfrps 启动时一次性加载到内存**（`rustls::ServerConfig` 持有），运行期不重新读取；证书文件更换需重启 rfrps 生效（首版不支持热重载，见非目标）。
- 命中 Proxy 后向 rfrpc 请求工作连接，流程同 TCP（8.2）。
- **HTTP 与 HTTPS 独立**：一个 `[[proxy]]` 的 `type` 只能是 `http` 或 `https` 之一。若同一后端需同时暴露 HTTP 和 HTTPS，应声明两个 Proxy 条目（配置 `name` 不同，即协议 `proxy_name` 不同），`custom_domains` 可相同，分别走 `vhost_http_port` / `vhost_https_port`。

### 8.5 异常与超时处理

**外部用户连接但 rfrpc 离线/无此 Proxy：**

- rfrps 收到外部用户连接时，若对应 Session 已断开或 Proxy 未注册 → 立即关闭用户连接，不等待。
- 若 rfrpc 在线 → rfrps 发 ReqWorkConn 后等待，超时 10s 未收到 StartWorkConn 则关闭用户连接并记日志。（M1 起即按需建立；M2 起优先取工作连接池，池空退化为按需建立，走本流程）

**工作连接建立超时：**

- rfrps 发出 ReqWorkConn 后，等待 StartWorkConn 的超时为 **10s**（固定，不可配）——这是 rfrps 侧最终兜底超时。
- rfrpc 收到 ReqWorkConn 后，建立到 rfrps 的 work TCP + TLS 握手 + 发 StartWorkConn 的总时长不应超过 **8s**（rfrpc 内部控制，超时则放弃并记日志）——这是 rfrpc 侧本地截止。
- **二者关系**：8s 是 rfrpc 本地截止（建连+TLS+发帧），2s 余量留给网络往返（StartWorkConn 从 rfrpc 到 rfrps 的传输）与调度抖动。正常情况下 rfrpc 在 8s 内完成并发出 StartWorkConn，rfrps 在剩余 2s 内收到。若 rfrpc 超时放弃不发 StartWorkConn，rfrps 侧靠 10s 兜底关闭用户连接。

**rfrpc 本地服务连不上：**

- rfrpc 在工作连接上回连 `local_ip:local_port` 失败时，直接关闭该工作连接（TCP FIN）。
- rfrps 通过工作连接的 TCP FIN 感知，同步关闭对应的外部用户连接并记日志。
- 工作连接首帧后为透传字节流（见 6.1），不在其上发送 Close 帧；Close 仅用于控制连接（见 6.2.2）。

**ReqWorkConn 失败回拒：**

- rfrpc 收到 ReqWorkConn 但无法建立工作连接（如本地资源耗尽）→ rfrpc 不发 StartWorkConn，直接关闭该预备 work TCP（若已建）；rfrps 侧靠 10s 超时兜底，关闭用户连接。
- 首版不引入 WorkConnFailed 消息，靠超时兜底，简化协议。

**rfrps 监听 remote_port 失败：**

- rfrps 在 NewProxy 校验通过后尝试 bind `remote_port`（TCP/UDP 类型），可能因权限不足（特权端口 <1024 非 root）、端口已被系统其他进程占用等原因失败。
- bind 失败时，rfrps 返回 `NewProxyResp{ok:false, error:"internal error"}`，不区分具体原因（避免回显系统信息）；rfrps 侧日志记录详细 bind 错误。
- rfrpc 收到 `"internal error"` 按 6.6 error 表处理：记日志，不重试该 Proxy，不中断其他 Proxy。
- HTTP/HTTPS 类型不涉及独立端口 bind（走共享 vhost 监听），无此场景。

### 8.6 UDP 代理

UDP 无连接，工作连接模型（一个用户连接对应一条工作连接）不直接适用，需按"会话"映射：

- rfrps 在 `vhost_http_port` 之外的 UDP 端口监听（每个 UDP 类型 Proxy 对应 `remote_port`）。
- rfrps 收到首个来自某 `<client_addr:port>` 的 UDP 包时，按源地址（`<ip:port>`）映射为一个 UDP 会话，向 rfrpc 发 ReqWorkConn。
- rfrpc 收到 ReqWorkConn 后，建立到 rfrps 的 work TCP（同 TCP 代理），发 StartWorkConn，然后建立到 `local_ip:local_port` 的本地 UDP socket。
- rfrps 将该 `<ip:port>` 的后续 UDP 包通过工作连接透传到 rfrpc；rfrpc 转发到本地 UDP socket。
- 本地 UDP 响应包由 rfrpc 经工作连接回传 rfrps，rfrps 按会话映射发回原 `<ip:port>`。
- 会话超时：某 `<ip:port>` **60s** 无活动则清理会话与对应工作连接（固定，不可配）。
- **本地 UDP socket 生命周期**：rfrpc 侧为该会话建立的本地 UDP socket（连向 `local_ip:local_port`）**生命周期跟随工作连接**——工作连接关闭（TCP FIN 或 rfrps 主动断开）即立即关闭本地 UDP socket，释放端口资源。会话 60s 超时清理时，先关工作连接再关本地 socket。二者超时不一致时（如工作连接因 rfrps 侧异常提前 FIN，但会话未到 60s）以**工作连接关闭为准**，会话同步清理。
- 工作连接承载方式：与 TCP 代理不同，UDP 在工作连接上仍用 8.2 的"首帧后透传字节流"模型，但需在字节流中区分每个 UDP 包边界——首版约定每个 UDP 包前加 **4 字节大端长度前缀**（u32），即工作连接上 UDP 数据帧格式为 `Length(4) + Data`。rfrps 与 rfrpc 双向按此分帧。
- **单包大小限制**：单个 UDP 包最大 **65507 字节**（IPv4 UDP payload 上限，即 65535 − 20 IP头 − 8 UDP头）。Length 前缀值超过此上限视为协议错误，丢弃该帧并记日志（不断开工作连接，仅丢该包）。实际应用中 UDP 包通常 < 1472 字节（以太网 MTU 1500 − 20 − 8），超限包多为异常。

> UDP 会话映射复杂度较高，M4 实现前单独评审（见风险表）。

---

## 9. 配置设计

### 9.1 服务端 rfrp-server.toml

```toml
[server]
bind_addr = "0.0.0.0"           # 控制监听地址，默认 0.0.0.0
bind_port = 7000                # 控制端口，默认 7000
token = "shared-secret"          # 鉴权 token
tls_enable = true                # 控制链路 TLS 总开关（M1 测试期可关，生产必须开）
tls_cert = "./cert.pem"          # 控制链路证书
tls_key  = "./key.pem"
work_conn_tls = true             # 是否要求工作连接走 TLS（true 时拒绝明文工作连接）

# TLS 分层说明（避免混淆）：
#   1) 控制链路 TLS：tls_enable + tls_cert/tls_key，加密 rfrps↔rfrpc 控制连接
#   2) 工作连接 TLS：work_conn_tls，加密 rfrps↔rfrpc 数据面工作连接（复用 tls_cert/tls_key 证书）
#   3) HTTPS vhost TLS：vhost_tls_cert/vhost_tls_key，终止 rfrps↔外部用户的 HTTPS（与 1/2 独立）
#   注意：HTTPS vhost 的 TLS 终止在 rfrps（见 8.4），工作连接承载的是解密后的明文；
#          work_conn_tls 控制的是 rfrps↔rfrpc 之间，与 rfrps↔外部用户之间无关。

[dashboard]                      # 整段可选，省略则不启用 Dashboard
addr = "0.0.0.0:7500"           # 独立 HTTP 服务，不走控制协议，与 bind_port 无关
user = "admin"
password = "changeme"            # 最小长度 6（见 9.4 校验），生产环境务必更换强密码

[proxy]
# 允许客户端使用的公网端口范围
allow_ports = "6000-6100,7001-7010"
# vhost 监听（http/https 各自可选，省略即 None 不监听该类型，无隐式默认 80/443）
# 注意：80/443 为特权端口（<1024），Linux 下需 root 运行或通过 setcap 授权：
#   sudo setcap 'cap_net_bind_service=+ep' /path/to/rfrp
# 非特权场景可改用 8080/8443 等高位端口
vhost_http_port  = 80
vhost_https_port = 443
# HTTPS vhost 证书（可与控制链路证书不同）
vhost_tls_cert = "./vhost-cert.pem"
vhost_tls_key  = "./vhost-key.pem"
# 说明：allow_ports 仅约束 TCP/UDP 类型的 remote_port；
#        vhost_http_port / vhost_https_port 独立于 allow_ports，不受其限制（由 rfrps 配置直接指定）。
#        vhost 端口与 allow_ports 区间重叠不报错，但语义不同（vhost 为共享监听，allow_ports 为 per-proxy 独占端口）。

[log]                            # 整段可选，默认 level=info、output=stderr、format=text
level = "info"                   # trace|debug|info|warn|error
output = "stderr"                # stderr 或 file:/path/to.log（以 file: 前缀标识文件路径，路径不可含冒号歧义）
format = "text"                  # text|json
```

### 9.2 客户端 rfrp-client.toml

```toml
[client]
server_addr = "your.server.com"
server_port = 7000
token = "shared-secret"
tls_enable = true
tls_server_name = "your.server.com"
work_conn_tls = true              # 工作连接是否走 TLS，默认 true
run_id_file = ""                  # run_id 持久化路径，空表示默认 ~/.rfrp/run_id

[[proxy]]
name = "ssh"
type = "tcp"
local_ip = "127.0.0.1"            # 本地回连地址，默认 127.0.0.1
local_port = 22
remote_port = 6000
pool_size = 1                     # 工作连接池大小，默认 1；0 表示禁用预热、纯按需建立

[[proxy]]
name = "web"
type = "http"
local_ip = "127.0.0.1"
local_port = 8080
custom_domains = ["dev.example.com"]

[log]                            # 整段可选，默认 level=info、output=stderr、format=text
level = "info"
output = "stderr"
format = "text"
```

### 9.3 配置覆盖优先级

CLI 参数 > 配置文件 > 默认值。

> 首版不支持环境变量配置。如需通过环境变量注入（如容器场景），可用 shell 在启动前渲染配置文件或拼 CLI 参数。

### 9.4 配置校验

配置解析后，启动业务逻辑前进行校验，任一失败则启动中止并打印明确错误：

- **端口范围合法**：`bind_port`、`remote_port`、`vhost_*_port` 在 1–65535；`allow_ports` 字符串可解析为区间集合。
- **字段一致性**：客户端 `tls_enable = true` 时 `tls_server_name` 必填且非空（rustls 用于证书 SNI 与校验），缺省或空串启动报错；rfrps 的 `tls_cert` / `tls_key` 成对存在。
- **token 校验**：M3 起 `token` 必须非空（客户端与服务端均校验），空 token 启动报错；M1 阶段跳过校验（见 12 阶段 1 说明）。
- **文件存在性**：`tls_cert`、`tls_key`、`vhost_tls_cert`、`vhost_tls_key` 指向的文件存在且可读。
- **proxy 唯一性**：客户端配置内 `[[proxy]]` 的 `name` 不重复，`remote_port` 不重复（同一客户端内）。服务端另有全局唯一校验，见 6.6。
- **类型与字段匹配**：`type = http/https` 必须有 `custom_domains`；`type = tcp/udp` 必须有 `remote_port`。
- **local_ip 格式**：`local_ip` 省略时默认 `127.0.0.1`；提供时必须可解析为合法 IPv4/IPv6 地址（`std::net::IpAddr` 解析）。
- **pool_size 通用**：`pool_size` 对所有代理类型（tcp/udp/http/https）生效，省略默认 1。类型为 u32，≥0；0 表示禁用预热纯按需（见 8.2）；建议上限 16（超过记警告但不拒绝，防止资源耗尽）。
- **vhost 端口与 proxy 类型交叉**：`vhost_http_port` 配置但无 HTTP 类型 proxy、`vhost_https_port` 配置但无 HTTPS 类型 proxy——视为**配置冗余，不报错**（vhost 监听仍启动，只是无流量，方便后续动态添加 proxy）。反之，有 HTTP/HTTPS proxy 但未配对应 vhost 端口——启动**报错**（proxy 无法路由）。
- **dashboard 段校验**（仅服务端，`[dashboard]` 段存在时）：
  - `addr` 可解析为 `SocketAddr`（含端口）。
  - `user` 与 `password` 非空；`password` 最小长度 6（避免弱口令）。
  - `addr` 端口不得与 `bind_port` / `vhost_http_port` / `vhost_https_port` 重复。
  - 若 `addr` 绑定 `0.0.0.0`（非 127.0.0.1），启动日志输出安全警告（见风险表 Dashboard 明文 Basic Auth）。
  - `[dashboard]` 段整体可省略，省略则不启用 Dashboard。

### 9.5 `allow_ports` 格式

逗号分隔的端口区间列表，每段为单端口或闭区间，容忍空格：

```
"6000-6100,7001-7010"     # 两个区间
"6000, 6005-6010 ,7000"   # 混合单端口与区间，容忍空格
"6000"                    # 单端口
""                        # 空字符串表示不限制（允许任意 1-65535 端口，含特权端口）
```

解析规则：
- 以逗号分隔，每段 trim 空格。
- 单端口：`1-65535` 的整数。
- 区间：`start-end`，start ≤ end，两端均为合法端口。
- 非法格式（如 `abc`、`7000-`、`8000-7000`）启动时报错中止。

---

## 10. 鉴权与加密

### 10.1 鉴权

- **Token**：rfrpc 与 rfrps 配置同一 token，登录时随 `Login` 携带，rfrps 常量时间比对。首版为单一 token，所有 rfrpc 共用；按客户端区分 token 留待后续版本。
- **Dashboard**：Basic Auth。

### 10.2 加密与错误回显

- **控制链路**：默认 TLS（rustls），证书可为自签或 Let's Encrypt；`tls_enable = false` 时跳过（仅 M1 测试期，生产必须开）。
- **数据链路**：工作连接默认同样走 TLS（详见 6.5），可选 `work_conn_tls = false` 关闭。
- **错误回显策略**（区分两类）：
  - **协议层错误**（版本不匹配、帧格式错误）：rfrps 回显具体原因（如 `"version mismatch"`），便于 rfrpc 适配或报错退出。
  - **鉴权层错误**（token 错误/缺失）：rfrps 仅记录日志并关闭连接，**不回显**具体原因，避免泄露鉴权状态。

---

## 11. 功能特性清单（首版验收）

- [ ] `rfrp server` 监听控制端口，接受多客户端登录
- [ ] Token 鉴权 + TLS 控制链路
- [ ] TCP 代理：外部端口 → 本地 TCP 服务
- [ ] UDP 代理：外部 UDP 端口 → 本地 UDP 服务
- [ ] HTTP 代理：基于 Host 的 vhost 路由
- [ ] HTTPS 代理：基于 SNI 的 vhost 路由，vhost 证书加载正确
- [ ] 心跳保活 + 断线指数退避重连
- [ ] 重连后 Proxy 恢复（run_id 复用，端口冲突按 6.6 处理）
- [ ] 工作连接池预热（per-Proxy，pool_size 可配，池命中/补充流程 work_id=0 语义，pool_size=0 纯按需模式），降低首包延迟
- [x] 优雅退出（信号触发资源回收，在途连接 30s 超时强制关闭，无端口泄漏）
- [ ] 工作连接 TLS（可配置，服务端优先）
- [ ] Dashboard：客户端/代理列表、连接数、流量统计
- [ ] 结构化日志（tracing），按级别过滤，支持 stderr 与 file 输出（`output = "file:/path/to.log"`）
- [ ] 配置文件 + CLI 参数双入口：CLI 参数覆盖配置同名字段，`-c` 与字段参数可组合（见 7.3、9.3）
- [ ] 单二进制 CLI：`rfrp server` / `rfrp client`
- [ ] Linux (gnu/musl) + Windows (MinGW-w64) 双平台构建与产物

---

## 12. 实现阶段与里程碑

### 阶段 0：地基（M0）
- workspace 骨架
- `rfrp-common`：error、protocol（frame + msg）、config 解析、auth 原语
- `rfrp-bin`：clap 子命令骨架（server/client 占位）
- 配套：`.cargo/config.toml`、examples、toolchain-setup.sh、systemd unit
- 测试：protocol/config/auth 单测、CLI 测试、集成测试与基准骨架
- 交付：单测覆盖协议编解码；`rfrp server` / `rfrp client` 可启动占位（打印 "placeholder, not implemented" 后退出 0）

### 阶段 1：TCP 跑通（M1，MVP）
- `rfrps` control + listener + router（TCP）
- `rfrpc` control + workconn + 本地回连（TCP）
- 登录、NewProxy、ReqWorkConn、StartWorkConn、双向桥接
- 交付：两台机器间 SSH 透传成功

> **⚠️ M1 无鉴权无 TLS，仅供内网测试，禁止公网部署。M3 完成后方可公网使用。**
>
> **M1 token 行为**：M1 阶段 rfrps 跳过 token 校验（Login 携带 token 但不比对，直接 `ok=true`）；token 字段可空可省。M3 起 token 校验生效，空 token 启动报错（见 9.4）。

### 阶段 2：健壮性（M2）
- 心跳、断线重连（指数退避）、run_id 复用
- 优雅退出（tokio signal）、资源回收
- 工作连接池预热
- 交付：拔网线恢复测试通过

**M2 细节约定：**

- **工作连接池预热**：工作连接池是 **per-Proxy** 的（每个 `proxy_name` 独立维护一个池，不跨 Proxy 共享）。每个 Proxy 维持 `pool_size` 个空闲工作连接（默认 1，可配）。rfrpc 在 NewProxy 成功后立即预建；rfrps 每收到一个外部用户连接，若对应 Proxy 的池非空则取一个并要求 rfrpc 补充一个（发 `ReqWorkConn{work_id=0}`，见 8.2）；池空时退化为按需建立。对 HTTP/HTTPS 类型同样生效（rfrps 命中 vhost 后向对应 Proxy 的池请求工作连接，池不跨域名共享）。
- **重连后 NewProxy 顺序**：rfrpc 重连 Login 成功后，按配置文件中 `[[proxy]]` 的声明顺序**串行**发送 NewProxy，逐个等待 NewProxyResp。串行便于错误定位与端口冲突逐条处理；并行优化留待后续版本。
- **优雅退出在途连接处理（M2d）**：rfrps 收到退出信号（SIGTERM/SIGINT）后，经统一 `shutdown` 令牌（`tokio_util::sync::CancellationToken`）停止 accept 与新 rfrpc 登录；所有长连接任务（控制循环 `handle_control_login`、代理监听 `proxy_accept_loop`）监听同一令牌，取消即干净退出（底层 TCP 关闭等效于发送 `Close`）。已在途的用户桥接（短任务）在 `GRACEFUL_SHUTDOWN_TIMEOUT`（默认 30s，测试可覆盖）宽限期内自然结束，超时后随进程退出强制关闭，无端口/资源泄漏。rfrpc 收到控制连接 EOF 后由重连机制处理；若 rfrpc 自身收到退出信号，则停止重连、干净退出（不陷入无限重连）。令牌取消封装为 `Server::shutdown_token()` / `Client::shutdown_token()`，便于测试模拟信号。

### 阶段 3：安全（M3）
- TLS 控制链路（rustls）
- 工作连接 TLS（见 6.5，服务端优先，可配置）
- Token 鉴权
- 交付：无 token/错误 token 拒绝；控制链路与工作连接抓包均为密文（work_conn_tls=true 时）

### 阶段 4：UDP / HTTP / HTTPS（M4）

> **前置评审**：UDP 会话映射复杂度较高（见 §8.6、风险表），M4 启动编码前须先单独评审 UDP 会话映射方案（源地址映射表结构、超时清理、4 字节长度前缀分帧的编解码实现），确认方案可行后再进入编码。若评审判定 UDP 风险过大，可降级为首版仅交付 HTTP/HTTPS，UDP 推迟到 v0.2。

- UDP 代理（tokio UDP socket + 会话映射，方案经前置评审确认）
- HTTP vhost（hyper Host 路由）
- HTTPS vhost（rustls SNI 路由，复用 M3 的 rustls 基础）
- 交付：本地 web 服务通过域名访问

> M4 的 HTTPS vhost 依赖 M3 引入的 rustls，须先完成 M3。

### 阶段 5：Dashboard 与可观测（M5）
- Dashboard API + 简易 Web 页面（Basic Auth）
- 指标暴露（/metrics，Prometheus 格式）
- tracing 结构化日志（text/json），支持 stderr 与 file 输出
- 交付：面板可见实时连接与流量

### 阶段 6：打磨与发布（M6）
- 集成测试、性能基准（Criterion）
- Debian 上交叉编译三产物：linux-gnu / linux-musl / windows-gnu
- Release 产物打包（tar.gz / zip）、SHA256 校验和
- 自签证书一键脚本（`scripts/gen-self-signed-cert.sh`）
- README 与部署文档
- 交付：v0.1.0 发布

---

## 13. 项目目录结构（落地后）

详见 [第 7.1 节](#71-目录结构)。补充产物目录：

```
rfrp/
├── examples/                 # 示例配置
│   ├── rfrp-server.toml
│   └── rfrp-client.toml
├── tests/                    # workspace 级集成测试（按代理类型分文件，随里程碑逐步添加）
│   ├── tcp_proxy.rs          # M1：TCP 全链路
│   ├── reconnect.rs          # M2：心跳、断线重连、Proxy 恢复
│   ├── tls_auth.rs           # M3：TLS 控制链路 + token 鉴权
│   ├── udp_proxy.rs          # M4：UDP 全链路
│   └── vhost.rs              # M4：HTTP/HTTPS vhost 全链路
├── benches/                  # 性能基准
│   └── forward.rs
├── .cargo/
│   └── config.toml           # 交叉编译配置（musl-gcc、mingw-w64 链接器）
├── deploy/
│   └── systemd/              # systemd unit 文件
│       ├── rfrp-server.service
│       └── rfrp-client.service
└── scripts/
    ├── toolchain-setup.sh    # Debian 开发机一键安装工具链
    ├── release.sh            # 在 Linux 上交叉编译并打包三产物
    └── gen-self-signed-cert.sh  # 一键生成自签 TLS 证书（控制链路/vhost 通用）
```

**Linux 部署安装步骤（配合 systemd）：**

```bash
# 1. 创建专用用户与目录
sudo useradd -r -s /usr/sbin/nologin rfrp
sudo mkdir -p /etc/rfrp /var/lib/rfrp /var/log/rfrp
sudo chown rfrp:rfrp /var/lib/rfrp /var/log/rfrp

# 2. 安装二进制
sudo cp target/release/rfrp /usr/local/bin/rfrp

# 3. 安装配置（从 examples 复制并修改）
sudo cp examples/rfrp-server.toml /etc/rfrp/rfrp-server.toml
sudo chown root:rfrp /etc/rfrp/rfrp-server.toml
sudo chmod 640 /etc/rfrp/rfrp-server.toml

# 4. 安装 systemd unit
sudo cp deploy/systemd/rfrp-server.service /etc/systemd/system/

# 5. 启动
sudo systemctl daemon-reload
sudo systemctl enable --now rfrp-server
```

> systemd unit 中 `WorkingDirectory=/etc/rfrp`、配置路径 `/etc/rfrp/rfrp-*.toml`、`run_id_file` 建议设为 `/var/lib/rfrp/run_id`。examples 仅供模板参考，实际部署需修改 token、证书路径、`allow_ports` 等。

**client 部署类似**：替换步骤 3/4 为 `rfrp-client.toml` 与 `rfrp-client.service`，`run_id_file` 同样建议 `/var/lib/rfrp/run_id`。client 侧无需 `allow_ports` 与 vhost 配置。

**Windows 部署（rfrp.exe，无 systemd）：**

Windows 产物为 `rfrp.exe`，无 systemd，手动部署或用任务计划程序/服务包装器（如 nssm）注册为服务。目录约定：

```
C:\rfrp\
├── rfrp.exe                    # 二进制
├── rfrp-server.toml            # 配置（server 模式）或 rfrp-client.toml（client 模式）
└── cert.pem / key.pem          # TLS 证书（按需）
```

- 配置文件路径通过 `-c` 参数显式指定，无固定路径约定（推荐放 `C:\rfrp\`）。
- `run_id` 默认路径 `~/.rfrp/run_id`，Windows 下解析为 `%USERPROFILE%\.rfrp\run_id`。若用 nssm 以服务账户运行，注意该账户的 `%USERPROFILE%` 路径；建议显式配置 `run_id_file = "C:\\rfrp\\run_id"`。
- 日志输出建议 `output = "file:C:\\rfrp\\rfrp.log"`（路径用反斜杠转义或正斜杠）。
- 服务注册（nssm 示例）：
  ```cmd
  nssm install rfrp-server C:\rfrp\rfrp.exe "server -c C:\rfrp\rfrp-server.toml"
  nssm set rfrp-server AppDirectory C:\rfrp
  nssm start rfrp-server
  ```
- 文件权限：Windows 侧不强制 chmod，配置/密钥文件 ACL 由管理员手动设置（首版不处理，仅警告，见风险表）。
- 信号：Windows 下 Ctrl-C 触发优雅退出（`tokio::signal::ctrl_c`）；nssm 服务停止时也会发送该信号。

---

## 14. 测试计划

### 14.1 单元测试

| 模块 | 重点 | 里程碑 |
|------|------|--------|
| protocol | 帧编解码、边界长度、畸形帧拒绝、版本不匹配 | M0 |
| config | 缺字段、类型错误、端口范围解析、`custom_domains` 数组长度边界（0/1/16/17 个元素）、`local_ip` 格式校验 | M0 |
| auth | token 常量时间比对（相等/不等/长度差异） | M0 |
| crypto | 证书加载、控制链路与工作连接 TLS 配置构建 | M3 |
| proxy/tcp | 桥接缓冲、半关闭、大包分段、本地连接失败时工作连接关闭行为（rfrpc 侧 EOF 传播、rfrps 侧用户连接同步断开） | M1 |

### 14.2 集成测试

| 范围 | 里程碑 |
|------|--------|
| TCP 全链路（in-process server + client，真实 socket 模拟用户，断言数据往返一致） | M1 |
| 心跳、断线重连、Proxy 恢复（拔连接模拟断线） | M2 |
| 工作连接池：池命中、补充（ReqWorkConn work_id=0）、pool_size=0 纯按需对比、per-Proxy 隔离验证 | M2 |
| TLS 控制链路 + token 鉴权拒绝 | M3 |
| UDP / HTTP / HTTPS vhost 全链路 | M4 |
| Dashboard API + 指标暴露 | M5 |

### 14.3 性能测试

> 里程碑：M6（M0 预留 benches/ 骨架，M6 填充并执行）。

分两类，分别用不同工具：

**A. Criterion 微基准（`benches/`，纳入 `cargo bench`）**

- 帧编解码吞吐（帧/秒）。
- 双向桥接单连接吞吐（字节/秒，in-process loopback）。
- 配置解析耗时。

**B. 外部压测（手动执行，不进 cargo bench）**

- 端到端吞吐：`iperf3` over rfrp vs 直连基线，目标达到直连吞吐的 **80%+**。
- 并发连接数：用 `wrk` 或自写脚本打 10k 短连接，目标无错误、无明显延迟抖动。
- 内存占用：1k 并发连接下 RSS 目标 **< 100 MB**，10k 并发目标 **< 500 MB**。
- 转发延迟：单连接 TCP 透传 P99 延迟（相对直连基线的额外开销）目标 **< 1ms**（本地 loopback），跨网络场景 **< 5ms**（相对直连同链路的增量）。用 `iperf3 --latency` 或两端打时间戳的专用脚本测量，取 1000 样本的 P50/P99/P999。
- 心跳-响应往返：Heartbeat 发出到 HeartbeatResp 收到的 P99 目标 **< 100ms**（空闲控制连接，同机房）。

### 14.4 混沌测试

- rfrps/rfrpc 随机强杀（Linux `kill -9`；Windows 产物在 Windows 测试机上用 `taskkill /F` 手动验证），验证无端口泄漏、无僵尸会话。
- 弱网：Linux 用 `tc netem` 丢包/延迟；Windows 端用 Clumsy 工具模拟（手动）。
- Linux 由开发机脚本执行，Windows 在发布前于测试机跑一轮冒烟。

---

## 15. 风险与对策

| 风险 | 影响 | 对策 |
|------|------|------|
| 协议设计需演进 | 升级困难 | 帧内置 version 字段，握手协商版本 |
| 连接/资源泄漏 | 长跑 OOM | 所有连接用 RAII + 超时 + 定期泄漏检测 |
| TLS 证书管理繁琐 | 部署门槛 | 支持自签 + 指定 CA，文档给一键脚本 |
| UDP 会话映射复杂 | 实现延期 | M4 单独评审，必要时首版只交付 TCP/HTTP |
| 安全漏洞 | 被滥用为开放代理 | 强制鉴权、端口白名单、Dashboard 限频 |
| Windows (mingw64) 交叉编译兼容 | 链接失败/运行时缺 DLL | Debian 统一安装 mingw-w64；`.cargo/config.toml` 静态链接 winpthread；依赖尽量纯 Rust 避免 C 库 |
| 平台行为差异（信号/UDP/路径） | 跨平台 bug | 平台分支集中在 `util::platform`；Windows 产物在测试机手动验证 |
| Windows 运行时未自动化测试 | 回归风险 | 开发机只编译不跑 Windows；用 Windows 虚拟机/物理机做发布前冒烟 |
| musl 静态二进制 DNS 解析异常 | musl 产物域名解析失败 | musl resolver 不读 nsswitch.conf；测试覆盖域名解析场景；必要时文档建议 glibc 产物用于生产 |
| M1 阶段无鉴权无 TLS | 误部署到公网 | M1 仅供内网测试，文档与启动日志明确警告；M3 完成后才可公网部署 |
| Dashboard 明文 Basic Auth | 误暴露公网导致密码泄露 | Dashboard 不走 HTTPS，仅限内网/本地访问；启动日志提示绑定地址，若绑 0.0.0.0 则警告 |
| 心跳超时误判（网络抖动） | 短暂网络抖动导致 10s 超时，触发误断连与重连，影响在途连接 | 心跳间隔 30s + 超时 10s 已留余量；重连复用 run_id 恢复 Proxy，在途用户连接受 8.5 超时保护；弱网场景可考虑调大超时（首版固定不可配，后续版本可配） |

---

## 16. 后续演进（v0.2+）

- STCP / XTCP 点对点穿透
- 负载均衡（多 client 同名 Proxy）
- 插件机制（HTTP 改写、认证回调）
- 配置热更新与 API 下发
- QUIC 传输层（quinn）

---

*本计划为 v0.1 蓝图，实现过程中按里程碑迭代修订。*

---

## 17. 实施进度

### 17.1 里程碑状态

| 里程碑 | 内容 | 状态 |
|--------|------|------|
| **M0** | 地基：workspace + rfrp-common + rfrp-bin 占位 | ✅ **已完成**（2026-08-14） |
| **M1** | TCP 跑通：rfrps/rfrpc 控制+监听+桥接 | ✅ **已完成**（2026-08-14） |
| M2 | 健壮性：心跳/重连/run_id/优雅退出/连接池 | ✅ 完成 |
| M3 | 安全：TLS 控制链路 + 工作连接 TLS + token 鉴权 | ⬜ 未开始 |
| M4 | UDP / HTTP / HTTPS（vhost） | ⬜ 未开始 |
| M5 | Dashboard 与可观测（指标/结构化日志） | ⬜ 未开始 |
| M6 | 打磨与发布（交叉编译三产物/打包/文档） | ⬜ 未开始 |

### 17.2 M0 交付物核对（对照 §12）

| §12 M0 要求 | 落地情况 |
|------------|----------|
| workspace 骨架 | ✅ 根 `Cargo.toml`（resolver=2，workspace 依赖集中管理） |
| `rfrp-common`：error / protocol(frame+msg) / config / auth | ✅ `src/{error,constants}.rs`、`src/protocol/{frame,msg}.rs`、`src/config/{mod,server,client}.rs`、`src/auth/mod.rs`、`src/util/platform.rs` |
| `rfrp-bin`：clap 子命令骨架（server/client 占位） | ✅ `src/{main,cli,logging}.rs`，`rfrp server`/`rfrp client` 打印 placeholder 后退出 0 |
| 配套：`.cargo/config.toml`、examples、toolchain-setup.sh、systemd | ✅ `.cargo/config.toml`、`examples/rfrp-{server,client}.toml`、`scripts/toolchain-setup.sh`、`deploy/systemd/rfrp-{server,client}.service` |
| 测试：protocol / config / auth 单测 | ✅ 28 个单测，覆盖帧编解码/截断/版本不匹配/超大帧、9 类消息 round-trip、端口范围解析、custom_domains 16/17 边界、dashboard 校验、token 常量时间比对 |
| 集成测试与基准骨架 | ✅ `crates/rfrp-common/benches/forward.rs`（Criterion 在 M6 填充） |
| 交付：单测覆盖协议编解码；`rfrp server`/`client` 可启动占位退出 0 | ✅ 实测通过 |

### 17.3 验证结果（M0 提交时）

```text
$ cargo build                 # Finished, 0 error
$ cargo test                  # 28 passed; 0 failed
$ cargo clippy --all-targets  # 无警告
$ cargo fmt --check           # FMT CLEAN
$ cargo run -q -- server -c examples/rfrp-server.toml   # 加载配置并打印 placeholder，exit 0
$ cargo run -q -- client -c examples/rfrp-client.toml   # 同上，exit 0
$ cargo run -q -- --help      # 子命令 server/client + 全局日志参数正常
```

### 17.4 已创建文件清单

```
rfrpv2/
├── Cargo.toml                      # workspace 根
├── .cargo/config.toml              # musl / windows-gnu 交叉编译
├── examples/
│   ├── rfrp-server.toml
│   └── rfrp-client.toml
├── scripts/toolchain-setup.sh
├── deploy/systemd/
│   ├── rfrp-server.service
│   └── rfrp-client.service
└── crates/
    ├── rfrp-common/
    │   ├── Cargo.toml
    │   ├── benches/forward.rs
    │   └── src/
    │       ├── lib.rs  constants.rs  error.rs  util/mod.rs  util/platform.rs
    │       ├── auth/mod.rs
    │       ├── protocol/{mod,frame,msg}.rs
    │       └── config/{mod,server,client}.rs
    ├── rfrps/
    │   ├── Cargo.toml
    │   └── src/{lib,server,control,listener,work,bridge}.rs
    ├── rfrpc/
    │   ├── Cargo.toml
    │   ├── tests/tcp_proxy.rs
    │   └── src/{lib,client,control,workconn,bridge}.rs
    └── rfrp-bin/
        ├── Cargo.toml
        └── src/{main,cli,logging}.rs
```

### 17.5 关键实现说明（M0）

- **帧编解码** `FrameCodec`：6 字节头（Version+MsgType+Length）、版本校验、单帧 16 MiB 上限、`Length=0` 合法、半包/粘包由 `Framed` 自动处理。
- **消息层** `Message`：`to_frame()`/`from_frame()` 按 `MsgType` 分发；`LoginResp.ok=false` 时 `session_id`/`work_conn_tls` 跳过序列化（见 §6.2.3）。
- **配置校验**：服务端 `[proxy]` 单表与客户端 `[[proxy]]` 数组严格区分；`allow_ports` 空=不限制；`custom_domains` ≤16；类型/字段匹配；dashboard 密码≥6。
- **鉴权原语** `auth::verify_token`：长度不同直接拒，等长按字节恒定时间异或，避免时序侧信道。

### 17.6 下一步

进入 **M2（健壮性）**：心跳/重连/run_id 复用/优雅退出/连接池（见 §12 阶段 2）。

---

### 17.7 M1 交付物核对（对照 §12 阶段 1）

| §12 M1 要求 | 落地情况 |
|------------|----------|
| `rfrps` 控制连接 + Listener + Router（TCP） | ✅ `crates/rfrps/{server,control,listener,work,bridge}.rs`：accept 后按首帧区分控制/工作连接；NewProxy 注册公网监听（TCP）；ReqWorkConn→工作连接桥接 |
| `rfrpc` 控制连接 + 工作连接 + 本地回连（TCP） | ✅ `crates/rfrpc/{client,control,workconn,bridge}.rs`：Login→注册代理→长驻控制循环；收到 ReqWorkConn 建工作连接并回连本地服务 |
| 登录 / NewProxy / ReqWorkConn / StartWorkConn / 双向桥接 | ✅ 全链路实现；桥接复用 `tokio::io::copy_bidirectional` |
| 交付：两台机器间 SSH 透传成功 | ✅ 集成测试 `crates/rfrpc/tests/tcp_proxy.rs` 进程内起 rfrps+rfrpc+本地 echo，经服务端 remote_port 完成小包/大包/多轮往返断言（等价于 SSH 透传的数据通路） |
| M1 无鉴权无 TLS（仅内网测试，见 §15 风险） | ✅ 控制/工作连接均为明文 TCP；Login `token` 不校验；启动走 M1 内部路径 |

### 17.8 新增 crate 与模块

| Crate | 模块 | 职责 |
|-------|------|------|
| `rfrps` | `server` | accept 循环、共享状态（`ServerState`：work_id 分配器 + 待处理工作连接表）、`Server::new/run` |
|  | `control` | 控制连接：Login 响应、NewProxy 注册、Heartbeat；Session 管理 |
|  | `listener` | 每个 `remote_port` 起 accept 循环，分配 work_id、登记待处理项、发 ReqWorkConn |
|  | `work` | 工作连接：按 work_id 取用户连接并桥接 |
|  | `bridge` | 双向字节泵 |
| `rfrpc` | `client` | 连接、Login、串行注册代理（await NewProxyResp）、长驻控制循环 |
|  | `control` | 控制连接：NewProxyResp 路由、ReqWorkConn→派发工作连接、Heartbeat |
|  | `workconn` | 建工作连接（发 StartWorkConn）、回连本地服务、桥接 |
|  | `bridge` | 双向字节泵 |

### 17.9 关键实现说明（M1）

- **连接分派**：服务端 `bind_port` 同时承载控制连接与工作连接（见 §4.2）。accept 后 `read_one_frame` 读首帧——`Login`→控制连接，`StartWorkConn`→工作连接，二者后续均为透传字节。
- **首帧边界**：`read_one_frame` 读完整首帧后**归还剩余 `TcpStream`**，桥接时不丢字节（控制协议帧与工作连接透传字节严格分界）。
- **收发并发**：控制连接用 `tokio::io::split` 拆读写半边，`FramedRead` 读 + 独立写任务（mpsc 转发），满足 §6.1 收发并发；监听任务经 `session.tx` 投递 ReqWorkConn。
- **work_id** 由服务端 `AtomicU64` 全局自增（≥1，0 保留），关联「用户连接 ↔ 工作连接」（见 §6.2.1）。
- **超时兜底**：用户连接等候工作连接超 `WORK_CONN_TIMEOUT_RFRPS`（默认 10s）则关闭，防止悬挂（§8.5）。
- **本地连不上**：客户端回连本地失败即关闭工作连接（TCP FIN），服务端用户侧同步断开（§8.2/§8.5）。
- **注册时序修正**：必须先 spawn 控制循环再发 NewProxy 并 await 响应，否则控制循环尚未运行、NewProxyResp 无人接收而超时（§8.1 串行注册）。
- **新增依赖**：`futures`（仅取 `StreamExt`/`SinkExt`）用于 split 后的 `FramedRead.next()` 与 `FramedWrite.send()`。
- **配置导出**：`LogSection` 服务端/客户端共用，已在 `rfrp_common::config` 以 `LogSection`/`ClientLogSection` 重新导出。

### 17.10 M1 验证结果

```text
$ cargo build                 # Finished, 0 error
$ cargo test                  # 30 单测 + 2 集成测试 passed; 0 failed
$ cargo clippy --all-targets  # 无警告
$ cargo fmt --check           # FMT CLEAN
```

集成测试覆盖：`tcp_proxy_roundtrip`（小包/64KiB 大包/5 轮往返）与 `tcp_proxy_rejects_unregistered_port`（未注册端口立即关闭/不可达）。

### 17.11 下一步

进入 **M2（健壮性）**：心跳/重连/run_id 复用/优雅退出/连接池（见 §12 阶段 2）。

---

### 17.12 M1 后整理：测试覆盖分析与工程化优化

> 本轮在 M1 交付基础上做了代码整理、测试充分性分析与工程化加固（未进入 M2）。

#### 17.12.1 测试覆盖分析

**现有覆盖（共 71 项）**

| 层 | 文件 | 数量 | 覆盖点 |
|----|------|------|--------|
| 协议/配置 | rfrp-common 单测 | 42 | 帧编解码、半包/截断/版本不匹配/超大帧、`read_one_frame` 首帧截断报错、9 类消息 round-trip、`from_frame` 未知 msg_type / 非法 JSON payload 报错、`ProxyType` 未知变体反序列化报错、LoginResp 条件序列化、端口范围解析、custom_domains 16/17 边界、dashboard 校验、token 常量时间比对；**配置解析**：server/client 示例文件完整结构断言、`[[proxy]]` 不被静默丢弃、非法 TOML / 字段类型错误 / 未知字段负路径 |
| 服务端 | rfrps 单测 | 13 | `bridge` 双向转发（duplex）、`next_work_id` 单调、NewProxy 拒绝（非 TCP / 缺 remote_port / 端口不允许 / 重名 / 端口被占用）、控制循环分支（newproxy→resp / heartbeat→resp / close→退出 / 心跳超时断开 / 非 Login 首帧报错 / 同名 run_id 重连替换旧会话 / 协议版本不匹配拒绝）、`handle_work_connection` 未知 work_id 安全关闭、工作连接池预热（work_id=0 入池 / 池命中桥接+补充） |
| 客户端 | rfrpc 单测 | 8 | `new_proxy_from_config` 字段映射、`run_id` 持久化复用、`control_loop` 分支（NewProxyResp 路由 / heartbeat 应答 / ReqWorkConn 派生且不退出 / close 退出 / LoginResp 路由）、`handle_work_conn` 未知 proxy 安全返回 |
| 集成 | rfrpc/tests | 6 | 真实示例配置全链路（example_smoke）、小包/64KiB/多轮往返、20 并发用户、多代理、未注册端口立即关闭、致命登录失败（auth failed）不无限重连即退出 |
| 集成 | rfrp-common/tests | 2 | `examples/rfrp-{server,client}.toml` 契约测试：断言解析后完整结构（含 `[[proxy]]` 数组、各段字段）|

> 注：集成测试现覆盖 `rfrpc/tests`（6）与 `rfrp-common/tests`（2）。`example_smoke` 加载真实示例跑 server+client；`config_files` 直接断言示例文件解析结构，防止 serde 字段名/键不匹配导致**静默丢弃**（见 §17.12.3/§17.12.4）。

**缺口与风险（截至 M2）**

- 控制循环分支（Heartbeat 响应、Close 清理、ReqWorkConn 派发、NewProxyResp 路由、LoginResp 路由、run_id 去重）**已在 M2a/M2b 补齐独立单测**（duplex 内存双工驱动，无需真实端口），§17.12.1 原缺口已关闭。
- 协议层边界（`from_frame` 未知 msg_type / 非法 JSON、未知 `ProxyType`、版本不匹配拒绝）**已补齐**（M2 整理），使致命登录路径端到端可达。
- 工作连接负路径：未知 proxy_name / 未知 work_id 已有单测；**本地服务不可达**仍仅集成 happy path 间接覆盖（直接断言需真实 server+本地监听，成本高；该路径逻辑已简单——`warn` 后关闭工作连接，§8.2/§8.5），后续可借 M2c 连接池专项测试覆盖。
- 超时/资源泄漏：待处理项清理已有单测覆盖（M2a 心跳超时）；优雅退出令牌机制与端到端退出已有单测（M2d：服务端/客户端 `control_loop_exits_on_shutdown` + 集成 `graceful_shutdown`）；真实 SIGTERM 信号路径与混沌测试留待 §14.4。
- 配置层「非法 TOML / 字段类型错误 / 未知字段」反例**已补齐**（见 §17.12.4）：配合 `deny_unknown_fields`，TOML 键拼写/重命名错误显式失败而非静默忽略。
- TLS/鉴权路径在 M1 按设计跳过，不在测试范围。

**结论**：M1 的 TCP 数据通路与关键拒绝路径已较充分；控制循环内部分支、工作连接负路径、资源清理是 M2 重点补强对象。

#### 17.12.2 本轮工程化优化清单

- **清理**：删除未使用的 `frame::framed_split` 及其导入；`bridge` 泛型化为 `AsyncRead + AsyncWrite + Unpin`，可复用/可测（见 §13）。
- **新增单测**：rfrps 6 项（注册拒绝 + work_id）、rfrpc 1 项（配置映射）、bridge 1 项（双向转发）、集成 2 项（并发用户、多代理）。
- **工具链**：`.gitignore`、`rustfmt.toml`(max_width=100)、`Makefile`(`fmt`/`fmt-check`/`clippy`/`build`/`test`/`ci`)、`.github/workflows/ci.yml`。
- **CI（§11）**：`fmt --check` + `clippy --all-targets -- -D warnings` + `build` + `test`，并加 musl / windows-gnu 交叉编译验证（三产物，§3.3.2）。
- **质量门等价**：本地 `make ci` 与 CI 一致；`clippy` 零容忍警告。

新增/调整文件：`rustfmt.toml`、`Makefile`、`.gitignore`、`.github/workflows/ci.yml`、`crates/rfrps/src/bridge.rs`（泛型 + 测试）、`crates/rfrpc/src/bridge.rs`（泛型）、`crates/rfrps/src/listener.rs`（单测）、`crates/rfrpc/src/client.rs`（单测）、`crates/rfrpc/tests/tcp_proxy.rs`（重构 + 2 集成测试）、`crates/rfrpc/tests/example_smoke.rs`（真实示例配置回归）、`crates/rfrp-common/tests/config_files.rs`（示例文件契约测试）、`crates/rfrp-common/src/config/{client,server}.rs`（deny_unknown_fields + 配置正/负例单测）、`crates/rfrp-common/src/protocol/frame.rs`（删 `framed_split`）。

#### 17.12.4 测试方法论改进（针对静默丢弃类 bug）

M1 启动「无输出」的根因是**配置解析静默丢弃 `[[proxy]]`**，而原测试只检查「解析是否成功」（`validate()` 在 0 代理时恒通过），故漏检。据此改进测试方法，防止同类问题复发：

1. **断言解析*内容*，而非仅「解析成功」**：配置单测必须断言 `proxies.len()`、各代理 `name/type/port`、`[client]/[server]/[dashboard]/[proxy]` 各段关键字段。仅 `from_str().is_ok()` 不足以证明字段被正确映射。
2. **真实示例文件契约测试**（`crates/rfrp-common/tests/config_files.rs`）：直接 `load_*` 仓库自带的 `examples/*.toml` 并断言完整结构。示例文件即「配置契约」，任何字段重命名/键不匹配都会在此暴露，且与产品用法同步演进。
3. **严格反序列化**（`#[serde(deny_unknown_fields)]` 于所有 config 结构体）：TOML 键拼写错误或重命名会显式报错（而非被 serde 默认忽略），把「静默丢弃」转为「启动即失败」。
4. **负路径测试**：非法 TOML、字段类型错误（`u16` 给字符串）、未知字段，均断言 `from_str` 返回 `Err`。
5. **专项回归测试** `proxy_array_not_silently_dropped`：独立验证 `[[proxy]]` 数组被解析且内容正确，与示例文件解耦，避免示例改动掩盖问题。

> 原则：对「结构化配置 / 协议解析」这类「字段名即契约」的场景，测试必须同时覆盖**正向内容断言**与**负向解析失败**，并优先用真实文件做端到端契约校验。

#### 17.12.3 已修复：客户端 `[[proxy]]` 静默丢弃（M1 启动无输出根因）

- **现象**：`cargo run -- client -c examples/rfrp-client.toml` 连接成功后**没有任何代理注册日志**，客户端看似「无输出」。
- **根因**：`ClientConfig` 的代理数组字段命名为 `proxies`，而 TOML 用 `[[proxy]]`（键为 `proxy`）。serde 默认忽略未知键，导致所有代理条目被静默丢弃（`proxies.len()==0`），控制循环无代理可注册 → 静默空转。单测 `full_client_config_validates` 此前只调 `validate()`（0 代理时恒通过），未捕获该回归。
- **修复**：`crates/rfrp-common/src/config/client.rs` 给 `proxies` 字段加 `#[serde(rename = "proxy")]`，与 `[[proxy]]` 对齐；并加固单测断言 `proxies.len()==2`，`example_smoke` 同时断言。
- **附带可观测性增强**（直接缓解「无输出」困惑）：控制/工作连接建立与代理注册/拒绝均补 `INFO/WARN` 日志（`connected to server` / `control connection established` / `registering proxies` / `received NewProxy` / `proxy registered (tcp)` / `proxy registered` / `proxy registration rejected`）。

#### 17.13 M2 推进（健壮性，已完成）

M2 = 阶段 2：心跳保活、断线重连（指数退避）、run_id 复用、优雅退出、工作连接池预热（§12 / §8.3 / §14.4）。为控制步幅与回归风险，拆分为可独立测试的子任务：

| 子任务 | 内容 | 状态 |
|----|----|----|
| M2a | 控制连接心跳保活 + 控制循环分支单测 | 完成 |
| M2b | 客户端断线检测 + 指数退避重连 + run_id 复用（服务端按 run_id 清理旧 Session 后恢复 Proxy） | 完成 |
| M2c | 工作连接池预热（per-Proxy，pool_size 可配，池命中/补充，work_id=0 语义） | 完成 |
| M2d | 优雅退出（tokio signal -> 资源回收，在途连接 GRACEFUL_SHUTDOWN_TIMEOUT 强制关闭） | 完成 |

**M2a 完成内容**
- 服务端控制连接主动发送 Heartbeat（每 HEARTBEAT_INTERVAL=30s），并通过 Notify + last_resp 时间戳实现超时断开（HEARTBEAT_TIMEOUT=10s 内未收到 HeartbeatResp 即清理 Session 并中止所有代理监听）。客户端维持对 Heartbeat 的 HeartbeatResp 应答（§8.3 服务端->客户端方向已生效；客户端->服务端主动心跳与重连在 M2b 补齐）。
- 控制循环泛型化（handle_control_login<S> / control_loop<S>，S: AsyncRead+AsyncWrite+Unpin+Send+'static）：既服务生产 TcpStream，也便于用内存双工流做单测。
- 补齐控制循环分支单测（§17.12.1 缺口）：服务端 newproxy->resp / heartbeat->resp / close->退出 / heartbeat 超时断开 / 非 Login 首帧报错；客户端 NewProxyResp->oneshot 路由 / heartbeat->应答 / ReqWorkConn->派生任务且不退出 / close->退出。全部经 tokio::io::duplex 内存双工驱动，无需真实端口。

**M2b 完成内容**
- 客户端长驻重连循环：`Client::run` 拆出 `connect_once`，按指数退避（RECONNECT_BACKOFF_INITIAL=1s 翻倍至 RECONNECT_BACKOFF_MAX=30s）在网络瞬断后重连；正常断开（Close）同样重连。
- run_id 复用：首次生成 `uuid` 并持久化到 `run_id_file`（默认 `$HOME/.rfrp/run_id`，Unix 0600）；重启复用同一 run_id，服务端据此恢复代理（§6.6 / §8.3）。
- 致命错误判定：`control_loop` 路由 `LoginResp` 到 `state.login_tx`；`connect_once` 据此区分——`auth failed` / `version mismatch` 视为致命，直接退出不重连；其它（端口占用等运行时拒绝 / 超时 / 网络错误）可恢复，重连。
- 服务端 run_id 去重：`ServerState.sessions` 维护 `run_id -> Session` 注册表；同名 run_id 新登录触发 `cleanup` 清理旧会话（中止其代理监听、清理 pending、经 `Session.stop: Notify` 唤醒旧控制循环退出），再注册新会话。会话结束（正常或心跳超时）仅当注册表内仍为自身条目时才移除，避免误删重连后的新会话。
- 新增单测：服务端「同名 run_id 重连替换旧会话」（旧控制循环被 stop 通知退出、新会话存活）；客户端 `LoginResp` 路由到 `login_tx`；`run_id` 持久化复用（临时文件）。

- 服务端加固：`handle_control_login` 现校验协议版本，不匹配直接回 `LoginResp{ok=false, "version mismatch"}` 并结束会话（客户端据此判定致命、不重连，§6.6）——此前仅客户端侧声明该致命路径，现已端到端可达（`login_version_mismatch_rejected`）。

**M2c 完成内容**
- 工作连接池预热（§8.2）：客户端注册代理后按 `pool_size`（>0）预建 N 条工作连接（StartWorkConn{work_id=0}），服务端归入 `Session.pools[proxy_name]` 空闲池；用户连接优先命中池直接桥接，并回 `ReqWorkConn{work_id=0}` 触发客户端补充。
- `handle_work_connection` 增加 `work_id==0` 分支（预热池连接入池，不查 pending）；`proxy_accept_loop` 增加池命中路径（命中即桥接+补充，未命中退回按需 work_id≥1）。
- `Session` 增加 `pools` 字段，断开/重连清理时一并关闭池内连接；`handle_work_connection` 经 `find_session_by_proxy` 按 proxy_name 定位所属会话池。
- 新增单测：服务端 `work_id=0` 预热连接入池（`pooled_work_connection_registered`）；集成 `tcp_proxy_pool_size_two` 多次往返命中预热池并被补充。

**M2d 完成内容**
- 优雅退出（§14.4 / §12.2）：`ServerState` 增加 `shutdown: CancellationToken`；`Server::run` 在 accept 循环中经 `tokio::select!` 监听该令牌，收到信号（Ctrl+C / SIGTERM，由 `spawn_signal_watcher` 触发）即停止接收新连接；随后在宽限期（默认 `GRACEFUL_SHUTDOWN_TIMEOUT=30s`，测试经 `Server::with_grace` 覆盖）内等待在途连接自然结束，超时后返回。长连接任务（控制循环 `handle_control_login`、代理监听 `proxy_accept_loop`）均监听同一令牌，取消即干净退出（底层 TCP 关闭等效于发送 `Close`），并由 `cleanup` 中止代理监听、清理 pending 与预热池。
- 客户端侧 `Client` 增加 `shutdown` 令牌；`Client::run` 在重连循环顶部检查令牌，`connect_once` 将令牌透传 `control_loop`（控制循环监听令牌即退出，`ctrl.await` 随之结束、run 干净退出）。收到 Ctrl+C / SIGTERM 时停止重连并退出，而非无限重连；仅致命 Login 失败仍按原路径返回错误。
- 令牌可被外部/测试触发：`Server::shutdown_token()` / `Client::shutdown_token()` 返回共享令牌 clone；集成测试直接 `cancel()` 模拟信号，无需真实 OS 信号。
- 新增测试：服务端/客户端各 `control_loop_exits_on_shutdown` 单测（令牌取消 -> 控制循环退出）；集成 `rfrps/tests/graceful_shutdown.rs::server_run_returns_after_shutdown_token`（run 在令牌取消后返回）；`rfrpc/tests/graceful_shutdown.rs`（client_run_exits_on_shutdown_without_infinite_reconnect + server_and_client_exit_cleanly_on_shutdown 端到端二者均干净退出）。

**测试计数**：全量 73 -> 78（rfrpc lib 8->9，rfrps lib 14->15，新增集成 rfrpc/tests/graceful_shutdown.rs 2、rfrps/tests/graceful_shutdown.rs 1）。
