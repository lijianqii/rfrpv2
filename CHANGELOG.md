# Changelog

本项目遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/) 与
[语义化版本](https://semver.org/lang/zh-CN/)。
版本号见 `Cargo.toml`；发布产物由 `make release` 生成。

## [Unreleased]

### Added

- **协议错误码**：`NewProxyResp.error` 统一为稳定错误码
  （`invalid type` / `invalid field` / `proxy_name exists` / `port not allowed` /
  `port occupied` / `domain conflict` / `internal error`，见 DESIGN §6.6），
  客户端据此分类处理；服务端日志记录失败详情（端口、域名占用者等）。
- **代理注册自动重试**：`port occupied` / `domain conflict` 等运行时冲突由客户端后台
  退避重试（2s→30s，约 2 分钟，覆盖旧会话释放端口的窗口），不再出现"连上但代理不可用"。
- **服务端指标补全**：`/metrics` 新增 `rfrp_sessions`、`rfrp_proxies`、`rfrp_pending_work`、
  `rfrp_udp_sessions`、`rfrp_pooled_work_conns`、`rfrp_uptime_seconds`；Dashboard 状态页显示
  版本/uptime/计数并 5s 自动刷新。
- **客户端状态端点（可选）**：`[client] status_addr` 启用 `/`、`/api/status`、`/metrics`，
  含连接状态、重连次数、工作连接与注册失败计数等指标。
- **启动日志摘要**：server/client 启动时打印版本与关键配置（bind、TLS、代理清单、
  allow_ports、dashboard 等；不打印 token）。
- **控制链路 RTT 指标**：由心跳 `ts` 回传计算，暴露 `rfrp_rtt_ms`（服务端）与
  `rfrp_client_rtt_ms`（客户端），并纳入各自 `/api/status`。
- **TLS 1.3 会话恢复**：服务端启用 rustls session ticket（默认不产生票据 → 无法恢复），
  跨网场景下重复握手从 2 RTT 降到 1 RTT（对 `work_conn_tls` + `pool_size = 0` 的
  SSH 场景收益明显）。
- **发布包补全**：Linux tar 与 Windows zip 现包含配置模板、包内 README（快速开始 +
  systemd 安装 + 校验）、LICENSE 与 systemd unit（Linux）。
- 极简 HTTP 工具下沉到 `rfrp-common::util::http`，Dashboard 与客户端状态端点共用。

### Fixed

- **客户端静默失联后无法重连**：控制连接半开（对端进程挂起、NAT/防火墙静默丢弃，无
  FIN/RST）时客户端会永久卡住。新增客户端侧心跳看门狗（30s 发送、10s 超时）与控制连接
  建连超时（10s）。
- **池中死连接导致用户连接被重置**：sshd/RDP 等服务踢除空闲预连接后，池中死连接会让
  用户连接立即失败。服务端出池前非阻塞探活，死连接丢弃并回退按需建立，意外数据
  （如 SSH banner）经 `PrependStream` 回灌不丢字节。
- **帧解码 DoS 隐患**：长度上限校验提前到等待 payload 之前，避免恶意头声称超大长度时
  缓冲区无界增长。
- **控制面背压**：关键消息（登录响应、注册响应、按需 ReqWorkConn）带超时发送，非关键
  消息（心跳响应、池补充）改用 `try_send`，控制写任务加 5s 超时，消除背压死锁。
- 连接计数改为原子检查+递增，避免并发尖峰突破 `max_active`。

### Changed

- **数据面桥接缓冲 8 KiB → 32 KiB**：大流量吞吐 +12%（loopback 实测），每连接内存
  约 55–145 KiB（视缓冲使用量）。
- **流量计数批量化**：本地累计、每 256 KiB / 1s / 连接关闭时写入全局计数器，消除高并发
  下多核争用同一 cache line。
- **UDP 下行帧缓冲复用**，高频转发下每包减少一次堆分配。
- 客户端 `pool_size = 1` 现在对 SSH 等有状态服务安全（见 Fixed 第二条），
  原"SSH 建议 `pool_size = 0`"的限制解除。
- README 排障补充：空闲会话保活（SSH `ServerAliveInterval`）、RDP UDP 传输可选方案、
  logrotate 示例、高 BDP 链路（BBR）建议。
- 模块与测试组织整理：`control/mod.rs` → `control.rs`，大模块测试拆到 `<module>/tests.rs`，
  `ServerState`/`PendingWork` 统一从 `state` 导入；四个 crate 补充 `description`。

### Security

- Windows 二进制嵌入 PE 版本信息、应用清单（asInvoker）与图标，降低杀毒软件启发式误报，
  详见 [docs/WINDOWS_ANTIVIRUS.md](docs/WINDOWS_ANTIVIRUS.md)。

## [0.1.0]

首个版本，覆盖 DESIGN 里程碑 M0–M6。

### Added

- 单二进制 CLI：`rfrp server` / `rfrp client`，CLI 参数覆盖配置文件。
- 协议：帧编解码（版本/类型/长度/payload）、9 种控制消息、JSON payload、协议版本协商。
- 鉴权与加密：token 鉴权（常量时间比对）、控制链路 TLS、工作连接 TLS（服务端优先）。
- 代理类型：TCP、UDP（长度前缀分帧 + 会话表）、HTTP vhost（Host 路由）、HTTPS vhost（SNI 路由）。
- 可靠性：心跳保活与超时断连、指数退避重连、`run_id` 复用恢复代理、工作连接池预热
  （`pool_size`，池命中/补充语义 `work_id=0`）、优雅退出（30s 宽限期）。
- 运维：Dashboard（Basic Auth + `/api/status` + `/metrics`）、Prometheus 指标、
  结构化日志（text/json，stderr/file）、systemd unit 示例、交叉编译发布脚本。
- 平台：Linux（gnu/musl）、Windows（MinGW-w64）构建产物；`TCP_NODELAY` 全链路启用。

[Unreleased]: https://github.com/lijianqii/rfrpv2/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/lijianqii/rfrpv2/releases/tag/v0.1.0
