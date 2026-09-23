# Changelog

本项目遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/) 与
[语义化版本](https://semver.org/lang/zh-CN/)。
版本号见 `Cargo.toml`；发布产物由 `make release` 生成。

## [Unreleased]

### Added

- **`--check` 配置校验模式**：`rfrp server -c x.toml --check` / `rfrp client -c x.toml --check`
  只加载、覆盖、校验配置并打印生效摘要（token 只显示"已设置 + 长度"，不打印明文），
  不监听端口、不建立连接，便于部署前与 CI 校验。
- **地址支持域名**：`server_addr`、`bind_addr`、`local_ip` 以及 `--server` / `--bind` 均支持
  域名（`frp.example.com`、`localhost`）与 `[IPv6]` 字面量。客户端每次重连都会重新解析
  `server_addr`，可跟上 DNS 变更；本地服务回连改用 `(host, port)` 形式，顺带修正了
  IPv6 字面量拼接成 `::1:22` 的隐患。
- **`rfrp client status`**：查询本地 `[client].status_addr` 端点并打印 `/api/status` 的 JSON，
  不必开浏览器；未启用状态端点时给出配置提示。
- **`rfrp completions <shell>`**：生成 bash / zsh / fish / PowerShell / elvish 补全脚本。
- **`[server].grace_secs`**：优雅退出宽限期从"只能 CLI 覆盖"变为可写入配置（默认 30，上限 3600），
  与其它运行参数一致。
- **看板展示代理细节**：代理卡片新增公网端口 / 域名（此前只有名字与类型，排障要跳到客户端页面）。

### Changed

- **并发控制会话上限**：新增全局 `MAX_SESSIONS`(1024) 与单来源 IP `MAX_SESSIONS_PER_IP`(256)。
  登录限速只统计失败次数，此前持有效 token 的客户端可用随机 `run_id` 无限建立控制会话
  （每个都会注册代理、占用 fd 与内存）；同一 `run_id` 的重连替换不计新增。
- **测试构造器去重**：`Session` / `ClientState` / `ProxyEntry` 的测试构造收敛到各自 crate 的
  `#[cfg(test)]` 助手（`test_session` / `test_state` / `test_entry`），后续增字段只需改一处。
- **清理陈旧的里程碑注释**：`auth` / `control` / 测试文件头部等处的 `M1`–`M5` 标记改为
  描述性文字（DESIGN 中的里程碑记录保留）。
- **帧解码属性测试**：新增 proptest 覆盖"任意字节流不 panic""编解码往返""超长 length 与
  版本不符先于 payload 被拒"，强化协议解析这一攻击面的保障。
- **未提供 `-c` 时返回非零退出码**：此前打印一句提示后返回 0，脚本 / 服务管理器会误判为
  "启动成功"。现改为打印提示（含示例路径与 `--help` 指引）并返回失败码。
- **失败日志补充上下文与可操作提示**：控制 / vhost / Dashboard 端口绑定失败会带上具体地址；
  服务端对 `port not allowed` 记录被拒端口与 `allow_ports` 范围；客户端对不可重试的注册失败
  输出 `hint=` 修正方向；本地服务回连失败日志带上 `local=<host:port>`。
- **配置文件与证书读取失败使用稳定错误文案**（如 `not found (os error 2)`），不再透出操作
  系统本地化文案。
- **共享状态锁改用 `parking_lot::Mutex`**：原 `std::sync::Mutex` 一旦某任务持锁 panic 就会
  中毒，之后所有 `lock().unwrap()` 都跟着 panic，导致相关功能整体失效——这与"保留
  `panic = "unwind"` 让单任务 panic 不影响进程"的设计相悖。现改用无中毒语义的
  `parking_lot::Mutex`（`rfrps` / `rfrpc` / `rfrp-common::util::ratelimit`），并去掉全部
  `lock().unwrap()`。日志写者与测试内的独立锁保持 `std::sync::Mutex`。
- **待处理工作连接改为单周期扫描清理**：原实现为每个按需用户连接派生一个 sleep 任务等待
  `WORK_CONN_TIMEOUT_RFRPS`，高连接速率下会同时存在大量睡眠任务。现由 `Server::run` 里的
  单个 1s 周期任务扫描 `pending` 表（`PendingWork` 记录登记时刻），超时项统一关闭；
  对应单测不再需要真实等待 10s。
- **客户端代理注册重试感知退出信号**：`retry_registration` 的退避等待改为 `select!` 监听
  shutdown，进程退出时不再需要等满一个退避周期（最长约 30s）。
- **客户端注册响应通道不再累积**：`register_one_proxy` 在发送失败 / 响应超时 / 通道关闭时
  移除 `state.resps` 条目，控制循环结束后再统一清空兜底。
- **文档**：README 补充 TCP 与 UDP 数据连接在控制面重连时的语义差异（TCP 数据连接保留、
  UDP 会话随控制会话清理）。

### Fixed

- **心跳看门狗在写侧失效时不再"装死"（客户端不重连 / 服务端不清理会话）**：心跳任务发送
  `Heartbeat` 失败时（出站通道已关闭，或写任务阻塞超过 `CONTROL_SEND_TIMEOUT` 导致通道满）
  旧实现直接 `continue` 跳过本轮——既不判定超时也不再尝试发送。半开连接（链路静默中断、
  无 FIN/RST）下读侧也不会返回，于是**客户端永久卡住不重连、服务端永久保留会话与其端口**。
  现在发送失败即判定控制连接失效，触发断开/重连；两侧各有回归测试
  `dead_write_side_triggers_heartbeat_disconnect`。
- **控制循环异常退出不再跳过清理**：服务端控制循环里 `Message::from_frame` 解码失败经 `?`
  提前返回，会跳过会话注销与 `cleanup`（幽灵会话、`remote_port` 不释放、心跳/写任务泄漏）；
  客户端同样会泄漏心跳与写任务。登录响应发送失败、客户端登录响应失败/超时也各自漏掉收尾
  （后者还会把控制任务分离、连旧连接一起泄漏）。现统一改为结束循环走收尾路径或显式 `abort`。
- **控制会话清理时结束在途 UDP 工作连接**：会话断开后 `cleanup` 只移除 UDP 代理注册并中止
  监听循环，但已在途的 UDP 工作连接仍持有 `Arc<UdpProxy>`（含 UDP socket），端口不会释放；
  客户端重连后重新注册同一 UDP 代理会持续得到 `port occupied`。现 `UdpProxy` 增加会话级
  `stop` 令牌，`cleanup` 时取消，工作连接据此退出并释放端口。
- **UDP 清理可能误删新会话的映射**：`sweep` 清理过期待配对项时，按源地址无条件删除
  `pending_client` 反查表；若该源地址在此期间已建立**新的**待配对项，映射会被抹掉，
  后续数据报被当成新会话处理（重复请求工作连接 + 丢包）。现在仅当反查表仍指向被清理的
  `work_id` 时才删除，并补了回归测试 `sweep_keeps_newer_pending_for_same_client`。
- **TCP 与 UDP 代理可共用同一 `remote_port`（RDP-UDP 必需）**：客户端配置校验把 TCP/UDP
  混在一个集合里判重，同号配置直接报 `duplicate remote_port`。但 RDP 客户端（mstsc）启用
  UDP 传输时会把 UDP 发往与 TCP **相同**的端口，于是用户只能给 UDP 换号 → UDP 探测打空 →
  **静默回退纯 TCP**（弱网体验变差）。现按协议分别判重（同协议内仍拒绝重复），并新增
  RDP 场景集成测试：同端口 TCP+UDP 同时注册、各自独立通流。
- **性能基准（`cargo bench`）已恢复可用**：协议升到 v2 后，`benches/forward.rs` 里的帧版本仍
  写死 `1`，被 `FrameCodec` 直接拒绝（`unsupported protocol version`），整个基准跑不起来；
  bridge 基准还每轮新建两对 TCP，跑满一万轮后本地端口被 TIME_WAIT 耗尽（`AddrNotAvailable`）。
  现改用 `PROTOCOL_VERSION`，并在**一条常驻连接**上测稳态吞吐（含采样规模限制），
  `docs/BENCHMARKS.md` 同步更新为实测的新基线 + 端到端数据 + `pool_size` 调优建议。
- **辅助监听 accept 出错不再永久退出**：代理端口监听、HTTP/HTTPS vhost 监听、
  Dashboard 与客户端状态端点此前都是"accept 出错即 `break`"——一次瞬时错误
  （`EMFILE`、对端握手期重置）就会让该监听永久停止，而控制连接与进程一切正常，
  用户侧只看到 connection refused，属最难排查的静默故障。现将退避策略抽到
  `util::accept::AcceptRetry`（成功清零、100ms→1s 封顶退避、日志按周期抑制）：
  这些监听**持续重试**直到恢复；主监听保持"连续失败达阈值则以非零码退出交服务管理器
  重启"的原语义，只是改用同一套退避与日志抑制。
- **预热工作连接池加上限**：`pool_size` 是客户端本地配置、不随协议上送，服务端此前
  没有自己的上限——持有有效 `work_conn_token` 的连接可以反复发 `StartWorkConn(work_id=0)`
  把空闲连接灌进池子（每条都是常驻 socket + 内存），直到会话结束。现在单个代理最多
  保留 `MAX_POOLED_WORK_CONNS_PER_PROXY`(32) 条，超出直接关闭并记 `warn`。
- **vhost 路由改为全局域名索引（O(1)）**：`find_proxy_by_domain` 原先持 `sessions` 锁
  遍历所有会话及其 `proxy_domains`，请求量或会话数上升后会把所有 vhost 请求串行化在
  这把锁上（注册路径也是 O(#sessions)）。现新增与 `proxy_index` 对称的
  `domain → (run_id, proxy_name)` 索引，注册时写入、会话清理时移除。
- **TCP/UDP 代理也会校验 `custom_domains` 全局唯一**：此前只有 vhost 代理做域名冲突
  校验，TCP 代理带 `custom_domains` 时会直接登记域名，可能抢占 vhost 域名并让同名
  请求路由到类型不匹配的代理（用户只会看到 404，注册方毫无察觉）。现在所有代理类型
  统一校验与登记。
- **集成测试随机失败（`make ci` 不可靠）**：根因有二 ——
  ① 用例用 `abort()` 关停测试服务端，而代理监听/控制写任务是独立 `tokio::spawn` 出来的，
  abort 只会留下一批仍持有端口与会话的"僵尸服务端"，使重连类用例要么被旧会话假性服务（假通过）、
  要么等不到端口释放（假失败）；② 同一测试二进制并行执行时，macOS 默认 fd 软上限（256）
  被连接数打满，随机报 `TooManyOpenFiles` / `ConnectionReset` / 连接超时。
  现在测试统一改用优雅退出令牌（`TestServer`/`TestClient` 句柄，`Drop` 时自动取消），
  启动时把 fd 软上限提到硬上限，测试端口改为非 ephemeral 区间自增分配（避免与 `bind(0)` 撞号），
  并移除 `tcp_proxy_connection_cap_enforced` 对探针连接释放时序的依赖。
- **`work_conn_tls` 默认值导致的配置报错难以理解**：`work_conn_tls` 默认 `true`，最小配置
  （只写 token）必然因缺少证书/`tls_server_name` 失败，但原报错只说
  "tls_enable or work_conn_tls"，用户无从下手。现报错点名真正触发的字段并给出改法
  （`set work_conn_tls=false for plaintext work connections`）。
- **客户端状态端点限频响应可读**：限频判断移到请求头读取之后（与 Dashboard 一致），
  否则被限频的连接会带着未读请求数据直接关闭，Windows/Linux 发送 RST，客户端拿到连接
  错误而非 429；新增限频集成测试（含 429 断言）。
- **示例配置不再硬编码开发机绝对路径**：`examples/rfrp-{server,client}.toml` 的证书路径
  改为相对配置文件目录（如 `./cert.pem`），使仓库自带的契约测试（`config_files`）与
  `example_smoke` 在任何机器/平台通过，恢复"克隆即可本地测试"。
- **Windows 混沌测试可编译可运行**：`rfrp-bin/tests/chaos.rs` 按平台门控（Unix 用
  SIGTERM/SIGINT/SIGKILL，Windows 用 CTRL_BREAK_EVENT / TerminateProcess），
  修复此前 Windows 下 `cargo test --all` 因 `libc::kill` 无法编译的问题；
  `libc`/`windows-sys` 改为按目标平台的 dev-dependencies。
- **Linux 下 `unused_mut` 告警**：`chaos.rs::rfrp_command` 的 `mut` 仅在 Windows 分支使用，
  Linux 上 `clippy -D warnings` 会失败；改为分平台构造命令，双平台零告警。
- **UDP 背压不再阻塞整代理收包循环**：会话/待配对通道满时改为 `try_send` 丢弃并计入
  `rfrp_udp_dropped_total`（此前 `send().await` 会因单个慢工作连接阻塞该代理所有客户端的数据报）。
- **vhost 代理注册补同名检查**：HTTP/HTTPS 重名与 TCP/UDP 一致返回 `proxy_name exists`，
  不再静默覆盖旧条目。
- **`example_smoke` 固定 Dashboard 端口竞态**：测试中 Dashboard 也改用 OS 分配端口，
  避免 CI/本机 7500 被占用导致偶发失败。

### Changed

- **全工程整理（配置样板 / 测试辅助 / 依赖 / 工具）**：
  - 测试与单测里的 `ServerSection` / `ClientSection` 字面量（32 处）改用
    `..Default::default()` 收敛默认字段——上次新增心跳配置时被迫改动 24 处字面量，
    今后新增配置字段不再需要这样扫一遍；
  - 测试辅助按主题归位：`rfrpc/tests/common` 拆为 `mod.rs`（核心）+ `udp.rs` + `http.rs`
    并统一再导出，`rfrps/tests` 的 `start_server` / `http_get` / `basic_auth` /
    `dashboard_config` 收敛到 `tests/common`；
  - `vhost::find_proxy_by_domain` 这层薄封装内联为 `ServerState::session_for_domain`
    （上一轮已改成 O(1) 全局索引，封装已无独立语义），测试同步更名；
  - 移除未使用的依赖：`rfrp-common` 的 `anyhow` / `uuid`、`rfrps` 与 `rfrp-bin` 的 `anyhow`；
  - 新增 `scripts/bench-udp.py`（`burst` / `latency` 两个子命令），把此前散落在临时目录的
    UDP 突发与延迟压测固化进仓库，`docs/BENCHMARKS.md` 补上复现方式与波动说明。
- **代码整理（UDP 数据面与测试辅助）**：`util/udp` 按"socket 配置 / 逐帧原语 / 批量路径"
  分区，并删掉重复的分配版读取（只保留一个读取原语）；服务端 UDP 处理抽出
  `remove_pending` / `register_session` / `forward_to_client` / `drain_socket_batch`，
  `handle_udp_work_conn` 主循环只留调度逻辑；测试辅助（UDP echo、代理构造、就绪等待、
  metrics 抓取、日志初始化）统一收敛到 `tests/common`，三个测试文件不再各留一份。
- **UDP 数据面批量化（线格式不变）**：此前每个数据报都要经历"每包一次唤醒 + 每包两次 write
  （TLS 下两个 record）+ 队列深度仅 16"，突发时服务端会大量丢包。现在：收包在同一唤醒内批量
  drain、会话队列 64、服务端按批合并成一次 write、读侧一次 read 解析多帧、双端 UDP 接收缓冲
  best-effort 调大（`UDP_*_BATCH` / `UDP_SESSION_QUEUE_DEPTH` / `UDP_SOCKET_RECV_BUF_BYTES`）。
  **每包仍保留独立的 4 字节长度前缀，线格式未变，无需协议协商。**
  实测单会话突发 2000×1000B（release、独立进程）：应用层丢包 1322 → 65，端到端回收 1.6% → 96.8%；
  RDP 典型速率（≈1000 pps）下丢包为 0。回归用例见 `crates/rfrpc/tests/udp_burst.rs`（`#[ignore]`，
  需 release 运行：`cargo test --release -p rfrpc --test udp_burst -- --ignored`）。
- **代码整理**：`Server::new` 的监听/证书加载拆分为小函数；`register_proxy` 的
  TCP/UDP/vhost 三段重复逻辑合并为统一流程；TLS 缺证书的报错措辞抽到
  `config` 模块一处；测试端口分配（含"非 ephemeral 区间自增 + 可用性探测"）
  下沉到 `rfrp-common::testutil`（`test-support` feature），消除各 crate 的重复副本；
  测试服务端句柄统一为 `TestServer`（不再混用裸 `JoinHandle`）。
- **release 不再使用 `panic = "abort"`**：服务端/客户端是"每条连接一个任务"的模型，
  `panic = "abort"` 会让任何任务级 panic（如锁中毒上的 `unwrap`）直接终止整个进程；
  保留 unwind 后任务级 panic 只影响该任务，进程继续服务其他连接。
- 四个 crate 增加 `#![forbid(unsafe_code)]`；`unsafe` 仅保留在**测试 crate** 中
  （Windows 混沌测试的进程控制、测试用 fd 上限提升），不进入发布产物。
- **Windows 信号处理同时监听 Ctrl-C 与 Ctrl-Break**：服务/脚本/测试可用
  `GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT)` 触发优雅退出（CTRL_C_EVENT 无法定向到进程组）。
- **主目录解析更健壮（双平台）**：`HOME`/`USERPROFILE` 为空串时视为缺失，
  Windows 继续回退 `HOMEDRIVE` + `HOMEPATH`；找不到主目录时回退当前目录。
- README：M4–M6 进度标记更新为已完成；修正服务端默认监听描述；
  `rfrp_udp_dropped_total` 说明补充背压丢包；补充 Windows 原生构建与 CI 说明。

### Added

- **心跳参数可配置**：`[server]` / `[client]` 新增 `heartbeat_interval_secs`（默认 30）与
  `heartbeat_timeout_secs`（默认 10，必须小于间隔）。弱网/高延迟链路可调大以降低误判断连，
  低时延直连可调小以更快感知失联；越界或 `timeout >= interval` 在配置校验阶段直接报错。
- **vhost 未命中返回 404**：`Host` 无法匹配任何代理（或代理类型与监听不符）时返回
  `HTTP/1.1 404 Not Found` + `Connection: close`，替代此前的静默断连；请求头本身读不全
  （超时/畸形/提前关闭）仍直接关闭。文档补充"vhost 按连接首请求路由"的语义：
  同一条 keep-alive 连接不要混用多个域名。
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
- **服务端诊断能力**：`rfrp_accepted_total` / `rfrp_accept_errors_total` / `rfrp_accepting`
  指标、`/healthz` 探活端点、每 5 分钟 `rfrps alive` 摘要日志——用于快速区分
  "SYN 未到达"（网络/防火墙）与"应用层故障"。
- **发布包补全**：Linux tar 与 Windows zip 现包含配置模板、包内 README（快速开始 +
  systemd 安装 + 校验）、LICENSE 与 systemd unit（Linux）。
- 极简 HTTP 工具下沉到 `rfrp-common::util::http`，Dashboard 与客户端状态端点共用。

### Security

- **工作连接鉴权（协议 v2，破坏性变更）**：此前工作连接无任何鉴权——任何能访问控制端口的人
  都可以 `StartWorkConn{proxy_name:<受害代理>, work_id:0}` 把连接注入预热池，使下一个用户连接
  被桥接到攻击者（中间人，可窃取 SSH/RDP 凭据）；也可凭顺序自增的 `work_id` 认领他人的待处理
  用户连接。现 `LoginResp` 下发 per-session 随机 `work_conn_token`，`StartWorkConn` 必须携带，
  服务端校验 token 与代理所属会话一致，并校验 pending 的会话/代理归属。**协议版本升到 2：
  客户端与服务端需同时升级**（旧客户端登录将收到 version mismatch 致命错误）。

### Added

- **补充 5 类边界测试**：TLS 1.3 会话恢复（真实握手指明 `HandshakeKind::Resumed`）、
  TLS 证书校验失败（不可信 CA / server_name 不匹配）、vhost 请求头慢速超时（slowloris）、
  重连退避重置条件（`should_reset_backoff`）、客户端状态端点限频。
- **每代理流量指标**：`rfrp_proxy_bytes_up_total` / `rfrp_proxy_bytes_down_total` /
  `rfrp_proxy_connections_total` / `rfrp_proxy_active_connections`（带 `proxy` 标签），
  Dashboard 状态页与 `/api/status` 同步展示每代理表格。
- **TCP keepalive 可配置且 Windows 生效**：新增 `[server]/[client] tcp_keepalive_secs`
  （默认 30，0 = 禁用）。Windows 此前完全禁用 keepalive，现统一同时设置空闲时间与探测间隔
  （历史问题的根因是只设空闲时间未设间隔），空闲 SSH/RDP 会话在 NAT 表项过期后可由内核探测发现。
- 客户端状态端点新增 `/healthz`（控制连接正常 200，否则 503），供监控/守护进程使用。

### Security

- **登录失败限速**：按来源 IP 统计，窗口（60s）内失败达到 `LOGIN_FAILURE_LIMIT`(10) 次后
  拒绝该 IP 的后续登录（不区分失败原因），防 token 穷举；成功登录清除计数。
- **单会话代理数上限** `MAX_PROXIES_PER_SESSION`(128)，新增错误码 `too many proxies`。

### Fixed

- **UDP 会话清理周期**：由"等于会话超时"改为超时的 1/4，使会话/待配对项实际存活时间接近
  配置超时（此前最坏可达 2× 超时）。
- **客户端状态端点限频**：与 Dashboard 一致（每 IP 100 次/分钟）；限频器下沉到
  `rfrp-common::util::ratelimit` 供两端共用。
- **示例配置 vhost 端口**：由 80/443 改为 8080/8443，避免非特权环境开箱即失败。
- 新增 `make bench` 入口。
- **慢速连接（slowloris）防护**：控制口 TLS 握手、HTTPS vhost 握手、vhost/Dashboard/
  客户端状态端点的请求头读取统一加 10s 整体超时（`TLS_HANDSHAKE_TIMEOUT` /
  `HTTP_HEAD_TIMEOUT`），此前连接后不发数据会长期占用任务与套接字。
- **UDP 待配对会话上限**：单代理上限 `MAX_PENDING_UDP_SESSIONS`(256)，超限丢包并计入
  `rfrp_udp_dropped_total`；此前伪造源地址可将每个 UDP 包放大为一次工作连接请求。
- **Dashboard 限频表不再无限增长**：超过 4096 条目时清理过期项（此前随不同源 IP 持续增长）。
- **重连风暴**：连接存活时间不足 `MIN_STABLE_CONNECTION_SECS`(60s) 时不再重置重连退避，
  避免"建立即断开"场景下的 1s 间隔无限重连。
- **心跳 pong 判定**：改用回传 ts 的时间戳判定（`pong_ts >= 本轮 ts`），消除 `Notify`
  许可残留导致的"漏检一轮"（此前最坏延迟一个心跳周期才检测到失联）。
- **JoinSet 未回收已完成任务**：accept 循环只 spawn 不 join，已完成任务条目持续累积
  （实测约 257 B/连接），长期运行内存持续增长；现运行期持续回收。
- **accept 循环健壮性**：`accept()` 出错不再直接终止循环（瞬时错误退避重试），
  连续失败达到阈值才以**非零退出码**退出，交由服务管理器重启；避免"进程仍在运行
  却不再接受连接"的静默故障。
- **首字节 peek 无超时**：连接后不发任何字节的对端（端口扫描、半开连接）此前会永久
  挂住任务与套接字，现按首帧超时（10s）关闭。
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
