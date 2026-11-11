//! 集中定义协议版本、超时、退避、上限等常量，避免散落 magic number。
//!
//! 各数值与 DESIGN.md §7.1 `rfrp-common::constants` 一节保持一致。

/// 协议版本号（DESIGN §6.4）。
///
/// v2：工作连接鉴权——`LoginResp` 下发 per-session `work_conn_token`，
/// `StartWorkConn` 必须携带并由服务端校验（防止未认证连接注入池/劫持 pending）。
pub const PROTOCOL_VERSION: u8 = 2;

/// 本实现接受的**最低**协议版本。
///
/// 与 [`PROTOCOL_VERSION`] 一起构成可接受区间 `[MIN, MAX]`：
/// 服务端在区间内按客户端声明的版本协商，区间外一律拒绝（DESIGN §6.4）。
///
/// v1 与 v2 **不兼容**（v2 强制工作连接鉴权），因此下限同样是 2：即使服务端
/// 的最高版本更高，也不会降级到 v1——降级需要两端都实现对应语义。
/// 将来发布 v3 时：若 v3 的消息结构与 v2 向后兼容，把上限提到 3 即可同时接受
/// v2 客户端（服务端按协商结果走 v2 语义）；否则把下限一并提到 3。
pub const PROTOCOL_MIN_VERSION: u8 = 2;

/// 判断某个协议版本是否在本实现支持的区间内。
pub fn is_supported_protocol_version(version: u8) -> bool {
    (PROTOCOL_MIN_VERSION..=PROTOCOL_VERSION).contains(&version)
}

/// 协商协议版本（DESIGN §6.4）。
///
/// 客户端在 `Login.version` 声明自己的版本，服务端在支持区间内选取：
/// - 命中 → 返回协商结果（当前实现即客户端声明的版本）；
/// - 未命中 → 返回 `None`，服务端拒绝登录（客户端按致命错误处理）。
///
/// 返回的版本会经 `LoginResp.protocol_version` 回传给客户端，双方据此使用
/// 同一套语义。
pub fn negotiate_protocol_version(client_version: u8) -> Option<u8> {
    is_supported_protocol_version(client_version).then_some(client_version)
}

/// 本实现支持的版本列表（用于错误提示与文档）。
pub const PROTOCOL_VERSIONS_SUPPORTED: &[u8] = &[PROTOCOL_VERSION];

/// 帧头长度：Version(1) + MsgType(1) + Length(4)。
pub const FRAME_HEADER_LEN: usize = 6;

/// 单帧 Payload 上限（16 MiB）。
pub const FRAME_MAX_PAYLOAD: u32 = 16 * 1024 * 1024;

/// 控制消息单帧上限（64 KiB）。
///
/// 控制消息（Login/NewProxy/心跳/Close 等）远小于此；服务端在认证前读取首帧时
/// 也使用该上限，避免未认证连接只发 6 字节头声明 16 MiB 就触发等量内存分配。
/// 配合 [`MAX_UNAUTHENTICATED_CONNECTIONS`]，未认证阶段的最坏预分配被限制在
/// 约 64 MiB（1024 × 64 KiB），而不是 16 GiB。
pub const CONTROL_FRAME_MAX_PAYLOAD: u32 = 64 * 1024;

/// work_id = 0 的保留值，表示「池化预备 / 补充池」语义（DESIGN §8.2）。
pub const WORK_ID_POOL_RESERVED: u64 = 0;

// ---- 超时（秒）----

/// 连接服务端（控制连接）的 TCP 建连超时：避免防火墙静默丢包时
/// 长时间无进展（Linux 默认 SYN 重试约 2 分钟）。
pub const CONNECT_TIMEOUT: u64 = 10;
/// 控制面登录响应等待超时。
pub const LOGIN_TIMEOUT: u64 = 10;
/// 单个 NewProxy 注册响应等待超时。
pub const NEW_PROXY_TIMEOUT: u64 = 10;
/// 服务端等待连接首帧（Login/StartWorkConn）的超时。
pub const FIRST_FRAME_TIMEOUT: u64 = 10;
/// TLS 握手超时（控制口与 HTTPS vhost）：防止半截握手长期占用任务与套接字。
pub const TLS_HANDSHAKE_TIMEOUT: u64 = 10;
/// HTTP 请求头读取总超时（Dashboard/状态端点/vhost）：防慢速请求（slowloris）。
pub const HTTP_HEAD_TIMEOUT: u64 = 10;
/// 单个 HTTP 响应写出总超时：客户端不读响应时不得永久占用任务。
pub const HTTP_RESPONSE_TIMEOUT: u64 = 30;
/// vhost HTTP/HTTPS 认证前（TLS/请求头阶段）的最大并发连接数。
///
/// 与主控制口的 `MAX_UNAUTHENTICATED_CONNECTIONS` 分开，避免 vhost 洪泛挤占控制登录。
pub const VHOST_MAX_PREAUTH_CONNECTIONS: usize = 1024;
/// Dashboard 单监听口最大并发请求数（含慢连接）。
pub const DASHBOARD_MAX_CONCURRENT_CONNECTIONS: usize = 256;
/// 客户端状态端点单监听口最大并发请求数。
pub const STATUS_MAX_CONCURRENT_CONNECTIONS: usize = 128;

/// 心跳发送间隔。
pub const HEARTBEAT_INTERVAL: u64 = 30;
/// 心跳响应等待超时，超时判定对端已死。
pub const HEARTBEAT_TIMEOUT: u64 = 10;
/// 心跳参数（间隔/超时）可配置范围的上下限（秒），见 §9.4 校验。
pub const HEARTBEAT_MIN_SECS: u64 = 1;
pub const HEARTBEAT_MAX_SECS: u64 = 3600;
/// rfrps 侧等待 StartWorkConn 的兜底超时。
pub const WORK_CONN_TIMEOUT_RFRPS: u64 = 10;
/// rfrps 侧重发 `ReqWorkConn` 的间隔（毫秒）。
///
/// 工作连接建立失败（客户端本地服务瞬时不可用、建连抖动）时，只发一次请求会让用户
/// 连接白等到 `WORK_CONN_TIMEOUT_RFRPS` 被关闭。这里在超时窗口内周期性重发，给客户端
/// 重试机会。重复请求是安全的：服务端按 `work_id` 取待处理项，取走即消费；后到的工作
/// 连接因找不到待处理项而被关闭。
pub const WORK_CONN_REQUEST_INTERVAL_MS: u64 = 3000;
/// rfrpc 侧建立工作连接的本地截止。
pub const WORK_CONN_TIMEOUT_RFRPC: u64 = 8;
/// UDP 会话无活动超时清理（默认值，可通过服务端配置覆盖）。
///
/// 取 300 秒而不是早期的 60 秒：RDP-UDP 会话在用户阅读/停顿期间可能长时间没有
/// 数据，过短的超时会反复关闭工作连接并触发重建，表现为 RDP-UDP 卡顿或回退 TCP。
pub const UDP_SESSION_TIMEOUT: u64 = 300;
/// `udp_session_timeout_secs` 可配置下限（秒）。
pub const UDP_SESSION_TIMEOUT_MIN_SECS: u64 = 1;
/// `udp_session_timeout_secs` 可配置上限（秒，24 小时）。
pub const UDP_SESSION_TIMEOUT_MAX_SECS: u64 = 24 * 60 * 60;
/// 服务端 accept 连续失败达到该次数后判定监听不可恢复，退出进程
/// 交由进程管理器（systemd/nssm）重启（约 1 分钟持续失败）。
pub const MAX_CONSECUTIVE_ACCEPT_ERRORS: u32 = 60;
/// 服务端存活摘要日志间隔（秒）：便于区分"进程卡死"与"网络不可达"。
pub const SERVER_ALIVE_LOG_INTERVAL: u64 = 300;

/// 优雅退出在途连接强制关闭超时。
pub const GRACEFUL_SHUTDOWN_TIMEOUT: u64 = 30;
/// `grace_secs` 可配置上限（秒，1 小时）。
pub const GRACEFUL_SHUTDOWN_TIMEOUT_MAX_SECS: u64 = 3600;

// ---- 重连退避（秒）----

/// 数据面 TCP keepalive 空闲时间（秒），用于长连接断线感知。
pub const TCP_KEEPALIVE_INTERVAL: u64 = 30;
/// TCP keepalive 探测间隔（秒）。
pub const TCP_KEEPALIVE_PROBE_INTERVAL: u64 = 5;

// ---- socket 缓冲 ----

/// 数据面 socket 收发缓冲目标值（字节，best-effort）。
///
/// 增大 socket 缓冲可减少大流量突发时的窗口溢出（实测 loopback RDP 吞吐
/// 14.8 → 206.6 MB/s，见 docs/BENCHMARKS.md「socket 缓冲」）。
///
/// **内核会截断**：Linux 按 `net.core.rmem_max` / `net.core.wmem_max` 封顶
/// （内核内部按 2 倍记账并回读该值）。系统默认 212992（208 KiB）时，
/// 请求 512 KiB 实际只生效 416 KiB；要真正拿到 512 KiB 需调大系统上限：
///
/// ```bash
/// sysctl -w net.core.rmem_max=1048576 net.core.wmem_max=1048576
/// ```
///
/// 设置失败不影响功能（best-effort），仅吞吐收益不生效。
pub const TCP_SOCKET_BUF_BYTES: usize = 512 * 1024;

// ---- Dashboard 会话 ----

/// Dashboard 会话 Cookie 默认有效期（秒，8 小时）。
pub const DASHBOARD_SESSION_TTL_SECS: u64 = 8 * 3600;
/// `session_ttl_secs` 可配置下限（秒，1 分钟）。
pub const DASHBOARD_SESSION_TTL_MIN_SECS: u64 = 60;
/// `session_ttl_secs` 可配置上限（秒，30 天）。
pub const DASHBOARD_SESSION_TTL_MAX_SECS: u64 = 30 * 24 * 3600;

// ---- 代理注册重试（运行时冲突，DESIGN §6.6）----

/// 注册失败（port occupied / domain conflict）后的首次重试延迟。
pub const PROXY_REGISTER_RETRY_INITIAL: u64 = 2;
/// 注册重试最大轮数（退避 2s→…→30s，约 2 分钟，覆盖旧会话释放端口的窗口）。
pub const PROXY_REGISTER_RETRY_MAX: u32 = 8;
/// 注册重试退避上限。
pub const PROXY_REGISTER_RETRY_MAX_DELAY: u64 = 30;
/// "配置类"注册失败的后台重试间隔（秒）。
///
/// 这类失败（如服务端尚未放行 `allow_ports`）重试不会立刻自愈，但可能随服务端配置或
/// 状态变化而恢复，且**不需要重启客户端**；因此用固定长间隔继续尝试。
pub const PROXY_REGISTER_RETRY_PERSISTENT_DELAY_SECS: u64 = 30;
/// "配置类"注册失败的后台重试轮数（约 10 分钟）。
pub const PROXY_REGISTER_RETRY_PERSISTENT_MAX: u32 = 20;

/// 重连退避初值。
pub const RECONNECT_BACKOFF_INITIAL: u64 = 1;
/// 重连退避上限。
pub const RECONNECT_BACKOFF_MAX: u64 = 30;

// ---- 上限 ----

/// 单个会话可注册的代理数上限（防认证客户端耗尽端口/内存）。
pub const MAX_PROXIES_PER_SESSION: usize = 128;
/// 全局并发控制会话上限。
///
/// 登录限速只统计**失败**次数，因此持有效 token 的客户端可以用随机 `run_id`
/// 不断建立控制会话（每个都会注册代理、占用 fd 与内存）。这里给会话总数设一个
/// 上界；同一 `run_id` 的重连替换不计新增。
pub const MAX_SESSIONS: usize = 1024;
/// 单个来源 IP 的并发控制会话上限。
///
/// 取值高于常见 NAT 后的客户端数量，避免误伤；作用是防止单一来源占满全局额度。
pub const MAX_SESSIONS_PER_IP: usize = 256;
/// 登录失败限速：窗口内允许的最大失败次数（超出后拒绝该 IP 的登录尝试）。
pub const LOGIN_FAILURE_LIMIT: u32 = 10;
/// 登录失败限速窗口（秒）。
pub const LOGIN_FAILURE_WINDOW: u64 = 60;
/// 连接存活时间低于该秒数视为"短命连接"，不重置重连退避（防抖动重连风暴）。
pub const MIN_STABLE_CONNECTION_SECS: u64 = 60;
/// 单个代理 custom_domains 元素上限。
pub const MAX_CUSTOM_DOMAINS: usize = 16;
/// 工作连接池默认大小。
pub const POOL_SIZE_DEFAULT: u32 = 1;
/// 池大小告警阈值（超过记警告）。
pub const POOL_SIZE_WARN_THRESHOLD: u32 = 16;
/// 客户端预热工作连接的全局并发上限，避免多代理大 pool_size 造成连接风暴。
pub const PREHEAT_MAX_CONCURRENT: usize = 16;
/// 客户端 `pool_size` 硬上限（与服务端单代理池上限对齐）。
///
/// 服务端最多保留 [`MAX_POOLED_WORK_CONNS_PER_PROXY`] 条池连接；客户端配置更大
/// 只会先建立再被拒绝。该上限同时防止配置笔误（如多写几个零）触发连接/任务风暴。
pub const POOL_SIZE_MAX: u32 = 32;
/// 单个代理在**服务端**保留的预热工作连接上限。
///
/// `pool_size` 是客户端本地配置、不随协议上送，服务端必须自己设上限：否则持有有效
/// work_conn_token 的连接可以不断发 `work_id=0` 把连接灌进池子，服务端会一直保留
/// （每条 = 一个 socket + 任务内存），直到会话结束。取 32：远高于推荐值
/// （`POOL_SIZE_WARN_THRESHOLD` = 16），正常客户端不可能触达。
pub const MAX_POOLED_WORK_CONNS_PER_PROXY: usize = 32;
/// 单个 UDP 代理同时允许的待配对会话上限（超出直接丢弃，防伪造源放大/耗尽）。
pub const MAX_PENDING_UDP_SESSIONS: usize = 256;
/// 单个 UDP 代理同时保留的已配对会话上限。
///
/// pending 上限只约束“尚未建立工作连接”的源地址；已配对会话若不设上限，
/// 持续更换源地址可让客户端不断建立工作连接并占用服务端/本机资源。
pub const MAX_UDP_SESSIONS_PER_PROXY: usize = 4096;
/// 单个 UDP 包最大字节数（IPv4 UDP payload 上限）。
pub const MAX_UDP_PACKET_SIZE: usize = 65507;
/// 每个 UDP 会话的待发送队列深度（数据报个数）。
///
/// 服务端收包循环与"往工作连接写"的任务之间是队列解耦的：队列越深，越能吸收
/// 视频/刷屏这类突发（实测深度 16 时，2000 包突发在应用层丢掉 1322 包）。
/// 内存上界 = 深度 × 单包大小，正常 RDP 包（≈1.4 KB）约 90 KB/会话。
pub const UDP_SESSION_QUEUE_DEPTH: usize = 64;
/// 一次从 UDP socket 连续收取的最大数据报个数（收包循环内批量 drain，减少唤醒/系统调用）。
pub const UDP_RECV_BATCH: usize = 64;
/// 一次批量写工作连接的最大数据报个数（合并系统调用；每包仍保留独立的 4 字节长度前缀）。
pub const UDP_WRITE_BATCH: usize = 32;
/// 一次从工作连接批量解析的最大数据报个数（一次 read 解析多帧，避免每包一次唤醒）。
pub const UDP_READ_BATCH: usize = 32;
/// 单个 UDP 代理所有待发送队列允许占用的最大字节数。
///
/// 队列深度按“包数”限制无法约束最大包场景：`64 × 65 KiB × 256 pending`
/// 可接近 1 GiB。这里再加一道按字节的上界，超限直接丢包并计入
/// `udp_dropped_total`；16 MiB 足够覆盖正常 RDP 突发，又不会让单代理
/// 在低内存机器上无界增长。
pub const UDP_PROXY_QUEUE_MAX_BYTES: usize = 16 * 1024 * 1024;
/// UDP 读/写缓冲的初始容量。按实际数据增长，避免每个会话一上来就占 64 KiB。
pub const UDP_BUF_INITIAL_BYTES: usize = 8 * 1024;
/// 包池只回收不超过该容量的缓冲；超大包缓冲用完即丢，防止池被 65 KiB 缓冲撑大。
pub const UDP_PACKET_POOL_MAX_BUF_BYTES: usize = 16 * 1024;
/// UDP socket 接收缓冲目标值（best-effort，内核可能按 rmem_max 截断）。
///
/// 突发时先由内核缓冲吸收，避免应用层队列还没腾出空间就丢包。
pub const UDP_SOCKET_RECV_BUF_BYTES: usize = 1 << 20;

/// 数据面桥接缓冲区大小（每方向，字节）。
///
/// tokio `copy_bidirectional` 默认 8 KiB；此处提高到 64 KiB 以减少大流量
/// （文件传输等）下的 read/write 系统调用次数，小包交互场景不受影响。
/// 每条约 2×该值 的连接内存开销。
///
/// 取舍见 docs/BENCHMARKS.md「数据面调优结论」：相对 32 KiB 吞吐 +7%，
/// 代价是每条连接多占 64 KiB 内存。
pub const BRIDGE_BUF_SIZE: usize = 64 * 1024;

/// 服务端最大并发用户连接数（防 DoS 兜底）。
pub const MAX_ACTIVE_CONNECTIONS: i64 = 65536;

/// 服务端同时处于首帧握手阶段（peek/TLS/首帧读取）的连接上限。
///
/// 该阶段尚未认证，不占用 `MAX_SESSIONS`；限流可防止大量半开连接在认证前
/// 占用任务/套接字（配合控制帧长度上限，避免每连接预分配大缓冲）。
pub const MAX_UNAUTHENTICATED_CONNECTIONS: usize = 1024;

// ---- 字符串长度上限（字节）----

pub const MAX_RUN_ID_LEN: usize = 64;
pub const MAX_TOKEN_LEN: usize = 256;
pub const MAX_PROXY_NAME_LEN: usize = 64;
pub const MAX_DOMAIN_LEN: usize = 253;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn supported_version_range() {
        // 当前区间为 [2,2]：v1 与 v2 不兼容，下限同样是 2。
        assert!(is_supported_protocol_version(PROTOCOL_VERSION));
        assert!(is_supported_protocol_version(PROTOCOL_MIN_VERSION));
        assert!(!is_supported_protocol_version(PROTOCOL_VERSION + 1));
        assert!(!is_supported_protocol_version(PROTOCOL_MIN_VERSION - 1));
        assert!(!is_supported_protocol_version(0));
        assert!(!is_supported_protocol_version(255));
    }

    #[test]
    fn negotiate_returns_version_in_range() {
        assert_eq!(
            negotiate_protocol_version(PROTOCOL_VERSION),
            Some(PROTOCOL_VERSION)
        );
        // 区间外一律拒绝（None），不降级。
        assert_eq!(negotiate_protocol_version(PROTOCOL_VERSION + 1), None);
        assert_eq!(negotiate_protocol_version(PROTOCOL_MIN_VERSION - 1), None);
    }

    #[test]
    fn supported_versions_list_matches_current() {
        assert!(PROTOCOL_VERSIONS_SUPPORTED.contains(&PROTOCOL_VERSION));
        for v in PROTOCOL_VERSIONS_SUPPORTED {
            assert!(
                is_supported_protocol_version(*v),
                "listed version {v} must be inside the accepted range"
            );
        }
    }
}
