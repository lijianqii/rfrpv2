//! 集中定义协议版本、超时、退避、上限等常量，避免散落 magic number。
//!
//! 各数值与 DESIGN.md §7.1 `rfrp-common::constants` 一节保持一致。

/// 协议版本号（DESIGN §6.4）。首版仅支持 1。
pub const PROTOCOL_VERSION: u8 = 1;

/// 帧头长度：Version(1) + MsgType(1) + Length(4)。
pub const FRAME_HEADER_LEN: usize = 6;

/// 单帧 Payload 上限（16 MiB）。
pub const FRAME_MAX_PAYLOAD: u32 = 16 * 1024 * 1024;

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

/// 心跳发送间隔。
pub const HEARTBEAT_INTERVAL: u64 = 30;
/// 心跳响应等待超时，超时判定对端已死。
pub const HEARTBEAT_TIMEOUT: u64 = 10;
/// rfrps 侧等待 StartWorkConn 的兜底超时。
pub const WORK_CONN_TIMEOUT_RFRPS: u64 = 10;
/// rfrpc 侧建立工作连接的本地截止。
pub const WORK_CONN_TIMEOUT_RFRPC: u64 = 8;
/// UDP 会话无活动超时清理。
pub const UDP_SESSION_TIMEOUT: u64 = 60;
/// 服务端 accept 连续失败达到该次数后判定监听不可恢复，退出进程
/// 交由进程管理器（systemd/nssm）重启（约 1 分钟持续失败）。
pub const MAX_CONSECUTIVE_ACCEPT_ERRORS: u32 = 60;
/// 服务端存活摘要日志间隔（秒）：便于区分"进程卡死"与"网络不可达"。
pub const SERVER_ALIVE_LOG_INTERVAL: u64 = 300;

/// 优雅退出在途连接强制关闭超时。
pub const GRACEFUL_SHUTDOWN_TIMEOUT: u64 = 30;

// ---- 重连退避（秒）----

/// 数据面 TCP keepalive 空闲时间（秒），用于长连接断线感知。
pub const TCP_KEEPALIVE_INTERVAL: u64 = 30;
/// TCP keepalive 探测间隔（秒）。
pub const TCP_KEEPALIVE_PROBE_INTERVAL: u64 = 5;

// ---- 代理注册重试（运行时冲突，DESIGN §6.6）----

/// 注册失败（port occupied / domain conflict）后的首次重试延迟。
pub const PROXY_REGISTER_RETRY_INITIAL: u64 = 2;
/// 注册重试最大轮数（退避 2s→…→30s，约 2 分钟，覆盖旧会话释放端口的窗口）。
pub const PROXY_REGISTER_RETRY_MAX: u32 = 8;
/// 注册重试退避上限。
pub const PROXY_REGISTER_RETRY_MAX_DELAY: u64 = 30;

/// 重连退避初值。
pub const RECONNECT_BACKOFF_INITIAL: u64 = 1;
/// 重连退避上限。
pub const RECONNECT_BACKOFF_MAX: u64 = 30;

// ---- 上限 ----

/// 单个代理 custom_domains 元素上限。
pub const MAX_CUSTOM_DOMAINS: usize = 16;
/// 工作连接池默认大小。
pub const POOL_SIZE_DEFAULT: u32 = 1;
/// 池大小告警阈值（超过记警告但不拒绝）。
pub const POOL_SIZE_WARN_THRESHOLD: u32 = 16;
/// 单个 UDP 包最大字节数（IPv4 UDP payload 上限）。
pub const MAX_UDP_PACKET_SIZE: usize = 65507;

/// 数据面桥接缓冲区大小（每方向，字节）。
///
/// tokio `copy_bidirectional` 默认 8 KiB；此处提高到 32 KiB 以减少大流量
/// （文件传输等）下的 read/write 系统调用次数，小包交互场景不受影响。
/// 每条约 2×该值 的连接内存开销。
pub const BRIDGE_BUF_SIZE: usize = 32 * 1024;

/// 服务端最大并发用户连接数（防 DoS 兜底）。
pub const MAX_ACTIVE_CONNECTIONS: i64 = 65536;

// ---- 字符串长度上限（字节）----

pub const MAX_RUN_ID_LEN: usize = 64;
pub const MAX_TOKEN_LEN: usize = 256;
pub const MAX_PROXY_NAME_LEN: usize = 64;
pub const MAX_DOMAIN_LEN: usize = 253;
pub const MAX_ERROR_LEN: usize = 512;
