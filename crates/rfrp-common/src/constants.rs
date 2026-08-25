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
/// 优雅退出在途连接强制关闭超时。
pub const GRACEFUL_SHUTDOWN_TIMEOUT: u64 = 30;

// ---- 重连退避（秒）----

/// 数据面 TCP keepalive 间隔（秒），用于长连接断线感知。
pub const TCP_KEEPALIVE_INTERVAL: u64 = 30;

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

// ---- 字符串长度上限（字节）----

pub const MAX_RUN_ID_LEN: usize = 64;
pub const MAX_TOKEN_LEN: usize = 256;
pub const MAX_PROXY_NAME_LEN: usize = 64;
pub const MAX_DOMAIN_LEN: usize = 253;
pub const MAX_ERROR_LEN: usize = 512;
