//! 控制会话与代理元信息。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rfrp_common::protocol::msg::ProxyType;
use rfrp_common::util::stream::BoxedStream;
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinHandle;

use super::Message;

/// 一个已注册代理的元信息。
pub struct ProxyEntry {
    /// 代理监听任务句柄（断开时清理）。
    pub handle: JoinHandle<()>,
    /// 代理类型（TCP/UDP/HTTP/HTTPS）。
    pub kind: ProxyType,
}

/// 一个 rfrpc 与服务端之间的控制连接会话。
pub struct Session {
    pub run_id: String,
    pub session_id: String,
    /// 出站控制消息通道（监听任务发 ReqWorkConn，本任务转交写任务）。
    pub tx: mpsc::Sender<Message>,
    /// 已注册代理（proxy_name -> 监听任务句柄 + 类型）。
    pub proxies: Mutex<HashMap<String, ProxyEntry>>,
    /// vhost 域名 -> proxy_name（HTTP/HTTPS 路由用）。
    pub proxy_domains: Mutex<HashMap<String, String>>,
    /// 断开 / 重连通知（§8.3）：清理旧会话或正常断开时唤醒控制循环退出。
    pub stop: Arc<Notify>,
    /// 预热工作连接池（proxy_name -> 空闲服务端侧工作流），按 §8.2 命中用户连接。
    /// 使用类型擦除以同时支持明文与 TLS 工作连接。
    pub pools: Mutex<HashMap<String, Vec<BoxedStream>>>,
}
