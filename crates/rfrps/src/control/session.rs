//! 控制会话与代理元信息。

use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

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
    /// TCP/UDP 的公网端口（vhost 代理为 `None`）。
    pub remote_port: Option<u16>,
    /// vhost 域名（TCP/UDP 代理通常为空）。
    pub custom_domains: Vec<String>,
}

/// 一个 rfrpc 与服务端之间的控制连接会话。
pub struct Session {
    pub run_id: String,
    pub session_id: String,
    /// 控制连接对端 IP（用于按来源限制并发会话数，见 `state::ServerState`）。
    pub peer_ip: std::net::IpAddr,
    /// 工作连接鉴权令牌（登录时随机生成，仅经 LoginResp 下发给该客户端）。
    /// 工作连接建立时校验，防止未认证连接注入预热池或劫持 pending（DESIGN §8.2）。
    pub work_conn_token: String,
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

/// 测试用会话构造器：所有测试共用一份，避免 `Session` 增字段时逐个构造点修改。
#[cfg(test)]
pub(crate) fn test_session(run_id: &str) -> Arc<Session> {
    test_session_with_token(run_id, "tok").0
}

/// 测试用代理条目：只关心类型，附带一个立即结束的占位任务句柄。
#[cfg(test)]
pub(crate) fn test_entry(kind: ProxyType) -> ProxyEntry {
    test_entry_with(kind, None, &[])
}

/// 同 [`test_entry`]，但可指定公网端口与域名。
#[cfg(test)]
pub(crate) fn test_entry_with(
    kind: ProxyType,
    remote_port: Option<u16>,
    domains: &[&str],
) -> ProxyEntry {
    ProxyEntry {
        handle: tokio::spawn(async {}),
        kind,
        remote_port,
        custom_domains: domains.iter().map(|d| d.to_string()).collect(),
    }
}

/// 同 [`test_session`]，但指定工作连接令牌并返回出站消息接收端
/// （需要断言服务端下发消息的测试使用）。
#[cfg(test)]
pub(crate) fn test_session_with_token(
    run_id: &str,
    token: &str,
) -> (Arc<Session>, mpsc::Receiver<Message>) {
    let (tx, rx) = mpsc::channel(8);
    let session = Arc::new(Session {
        run_id: run_id.to_string(),
        session_id: "s".to_string(),
        peer_ip: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        work_conn_token: token.to_string(),
        tx,
        proxies: Mutex::new(HashMap::new()),
        proxy_domains: Mutex::new(HashMap::new()),
        stop: Arc::new(Notify::new()),
        pools: Mutex::new(HashMap::new()),
    });
    (session, rx)
}
