//! 控制消息类型（DESIGN §6.2）与 `Message` 编解码封装。
//!
//! 所有 Payload 用 JSON 序列化（DESIGN §6.2.3）。`Message` 在 `Frame` 之上提供
//! 类型安全的收发：`to_frame()` 按消息类型填帧头并序列化 Payload，
//! `from_frame()` 按帧头 `msg_type` 反序列化到对应结构。

use crate::constants::PROTOCOL_VERSION;
use crate::error::{protocol, Result};
use crate::protocol::frame::Frame;
use serde::{Deserialize, Serialize};

// ---- MsgType 常量（DESIGN §6.2）----
pub const MSG_LOGIN: u8 = 0x01;
pub const MSG_LOGIN_RESP: u8 = 0x02;
pub const MSG_NEW_PROXY: u8 = 0x03;
pub const MSG_NEW_PROXY_RESP: u8 = 0x04;
pub const MSG_HEARTBEAT: u8 = 0x05;
pub const MSG_HEARTBEAT_RESP: u8 = 0x06;
pub const MSG_REQ_WORK_CONN: u8 = 0x07;
pub const MSG_START_WORK_CONN: u8 = 0x08;
pub const MSG_CLOSE: u8 = 0x09;

/// 代理类型（小写枚举，见 DESIGN §6.2.1）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProxyType {
    Tcp,
    Udp,
    Http,
    Https,
}

/// 登录鉴权（C→S，不携带 proxy）。DESIGN §6.2。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Login {
    pub run_id: String,
    pub token: String,
    pub version: u8,
}

/// 登录结果（S→C）。`ok=false` 时 `session_id`/`work_conn_tls` 省略。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LoginResp {
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub work_conn_tls: Option<bool>,
    /// 工作连接鉴权令牌（per-session 随机值，仅下发给已登录客户端）。
    /// `StartWorkConn` 必须携带，服务端据此校验工作连接归属（DESIGN §6.2.1/§8.2）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub work_conn_token: Option<String>,
}

/// 注册单个代理（C→S）。DESIGN §6.2。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NewProxy {
    pub proxy_name: String,
    pub r#type: ProxyType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_domains: Option<Vec<String>>,
}

/// 注册结果（S→C）。`proxy_name` 与对应 NewProxy 一致（见 §6.1 关联约定）。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NewProxyResp {
    pub proxy_name: String,
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// `NewProxyResp.error` 的稳定错误码（DESIGN §6.6）。
///
/// 小写英文标识符、**不含变量**（端口号/域名等细节只写服务端日志），
/// 便于客户端按精确匹配分类处理（如对运行时冲突做重试）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProxyError {
    /// 代理类型非法。
    InvalidType,
    /// 字段缺失或格式错误（含缺失 remote_port / custom_domains）。
    InvalidField,
    /// `proxy_name` 已存在。
    NameExists,
    /// `remote_port` 超出 `allow_ports`。
    PortNotAllowed,
    /// `remote_port` 被占用（**可重试**：如旧会话尚未释放端口）。
    PortOccupied,
    /// vhost 域名与其他代理冲突（**可重试**）。
    DomainConflict,
    /// 其他内部错误（不可重试）。
    Internal,
}

impl ProxyError {
    /// 协议错误码字符串。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InvalidType => "invalid type",
            Self::InvalidField => "invalid field",
            Self::NameExists => "proxy_name exists",
            Self::PortNotAllowed => "port not allowed",
            Self::PortOccupied => "port occupied",
            Self::DomainConflict => "domain conflict",
            Self::Internal => "internal error",
        }
    }

    /// 是否可重试（运行时冲突，端口/域名释放后可成功）。
    pub fn is_retryable(self) -> bool {
        matches!(self, Self::PortOccupied | Self::DomainConflict)
    }

    /// 从错误码字符串解析（未知码返回 `None`）。
    pub fn from_code(code: &str) -> Option<Self> {
        [
            Self::InvalidType,
            Self::InvalidField,
            Self::NameExists,
            Self::PortNotAllowed,
            Self::PortOccupied,
            Self::DomainConflict,
            Self::Internal,
        ]
        .into_iter()
        .find(|e| e.as_str() == code)
    }
}

impl std::fmt::Display for ProxyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// 心跳（双向）。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Heartbeat {
    pub ts: u64,
}

/// 心跳回应（双向），`ts` 原样回传。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HeartbeatResp {
    pub ts: u64,
}

/// 请求建立工作连接（S→C）。`work_id=0` 为补充池场景。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReqWorkConn {
    pub proxy_name: String,
    pub work_id: u64,
}

/// 工作连接首帧（C→S）。其后转透传，`work_id` 透传回传。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StartWorkConn {
    pub proxy_name: String,
    pub work_id: u64,
    /// 工作连接鉴权令牌（来自 `LoginResp.work_conn_token`）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub work_conn_token: Option<String>,
}

/// 主动关闭通知（双向，控制连接层）。`reason` 可选。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Close {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// 类型安全的控制消息。
#[derive(Debug, Clone, PartialEq)]
pub enum Message {
    Login(Login),
    LoginResp(LoginResp),
    NewProxy(NewProxy),
    NewProxyResp(NewProxyResp),
    Heartbeat(Heartbeat),
    HeartbeatResp(HeartbeatResp),
    ReqWorkConn(ReqWorkConn),
    StartWorkConn(StartWorkConn),
    Close(Close),
}

impl Message {
    /// 返回该消息对应的 `MsgType` 常量。
    pub fn msg_type(&self) -> u8 {
        match self {
            Message::Login(_) => MSG_LOGIN,
            Message::LoginResp(_) => MSG_LOGIN_RESP,
            Message::NewProxy(_) => MSG_NEW_PROXY,
            Message::NewProxyResp(_) => MSG_NEW_PROXY_RESP,
            Message::Heartbeat(_) => MSG_HEARTBEAT,
            Message::HeartbeatResp(_) => MSG_HEARTBEAT_RESP,
            Message::ReqWorkConn(_) => MSG_REQ_WORK_CONN,
            Message::StartWorkConn(_) => MSG_START_WORK_CONN,
            Message::Close(_) => MSG_CLOSE,
        }
    }

    /// 序列化为 `Frame`（填版本、MsgType，JSON 序列化 Payload）。
    pub fn to_frame(&self) -> Result<Frame> {
        let payload = match self {
            Message::Login(m) => serde_json::to_vec(m)?,
            Message::LoginResp(m) => serde_json::to_vec(m)?,
            Message::NewProxy(m) => serde_json::to_vec(m)?,
            Message::NewProxyResp(m) => serde_json::to_vec(m)?,
            Message::Heartbeat(m) => serde_json::to_vec(m)?,
            Message::HeartbeatResp(m) => serde_json::to_vec(m)?,
            Message::ReqWorkConn(m) => serde_json::to_vec(m)?,
            Message::StartWorkConn(m) => serde_json::to_vec(m)?,
            Message::Close(m) => serde_json::to_vec(m)?,
        };
        Ok(Frame::new(PROTOCOL_VERSION, self.msg_type(), payload))
    }

    /// 从 `Frame` 反序列化为 `Message`，按 `msg_type` 分发。
    pub fn from_frame(frame: &Frame) -> Result<Message> {
        let m = match frame.msg_type {
            MSG_LOGIN => Message::Login(serde_json::from_slice(&frame.payload)?),
            MSG_LOGIN_RESP => Message::LoginResp(serde_json::from_slice(&frame.payload)?),
            MSG_NEW_PROXY => Message::NewProxy(serde_json::from_slice(&frame.payload)?),
            MSG_NEW_PROXY_RESP => Message::NewProxyResp(serde_json::from_slice(&frame.payload)?),
            MSG_HEARTBEAT => Message::Heartbeat(serde_json::from_slice(&frame.payload)?),
            MSG_HEARTBEAT_RESP => Message::HeartbeatResp(serde_json::from_slice(&frame.payload)?),
            MSG_REQ_WORK_CONN => Message::ReqWorkConn(serde_json::from_slice(&frame.payload)?),
            MSG_START_WORK_CONN => Message::StartWorkConn(serde_json::from_slice(&frame.payload)?),
            MSG_CLOSE => Message::Close(serde_json::from_slice(&frame.payload)?),
            other => {
                return Err(protocol(format!("unknown msg_type: {other:#x}")));
            }
        };
        Ok(m)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(m: Message) {
        let frame = m.to_frame().unwrap();
        assert_eq!(frame.msg_type, m.msg_type());
        let back = Message::from_frame(&frame).unwrap();
        assert_eq!(back, m);
    }

    #[test]
    fn login_roundtrip() {
        roundtrip(Message::Login(Login {
            run_id: "abc".into(),
            token: "secret".into(),
            version: PROTOCOL_VERSION,
        }));
    }

    #[test]
    fn login_resp_ok_omits_optional() {
        // ok=true：不携带 error/session_id/work_conn_tls 时不应序列化出来。
        let m = Message::LoginResp(LoginResp {
            ok: true,
            error: None,
            session_id: None,
            work_conn_tls: None,
            work_conn_token: None,
        });
        let json = String::from_utf8(m.to_frame().unwrap().payload).unwrap();
        assert!(!json.contains("session_id"));
        assert!(!json.contains("error"));
        roundtrip(m);
    }

    #[test]
    fn login_resp_err_omits_session() {
        let m = Message::LoginResp(LoginResp {
            ok: false,
            error: Some("version mismatch".into()),
            session_id: None,
            work_conn_tls: None,
            work_conn_token: None,
        });
        let json = String::from_utf8(m.to_frame().unwrap().payload).unwrap();
        assert!(json.contains("version mismatch"));
        assert!(!json.contains("session_id"));
        roundtrip(m);
    }

    #[test]
    fn new_proxy_tcp_roundtrip() {
        roundtrip(Message::NewProxy(NewProxy {
            proxy_name: "ssh".into(),
            r#type: ProxyType::Tcp,
            remote_port: Some(6000),
            custom_domains: None,
        }));
    }

    #[test]
    fn new_proxy_http_roundtrip() {
        roundtrip(Message::NewProxy(NewProxy {
            proxy_name: "web".into(),
            r#type: ProxyType::Https,
            remote_port: None,
            custom_domains: Some(vec!["dev.example.com".into()]),
        }));
    }

    #[test]
    fn all_variants_roundtrip() {
        roundtrip(Message::NewProxyResp(NewProxyResp {
            proxy_name: "ssh".into(),
            ok: true,
            error: None,
        }));
        roundtrip(Message::Heartbeat(Heartbeat { ts: 123 }));
        roundtrip(Message::HeartbeatResp(HeartbeatResp { ts: 123 }));
        roundtrip(Message::ReqWorkConn(ReqWorkConn {
            proxy_name: "ssh".into(),
            work_id: 7,
        }));
        roundtrip(Message::StartWorkConn(StartWorkConn {
            proxy_name: "ssh".into(),
            work_id: 7,
            work_conn_token: None,
        }));
        roundtrip(Message::Close(Close {
            reason: Some("shutdown".into()),
        }));
        roundtrip(Message::Close(Close { reason: None }));
    }

    #[test]
    fn from_frame_unknown_msg_type_errors() {
        let f = crate::protocol::frame::Frame::new(PROTOCOL_VERSION, 0x7f, b"{}".to_vec());
        assert!(Message::from_frame(&f).is_err());
    }

    #[test]
    fn from_frame_invalid_json_payload_errors() {
        let f = crate::protocol::frame::Frame::new(
            PROTOCOL_VERSION,
            MSG_HEARTBEAT,
            b"not-json".to_vec(),
        );
        assert!(Message::from_frame(&f).is_err());
    }

    #[test]
    fn proxy_type_unknown_variant_errors() {
        assert!(serde_json::from_str::<ProxyType>(r#""ftp""#).is_err());
    }

    #[test]
    fn from_frame_wrong_shape_payload_errors() {
        // 合法 msg_type，但 payload JSON 缺必填字段（结构体形状不匹配）-> 反序列化错误。
        let f = Frame::new(PROTOCOL_VERSION, MSG_HEARTBEAT, b"{}".to_vec());
        assert!(Message::from_frame(&f).is_err());
    }
}

#[cfg(test)]
mod proxy_error_tests {
    use super::ProxyError;

    #[test]
    fn codes_match_design_contract() {
        // DESIGN §6.6：小写英文标识符、不含变量，客户端按精确匹配分类。
        assert_eq!(ProxyError::InvalidType.as_str(), "invalid type");
        assert_eq!(ProxyError::InvalidField.as_str(), "invalid field");
        assert_eq!(ProxyError::NameExists.as_str(), "proxy_name exists");
        assert_eq!(ProxyError::PortNotAllowed.as_str(), "port not allowed");
        assert_eq!(ProxyError::PortOccupied.as_str(), "port occupied");
        assert_eq!(ProxyError::DomainConflict.as_str(), "domain conflict");
        assert_eq!(ProxyError::Internal.as_str(), "internal error");
    }

    #[test]
    fn retryable_classification() {
        assert!(ProxyError::PortOccupied.is_retryable());
        assert!(ProxyError::DomainConflict.is_retryable());
        for e in [
            ProxyError::InvalidType,
            ProxyError::InvalidField,
            ProxyError::NameExists,
            ProxyError::PortNotAllowed,
            ProxyError::Internal,
        ] {
            assert!(!e.is_retryable(), "{e} must not be retryable");
        }
    }

    #[test]
    fn from_code_roundtrip_and_unknown() {
        for e in [
            ProxyError::InvalidType,
            ProxyError::InvalidField,
            ProxyError::NameExists,
            ProxyError::PortNotAllowed,
            ProxyError::PortOccupied,
            ProxyError::DomainConflict,
            ProxyError::Internal,
        ] {
            assert_eq!(ProxyError::from_code(e.as_str()), Some(e));
        }
        assert_eq!(ProxyError::from_code("boom"), None);
        assert_eq!(ProxyError::from_code(""), None);
    }
}
