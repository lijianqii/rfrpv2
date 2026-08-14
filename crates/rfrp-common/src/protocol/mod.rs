//! 协议层：帧编解码（frame）与消息类型（msg）。
//!
//! 帧格式见 DESIGN §6.1：
//! ```text
//! +----------+----------+----------+----------------+
//! | Version  | MsgType  |  Length  |    Payload     |
//! |  1 byte  |  1 byte  | 4 bytes  |  Length bytes  |
//! +----------+----------+----------+----------------+
//! ```
//! `Length=0` 合法（无 Payload）。数据面工作连接首帧后转透传，不再加帧头。

pub mod frame;
pub mod msg;
