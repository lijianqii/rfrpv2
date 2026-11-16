#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use rfrp_common::protocol::frame::FrameCodec;
use rfrp_common::protocol::msg::Message;
use tokio_util::codec::Decoder;

fuzz_target!(|data: &[u8]| {
    // 帧解析是外部输入的入口：任意字节都不得 panic，合法帧再交给消息层
    // （未知 msg_type / 非法 JSON 应返回错误而不是崩溃）。
    let mut buf = BytesMut::from(data);
    if let Ok(Some(frame)) = FrameCodec.decode(&mut buf) {
        let _ = Message::from_frame(&frame);
    }
});
