//! 端到端验证致命登录失败（auth failed）时客户端不进入无限重连，而是退出（§8.1）。

use futures::SinkExt;
use rfrp_common::config::{ClientConfig, ClientSection};
use rfrp_common::protocol::frame::{read_one_frame, FrameCodec, FramedWrite};
use rfrp_common::protocol::msg::*;
use tokio::net::TcpListener;

#[tokio::test]
async fn fatal_login_exits_without_reconnect() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // 模拟服务端：读 Login 首帧，回 LoginResp{ok=false, "auth failed"} 后关闭。
    let srv = tokio::spawn(async move {
        let (stream, _peer) = listener.accept().await.unwrap();
        let (frame, stream) = read_one_frame(stream).await.unwrap();
        assert!(matches!(Message::from_frame(&frame), Ok(Message::Login(_))));
        let mut w = FramedWrite::new(stream, FrameCodec);
        w.send(
            Message::LoginResp(LoginResp {
                ok: false,
                error: Some("auth failed".into()),
                session_id: None,
                work_conn_tls: None,
            })
            .to_frame()
            .unwrap(),
        )
        .await
        .unwrap();
    });

    let dir = std::env::temp_dir().join(format!("rfrp-test-{}", uuid::Uuid::new_v4()));
    let run_id_file = dir.join("run_id");
    let cfg = ClientConfig {
        client: ClientSection {
            server_addr: addr.ip().to_string(),
            server_port: addr.port(),
            run_id_file: Some(run_id_file.to_string_lossy().to_string()),
            ..Default::default()
        },
        ..Default::default()
    };

    // 致命失败：run() 应在数秒内返回 Err（而非无限重连）。
    let res = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        rfrpc::client::Client::new(cfg).unwrap().run(),
    )
    .await;
    assert!(
        res.is_ok(),
        "client must exit on fatal login, not reconnect forever"
    );
    assert!(res.unwrap().is_err(), "fatal login should yield Err");

    srv.await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
