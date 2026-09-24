//! 端到端验证致命失败（鉴权失败 / TLS 证书校验失败）时客户端不进入无限重连，而是退出（§8.1）。

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
                work_conn_token: None,
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

#[tokio::test]
async fn fatal_tls_certificate_error_exits_without_reconnect() {
    // 自签服务端 + 客户端未配置 tls_ca（使用系统根证书）→ 证书校验必然失败。
    // 这属于配置问题，重试不会自愈：客户端应直接退出，而不是无限退避重连。
    let base = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let server_tls = rfrp_common::crypto::ServerTls::new(
        &base.join("tests/certs/server-cert.pem"),
        &base.join("tests/certs/server-key.pem"),
    )
    .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let srv = tokio::spawn(async move {
        if let Ok((stream, _peer)) = listener.accept().await {
            // 客户端校验失败会中止握手，服务端 accept 返回错误属预期。
            let _ = server_tls.accept(stream).await;
        }
    });

    let dir = std::env::temp_dir().join(format!("rfrp-tls-{}", uuid::Uuid::new_v4()));
    let cfg = ClientConfig {
        client: ClientSection {
            server_addr: addr.ip().to_string(),
            server_port: addr.port(),
            tls_enable: true,
            tls_server_name: Some("localhost".into()),
            run_id_file: Some(dir.join("run_id").to_string_lossy().to_string()),
            ..Default::default()
        },
        ..Default::default()
    };

    let res = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        rfrpc::client::Client::new(cfg).unwrap().run(),
    )
    .await;
    assert!(
        res.is_ok(),
        "client must exit on fatal TLS error, not reconnect forever"
    );
    assert!(res.unwrap().is_err(), "fatal TLS error should yield Err");

    srv.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn fatal_login_version_mismatch_exits_without_reconnect() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // 模拟服务端：读 Login 首帧，回 LoginResp{ok=false, "version mismatch"} 后关闭。
    let srv = tokio::spawn(async move {
        let (stream, _peer) = listener.accept().await.unwrap();
        let (frame, stream) = read_one_frame(stream).await.unwrap();
        assert!(matches!(Message::from_frame(&frame), Ok(Message::Login(_))));
        let mut w = FramedWrite::new(stream, FrameCodec);
        w.send(
            Message::LoginResp(LoginResp {
                ok: false,
                error: Some("version mismatch".into()),
                session_id: None,
                work_conn_tls: None,
                work_conn_token: None,
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

    // 致命失败（版本不匹配）：run() 应返回 Err 而非无限重连。
    let res = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        rfrpc::client::Client::new(cfg).unwrap().run(),
    )
    .await;
    assert!(
        res.is_ok(),
        "client must exit on fatal version mismatch, not reconnect forever"
    );
    assert!(res.unwrap().is_err(), "fatal login should yield Err");

    srv.await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
