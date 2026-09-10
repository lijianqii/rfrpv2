//! 客户端主循环与 run_id 管理单元测试。

use super::*;
use rfrp_common::config::{ClientConfig, ClientProxy, ClientSection};
use rfrp_common::constants::MAX_RUN_ID_LEN;
use rfrp_common::protocol::msg::{NewProxy, ProxyType};

#[test]
fn new_proxy_from_config_maps_fields() {
    let p = ClientProxy {
        name: "web".into(),
        r#type: ProxyType::Tcp,
        local_ip: "10.0.0.2".into(),
        local_port: 22,
        remote_port: Some(8022),
        custom_domains: Some(vec!["a.example.com".into()]),
        pool_size: 2,
    };
    let np: NewProxy = new_proxy_from_config(&p);
    assert_eq!(np.proxy_name, "web");
    assert_eq!(np.r#type, ProxyType::Tcp);
    assert_eq!(np.remote_port, Some(8022));
    assert_eq!(np.custom_domains, Some(vec!["a.example.com".into()]));
}

#[test]
fn run_id_persisted_and_reused() {
    let dir = std::env::temp_dir().join(format!("rfrp-test-{}", uuid::Uuid::new_v4()));
    let path = dir.join("run_id");
    let r1 = Client::new(cfg_for(&path)).unwrap().load_or_create_run_id();
    let r2 = Client::new(cfg_for(&path)).unwrap().load_or_create_run_id();
    assert!(!r1.is_empty());
    assert_eq!(r1, r2, "run_id must be reused across starts");
    let _ = std::fs::remove_dir_all(&dir);
}

// 构造仅覆盖 run_id_file 的 ClientConfig，避免依赖完整字段。
fn cfg_for(path: &std::path::Path) -> ClientConfig {
    ClientConfig {
        client: ClientSection {
            run_id_file: Some(path.to_string_lossy().to_string()),
            status_addr: None,
            ..Default::default()
        },
        ..Default::default()
    }
}

#[test]
fn new_fails_fast_when_tls_required_but_unconfigurable() {
    // ClientTls 缓存提前到 Client::new：work_conn_tls=true 但缺 tls_server_name，
    // 应立刻报错而非等到首次重连才发现（§6.5 负路径）。
    let cfg = ClientConfig {
        client: ClientSection {
            work_conn_tls: true,
            tls_server_name: None,
            ..Default::default()
        },
        ..Default::default()
    };
    let err = match Client::new(cfg) {
        Ok(_) => panic!("expected Err when TLS required but not configurable"),
        Err(e) => e,
    };
    assert!(err.to_string().contains("tls_server_name"), "{err}");
}

#[test]
fn run_id_empty_file_regenerates() {
    // 文件存在但内容为空/空白：应重新生成非空 run_id（§6.6）。
    let dir = std::env::temp_dir().join(format!("rfrp-test-{}", uuid::Uuid::new_v4()));
    let path = dir.join("run_id");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(&path, "   \n").unwrap();
    let rid = Client::new(cfg_for(&path)).unwrap().load_or_create_run_id();
    assert!(!rid.trim().is_empty());
    assert_ne!(rid, "   \n");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn run_id_too_long_regenerates() {
    // 文件内容超过 MAX_RUN_ID_LEN：应重新生成合规 run_id（§6.6）。
    let dir = std::env::temp_dir().join(format!("rfrp-test-{}", uuid::Uuid::new_v4()));
    let path = dir.join("run_id");
    std::fs::create_dir_all(&dir).unwrap();
    let long = "x".repeat(MAX_RUN_ID_LEN + 1);
    std::fs::write(&path, &long).unwrap();
    let rid = Client::new(cfg_for(&path)).unwrap().load_or_create_run_id();
    assert_ne!(rid, long);
    assert!(rid.len() <= MAX_RUN_ID_LEN);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn run_id_non_uuid_regenerates() {
    // 文件内容不是合法 UUID 时，应重新生成（DESIGN §6.2.1）。
    let dir = std::env::temp_dir().join(format!("rfrp-test-{}", uuid::Uuid::new_v4()));
    let path = dir.join("run_id");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(&path, "not-a-uuid").unwrap();
    let rid = Client::new(cfg_for(&path)).unwrap().load_or_create_run_id();
    assert!(uuid::Uuid::parse_str(&rid).is_ok());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn run_id_empty_string_uses_default_path() {
    // DESIGN §9.2：`run_id_file = ""` 应等价于未配置，使用默认 `~/.rfrp/run_id`。
    let none = resolve_run_id_path(&None);
    let empty = resolve_run_id_path(&Some(String::new()));
    let blank = resolve_run_id_path(&Some("   ".to_string()));
    assert_eq!(empty, none);
    assert_eq!(blank, none);

    let custom = resolve_run_id_path(&Some("/tmp/custom-run-id".to_string()));
    assert_eq!(custom, PathBuf::from("/tmp/custom-run-id"));
}

#[tokio::test]
async fn reconnect_delay_completes_without_shutdown() {
    // 未收到退出信号时，wait_for_reconnect 应等满退避并返回 true。
    let shutdown = CancellationToken::new();
    let ok = wait_for_reconnect(Duration::from_millis(10), &shutdown).await;
    assert!(ok);
}
#[tokio::test]
async fn reconnect_delay_is_interruptible_by_shutdown() {
    // 若退出信号落在退避 sleep 期间，wait_for_reconnect 应立即返回 false，
    // 避免客户端在 30s 退避期间无法及时退出（§8.3 / §14.4）。
    let shutdown = CancellationToken::new();
    let shutdown_for_task = shutdown.clone();
    let start = tokio::time::Instant::now();
    let task = tokio::spawn(async move {
        wait_for_reconnect(
            Duration::from_secs(RECONNECT_BACKOFF_MAX),
            &shutdown_for_task,
        )
        .await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    shutdown.cancel();

    let ok = tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("wait_for_reconnect must return promptly after shutdown")
        .unwrap();
    assert!(!ok, "shutdown during backoff should abort the wait");
    assert!(
        start.elapsed() < Duration::from_secs(1),
        "should not wait out the full backoff after shutdown"
    );
}
