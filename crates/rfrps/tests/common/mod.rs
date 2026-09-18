//! rfrps 集成测试共享工具。
#![allow(dead_code)]

use rfrp_common::config::{LogSection, ProxySection, ServerConfig, ServerSection};

/// 测试端口分配（统一实现见 [`rfrp_common::testutil::free_port`]）。
#[allow(unused_imports)] // 各测试二进制按需使用，未使用时不报错。
pub use rfrp_common::testutil::free_port;

/// 基础服务端配置：回环地址、随机控制端口、关闭 TLS 与 Dashboard。
pub fn base_server_config() -> ServerConfig {
    ServerConfig {
        server: ServerSection {
            bind_addr: "127.0.0.1".into(),
            bind_port: 0,
            token: "".into(),
            tls_enable: false,
            tls_cert: None,
            tls_key: None,
            work_conn_tls: false,
            tcp_keepalive_secs: None,
            heartbeat_interval_secs: None,
            heartbeat_timeout_secs: None,
        },
        dashboard: None,
        proxy: ProxySection::default(),
        log: LogSection::default(),
    }
}
