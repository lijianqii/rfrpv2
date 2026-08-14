//! 客户端主控：连接服务端、登录、按配置串行注册代理，然后长驻控制循环。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::Result as AnyResult;
use rfrp_common::config::{ClientConfig, ClientProxy};
use rfrp_common::error::Result as RfrpResult;
use rfrp_common::protocol::msg::*;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Duration;

use crate::control;

/// 客户端运行时共享状态（控制循环与注册逻辑共享）。
pub struct ClientState {
    pub server_addr: std::net::SocketAddr,
    pub proxies: Vec<ClientProxy>,
    /// proxy_name → NewProxyResp 的一次性回传通道（注册时 await）。
    pub resps: Mutex<HashMap<String, oneshot::Sender<NewProxyResp>>>,
}

/// rfrpc 客户端实例。
pub struct Client {
    config: ClientConfig,
}

impl Client {
    pub fn new(config: ClientConfig) -> RfrpResult<Self> {
        Ok(Self { config })
    }

    /// 连接服务端并完成注册，随后长驻控制循环直到断开。
    pub async fn run(self) -> AnyResult<()> {
        let server_addr = self.config.client.server_socket_addr()?;
        let stream = TcpStream::connect(server_addr).await?;
        tracing::info!(server = %server_addr, "connected to server");

        let state = Arc::new(ClientState {
            server_addr,
            proxies: self.config.proxies.clone(),
            resps: Mutex::new(HashMap::new()),
        });
        let (tx, rx) = mpsc::channel::<Message>(64);

        // 先启动控制循环（消费 rx、处理 NewProxyResp / ReqWorkConn / Heartbeat），
        // 否则注册阶段 await NewProxyResp 会因控制循环尚未运行而超时（见 DESIGN §8.1）。
        let ctrl = tokio::spawn(control::control_loop(
            stream,
            rx,
            state.clone(),
            self.config.clone(),
        ));
        tracing::info!(count = state.proxies.len(), "registering proxies");

        // 串行注册代理：每个 NewProxy 发送后等待对应 NewProxyResp。
        for p in &state.proxies {
            let (otx, orx) = oneshot::channel();
            state.resps.lock().unwrap().insert(p.name.clone(), otx);
            let np = new_proxy_from_config(p);
            if tx.send(Message::NewProxy(np)).await.is_err() {
                anyhow::bail!("control connection closed during proxy registration");
            }
            match tokio::time::timeout(Duration::from_secs(10), orx).await {
                Ok(Ok(resp)) => {
                    if resp.ok {
                        tracing::info!(proxy = %p.name, "proxy registered");
                    } else {
                        tracing::warn!(proxy = %p.name, error = ?resp.error, "proxy registration rejected");
                    }
                }
                Ok(Err(_)) => {
                    tracing::warn!(proxy = %p.name, "registration response channel dropped")
                }
                Err(_) => tracing::warn!(proxy = %p.name, "registration response timeout"),
            }
        }

        // 注册完成后长驻控制循环（直到断开）。
        let _ = ctrl.await;
        Ok(())
    }
}

/// 由配置条目构造 `NewProxy` 控制消息。
pub fn new_proxy_from_config(p: &ClientProxy) -> NewProxy {
    NewProxy {
        proxy_name: p.name.clone(),
        r#type: p.r#type,
        remote_port: p.remote_port,
        custom_domains: p.custom_domains.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
}
