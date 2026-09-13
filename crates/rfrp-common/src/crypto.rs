//! TLS 封装（rustls + tokio-rustls）。
//!
//! 负责加载服务端证书/私钥、构建客户端根证书库，并封装 `TlsAcceptor` / `TlsConnector`，
//! 供 `rfrps` / `rfrpc` 使用。当前只支持控制链路与工作连接的 TLS，不涉及 HTTPS vhost。

use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, ServerName};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::TlsConnector;

use crate::config::ClientSection;
use crate::error::{config, Error, Result};

/// 确保 rustls 使用 ring 作为默认 CryptoProvider。
/// 项目通过 `rustls` 的 `ring` feature 提供加密后端；
/// 在构建任何 TLS 配置前调用一次，避免多 provider 时无法自动选择。
fn ensure_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

pub use tokio_rustls::client::TlsStream as ClientTlsStream;
pub use tokio_rustls::server::TlsStream as ServerTlsStream;

/// 从 PEM 文件加载服务端 TLS 配置（证书 + 私钥）。
pub fn load_server_tls(cert_path: &Path, key_path: &Path) -> Result<ServerConfig> {
    let cert_file = File::open(cert_path)
        .map_err(|e| config(format!("cannot read TLS cert {}: {e}", cert_path.display())))?;
    let mut cert_reader = BufReader::new(cert_file);
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_reader)
        .collect::<std::result::Result<_, _>>()
        .map_err(|e| config(format!("invalid TLS cert {}: {e}", cert_path.display())))?;
    if certs.is_empty() {
        return Err(config(format!(
            "no certificate found in {}",
            cert_path.display()
        )));
    }

    let key_file = File::open(key_path)
        .map_err(|e| config(format!("cannot read TLS key {}: {e}", key_path.display())))?;
    let mut key_reader = BufReader::new(key_file);
    let key = rustls_pemfile::private_key(&mut key_reader)
        .map_err(|e| config(format!("invalid TLS key {}: {e}", key_path.display())))?
        .ok_or_else(|| config(format!("no private key found in {}", key_path.display())))?;

    let mut cfg = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| config(format!("failed to build server TLS config: {e}")))?;
    enable_session_resumption(&mut cfg)?;
    Ok(cfg)
}

/// 启用 TLS 1.3 会话票据（session ticket），使重复握手可恢复。
///
/// rustls 服务端默认 `ticketer = NeverProducesTickets`，即 **TLS 1.3 无法恢复会话**；
/// 对频繁建立工作连接的场景（如 SSH + `pool_size = 0`），每次连接都要做完整握手
/// （跨网 2 RTT）。启用票据后恢复握手只需 1 RTT。TLS 1.2 的会话缓存默认已启用。
fn enable_session_resumption(cfg: &mut ServerConfig) -> Result<()> {
    cfg.ticketer = rustls::crypto::ring::Ticketer::new()
        .map_err(|e| config(format!("failed to init TLS ticketer: {e}")))?;
    Ok(())
}

/// 从 PEM 文件加载客户端根证书；未指定 CA 时使用 webpki 内置根证书。
fn load_root_cert_store(ca_path: Option<&Path>) -> Result<RootCertStore> {
    let mut roots = RootCertStore::empty();
    match ca_path {
        Some(path) => {
            let file = File::open(path)
                .map_err(|e| config(format!("cannot read CA file {}: {e}", path.display())))?;
            let mut reader = BufReader::new(file);
            for cert in rustls_pemfile::certs(&mut reader) {
                let cert =
                    cert.map_err(|e| config(format!("invalid CA cert {}: {e}", path.display())))?;
                roots.add(cert).map_err(|e| {
                    config(format!("failed to add CA cert {}: {e}", path.display()))
                })?;
            }
            if roots.is_empty() {
                return Err(config(format!(
                    "no CA certificate found in {}",
                    path.display()
                )));
            }
        }
        None => {
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        }
    }
    Ok(roots)
}

/// 服务端 TLS 封装：持有 `TlsAcceptor`。
#[derive(Clone)]
pub struct ServerTls {
    acceptor: TlsAcceptor,
}

impl ServerTls {
    /// 从证书/私钥路径构建。
    pub fn new(cert_path: &Path, key_path: &Path) -> Result<Self> {
        ensure_crypto_provider();
        let config = load_server_tls(cert_path, key_path)?;
        tracing::debug!(cert = %cert_path.display(), "server TLS config loaded");
        Ok(Self {
            acceptor: TlsAcceptor::from(Arc::new(config)),
        })
    }

    /// 对已建立的 TCP 流执行 TLS 握手。
    pub async fn accept<S>(&self, stream: S) -> Result<ServerTlsStream<S>>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        self.acceptor
            .accept(stream)
            .await
            .map_err(|e| Error::Other(format!("TLS accept failed: {e}")))
    }
}

/// 客户端 TLS 封装：持有 `TlsConnector` 与要校验的 `ServerName`。
#[derive(Clone)]
pub struct ClientTls {
    connector: TlsConnector,
    server_name: ServerName<'static>,
}

impl ClientTls {
    /// 根据客户端配置构建。
    pub fn new(section: &ClientSection) -> Result<Self> {
        ensure_crypto_provider();
        let server_name = section.tls_server_name.as_deref().ok_or_else(|| {
            crate::error::config("tls_server_name is required when TLS is enabled")
        })?;
        let server_name = ServerName::try_from(server_name.to_string())
            .map_err(|e| crate::error::config(format!("invalid tls_server_name: {e}")))?;

        let ca_path = section.tls_ca.as_deref().map(Path::new);
        let roots = load_root_cert_store(ca_path)?;
        let config = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        tracing::debug!(server_name = ?server_name, "client TLS config loaded");

        Ok(Self {
            connector: TlsConnector::from(Arc::new(config)),
            server_name,
        })
    }

    /// 对已建立的 TCP 流执行 TLS 握手。
    pub async fn connect<S>(&self, stream: S) -> Result<ClientTlsStream<S>>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        self.connector
            .connect(self.server_name.clone(), stream)
            .await
            .map_err(|e| Error::Other(format!("TLS connect failed: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ClientSection;
    use rustls::HandshakeKind;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    fn example_dir() -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples")
    }

    fn client_section(server_name: &str, ca: Option<&Path>) -> ClientSection {
        ClientSection {
            tls_server_name: Some(server_name.to_string()),
            tls_ca: ca.map(|p| p.display().to_string()),
            ..Default::default()
        }
    }

    #[test]
    fn client_tls_requires_server_name() {
        let cfg = ClientSection {
            tls_enable: true,
            tls_server_name: None,
            ..Default::default()
        };
        assert!(ClientTls::new(&cfg).is_err());
    }

    #[test]
    fn client_tls_accepts_server_name() {
        let cfg = ClientSection {
            tls_server_name: Some("example.com".into()),
            ..Default::default()
        };
        assert!(ClientTls::new(&cfg).is_ok());
    }

    #[test]
    fn server_tls_enables_tls13_session_resumption() {
        // rustls 服务端默认不产生 TLS 1.3 票据；启用后 ticketer.enabled() 为 true。
        let dir = example_dir();
        let cfg = load_server_tls(&dir.join("cert.pem"), &dir.join("key.pem"))
            .expect("load example cert");
        assert!(
            cfg.ticketer.enabled(),
            "TLS 1.3 session tickets must be enabled for resumption"
        );
    }

    /// 用一个自签服务端证书与指定客户端参数发起一次真实 TLS 握手，断言客户端拒绝。
    async fn expect_tls_rejected(server_name: &str, ca_file: Option<&str>) {
        let dir = example_dir();
        let server_tls =
            ServerTls::new(&dir.join("cert.pem"), &dir.join("key.pem")).expect("server tls");
        let ca = ca_file.map(|f| dir.join(f));
        let client_tls =
            ClientTls::new(&client_section(server_name, ca.as_deref())).expect("client tls");

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            // 客户端会因证书校验失败中止握手，服务端 accept 返回错误属预期。
            let _ = server_tls.accept(stream).await;
        });

        let result = client_tls
            .connect(TcpStream::connect(addr).await.unwrap())
            .await;
        assert!(
            result.is_err(),
            "client must reject the TLS handshake (server_name={server_name})"
        );
        let _ = server.await;
    }

    #[tokio::test]
    async fn client_tls_rejects_certificate_from_untrusted_ca() {
        // 不配置 tls_ca 时使用系统/webpki 内置根证书；示例自签服务端必须被拒绝。
        // （同为“错 CA”场景：即便证书本身有效，不在信任链中也不得放行。）
        expect_tls_rejected("localhost", None).await;
    }

    #[tokio::test]
    async fn client_tls_rejects_server_name_mismatch() {
        // CA 可信但 server_name 与证书 SAN 不匹配：必须拒绝（防中间人）。
        expect_tls_rejected("wrong.example.com", Some("ca.pem")).await;
    }

    #[tokio::test]
    async fn tls13_session_resumption_occurs_on_second_connection() {
        // 真实握手验证：第一次 Full，第二次用同一客户端配置应命中会话恢复（Resumed）。
        // 仅断言 ticketer 已启用不足以防止“ticket 未下发/未缓存”的回归。
        let dir = example_dir();
        let server_tls =
            ServerTls::new(&dir.join("cert.pem"), &dir.join("key.pem")).expect("server tls");
        let client_tls = ClientTls::new(&client_section("localhost", Some(&dir.join("ca.pem"))))
            .expect("client tls");

        let listener = Arc::new(TcpListener::bind("127.0.0.1:0").await.unwrap());
        let addr = listener.local_addr().unwrap();

        let mut kinds = Vec::new();
        for _ in 0..2 {
            let listener = listener.clone();
            let server_tls = server_tls.clone();
            let accept = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut tls = server_tls.accept(stream).await.unwrap();
                // 一次完整往返，确保双方握手完成后服务端已下发 NewSessionTicket。
                let mut b = [0u8; 1];
                tls.read_exact(&mut b).await.unwrap();
                tls.write_all(b"x").await.unwrap();
                tls.flush().await.unwrap();
                // 保持片刻，让客户端有时间读取并处理票据。
                tokio::time::sleep(Duration::from_millis(100)).await;
            });

            let mut tls = client_tls
                .connect(TcpStream::connect(addr).await.unwrap())
                .await
                .expect("client handshake");
            tls.write_all(b"x").await.unwrap();
            let mut b = [0u8; 1];
            tls.read_exact(&mut b).await.unwrap();
            // 若有 NewSessionTicket（或后续字节），在关闭前读完/超时，确保票据入缓存。
            let mut extra = [0u8; 1];
            let _ = tokio::time::timeout(Duration::from_millis(100), tls.read(&mut extra)).await;
            kinds.push(tls.get_ref().1.handshake_kind());
            drop(tls);
            accept.await.unwrap();
        }

        assert_eq!(
            kinds[0],
            Some(HandshakeKind::Full),
            "first handshake is full"
        );
        assert_eq!(
            kinds[1],
            Some(HandshakeKind::Resumed),
            "second handshake must resume the TLS 1.3 session"
        );
    }
}
