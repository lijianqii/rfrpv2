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

    ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| config(format!("failed to build server TLS config: {e}")))
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
}
