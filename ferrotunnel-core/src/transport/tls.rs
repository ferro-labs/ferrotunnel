//! TLS transport using rustls

use super::socket_tuning::configure_socket_silent;
use super::BoxedStream;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use rustls_pki_types::pem::PemObject;
use std::io::{self, ErrorKind};
use std::path::Path;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio_rustls::{TlsAcceptor, TlsConnector};

#[derive(Debug, Clone, Default)]
pub struct TlsTransportConfig {
    pub ca_cert_path: Option<String>,
    pub cert_path: String,
    pub key_path: String,
    pub server_name: Option<String>,
    pub client_auth: bool,
    /// Skip certificate verification (insecure, for self-signed certs)
    pub skip_verify: bool,
}

impl TlsTransportConfig {
    /// Build from common [`ferrotunnel_common::TlsConfig`] when TLS is enabled; returns `None` otherwise.
    #[must_use]
    pub fn from_common(config: &ferrotunnel_common::TlsConfig) -> Option<Self> {
        if !config.enabled {
            return None;
        }
        Some(Self {
            ca_cert_path: config
                .ca_cert_path
                .as_ref()
                .map(|p| p.to_string_lossy().to_string()),
            cert_path: config
                .cert_path
                .as_ref()
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_default(),
            key_path: config
                .key_path
                .as_ref()
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_default(),
            server_name: config.server_name.clone(),
            client_auth: config.client_auth,
            skip_verify: config.skip_verify,
        })
    }
}

fn load_certs(path: &Path) -> io::Result<Vec<CertificateDer<'static>>> {
    CertificateDer::pem_file_iter(path)
        .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))
}

fn load_private_key(path: &Path) -> io::Result<PrivateKeyDer<'static>> {
    PrivateKeyDer::from_pem_file(path).map_err(|e| io::Error::new(ErrorKind::InvalidData, e))
}

/// A verifier that accepts any certificate (insecure, for self-signed certs)
#[derive(Debug)]
struct InsecureServerCertVerifier;

impl rustls::client::danger::ServerCertVerifier for InsecureServerCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
            rustls::SignatureScheme::ED25519,
        ]
    }
}

pub fn create_client_config(config: &TlsTransportConfig) -> io::Result<Arc<ClientConfig>> {
    create_client_config_with_context(config, "TLS", config.server_name.as_deref())
}

pub(crate) fn create_client_config_with_context(
    config: &TlsTransportConfig,
    transport: &'static str,
    server_name: Option<&str>,
) -> io::Result<Arc<ClientConfig>> {
    let builder = ClientConfig::builder();

    let builder = if config.skip_verify {
        warn_insecure_verification(transport, server_name);
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(InsecureServerCertVerifier))
    } else {
        let mut root_store = RootCertStore::empty();
        if let Some(ca_path) = &config.ca_cert_path {
            let ca_certs = load_certs(Path::new(ca_path))?;
            for cert in ca_certs {
                root_store.add(cert).map_err(|e| {
                    io::Error::new(ErrorKind::InvalidData, format!("invalid CA cert: {e}"))
                })?;
            }
        } else {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "CA certificate path required for TLS (or use --tls-skip-verify)",
            ));
        }
        builder.with_root_certificates(root_store)
    };

    let client_config = if !config.cert_path.is_empty() && !config.key_path.is_empty() {
        let certs = load_certs(Path::new(&config.cert_path))?;
        let key = load_private_key(Path::new(&config.key_path))?;
        builder
            .with_client_auth_cert(certs, key)
            .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?
    } else {
        builder.with_no_client_auth()
    };

    Ok(Arc::new(client_config))
}

fn warn_insecure_verification(transport: &'static str, server_name: Option<&str>) {
    let server_name = server_name.unwrap_or("<runtime server name>");
    tracing::warn!(
        transport,
        server_name,
        "certificate verification is disabled; peer identity will not be authenticated"
    );
}

pub fn create_server_config(config: &TlsTransportConfig) -> io::Result<Arc<ServerConfig>> {
    let certs = load_certs(Path::new(&config.cert_path))?;
    let key = load_private_key(Path::new(&config.key_path))?;

    let builder = ServerConfig::builder();

    let server_config = if config.client_auth {
        let mut root_store = RootCertStore::empty();
        if let Some(ca_path) = &config.ca_cert_path {
            let ca_certs = load_certs(Path::new(ca_path))?;
            for cert in ca_certs {
                root_store.add(cert).map_err(|e| {
                    io::Error::new(ErrorKind::InvalidData, format!("invalid CA cert: {e}"))
                })?;
            }
        } else {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "CA certificate path required for client authentication",
            ));
        }
        builder
            .with_client_cert_verifier(
                rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store))
                    .build()
                    .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?,
            )
            .with_single_cert(certs, key)
            .map_err(|e| io::Error::new(ErrorKind::InvalidData, format!("TLS config error: {e}")))?
    } else {
        builder
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| io::Error::new(ErrorKind::InvalidData, format!("TLS config error: {e}")))?
    };

    Ok(Arc::new(server_config))
}

pub async fn connect(addr: &str, config: &TlsTransportConfig) -> io::Result<BoxedStream> {
    let configured_server_name = config.server_name.is_some();
    let server_name = config
        .server_name
        .clone()
        .unwrap_or_else(|| addr.split(':').next().unwrap_or("localhost").to_string());
    let client_config = create_client_config_with_context(config, "TLS", Some(&server_name))?;
    let connector = TlsConnector::from(client_config);

    let tcp_stream = TcpStream::connect(addr).await?;
    configure_socket_silent(&tcp_stream);

    let server_name = ServerName::try_from(server_name).map_err(|e| {
        let message = if configured_server_name {
            format!("invalid server name: {e}")
        } else {
            format!("invalid host in address '{addr}': {e}")
        };
        io::Error::new(ErrorKind::InvalidInput, message)
    })?;

    let tls_stream = connector.connect(server_name, tcp_stream).await?;
    Ok(Box::pin(tls_stream))
}

pub async fn accept_tls(
    tcp_stream: TcpStream,
    config: &TlsTransportConfig,
) -> io::Result<tokio_rustls::server::TlsStream<TcpStream>> {
    let server_config = create_server_config(config)?;
    let acceptor = TlsAcceptor::from(server_config);
    acceptor.accept(tcp_stream).await
}

#[cfg(test)]
mod tests {
    use super::TlsTransportConfig;
    use ferrotunnel_common::config::TlsConfig;

    #[test]
    fn from_common_preserves_skip_verify() {
        let config = TlsConfig {
            enabled: true,
            skip_verify: true,
            server_name: Some("localhost".to_string()),
            ..Default::default()
        };

        let tls = TlsTransportConfig::from_common(&config).expect("TLS should be enabled");

        assert!(tls.skip_verify);
        assert_eq!(tls.server_name.as_deref(), Some("localhost"));
    }
}
