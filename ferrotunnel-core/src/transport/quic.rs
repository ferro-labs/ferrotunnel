//! QUIC transport using quinn
//!
//! Provides QUIC-based connections with built-in TLS 1.3 encryption,
//! native stream multiplexing, and optional 0-RTT reconnection.

use super::tls;
use ferrotunnel_common::QuicConfig;
use quinn::{ClientConfig, Endpoint, ServerConfig, VarInt};
use rustls::pki_types::ServerName;
use std::io::{self, ErrorKind};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

/// Default max idle timeout for QUIC connections (30 seconds).
const DEFAULT_MAX_IDLE_TIMEOUT_SECS: u64 = 30;

/// Default keep-alive interval (10 seconds).
const DEFAULT_KEEP_ALIVE_INTERVAL_SECS: u64 = 10;

/// QUIC-specific transport configuration.
#[derive(Debug, Clone)]
pub struct QuicTransportConfig {
    pub cert_path: String,
    pub key_path: String,
    pub ca_cert_path: Option<String>,
    pub server_name: Option<String>,
    pub skip_verify: bool,
    pub enable_0rtt: bool,
    pub max_idle_timeout: Duration,
    pub keep_alive_interval: Duration,
}

impl Default for QuicTransportConfig {
    fn default() -> Self {
        Self {
            cert_path: String::new(),
            key_path: String::new(),
            ca_cert_path: None,
            server_name: None,
            skip_verify: false,
            enable_0rtt: false,
            max_idle_timeout: Duration::from_secs(DEFAULT_MAX_IDLE_TIMEOUT_SECS),
            keep_alive_interval: Duration::from_secs(DEFAULT_KEEP_ALIVE_INTERVAL_SECS),
        }
    }
}

impl QuicTransportConfig {
    /// Build from common [`QuicConfig`] when QUIC is enabled; returns `None` otherwise.
    #[must_use]
    pub fn from_common(config: &QuicConfig) -> Option<Self> {
        if !config.enabled {
            return None;
        }
        Some(Self {
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
            ca_cert_path: config
                .ca_cert_path
                .as_ref()
                .map(|p| p.to_string_lossy().to_string()),
            server_name: config.server_name.clone(),
            skip_verify: config.skip_verify,
            enable_0rtt: config.enable_0rtt,
            max_idle_timeout: Duration::from_secs(
                config
                    .max_idle_timeout_secs
                    .unwrap_or(DEFAULT_MAX_IDLE_TIMEOUT_SECS),
            ),
            keep_alive_interval: Duration::from_secs(
                config
                    .keep_alive_interval_secs
                    .unwrap_or(DEFAULT_KEEP_ALIVE_INTERVAL_SECS),
            ),
        })
    }
}

/// Create a quinn `ClientConfig` by reusing the existing rustls TLS config infrastructure.
pub fn create_quinn_client_config(config: &QuicTransportConfig) -> io::Result<ClientConfig> {
    // Reuse TLS config creation from tls.rs
    let tls_config = tls::TlsTransportConfig {
        ca_cert_path: config.ca_cert_path.clone(),
        cert_path: config.cert_path.clone(),
        key_path: config.key_path.clone(),
        server_name: config.server_name.clone(),
        client_auth: false,
        skip_verify: config.skip_verify,
    };

    let rustls_config = tls::create_client_config(&tls_config)?;

    let mut transport = quinn::TransportConfig::default();
    transport.max_idle_timeout(Some(
        config
            .max_idle_timeout
            .try_into()
            .map_err(|e| io::Error::new(ErrorKind::InvalidInput, format!("idle timeout: {e}")))?,
    ));
    transport.keep_alive_interval(Some(config.keep_alive_interval));

    let mut client_config =
        ClientConfig::new(Arc::new(quinn::crypto::rustls::QuicClientConfig::try_from(
            rustls::ClientConfig::clone(&rustls_config),
        ).map_err(|e| {
            io::Error::new(ErrorKind::InvalidData, format!("QUIC client config: {e}"))
        })?));
    client_config.transport_config(Arc::new(transport));

    Ok(client_config)
}

/// Create a quinn `ServerConfig` by reusing the existing rustls TLS config infrastructure.
pub fn create_quinn_server_config(config: &QuicTransportConfig) -> io::Result<ServerConfig> {
    let tls_config = tls::TlsTransportConfig {
        ca_cert_path: config.ca_cert_path.clone(),
        cert_path: config.cert_path.clone(),
        key_path: config.key_path.clone(),
        server_name: config.server_name.clone(),
        client_auth: false,
        skip_verify: false,
    };

    let rustls_config = tls::create_server_config(&tls_config)?;

    let mut transport = quinn::TransportConfig::default();
    transport.max_idle_timeout(Some(
        config
            .max_idle_timeout
            .try_into()
            .map_err(|e| io::Error::new(ErrorKind::InvalidInput, format!("idle timeout: {e}")))?,
    ));

    let quic_server_config = quinn::crypto::rustls::QuicServerConfig::try_from(
        rustls::ServerConfig::clone(&rustls_config),
    )
    .map_err(|e| io::Error::new(ErrorKind::InvalidData, format!("QUIC server config: {e}")))?;

    let mut server_config = ServerConfig::with_crypto(Arc::new(quic_server_config));
    server_config.transport_config(Arc::new(transport));

    Ok(server_config)
}

/// Create a client endpoint bound to an ephemeral UDP port.
pub fn create_client_endpoint(config: &QuicTransportConfig) -> io::Result<Endpoint> {
    let client_config = create_quinn_client_config(config)?;
    let mut endpoint = Endpoint::client("0.0.0.0:0".parse().map_err(|e| {
        io::Error::new(ErrorKind::InvalidInput, format!("bind address: {e}"))
    })?)?;
    endpoint.set_default_client_config(client_config);
    Ok(endpoint)
}

/// Create a server endpoint bound to the given UDP address.
pub fn create_server_endpoint(
    config: &QuicTransportConfig,
    bind_addr: SocketAddr,
) -> io::Result<Endpoint> {
    let server_config = create_quinn_server_config(config)?;
    Endpoint::server(server_config, bind_addr)
}

/// Connect to a QUIC server.
pub async fn connect(
    addr: &str,
    config: &QuicTransportConfig,
) -> io::Result<quinn::Connection> {
    let endpoint = create_client_endpoint(config)?;

    let socket_addr: SocketAddr = addr
        .parse()
        .map_err(|e| io::Error::new(ErrorKind::InvalidInput, format!("invalid address: {e}")))?;

    let server_name = if let Some(name) = &config.server_name {
        name.clone()
    } else {
        addr.split(':')
            .next()
            .unwrap_or("localhost")
            .to_string()
    };

    // Validate server name
    let _: ServerName<'_> = ServerName::try_from(server_name.as_str()).map_err(|e| {
        io::Error::new(ErrorKind::InvalidInput, format!("invalid server name: {e}"))
    })?;

    let connection = endpoint
        .connect(socket_addr, &server_name)
        .map_err(|e| io::Error::new(ErrorKind::ConnectionRefused, format!("QUIC connect: {e}")))?
        .await
        .map_err(|e| {
            io::Error::new(
                ErrorKind::ConnectionRefused,
                format!("QUIC handshake: {e}"),
            )
        })?;

    Ok(connection)
}

/// Accept a QUIC connection from the endpoint.
pub async fn accept(endpoint: &Endpoint) -> io::Result<(quinn::Connection, SocketAddr)> {
    let incoming = endpoint
        .accept()
        .await
        .ok_or_else(|| io::Error::new(ErrorKind::ConnectionAborted, "endpoint closed"))?;

    let addr = incoming.remote_address();

    let connection = incoming.await.map_err(|e| {
        io::Error::new(
            ErrorKind::ConnectionRefused,
            format!("QUIC accept: {e}"),
        )
    })?;

    Ok((connection, addr))
}

/// Close a QUIC connection gracefully.
pub fn close_connection(connection: &quinn::Connection) {
    connection.close(VarInt::from_u32(0), b"goodbye");
}
