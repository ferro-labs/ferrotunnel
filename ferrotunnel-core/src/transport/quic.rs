//! QUIC transport using quinn
//!
//! Provides QUIC-based connections with built-in TLS 1.3 encryption and
//! native stream multiplexing. The 0-RTT option currently uses a full handshake.

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
    pub client_auth: bool,
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
            client_auth: false,
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
            client_auth: false,
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
    create_quinn_client_config_with_target(config, config.server_name.as_deref())
}

fn create_quinn_client_config_with_target(
    config: &QuicTransportConfig,
    server_name: Option<&str>,
) -> io::Result<ClientConfig> {
    // Reuse TLS config creation from tls.rs
    let tls_config = tls::TlsTransportConfig {
        ca_cert_path: config.ca_cert_path.clone(),
        cert_path: config.cert_path.clone(),
        key_path: config.key_path.clone(),
        server_name: config.server_name.clone(),
        client_auth: false,
        skip_verify: config.skip_verify,
    };

    let rustls_config = tls::create_client_config_with_context(&tls_config, "QUIC", server_name)?;

    let mut transport = quinn::TransportConfig::default();
    transport.max_idle_timeout(Some(config.max_idle_timeout.try_into().map_err(|e| {
        io::Error::new(ErrorKind::InvalidInput, format!("idle timeout: {e}"))
    })?));
    transport.keep_alive_interval(Some(config.keep_alive_interval));
    transport.max_concurrent_bidi_streams(VarInt::from_u32(256));

    let mut client_config = ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(rustls::ClientConfig::clone(
            &rustls_config,
        ))
        .map_err(|e| io::Error::new(ErrorKind::InvalidData, format!("QUIC client config: {e}")))?,
    ));
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
        client_auth: config.client_auth,
        skip_verify: false,
    };

    let rustls_config = tls::create_server_config(&tls_config)?;

    let mut transport = quinn::TransportConfig::default();
    transport.max_idle_timeout(Some(config.max_idle_timeout.try_into().map_err(|e| {
        io::Error::new(ErrorKind::InvalidInput, format!("idle timeout: {e}"))
    })?));
    transport.max_concurrent_bidi_streams(VarInt::from_u32(256));

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
    create_client_endpoint_with_target(config, config.server_name.as_deref())
}

fn create_client_endpoint_with_target(
    config: &QuicTransportConfig,
    server_name: Option<&str>,
) -> io::Result<Endpoint> {
    let client_config = create_quinn_client_config_with_target(config, server_name)?;
    let mut endpoint = Endpoint::client(
        "0.0.0.0:0"
            .parse()
            .map_err(|e| io::Error::new(ErrorKind::InvalidInput, format!("bind address: {e}")))?,
    )?;
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
///
/// Returns both the [`Endpoint`] and the [`quinn::Connection`]. The caller owns the
/// endpoint lifecycle and should call `endpoint.wait_idle().await` after the session
/// ends to ensure the underlying UDP socket is released.
pub async fn connect(
    addr: &str,
    config: &QuicTransportConfig,
) -> io::Result<(Endpoint, quinn::Connection)> {
    let (host, port) = split_host_port(addr)?;
    let socket_addr = tokio::net::lookup_host((host.as_str(), port))
        .await?
        .next()
        .ok_or_else(|| io::Error::new(ErrorKind::AddrNotAvailable, "address resolution failed"))?;

    let server_name = config.server_name.clone().unwrap_or(host);
    let endpoint = create_client_endpoint_with_target(config, Some(&server_name))?;

    // Validate server name
    let _: ServerName<'_> = ServerName::try_from(server_name.as_str()).map_err(|e| {
        io::Error::new(ErrorKind::InvalidInput, format!("invalid server name: {e}"))
    })?;

    if config.enable_0rtt {
        tracing::warn!("QUIC 0-RTT is not yet implemented; falling back to full handshake");
    }

    let connection = endpoint
        .connect(socket_addr, &server_name)
        .map_err(|e| io::Error::new(ErrorKind::ConnectionRefused, format!("QUIC connect: {e}")))?
        .await
        .map_err(|e| {
            io::Error::new(ErrorKind::ConnectionRefused, format!("QUIC handshake: {e}"))
        })?;

    Ok((endpoint, connection))
}

fn split_host_port(addr: &str) -> io::Result<(String, u16)> {
    if let Ok(socket_addr) = addr.parse::<SocketAddr>() {
        return Ok((socket_addr.ip().to_string(), socket_addr.port()));
    }

    if let Some(rest) = addr.strip_prefix('[') {
        let Some((host, port_str)) = rest.split_once("]:") else {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                format!("invalid bracketed address: {addr}"),
            ));
        };
        let port = port_str.parse::<u16>().map_err(|e| {
            io::Error::new(
                ErrorKind::InvalidInput,
                format!("invalid port in address: {e}"),
            )
        })?;
        return Ok((host.to_string(), port));
    }

    let Some((host, port_str)) = addr.rsplit_once(':') else {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            format!("missing port in address: {addr}"),
        ));
    };
    let port = port_str.parse::<u16>().map_err(|e| {
        io::Error::new(
            ErrorKind::InvalidInput,
            format!("invalid port in address: {e}"),
        )
    })?;
    Ok((host.to_string(), port))
}

/// Accept a QUIC connection from the endpoint.
///
/// The caller owns the endpoint lifecycle and is responsible for closing it
/// (e.g. via `endpoint.wait_idle().await`) when the server shuts down.
pub async fn accept(endpoint: &Endpoint) -> io::Result<(quinn::Connection, SocketAddr)> {
    let incoming = endpoint
        .accept()
        .await
        .ok_or_else(|| io::Error::new(ErrorKind::ConnectionAborted, "endpoint closed"))?;

    let addr = incoming.remote_address();

    let connection = incoming
        .await
        .map_err(|e| io::Error::new(ErrorKind::ConnectionRefused, format!("QUIC accept: {e}")))?;

    Ok((connection, addr))
}

/// Close a QUIC connection gracefully.
pub fn close_connection(connection: &quinn::Connection) {
    connection.close(VarInt::from_u32(0), b"goodbye");
}
