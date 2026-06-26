//! Embeddable tunnel server with builder pattern.
//!
//! # Example
//!
//! ```rust,no_run
//! use ferrotunnel::Server;
//!
//! # async fn example() -> ferrotunnel::Result<()> {
//! let mut server = Server::builder()
//!     .bind("0.0.0.0:7835".parse().unwrap())
//!     .http_bind("0.0.0.0:8080".parse().unwrap())
//!     .token("my-secret-token")
//!     .build()?;
//!
//! server.start().await?;
//! tokio::signal::ctrl_c().await?;
//! server.shutdown().await?;
//! # Ok(())
//! # }
//! ```

use crate::config::ServerConfig;
use ferrotunnel_common::config::TlsConfig;
use ferrotunnel_common::{Result, TunnelError};
use ferrotunnel_core::transport::{tls::TlsTransportConfig, TransportConfig};
use ferrotunnel_core::TunnelServer;
use ferrotunnel_http::HttpIngress;
#[cfg(feature = "http3")]
use ferrotunnel_http::{Http3Ingress, Http3IngressConfig};
use ferrotunnel_plugin::PluginRegistry;
use std::net::SocketAddr;
#[cfg(feature = "http3")]
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{watch, RwLock};
use tokio::task::JoinHandle;
use tracing::info;

/// A tunnel server that can be embedded in your application.
///
/// Use [`Server::builder()`] to create a new server with the builder pattern.
#[derive(Debug)]
pub struct Server {
    config: ServerConfig,
    transport_config: TransportConfig,
    shutdown_tx: Option<watch::Sender<bool>>,
    task: Option<JoinHandle<Result<()>>>,
}

/// Builder for constructing a [`Server`] with ergonomic configuration.
#[derive(Debug, Default)]
pub struct ServerBuilder {
    config: ServerConfig,
    transport_config: Option<TransportConfig>,
}

impl Server {
    /// Create a new server builder.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use ferrotunnel::Server;
    ///
    /// let server = Server::builder()
    ///     .bind("0.0.0.0:7835".parse().unwrap())
    ///     .token("secret")
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn builder() -> ServerBuilder {
        ServerBuilder::default()
    }

    /// Start the tunnel server in the background.
    ///
    /// This launches the configured tunnel and ingress services. The server runs until
    /// [`shutdown()`](Self::shutdown) or [`stop()`](Self::stop) is called, or until one
    /// of the service tasks exits.
    ///
    /// # Errors
    ///
    /// Returns an error if the server is already running.
    pub async fn start(&mut self) -> Result<()> {
        if self.is_running() {
            return Err(TunnelError::InvalidState("server already started".into()));
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }

        let config = self.config.clone();
        let transport_config = self.transport_config.clone();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        self.shutdown_tx = Some(shutdown_tx);

        self.task = Some(tokio::spawn(async move {
            Self::run_services(config, transport_config, shutdown_rx).await
        }));

        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    async fn run_services(
        config: ServerConfig,
        transport_config: TransportConfig,
        mut shutdown_rx: watch::Receiver<bool>,
    ) -> Result<()> {
        info!("Starting `FerroTunnel` Server");
        info!("  Tunnel bind: {}", config.bind_addr);
        info!("  HTTP bind: {}", config.http_bind_addr);
        #[cfg(feature = "http3")]
        if let Some(http3_bind_addr) = config.http3_bind_addr {
            info!("  HTTP/3 bind: {} (UDP)", http3_bind_addr);
        }

        #[cfg(feature = "quic")]
        let tunnel_transport = transport_config.clone();
        let tunnel_server =
            TunnelServer::new(config.bind_addr, config.token).with_transport(transport_config);

        // Initialize plugins
        let mut registry = PluginRegistry::new();
        // Add default plugins
        registry.register(Arc::new(RwLock::new(
            ferrotunnel_plugin::builtin::LoggerPlugin::new(),
        )));

        if let Err(e) = registry.init_all().await {
            tracing::error!("Failed to initialize plugins: {}", e);
            // We continue starting the server even if plugins fail, but we log it.
            // Alternatively, we could return the error: return Err(TunnelError::InvalidState(format!("Plugin init failed: {e}")));
        }

        let registry = Arc::new(registry);
        let sessions = tunnel_server.sessions();
        #[cfg(feature = "http3")]
        let alt_svc_header = config.http3_bind_addr.and_then(|addr| {
            Http3IngressConfig::default()
                .alt_svc_header_value(addr)
                .ok()
        });

        let ingress = HttpIngress::new(config.http_bind_addr, sessions.clone(), registry.clone());
        // Shadow (rather than `mut`) so the binding stays immutable when the
        // `http3` feature is disabled and `alt_svc_header` does not exist.
        #[cfg(feature = "http3")]
        let ingress = match alt_svc_header {
            Some(value) => ingress.with_alt_svc_header(value),
            None => ingress,
        };

        // Spawn both services
        #[cfg(feature = "quic")]
        let mut tunnel_handle = {
            let tunnel_bind_addr = config.bind_addr;
            tokio::spawn(async move {
                if matches!(tunnel_transport, TransportConfig::Quic(_)) {
                    tunnel_server.run_quic(tunnel_bind_addr).await
                } else {
                    tunnel_server.run().await
                }
            })
        };
        #[cfg(not(feature = "quic"))]
        let mut tunnel_handle = tokio::spawn(async move { tunnel_server.run().await });
        let mut ingress_handle = tokio::spawn(async move { ingress.start().await });

        #[cfg(feature = "http3")]
        let http3_handle = if let Some(http3_bind_addr) = config.http3_bind_addr {
            let cert_path = config.http3_cert_path.clone().ok_or_else(|| {
                TunnelError::Config("HTTP/3 ingress requires certificate path".into())
            })?;
            let key_path = config
                .http3_key_path
                .clone()
                .ok_or_else(|| TunnelError::Config("HTTP/3 ingress requires key path".into()))?;
            let http3_config = Http3IngressConfig::with_certs(
                cert_path.to_string_lossy().to_string(),
                key_path.to_string_lossy().to_string(),
            );
            let http3_ingress =
                Http3Ingress::new(http3_bind_addr, sessions, registry, http3_config);
            let http3_shutdown_rx = shutdown_rx.clone();
            Some(tokio::spawn(async move {
                http3_ingress.start_with_shutdown(http3_shutdown_rx).await
            }))
        } else {
            None
        };

        // Wait for shutdown or either service to exit
        #[cfg(feature = "http3")]
        if let Some(mut http3_handle) = http3_handle {
            let result = tokio::select! {
                result = &mut tunnel_handle => Self::service_result("Tunnel", result),
                result = &mut ingress_handle => Self::service_result("Ingress", result),
                result = &mut http3_handle => Self::service_result("HTTP/3 ingress", result),
                changed = shutdown_rx.changed() => {
                    Self::log_shutdown_result(&changed);
                    Ok(())
                }
            };
            tunnel_handle.abort();
            ingress_handle.abort();
            http3_handle.abort();
            return result;
        }

        let result = tokio::select! {
            result = &mut tunnel_handle => Self::service_result("Tunnel", result),
            result = &mut ingress_handle => Self::service_result("Ingress", result),
            changed = shutdown_rx.changed() => {
                Self::log_shutdown_result(&changed);
                Ok(())
            }
        };
        tunnel_handle.abort();
        ingress_handle.abort();

        result
    }

    fn service_result(
        name: &str,
        result: std::result::Result<Result<()>, tokio::task::JoinError>,
    ) -> Result<()> {
        match result {
            Ok(inner) => inner,
            Err(e) => Err(TunnelError::Connection(format!(
                "{name} task panicked: {e}"
            ))),
        }
    }

    fn log_shutdown_result(changed: &std::result::Result<(), watch::error::RecvError>) {
        match changed {
            Ok(()) => {
                info!("Server shutdown requested");
            }
            Err(_) => {
                info!("Server shutdown channel closed");
            }
        }
    }

    /// Shutdown the tunnel server and wait for cleanup.
    ///
    /// This will gracefully shut down the server and close all connections.
    pub async fn shutdown(&mut self) -> Result<()> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(true);
        }
        if let Some(task) = self.task.take() {
            match task.await {
                Ok(result) => result?,
                Err(e) => {
                    return Err(TunnelError::Connection(format!(
                        "Server task panicked: {e}"
                    )));
                }
            }
        }
        Ok(())
    }

    /// Signal the server to stop (non-blocking).
    ///
    /// Use [`shutdown()`](Self::shutdown) if you need to wait for cleanup.
    pub fn stop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(true);
        }
    }

    /// Check if the server is currently running.
    pub fn is_running(&self) -> bool {
        if let Some(task) = &self.task {
            !task.is_finished()
        } else {
            false
        }
    }

    /// Get the current configuration.
    pub fn config(&self) -> &ServerConfig {
        &self.config
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        // Best-effort signal shutdown on drop
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(true);
        }
    }
}

impl ServerBuilder {
    /// Set the address to bind the tunnel control plane.
    ///
    /// Default: `0.0.0.0:7835`
    #[must_use]
    pub fn bind(mut self, addr: SocketAddr) -> Self {
        self.config.bind_addr = addr;
        self
    }

    /// Set the address to bind the HTTP ingress.
    ///
    /// Default: `0.0.0.0:8080`
    #[must_use]
    pub fn http_bind(mut self, addr: SocketAddr) -> Self {
        self.config.http_bind_addr = addr;
        self
    }

    /// Enable HTTP/3 ingress on the given UDP address.
    ///
    /// HTTP/3 requires a TLS certificate and private key for QUIC.
    #[cfg(feature = "http3")]
    #[must_use]
    pub fn http3(
        mut self,
        addr: SocketAddr,
        cert_path: impl Into<PathBuf>,
        key_path: impl Into<PathBuf>,
    ) -> Self {
        self.config.http3_bind_addr = Some(addr);
        self.config.http3_cert_path = Some(cert_path.into());
        self.config.http3_key_path = Some(key_path.into());
        self
    }

    /// Set the authentication token.
    ///
    /// Clients must provide this token to connect.
    #[must_use]
    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.config.token = token.into();
        self
    }

    /// Configure TLS for the server.
    ///
    /// When enabled, the server will use TLS for all connections.
    #[must_use]
    pub fn tls(mut self, config: &TlsConfig) -> Self {
        if let Some(tls) = TlsTransportConfig::from_common(config) {
            self.transport_config = Some(TransportConfig::Tls(tls));
        }
        self
    }

    /// Configure QUIC transport for the server.
    ///
    /// When enabled, the server will accept QUIC connections for the tunnel control plane.
    /// QUIC requires TLS 1.3 (built-in), so certificate and key paths are required.
    #[cfg(feature = "quic")]
    #[must_use]
    pub fn quic(mut self, config: &ferrotunnel_common::QuicConfig) -> Self {
        if let Some(quic) =
            ferrotunnel_core::transport::quic::QuicTransportConfig::from_common(config)
        {
            self.transport_config = Some(TransportConfig::Quic(quic));
        }
        self
    }

    /// Build the server with the configured options.
    ///
    /// # Errors
    ///
    /// Returns an error if required configuration is missing:
    /// - `token` must be set
    pub fn build(self) -> Result<Server> {
        self.config.validate()?;
        Ok(Server {
            config: self.config,
            transport_config: self.transport_config.unwrap_or_default(),
            shutdown_tx: None,
            task: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_builder_success() {
        let server = Server::builder()
            .bind("127.0.0.1:7835".parse().unwrap())
            .http_bind("127.0.0.1:8080".parse().unwrap())
            .token("secret-token")
            .build();
        assert!(server.is_ok());
    }

    #[test]
    fn test_server_builder_with_all_options() {
        let server = Server::builder()
            .bind("0.0.0.0:9000".parse().unwrap())
            .http_bind("0.0.0.0:9001".parse().unwrap())
            .token("my-token")
            .build()
            .expect("should build successfully");

        assert_eq!(server.config().bind_addr, "0.0.0.0:9000".parse().unwrap());
        assert_eq!(
            server.config().http_bind_addr,
            "0.0.0.0:9001".parse().unwrap()
        );
        assert_eq!(server.config().token, "my-token");
    }

    #[test]
    fn test_server_builder_missing_token() {
        let result = Server::builder()
            .bind("127.0.0.1:7835".parse().unwrap())
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("token"));
    }

    #[test]
    fn test_server_builder_default_addresses() {
        let server = Server::builder()
            .token("secret")
            .build()
            .expect("should use default addresses");

        assert_eq!(
            server.config().bind_addr,
            SocketAddr::from(([0, 0, 0, 0], 7835))
        );
        assert_eq!(
            server.config().http_bind_addr,
            SocketAddr::from(([0, 0, 0, 0], 8080))
        );
    }

    #[test]
    fn test_server_not_running_initially() {
        let server = Server::builder()
            .token("secret")
            .build()
            .expect("should build");

        assert!(!server.is_running());
    }

    #[tokio::test]
    async fn test_server_start_shutdown_lifecycle() {
        let mut server = Server::builder()
            .bind("127.0.0.1:0".parse().unwrap())
            .http_bind("127.0.0.1:0".parse().unwrap())
            .token("secret")
            .build()
            .expect("should build");

        server.start().await.expect("server should start");
        assert!(server.is_running());

        let err = server
            .start()
            .await
            .expect_err("second start should fail while running");
        assert!(err.to_string().contains("already started"));

        server.shutdown().await.expect("server should shut down");
        assert!(!server.is_running());
    }

    #[test]
    fn test_server_builder_tls_disabled() {
        let tls = TlsConfig {
            enabled: false,
            ..Default::default()
        };
        let server = Server::builder()
            .token("secret")
            .tls(&tls)
            .build()
            .expect("should build");

        // Server should work with TLS disabled
        assert!(!server.config().token.is_empty());
    }
}
