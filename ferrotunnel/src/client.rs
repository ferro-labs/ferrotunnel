//! Embeddable tunnel client with builder pattern.
//!
//! # Example
//!
//! ```rust,no_run
//! use ferrotunnel::Client;
//!
//! # async fn example() -> ferrotunnel::Result<()> {
//! let mut client = Client::builder()
//!     .server_addr("tunnel.example.com:7835")
//!     .token("my-secret-token")
//!     .local_addr("127.0.0.1:8080")
//!     .build()?;
//!
//! let info = client.start().await?;
//! println!("Connected! Session: {:?}", info.session_id);
//! # Ok(())
//! # }
//! ```

use crate::config::{ClientConfig, TunnelInfo};
use ferrotunnel_common::config::{LimitsConfig, TlsConfig};
use ferrotunnel_common::{Result, TunnelError};
use ferrotunnel_core::transport::{tls::TlsTransportConfig, TransportConfig};
use ferrotunnel_core::TunnelClient;
use ferrotunnel_http::HttpProxy;
use ferrotunnel_protocol::frame::Protocol;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tracing::{error, info};
use uuid::Uuid;

type StreamDispatch = Arc<dyn Fn(ferrotunnel_core::stream::VirtualStream) + Send + Sync>;
type TunnelInfoTx = Arc<Mutex<Option<oneshot::Sender<TunnelInfo>>>>;

/// A tunnel client that can be embedded in your application.
///
/// Use [`Client::builder()`] to create a new client with the builder pattern.
#[derive(Debug)]
pub struct Client {
    config: ClientConfig,
    transport_config: TransportConfig,
    shutdown_tx: Option<watch::Sender<bool>>,
    task: Option<JoinHandle<()>>,
}

/// Builder for constructing a [`Client`] with ergonomic configuration.
#[derive(Debug, Default)]
pub struct ClientBuilder {
    config: ClientConfig,
    transport_config: Option<TransportConfig>,
}

impl Client {
    /// Create a new client builder.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use ferrotunnel::Client;
    ///
    /// let client = Client::builder()
    ///     .server_addr("localhost:7835")
    ///     .token("secret")
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    /// Start the tunnel client and connect to the server.
    ///
    /// Returns [`TunnelInfo`] with connection details on success.
    /// The client will run until [`shutdown()`](Self::shutdown) is called or the connection fails.
    ///
    /// If `auto_reconnect` is enabled, the client will automatically
    /// reconnect on connection loss.
    ///
    /// # Errors
    ///
    /// Returns an error if the client is already running.
    pub async fn start(&mut self) -> Result<TunnelInfo> {
        if self.task.is_some() {
            return Err(TunnelError::InvalidState("client already started".into()));
        }

        let config = self.config.clone();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        self.shutdown_tx = Some(shutdown_tx);

        let (info_tx, info_rx) = oneshot::channel();

        let server_addr = config.server_addr.clone();
        let token = config.token.clone();
        let local_addr = config.local_addr.clone();
        let tunnel_id = config.tunnel_id.clone();
        let auto_reconnect = config.auto_reconnect;
        let reconnect_delay = config.reconnect_delay;
        let limits = config.limits.clone();
        let transport_config = self.transport_config.clone();

        let info_tx = Arc::new(Mutex::new(Some(info_tx)));

        let task = tokio::spawn(async move {
            let proxy = Arc::new(HttpProxy::new(local_addr.clone()));
            let proxy_dispatch: StreamDispatch = Arc::new(move |stream| {
                if stream.protocol() == Protocol::GRPC {
                    proxy.handle_grpc_stream(stream);
                } else {
                    proxy.handle_stream(stream);
                }
            });
            let mut shutdown_rx = shutdown_rx;

            loop {
                let mut client = TunnelClient::new(server_addr.clone(), token.clone())
                    .with_transport(transport_config.clone())
                    .with_limits(limits.clone());
                if let Some(ref id) = tunnel_id {
                    client = client.with_tunnel_id(id.clone());
                }
                let connect_result = tokio::select! {
                    result = Self::connect_once(
                        client,
                        transport_config.clone(),
                        proxy_dispatch.clone(),
                        #[cfg(feature = "quic")]
                        local_addr.clone(),
                        info_tx.clone(),
                    ) => result,
                    _ = shutdown_rx.changed() => {
                        info!("Client shutdown requested");
                        break;
                    }
                };

                match connect_result {
                    Ok(()) => {
                        info!("Client finished normally");
                        break;
                    }
                    Err(e) => {
                        error!("Connection error: {}", e);
                        if !auto_reconnect {
                            break;
                        }
                        info!("Reconnecting in {:?}...", reconnect_delay);
                        tokio::time::sleep(reconnect_delay).await;
                    }
                }
            }
        });

        self.task = Some(task);

        // Wait for initial connection
        info_rx
            .await
            .map_err(|_| TunnelError::Connection("Failed to establish connection".into()))
    }

    async fn connect_once(
        mut client: TunnelClient,
        transport_config: TransportConfig,
        proxy_dispatch: StreamDispatch,
        #[cfg(feature = "quic")] local_addr: String,
        info_tx: TunnelInfoTx,
    ) -> Result<()> {
        match transport_config {
            #[cfg(feature = "quic")]
            TransportConfig::Quic(_) => {
                client
                    .connect_and_run_quic_with_callback(
                        move |stream| {
                            let local_addr = local_addr.clone();
                            async move {
                                Self::spawn_quic_bridge(local_addr, stream);
                            }
                        },
                        move |session_id| Self::publish_tunnel_info(&info_tx, session_id),
                    )
                    .await
            }
            _ => {
                client
                    .connect_and_run_with_callback(
                        move |stream| {
                            let proxy_dispatch = proxy_dispatch.clone();
                            async move {
                                proxy_dispatch(stream);
                            }
                        },
                        move |session_id| Self::publish_tunnel_info(&info_tx, session_id),
                    )
                    .await
            }
        }
    }

    fn publish_tunnel_info(info_tx: &TunnelInfoTx, session_id: Uuid) {
        if let Ok(mut lock) = info_tx.lock() {
            if let Some(tx) = lock.take() {
                let _ = tx.send(TunnelInfo {
                    session_id: Some(session_id),
                    public_url: None,
                });
            }
        }
    }

    #[cfg(feature = "quic")]
    fn spawn_quic_bridge(local_addr: String, stream: ferrotunnel_core::stream::QuicVirtualStream) {
        tokio::spawn(async move {
            match tokio::net::TcpStream::connect(&local_addr).await {
                Ok(mut local_stream) => {
                    let mut tunnel_stream = stream;
                    let _ =
                        tokio::io::copy_bidirectional(&mut tunnel_stream, &mut local_stream).await;
                }
                Err(e) => {
                    error!("Failed to connect to local service {}: {}", local_addr, e);
                }
            }
        });
    }

    /// Shutdown the tunnel client and wait for cleanup.
    ///
    /// This will gracefully shut down the connection to the server
    /// and wait for the background task to complete.
    pub async fn shutdown(&mut self) -> Result<()> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(true);
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
        Ok(())
    }

    /// Signal the client to stop (non-blocking).
    ///
    /// Use [`shutdown()`](Self::shutdown) if you need to wait for cleanup.
    pub fn stop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(true);
        }
    }

    /// Check if the client is currently running.
    pub fn is_running(&self) -> bool {
        if let Some(task) = &self.task {
            !task.is_finished()
        } else {
            false
        }
    }

    /// Get the current configuration.
    pub fn config(&self) -> &ClientConfig {
        &self.config
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        // Best-effort signal shutdown on drop
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(true);
        }
    }
}

impl ClientBuilder {
    /// Set the server address to connect to.
    ///
    /// Format: `host:port` (e.g., `"tunnel.example.com:7835"`)
    #[must_use]
    pub fn server_addr(mut self, addr: impl Into<String>) -> Self {
        self.config.server_addr = addr.into();
        self
    }

    /// Set the authentication token.
    #[must_use]
    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.config.token = token.into();
        self
    }

    /// Set the local address to forward traffic to.
    ///
    /// Default: `"127.0.0.1:8080"`
    #[must_use]
    pub fn local_addr(mut self, addr: impl Into<String>) -> Self {
        self.config.local_addr = addr.into();
        self
    }

    /// Set the tunnel ID used for HTTP routing (matched against the Host header).
    ///
    /// If not set, the server assigns a random UUID as the tunnel ID.
    #[must_use]
    pub fn tunnel_id(mut self, id: impl Into<String>) -> Self {
        self.config.tunnel_id = Some(id.into());
        self
    }

    /// Enable or disable automatic reconnection.
    ///
    /// Default: `true`
    #[must_use]
    pub fn auto_reconnect(mut self, enabled: bool) -> Self {
        self.config.auto_reconnect = enabled;
        self
    }

    /// Set the delay between reconnection attempts.
    ///
    /// Default: 5 seconds
    #[must_use]
    pub fn reconnect_delay(mut self, delay: Duration) -> Self {
        self.config.reconnect_delay = delay;
        self
    }

    /// Configure resource limits.
    #[must_use]
    pub fn limits(mut self, limits: &LimitsConfig) -> Self {
        self.config.limits = limits.clone();
        self
    }

    /// Configure TLS for the connection.
    ///
    /// When enabled, the client will use TLS to connect to the server.
    #[must_use]
    pub fn tls(mut self, config: &TlsConfig) -> Self {
        if let Some(tls) = TlsTransportConfig::from_common(config) {
            self.transport_config = Some(TransportConfig::Tls(tls));
        }
        self
    }

    /// Configure QUIC transport for the client.
    ///
    /// When enabled, the client will connect to the server using QUIC.
    /// QUIC provides built-in encryption, native stream multiplexing,
    /// and optional 0-RTT reconnection.
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

    /// Build the client with the configured options.
    ///
    /// # Errors
    ///
    /// Returns an error if required configuration is missing:
    /// - `server_addr` must be set
    /// - `token` must be set
    pub fn build(self) -> Result<Client> {
        self.config.validate()?;
        Ok(Client {
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
    fn test_client_builder_success() {
        let client = Client::builder()
            .server_addr("localhost:7835")
            .token("secret-token")
            .local_addr("127.0.0.1:9000")
            .build();
        assert!(client.is_ok());
    }

    #[test]
    fn test_client_builder_with_all_options() {
        let client = Client::builder()
            .server_addr("tunnel.example.com:7835")
            .token("my-token")
            .local_addr("127.0.0.1:3000")
            .auto_reconnect(false)
            .reconnect_delay(Duration::from_secs(10))
            .limits(&LimitsConfig {
                max_frame_bytes: 4096,
                ..Default::default()
            })
            .build()
            .expect("should build successfully");

        assert_eq!(client.config().server_addr, "tunnel.example.com:7835");
        assert_eq!(client.config().token, "my-token");
        assert_eq!(client.config().local_addr, "127.0.0.1:3000");
        assert!(!client.config().auto_reconnect);
        assert_eq!(client.config().reconnect_delay, Duration::from_secs(10));
        assert_eq!(client.config().limits.max_frame_bytes, 4096);
    }

    #[test]
    fn test_client_builder_missing_server_addr() {
        let result = Client::builder()
            .token("secret")
            .local_addr("127.0.0.1:8080")
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("server_addr"));
    }

    #[test]
    fn test_client_builder_missing_token() {
        let result = Client::builder()
            .server_addr("localhost:7835")
            .local_addr("127.0.0.1:8080")
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("token"));
    }

    #[test]
    fn test_client_builder_default_local_addr() {
        let client = Client::builder()
            .server_addr("localhost:7835")
            .token("secret")
            .build()
            .expect("should use default local_addr");

        assert_eq!(client.config().local_addr, "127.0.0.1:8080");
    }

    #[test]
    fn test_client_not_running_initially() {
        let client = Client::builder()
            .server_addr("localhost:7835")
            .token("secret")
            .build()
            .expect("should build");

        assert!(!client.is_running());
    }

    #[test]
    fn test_client_builder_tls_disabled() {
        let tls = TlsConfig {
            enabled: false,
            ..Default::default()
        };
        let client = Client::builder()
            .server_addr("localhost:7835")
            .token("secret")
            .tls(&tls)
            .build()
            .expect("should build");

        // TLS disabled means no transport config set
        assert!(!client.config().server_addr.is_empty());
    }
}
