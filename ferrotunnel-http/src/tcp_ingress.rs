//! TCP ingress for raw socket tunneling
//!
//! Provides protocol-agnostic TCP forwarding through the tunnel.
//! Useful for database connections, SSH, and custom protocols.

use ferrotunnel_common::Result;
use ferrotunnel_core::tunnel::session::SessionStoreBackend;
use ferrotunnel_protocol::frame::Protocol;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tracing::{error, info, warn};

/// Configuration for TCP ingress limits and timeouts
#[derive(Debug, Clone)]
pub struct TcpIngressConfig {
    /// Maximum concurrent TCP connections (default: 1000)
    pub max_connections: usize,
    /// Timeout for establishing tunnel connection (default: 10s)
    pub connection_timeout: Duration,
    /// Idle timeout for inactive connections (default: 5 minutes)
    pub idle_timeout: Duration,
    /// Buffer size for bidirectional copy (default: 64KB)
    pub buffer_size: usize,
}

impl Default for TcpIngressConfig {
    fn default() -> Self {
        Self {
            max_connections: 1000,
            connection_timeout: Duration::from_secs(10),
            idle_timeout: Duration::from_mins(5),
            buffer_size: 64 * 1024,
        }
    }
}

/// TCP ingress server for raw socket tunneling
pub struct TcpIngress {
    addr: SocketAddr,
    sessions: SessionStoreBackend,
    config: TcpIngressConfig,
    connection_semaphore: Arc<Semaphore>,
}

impl TcpIngress {
    /// Create a new TCP ingress with default configuration
    pub fn new(addr: SocketAddr, sessions: SessionStoreBackend) -> Self {
        Self::with_config(addr, sessions, TcpIngressConfig::default())
    }

    /// Create a new TCP ingress with custom configuration
    pub fn with_config(
        addr: SocketAddr,
        sessions: SessionStoreBackend,
        config: TcpIngressConfig,
    ) -> Self {
        let connection_semaphore = Arc::new(Semaphore::new(config.max_connections));
        Self {
            addr,
            sessions,
            config,
            connection_semaphore,
        }
    }

    /// Start the TCP ingress server
    pub async fn start(self) -> Result<()> {
        let listener = TcpListener::bind(self.addr).await?;
        info!("TCP Ingress listening on {}", self.addr);

        loop {
            let (stream, peer_addr) = listener.accept().await?;

            // CRITICAL: Set TCP_NODELAY to disable Nagle's algorithm for low latency
            if let Err(e) = stream.set_nodelay(true) {
                warn!("Failed to set TCP_NODELAY for {}: {}", peer_addr, e);
            }

            // Acquire connection permit (limit concurrent connections)
            let Ok(permit) = self.connection_semaphore.clone().try_acquire_owned() else {
                warn!(
                    "Max TCP connections reached, rejecting connection from {}",
                    peer_addr
                );
                drop(stream);
                continue;
            };

            // Find an active tunnel with TCP capability
            let Some(multiplexer) = self.sessions.find_multiplexer_with_capability("tcp") else {
                warn!(
                    "No active tunnel with 'tcp' capability for connection from {}",
                    peer_addr
                );
                drop(stream);
                continue;
            };

            let config = self.config.clone();
            tokio::spawn(async move {
                let _permit = permit; // Hold permit until connection closes

                if let Err(e) = handle_tcp_connection(stream, multiplexer, peer_addr, config).await
                {
                    error!(
                        peer_addr = %peer_addr,
                        error = %e,
                        "TCP tunnel connection failed"
                    );
                }
            });
        }
    }
}

/// Handle a single TCP connection through the tunnel
async fn handle_tcp_connection(
    client_stream: TcpStream,
    multiplexer: ferrotunnel_core::stream::AnyMultiplexer,
    peer_addr: SocketAddr,
    config: TcpIngressConfig,
) -> Result<()> {
    let start = Instant::now();

    // Open virtual stream through tunnel with timeout
    let tunnel_stream = tokio::time::timeout(
        config.connection_timeout,
        multiplexer.open_stream(Protocol::TCP),
    )
    .await
    .map_err(|_| {
        std::io::Error::new(std::io::ErrorKind::TimedOut, "Tunnel connection timeout")
    })??;

    // Bidirectional copy with idle timeout
    let copy_result = tokio::time::timeout(
        config.idle_timeout,
        copy_bidirectional_with_metrics(client_stream, tunnel_stream, peer_addr),
    )
    .await;

    match copy_result {
        Ok(Ok((to_client, to_server))) => {
            let duration = start.elapsed();
            info!(
                peer_addr = %peer_addr,
                duration_ms = duration.as_millis(),
                bytes_tx = to_server,
                bytes_rx = to_client,
                "TCP tunnel closed"
            );
        }
        Ok(Err(e)) => {
            error!(
                peer_addr = %peer_addr,
                error = %e,
                "TCP copy error"
            );
        }
        Err(_) => {
            warn!(
                peer_addr = %peer_addr,
                "TCP connection idle timeout"
            );
        }
    }

    Ok(())
}

/// Buffer size for bidirectional copy. Must match MAX_DATA_FRAME_PAYLOAD in multiplexer
/// to avoid "Frame too large" decoder desync when payload bytes (e.g. 0x78787878) are
/// misread as frame length.
const TCP_COPY_BUFFER_SIZE: usize = 64 * 1024; // 64KB

/// Bidirectional copy with metrics tracking
async fn copy_bidirectional_with_metrics<A, B>(
    mut a: A,
    mut b: B,
    _peer_addr: SocketAddr,
) -> std::io::Result<(u64, u64)>
where
    A: AsyncRead + AsyncWrite + Unpin,
    B: AsyncRead + AsyncWrite + Unpin,
{
    let result = tokio::io::copy_bidirectional_with_sizes(
        &mut a,
        &mut b,
        TCP_COPY_BUFFER_SIZE,
        TCP_COPY_BUFFER_SIZE,
    )
    .await?;

    #[cfg(feature = "metrics")]
    if let Some(m) = ferrotunnel_observability::tunnel_metrics() {
        m.record_bytes((result.0 + result.1) as usize);
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tcp_ingress_config_default() {
        let config = TcpIngressConfig::default();
        assert_eq!(config.max_connections, 1000);
        assert_eq!(config.connection_timeout, Duration::from_secs(10));
        assert_eq!(config.idle_timeout, Duration::from_mins(5));
        assert_eq!(config.buffer_size, 64 * 1024);
    }

    #[test]
    fn test_tcp_ingress_creation() {
        let sessions = SessionStoreBackend::default();
        let addr = "127.0.0.1:5000".parse().unwrap();
        let ingress = TcpIngress::new(addr, sessions);
        assert_eq!(ingress.addr, addr);
    }

    #[test]
    fn test_tcp_ingress_custom_config() {
        let sessions = SessionStoreBackend::default();
        let addr = "127.0.0.1:5000".parse().unwrap();
        let config = TcpIngressConfig {
            max_connections: 500,
            connection_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_mins(1),
            buffer_size: 32 * 1024,
        };
        let ingress = TcpIngress::with_config(addr, sessions, config.clone());
        assert_eq!(ingress.config.max_connections, 500);
    }
}
