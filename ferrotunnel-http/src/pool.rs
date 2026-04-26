//! Connection pooling for HTTP/1.1 and HTTP/2 client connections
//!
//! This module provides connection reuse to avoid TCP handshake and HTTP protocol overhead.
//! HTTP/1.1 connections are pooled in a LIFO queue, while HTTP/2 uses a single multiplexed connection.

use hyper::client::conn::{http1, http2};
use hyper_util::rt::TokioIo;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tracing::debug;

/// Connection pool configuration
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Maximum idle connections per host (default: 32)
    pub max_idle_per_host: usize,
    /// Idle timeout for connections (default: 90s)
    pub idle_timeout: Duration,
    /// Prefer HTTP/2 when available (default: false)
    pub prefer_h2: bool,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_idle_per_host: 32,
            idle_timeout: Duration::from_secs(90),
            prefer_h2: false,
        }
    }
}

/// Connection pool errors
#[derive(Debug, Error)]
pub enum ConnectionPoolError {
    #[error("Connection error: {0}")]
    Connection(String),
    #[error("Handshake error: {0}")]
    Handshake(String),
    #[error("Pool is full")]
    PoolFull,
    #[error("No available connection")]
    NoConnection,
}

/// Pooled HTTP/1.1 connection with metadata
struct PooledH1Connection {
    sender: http1::SendRequest<BoxBody>,
    last_used: Instant,
}

/// Connection pool for HTTP/1.1 and HTTP/2
pub struct ConnectionPool {
    target_addr: String,
    config: PoolConfig,
    /// HTTP/1.1 idle connections (LIFO for cache warmth)
    h1_pool: Arc<Mutex<VecDeque<PooledH1Connection>>>,
    /// HTTP/2 multiplexed connection (shared across all requests)
    h2_connection: Arc<Mutex<Option<http2::SendRequest<BoxBody>>>>,
}

/// Boxed body type used for both HTTP/1.1 and HTTP/2 connections.
/// Uses `bytes::Bytes` for data chunks and a boxed error for flexibility.
type BoxBody =
    http_body_util::combinators::BoxBody<bytes::Bytes, Box<dyn std::error::Error + Send + Sync>>;

impl ConnectionPool {
    /// Create a new connection pool
    pub fn new(target_addr: String, config: PoolConfig) -> Self {
        let pool = Self {
            target_addr,
            config,
            h1_pool: Arc::new(Mutex::new(VecDeque::new())),
            h2_connection: Arc::new(Mutex::new(None)),
        };

        // Spawn background eviction task only if we're in a tokio runtime
        if tokio::runtime::Handle::try_current().is_ok() {
            let eviction_pool = pool.h1_pool.clone();
            let eviction_timeout = pool.config.idle_timeout;
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    Self::evict_expired_internal(eviction_pool.clone(), eviction_timeout).await;
                }
            });
        }

        pool
    }

    /// Acquire an HTTP/1.1 connection from the pool or create a new one
    pub async fn acquire_h1(&self) -> Result<http1::SendRequest<BoxBody>, ConnectionPoolError> {
        // Try to reuse an idle connection
        loop {
            let mut pool = self.h1_pool.lock().await;

            if let Some(mut conn) = pool.pop_back() {
                // Check if connection is still valid
                if !conn.sender.is_closed() && conn.last_used.elapsed() < self.config.idle_timeout {
                    debug!("Reusing HTTP/1.1 connection from pool");
                    conn.last_used = Instant::now();
                    return Ok(conn.sender);
                }
                // Connection expired or closed, try next one
                debug!("Discarding expired/closed HTTP/1.1 connection");
                continue;
            }

            // No valid connection in pool, create a new one
            break;
        }

        debug!("Creating new HTTP/1.1 connection to {}", self.target_addr);
        let stream = TcpStream::connect(&self.target_addr)
            .await
            .map_err(|e| ConnectionPoolError::Connection(e.to_string()))?;

        ferrotunnel_core::transport::socket_tuning::configure_socket_silent(&stream);
        let io = TokioIo::new(stream);

        let (sender, conn) = http1::handshake(io)
            .await
            .map_err(|e| ConnectionPoolError::Handshake(e.to_string()))?;

        // Spawn connection driver
        tokio::spawn(async move {
            if let Err(e) = conn.with_upgrades().await {
                debug!("HTTP/1.1 connection error: {:?}", e);
            }
        });

        Ok(sender)
    }

    /// Release an HTTP/1.1 connection back to the pool
    pub async fn release_h1(&self, sender: http1::SendRequest<BoxBody>) {
        // Don't return closed connections to the pool
        if sender.is_closed() {
            debug!("Not returning closed connection to pool");
            return;
        }

        let mut pool = self.h1_pool.lock().await;

        // Enforce per-host limit
        if pool.len() >= self.config.max_idle_per_host {
            debug!("HTTP/1.1 pool full, dropping connection");
            return;
        }

        pool.push_back(PooledH1Connection {
            sender,
            last_used: Instant::now(),
        });
        debug!(
            "Released HTTP/1.1 connection to pool (size: {})",
            pool.len()
        );
    }

    /// Acquire an HTTP/2 connection (multiplexed, shared)
    pub async fn acquire_h2(&self) -> Result<http2::SendRequest<BoxBody>, ConnectionPoolError> {
        let mut h2_conn = self.h2_connection.lock().await;

        // Check if we have a valid H2 connection
        if let Some(ref sender) = *h2_conn {
            if sender.is_ready() {
                debug!("Reusing existing HTTP/2 connection");
                return Ok(sender.clone());
            }
            debug!("HTTP/2 connection not ready, creating new one");
        }

        // Create new HTTP/2 connection
        debug!("Creating new HTTP/2 connection to {}", self.target_addr);
        let stream = TcpStream::connect(&self.target_addr)
            .await
            .map_err(|e| ConnectionPoolError::Connection(e.to_string()))?;

        ferrotunnel_core::transport::socket_tuning::configure_socket_silent(&stream);
        let io = TokioIo::new(stream);

        let (sender, conn) = http2::handshake(hyper_util::rt::TokioExecutor::new(), io)
            .await
            .map_err(|e| ConnectionPoolError::Handshake(e.to_string()))?;

        // Spawn connection driver
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                debug!("HTTP/2 connection error: {:?}", e);
            }
        });

        *h2_conn = Some(sender.clone());
        Ok(sender)
    }

    /// Evict expired connections from the pool
    pub async fn evict_expired(&self) {
        Self::evict_expired_internal(self.h1_pool.clone(), self.config.idle_timeout).await;
    }

    async fn evict_expired_internal(
        pool: Arc<Mutex<VecDeque<PooledH1Connection>>>,
        timeout: Duration,
    ) {
        let mut pool = pool.lock().await;
        let original_len = pool.len();

        pool.retain(|conn| !conn.sender.is_closed() && conn.last_used.elapsed() < timeout);

        let evicted = original_len - pool.len();
        if evicted > 0 {
            debug!("Evicted {} expired HTTP/1.1 connections", evicted);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_config_default() {
        let config = PoolConfig::default();
        assert_eq!(config.max_idle_per_host, 32);
        assert_eq!(config.idle_timeout, Duration::from_secs(90));
        assert!(!config.prefer_h2);
    }

    #[test]
    fn test_connection_pool_new() {
        let config = PoolConfig::default();
        let pool = ConnectionPool::new("127.0.0.1:8080".to_string(), config);
        assert_eq!(pool.target_addr, "127.0.0.1:8080");
    }

    #[test]
    fn test_pool_config_custom() {
        let config = PoolConfig {
            max_idle_per_host: 10,
            idle_timeout: Duration::from_mins(1),
            prefer_h2: true,
        };
        assert_eq!(config.max_idle_per_host, 10);
        assert_eq!(config.idle_timeout, Duration::from_mins(1));
        assert!(config.prefer_h2);
    }
}
