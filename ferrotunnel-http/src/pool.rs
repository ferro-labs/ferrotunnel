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
    ///
    /// The pool mutex is only held while inspecting/storing the cached sender;
    /// the TCP connect and HTTP/2 handshake run with the lock released so that
    /// concurrent callers are not serialized behind a full reconnect. If two
    /// callers race to dial, the loser drops its freshly created sender (whose
    /// driver future resolves on drop, so no task leaks) and reuses the winner's.
    pub async fn acquire_h2(&self) -> Result<http2::SendRequest<BoxBody>, ConnectionPoolError> {
        // Fast path: reuse a ready cached connection, then drop the guard.
        {
            let h2_conn = self.h2_connection.lock().await;
            if let Some(ref sender) = *h2_conn {
                if sender.is_ready() {
                    debug!("Reusing existing HTTP/2 connection");
                    return Ok(sender.clone());
                }
                debug!("HTTP/2 connection not ready, creating new one");
            }
        }

        // Create a new HTTP/2 connection with the lock released.
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

        // Re-acquire the lock and double-check: another caller may have stored a
        // ready connection while we were dialing. If so, reuse theirs and drop ours.
        let mut h2_conn = self.h2_connection.lock().await;
        if let Some(ref existing) = *h2_conn {
            if existing.is_ready() {
                debug!("Discarding redundant HTTP/2 connection, reusing concurrent one");
                return Ok(existing.clone());
            }
        }

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
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Spawn a minimal HTTP/2 upstream stub.
    ///
    /// Accepts TCP connections and completes a real HTTP/2 handshake so that a
    /// client `SendRequest` becomes ready. `delay` is applied per connection
    /// before serving, to simulate a slow upstream. Returns the bound address,
    /// a counter of accepted inbound TCP connections, and the accept-loop task.
    async fn spawn_h2_stub(
        delay: Duration,
    ) -> (String, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
        use bytes::Bytes;
        use http_body_util::Full;
        use hyper::body::Incoming;
        use hyper::server::conn::http2 as server_http2;
        use hyper::service::service_fn;
        use hyper::{Request, Response};
        use hyper_util::rt::TokioExecutor;
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let count = Arc::new(AtomicUsize::new(0));
        let accept_count = count.clone();

        let handle = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                accept_count.fetch_add(1, Ordering::SeqCst);
                let conn_delay = delay;
                tokio::spawn(async move {
                    if !conn_delay.is_zero() {
                        tokio::time::sleep(conn_delay).await;
                    }
                    let io = TokioIo::new(stream);
                    let _ = server_http2::Builder::new(TokioExecutor::new())
                        .serve_connection(
                            io,
                            service_fn(|_req: Request<Incoming>| async move {
                                Ok::<_, std::convert::Infallible>(Response::new(Full::new(
                                    Bytes::from_static(b"ok"),
                                )))
                            }),
                        )
                        .await;
                });
            }
        });

        (addr, count, handle)
    }

    /// Wait until the HTTP/2 sender finishes its handshake and is ready.
    async fn wait_until_ready(sender: &http2::SendRequest<BoxBody>) {
        for _ in 0..200u32 {
            if sender.is_ready() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("HTTP/2 sender never became ready");
    }

    /// Wait until the stub has accepted at least `target` inbound connections.
    async fn wait_for_accepts(accepted: &Arc<AtomicUsize>, target: usize) {
        for _ in 0..200u32 {
            if accepted.load(Ordering::SeqCst) >= target {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!(
            "stub accepted {} connections, expected at least {target}",
            accepted.load(Ordering::SeqCst)
        );
    }

    #[tokio::test]
    async fn acquire_h2_reuses_connection_without_redial() {
        // #133: a second acquire_h2 must reuse the cached ready connection and
        // must not open a new inbound TCP connection.
        let (addr, accepted, _stub) = spawn_h2_stub(Duration::ZERO).await;
        let pool = ConnectionPool::new(addr, PoolConfig::default());

        let sender = pool.acquire_h2().await.expect("first acquire_h2");
        // The client `SendRequest` can report ready before the server's
        // userspace accept() runs, so confirm the dial actually landed.
        wait_for_accepts(&accepted, 1).await;
        wait_until_ready(&sender).await;
        assert_eq!(
            accepted.load(Ordering::SeqCst),
            1,
            "first acquire dials once"
        );

        let _second = pool.acquire_h2().await.expect("second acquire_h2");
        // Allow any erroneous extra dial to be accepted before asserting.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            accepted.load(Ordering::SeqCst),
            1,
            "second acquire_h2 must reuse the cached connection"
        );
    }

    #[tokio::test]
    async fn concurrent_acquire_h2_is_not_serialized_by_the_lock() {
        // #133: with the mutex released across connect + handshake, N concurrent
        // dials overlap and finish in roughly one per-dial delay. The old code
        // held the lock across I/O, serializing callers. Bounds are generous to
        // stay non-flaky while still well under N x per-dial delay.
        let per_dial = Duration::from_millis(200);
        let n: u32 = 6;
        let (addr, _accepted, _stub) = spawn_h2_stub(per_dial).await;
        let pool = Arc::new(ConnectionPool::new(addr, PoolConfig::default()));

        let start = Instant::now();
        let mut handles = Vec::new();
        for _ in 0..n {
            let pool = pool.clone();
            handles.push(tokio::spawn(async move { pool.acquire_h2().await.is_ok() }));
        }
        for handle in handles {
            assert!(handle.await.unwrap(), "acquire_h2 should succeed");
        }
        let elapsed = start.elapsed();

        let bound = per_dial * n / 2;
        assert!(
            elapsed < bound,
            "concurrent acquire_h2 took {elapsed:?}, expected < {bound:?}"
        );
    }

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
