use bytes::Bytes;
use ferrotunnel_core::stream::VirtualStream;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::{http1, http2};
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::service::TowerToHyperService;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tower::{Layer, Service};

use crate::pool::{ConnectionPool, PoolConfig};
#[derive(Debug)]
pub enum ProxyError {
    Hyper(hyper::Error),
    Custom(String),
}

impl std::fmt::Display for ProxyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProxyError::Hyper(e) => write!(f, "Hyper error: {e}"),
            ProxyError::Custom(s) => write!(f, "Proxy error: {s}"),
        }
    }
}

impl std::error::Error for ProxyError {}

impl From<hyper::Error> for ProxyError {
    fn from(e: hyper::Error) -> Self {
        ProxyError::Hyper(e)
    }
}

impl From<std::convert::Infallible> for ProxyError {
    fn from(_: std::convert::Infallible) -> Self {
        unreachable!()
    }
}

use tracing::error;

type BoxBody = http_body_util::combinators::BoxBody<Bytes, ProxyError>;

/// Service that forwards requests to a local TCP port.
#[derive(Clone)]
pub struct LocalProxyService {
    pool: Arc<ConnectionPool>,
    use_h2: bool,
}

impl LocalProxyService {
    pub fn new(target_addr: String) -> Self {
        let pool = Arc::new(ConnectionPool::new(target_addr, PoolConfig::default()));
        Self {
            pool,
            use_h2: false,
        }
    }

    pub fn with_pool(pool: Arc<ConnectionPool>) -> Self {
        Self {
            pool,
            use_h2: false,
        }
    }

    /// Create a service that uses HTTP/2 for forwarding (required for gRPC).
    pub fn with_pool_h2(pool: Arc<ConnectionPool>) -> Self {
        Self { pool, use_h2: true }
    }
}

use hyper::body::Body;

impl<B> Service<Request<B>> for LocalProxyService
where
    B: Body<Data = Bytes> + Send + Sync + 'static,
    B::Error: Into<ProxyError> + std::error::Error + Send + Sync + 'static,
{
    type Response = Response<BoxBody>;
    type Error = hyper::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    #[allow(clippy::too_many_lines)]
    fn call(&mut self, mut req: Request<B>) -> Self::Future {
        let pool = self.pool.clone();
        let use_h2 = self.use_h2;
        Box::pin(async move {
            // gRPC path: forward over HTTP/2, which preserves trailers
            if use_h2 {
                let req = req.map(|b| {
                    b.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync + 'static>)
                        .boxed()
                });
                let mut sender = match pool.acquire_h2().await {
                    Ok(s) => s,
                    Err(e) => {
                        error!("Failed to acquire HTTP/2 connection from pool: {e}");
                        return Ok(error_response(
                            StatusCode::BAD_GATEWAY,
                            &format!("Failed to connect to local service: {e}"),
                        ));
                    }
                };
                return match sender.send_request(req).await {
                    Ok(res) => {
                        let (parts, body) = res.into_parts();
                        Ok(Response::from_parts(
                            parts,
                            body.map_err(Into::into).boxed(),
                        ))
                    }
                    Err(e) => {
                        error!("Failed to proxy gRPC request: {e}");
                        Ok(error_response(StatusCode::BAD_GATEWAY, "Proxy error"))
                    }
                };
            }

            let is_upgrade = req
                .headers()
                .get(hyper::header::UPGRADE)
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| v.eq_ignore_ascii_case("websocket"));

            let server_upgrade = if is_upgrade {
                Some(hyper::upgrade::on(&mut req))
            } else {
                None
            };

            // Try to acquire connection from pool
            let mut sender = match pool.acquire_h1().await {
                Ok(s) => s,
                Err(e) => {
                    error!("Failed to acquire connection from pool: {e}");
                    return Ok(error_response(
                        StatusCode::BAD_GATEWAY,
                        &format!("Failed to connect to local service: {e}"),
                    ));
                }
            };

            let req = req.map(|b| {
                b.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync + 'static>)
                    .boxed()
            });

            match sender.send_request(req).await {
                Ok(res) => {
                    if is_upgrade && res.status() == StatusCode::SWITCHING_PROTOCOLS {
                        // Don't return upgraded connections to pool
                        let upstream_headers = res.headers().clone();
                        let local_upgrade = hyper::upgrade::on(res);

                        if let Some(server_upgrade) = server_upgrade {
                            tokio::spawn(async move {
                                let (local_result, server_result) =
                                    tokio::join!(local_upgrade, server_upgrade);

                                let local_upgraded = match local_result {
                                    Ok(u) => u,
                                    Err(e) => {
                                        error!("Local upgrade failed: {e}");
                                        return;
                                    }
                                };
                                let server_upgraded = match server_result {
                                    Ok(u) => u,
                                    Err(e) => {
                                        error!("Server upgrade failed: {e}");
                                        return;
                                    }
                                };

                                let mut local_io = TokioIo::new(local_upgraded);
                                let mut server_io = TokioIo::new(server_upgraded);
                                let _ =
                                    tokio::io::copy_bidirectional(&mut local_io, &mut server_io)
                                        .await;
                            });
                        }

                        let mut builder =
                            Response::builder().status(StatusCode::SWITCHING_PROTOCOLS);
                        for (key, value) in &upstream_headers {
                            builder = builder.header(key, value);
                        }
                        Ok(builder
                            .body(
                                Full::new(Bytes::new())
                                    .map_err(|_| ProxyError::Custom("unreachable".into()))
                                    .boxed(),
                            )
                            .unwrap_or_else(|_| {
                                error_response(
                                    StatusCode::INTERNAL_SERVER_ERROR,
                                    "Failed to build upgrade response",
                                )
                            }))
                    } else {
                        // Return connection to pool for reuse
                        pool.release_h1(sender).await;

                        let (parts, body) = res.into_parts();
                        let boxed_body = body.map_err(Into::into).boxed();
                        Ok(Response::from_parts(parts, boxed_body))
                    }
                }
                Err(e) => {
                    // Don't return broken connections to pool
                    error!("Failed to proxy request: {e}");
                    Ok(error_response(StatusCode::BAD_GATEWAY, "Proxy error"))
                }
            }
        })
    }
}

/// Pre-allocated bytes for common error bodies (avoids allocation in hot/error path).
const MSG_PROXY_ERROR: &[u8] = b"Proxy error";
const MSG_INTERNAL_ERROR: &[u8] = b"Internal error";

/// Builds a plain-text error response. Shared by proxy and CLI dashboard middleware.
/// Uses static bytes for common messages to avoid allocation.
pub fn error_response(status: StatusCode, msg: &str) -> Response<BoxBody> {
    let bytes = if msg == "Proxy error" {
        Bytes::from_static(MSG_PROXY_ERROR)
    } else {
        Bytes::copy_from_slice(msg.as_bytes())
    };
    Response::builder()
        .status(status)
        .body(
            Full::new(bytes)
                .map_err(|_| ProxyError::Custom("Error construction failed".into()))
                .boxed(),
        )
        .unwrap_or_else(|_| {
            Response::new(
                Full::new(Bytes::from_static(MSG_INTERNAL_ERROR))
                    .map_err(|_| ProxyError::Custom("Error construction failed".into()))
                    .boxed(),
            )
        })
}

#[derive(Clone)]
pub struct HttpProxy<L> {
    target_addr: String,
    layer: L,
    pool: Arc<ConnectionPool>,
}

impl HttpProxy<tower::layer::util::Identity> {
    pub fn new(target_addr: String) -> Self {
        let pool = Arc::new(ConnectionPool::new(
            target_addr.clone(),
            PoolConfig::default(),
        ));
        Self {
            target_addr,
            layer: tower::layer::util::Identity::new(),
            pool,
        }
    }

    pub fn with_pool_config(target_addr: String, pool_config: PoolConfig) -> Self {
        let pool = Arc::new(ConnectionPool::new(target_addr.clone(), pool_config));
        Self {
            target_addr,
            layer: tower::layer::util::Identity::new(),
            pool,
        }
    }
}

impl<L> HttpProxy<L> {
    /// Pool used to serve gRPC streams.
    ///
    /// Returns a clone of the shared pool so every gRPC stream multiplexes over
    /// the same upstream HTTP/2 connection and shares one eviction task, instead
    /// of allocating a fresh pool (and tasks/connections) per stream (see #134).
    fn grpc_pool(&self) -> Arc<ConnectionPool> {
        self.pool.clone()
    }

    pub fn with_layer<NewL>(self, layer: NewL) -> HttpProxy<NewL> {
        HttpProxy {
            target_addr: self.target_addr,
            layer,
            pool: self.pool,
        }
    }

    pub fn handle_stream(&self, stream: VirtualStream)
    where
        L: Layer<LocalProxyService> + Clone + Send + 'static,
        L::Service: Service<Request<Incoming>, Response = Response<BoxBody>, Error = hyper::Error>
            + Send
            + Clone
            + 'static,
        <L::Service as Service<Request<Incoming>>>::Future: Send,
    {
        let service = self
            .layer
            .clone()
            .layer(LocalProxyService::with_pool(self.pool.clone()));
        let hyper_service = TowerToHyperService::new(service);
        let io = TokioIo::new(stream);

        tokio::spawn(async move {
            let _ = http1::Builder::new()
                .serve_connection(io, hyper_service)
                .with_upgrades()
                .await;
        });
    }

    /// Serve an incoming gRPC `VirtualStream` over HTTP/2.
    ///
    /// gRPC requires HTTP/2 end-to-end so that trailers (`grpc-status`,
    /// `grpc-message`) are propagated correctly. This method reuses the shared
    /// pool's multiplexed HTTP/2 connection (acquired via `acquire_h2()`) to
    /// forward requests, so every gRPC stream shares one upstream connection and
    /// eviction task rather than allocating a fresh pool per stream.
    pub fn handle_grpc_stream(&self, stream: VirtualStream)
    where
        L: Layer<LocalProxyService> + Clone + Send + 'static,
        L::Service: Service<Request<Incoming>, Response = Response<BoxBody>, Error = hyper::Error>
            + Send
            + Clone
            + 'static,
        <L::Service as Service<Request<Incoming>>>::Future: Send,
    {
        let service = self
            .layer
            .clone()
            .layer(LocalProxyService::with_pool_h2(self.grpc_pool()));
        let hyper_service = TowerToHyperService::new(service);
        let io = TokioIo::new(stream);

        tokio::spawn(async move {
            let _ = http2::Builder::new(TokioExecutor::new())
                .serve_connection(io, hyper_service)
                .await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;
    use hyper::{body::Bytes, Request};
    use tower::Service;

    #[test]
    fn test_proxy_error_display_hyper() {
        // We can't easily create a real hyper error, but we can test Custom
        let err = ProxyError::Custom("test error".to_string());
        assert!(err.to_string().contains("test error"));
    }

    #[test]
    fn test_proxy_error_custom_display() {
        let err = ProxyError::Custom("connection failed".to_string());
        let display = format!("{err}");
        assert!(display.contains("Proxy error"));
        assert!(display.contains("connection failed"));
    }

    #[test]
    fn test_local_proxy_service_new() {
        let service = LocalProxyService::new("127.0.0.1:8080".to_string());
        // Service is created successfully with pool
        let _ = service;
    }

    #[test]
    fn test_local_proxy_service_clone() {
        let service = LocalProxyService::new("localhost:3000".to_string());
        let _cloned = service.clone();
        // Service can be cloned successfully
    }

    #[tokio::test]
    async fn test_proxy_connection_error() {
        // Create a service pointing to a closed port (assuming 127.0.0.1:12345 is closed)
        let mut service = LocalProxyService::new("127.0.0.1:12345".to_string());

        let req = Request::builder()
            .uri("http://example.com")
            .body(Full::new(Bytes::from("test")))
            .unwrap();

        // The service should return a 502 Bad Gateway response
        let response = service.call(req).await.unwrap();

        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);

        let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
        let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();
        assert!(body_str.contains("Failed to connect"));
    }

    #[test]
    fn test_error_response_bad_gateway() {
        let resp = error_response(StatusCode::BAD_GATEWAY, "Backend unavailable");
        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
    }

    #[test]
    fn test_error_response_not_found() {
        let resp = error_response(StatusCode::NOT_FOUND, "Route not found");
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn test_error_response_internal_error() {
        let resp = error_response(StatusCode::INTERNAL_SERVER_ERROR, "Unexpected error");
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn test_http_proxy_new() {
        let proxy = HttpProxy::new("127.0.0.1:8080".to_string());
        assert_eq!(proxy.target_addr, "127.0.0.1:8080");
    }

    #[test]
    fn test_http_proxy_with_layer() {
        let proxy = HttpProxy::new("127.0.0.1:8080".to_string());
        let _layered = proxy.with_layer(tower::layer::util::Identity::new());
        // Just verify it compiles and runs
    }

    #[test]
    fn grpc_stream_reuses_shared_pool() {
        // #134: handle_grpc_stream must reuse the proxy's shared pool rather than
        // allocating a brand-new ConnectionPool per VirtualStream. grpc_pool() is
        // the single source of truth the handler uses for its H2 pool.
        let proxy = HttpProxy::new("127.0.0.1:8080".to_string());
        let grpc_pool = proxy.grpc_pool();

        // Identity: the gRPC pool is the same allocation as the shared pool.
        assert!(
            Arc::ptr_eq(&grpc_pool, &proxy.pool),
            "handle_grpc_stream must reuse self.pool, not allocate a new pool"
        );
        // Refcount: grpc_pool is a clone of the shared Arc, not a fresh one.
        assert_eq!(Arc::strong_count(&proxy.pool), 2);
    }
}
