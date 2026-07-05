use crate::ingress::parse_and_normalize_host;
use bytes::{Buf, Bytes};
use ferrotunnel_common::{Result, TunnelError};
use ferrotunnel_core::transport::tls::TlsTransportConfig;
use ferrotunnel_core::tunnel::session::SessionStoreBackend;
use ferrotunnel_plugin::{PluginAction, PluginRegistry, RequestContext, ResponseContext};
use ferrotunnel_protocol::frame::Protocol;
use h3::quic::{BidiStream, RecvStream, SendStream};
use h3_quinn::quinn::{Endpoint, ServerConfig, TransportConfig, VarInt};
use http_body_util::BodyExt;
use hyper::body::Frame;
use hyper::header::{HeaderName, HeaderValue, HOST};
use hyper::{Request, Response, StatusCode, Uri};
use hyper_util::rt::{TokioExecutor, TokioIo};
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};
use tokio::sync::Semaphore;
use tracing::{error, info, warn};

const H3_ALPN: &[u8] = b"h3";

type BoxBody = http_body_util::combinators::BoxBody<Bytes, io::Error>;

// Section 12.5: #[non_exhaustive] for semver-safe config evolution.
// Construct via `Http3IngressConfig::default()` + struct update syntax.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct Http3IngressConfig {
    pub cert_path: String,
    pub key_path: String,
    pub ca_cert_path: Option<String>,
    pub client_auth: bool,
    pub max_connections: usize,
    pub max_request_body_size: usize,
    pub max_response_size: usize,
    pub handshake_timeout: Duration,
    pub response_timeout: Duration,
    pub alt_svc_max_age: u64,
    /// QUIC connection idle timeout (Section 2)
    pub idle_timeout: Duration,
    /// QUIC keep-alive interval (Section 2)
    pub keep_alive_interval: Duration,
    /// Maximum concurrent bidirectional streams per connection (Section 7)
    pub max_concurrent_bidi_streams: u32,
}

impl Default for Http3IngressConfig {
    fn default() -> Self {
        Self {
            cert_path: String::new(),
            key_path: String::new(),
            ca_cert_path: None,
            client_auth: false,
            max_connections: 10_000,
            max_request_body_size: 100 * 1024 * 1024,
            max_response_size: 100 * 1024 * 1024,
            handshake_timeout: Duration::from_secs(10),
            response_timeout: Duration::from_mins(1),
            alt_svc_max_age: 86_400,
            idle_timeout: Duration::from_secs(30),
            keep_alive_interval: Duration::from_secs(10),
            max_concurrent_bidi_streams: 256,
        }
    }
}

impl Http3IngressConfig {
    /// Create a config with the required TLS certificate paths; all other
    /// fields use their defaults.
    #[must_use]
    pub fn with_certs(cert_path: String, key_path: String) -> Self {
        Self {
            cert_path,
            key_path,
            ..Self::default()
        }
    }

    #[must_use]
    pub fn ca_cert_path(mut self, path: Option<String>) -> Self {
        self.ca_cert_path = path;
        self
    }

    #[must_use]
    pub fn client_auth(mut self, enabled: bool) -> Self {
        self.client_auth = enabled;
        self
    }

    /// Builds an `Alt-Svc` header advertising both `h3` and `h3-29` (Section 12.6).
    pub fn alt_svc_header_value(
        &self,
        addr: SocketAddr,
    ) -> std::result::Result<HeaderValue, String> {
        let port = addr.port();
        let ma = self.alt_svc_max_age;
        HeaderValue::from_str(&format!(
            "h3=\":{port}\"; ma={ma}, h3-29=\":{port}\"; ma={ma}"
        ))
        .map_err(|e| format!("invalid Alt-Svc value: {e}"))
    }
}

// ---------------------------------------------------------------------------
// Section 1: Streaming request body via channel
// ---------------------------------------------------------------------------

struct Http3RequestBody {
    receiver: tokio::sync::mpsc::Receiver<std::result::Result<Bytes, io::Error>>,
    bytes_read: usize,
    max_size: usize,
}

impl hyper::body::Body for Http3RequestBody {
    type Data = Bytes;
    type Error = io::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<std::result::Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        match this.receiver.poll_recv(cx) {
            Poll::Ready(Some(Ok(bytes))) => {
                this.bytes_read += bytes.len();
                if this.bytes_read > this.max_size {
                    Poll::Ready(Some(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "body too large",
                    ))))
                } else {
                    Poll::Ready(Some(Ok(Frame::data(bytes))))
                }
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

type BodyChunkReceiver = tokio::sync::mpsc::Receiver<std::result::Result<Bytes, io::Error>>;
type StreamReturnReceiver<S> = tokio::sync::oneshot::Receiver<h3::server::RequestStream<S, Bytes>>;

fn spawn_body_reader<S>(
    mut h3_stream: h3::server::RequestStream<S, Bytes>,
    max_size: usize,
) -> (BodyChunkReceiver, StreamReturnReceiver<S>)
where
    S: BidiStream<Bytes> + Send + 'static,
    S::RecvStream: RecvStream + Send,
    S::SendStream: SendStream<Bytes> + Send,
{
    let (body_tx, body_rx) = tokio::sync::mpsc::channel(8);
    let (stream_tx, stream_rx) = tokio::sync::oneshot::channel();

    tokio::spawn(async move {
        let mut bytes_read = 0usize;
        loop {
            match h3_stream.recv_data().await {
                Ok(Some(mut chunk)) => {
                    let len = chunk.remaining();
                    if len == 0 {
                        continue;
                    }
                    // Pre-validate remaining budget before copy_to_bytes (Section 1.4)
                    if bytes_read.saturating_add(len) > max_size {
                        let _ = body_tx
                            .send(Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "body too large",
                            )))
                            .await;
                        break;
                    }
                    bytes_read += len;
                    let bytes = chunk.copy_to_bytes(len);
                    if body_tx.send(Ok(bytes)).await.is_err() {
                        break;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    let _ = body_tx
                        .send(Err(io::Error::other(format!("h3 recv error: {e}"))))
                        .await;
                    break;
                }
            }
        }
        let _ = stream_tx.send(h3_stream);
    });

    (body_rx, stream_rx)
}

// ---------------------------------------------------------------------------
// Http3Ingress
// ---------------------------------------------------------------------------

pub struct Http3Ingress {
    addr: SocketAddr,
    sessions: SessionStoreBackend,
    registry: Arc<PluginRegistry>,
    config: Arc<Http3IngressConfig>,
    connection_semaphore: Arc<Semaphore>,
}

impl Http3Ingress {
    #[must_use]
    pub fn new(
        addr: SocketAddr,
        sessions: SessionStoreBackend,
        registry: Arc<PluginRegistry>,
        config: Http3IngressConfig,
    ) -> Self {
        let connection_semaphore = Arc::new(Semaphore::new(config.max_connections));
        Self {
            addr,
            sessions,
            registry,
            config: Arc::new(config),
            connection_semaphore,
        }
    }

    /// Start the HTTP/3 ingress (blocks forever).
    pub async fn start(self) -> Result<()> {
        let (tx, rx) = tokio::sync::watch::channel(false);
        // Never fires — start_with_shutdown with a parked receiver.
        drop(tx);
        self.start_with_shutdown(rx).await
    }

    /// Start the HTTP/3 ingress with an external shutdown signal (Section 10).
    pub async fn start_with_shutdown(
        self,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<()> {
        let endpoint = create_http3_endpoint(&self.config, self.addr)?;
        info!("HTTP/3 Ingress listening on {} (UDP)", self.addr);

        loop {
            tokio::select! {
                biased;
                _ = shutdown.changed() => {
                    info!("HTTP/3 ingress shutting down, draining...");
                    endpoint.close(VarInt::from_u32(0), b"server shutdown");
                    endpoint.wait_idle().await;
                    return Ok(());
                }
                accepted = endpoint.accept() => {
                    let Some(incoming) = accepted else {
                        return Err(TunnelError::Connection("HTTP/3 endpoint closed".into()));
                    };
                    let peer_addr = incoming.remote_address();

                    let Ok(permit) = self.connection_semaphore.clone().try_acquire_owned() else {
                        // Section 8: structured warn with peer_addr and limit
                        warn!(
                            peer = %peer_addr,
                            max = self.config.max_connections,
                            "HTTP/3 max_connections reached, rejecting"
                        );
                        continue;
                    };

                    let sessions = self.sessions.clone();
                    let registry = self.registry.clone();
                    let config = Arc::clone(&self.config);

                    tokio::spawn(async move {
                        let _permit = permit;
                        let connection = match incoming.await {
                            Ok(connection) => connection,
                            Err(e) => {
                                error!("HTTP/3 QUIC handshake failed from {peer_addr}: {e}");
                                return;
                            }
                        };

                        if let Err(e) =
                            handle_connection(connection, sessions, registry, peer_addr, config)
                                .await
                        {
                            error!("HTTP/3 connection error from {peer_addr}: {e}");
                        }
                    });
                }
            }
        }
    }
}

fn create_http3_endpoint(config: &Http3IngressConfig, bind_addr: SocketAddr) -> Result<Endpoint> {
    // Section 3: rustls 0.23 requires an explicit crypto provider;
    // ferrotunnel standardizes on ring (see AGENTS.md).
    use std::sync::Once;
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });

    if config.cert_path.is_empty() || config.key_path.is_empty() {
        return Err(TunnelError::Config(
            "HTTP/3 ingress requires certificate and key paths".into(),
        ));
    }

    let tls_config = TlsTransportConfig {
        ca_cert_path: config.ca_cert_path.clone(),
        cert_path: config.cert_path.clone(),
        key_path: config.key_path.clone(),
        server_name: None,
        client_auth: config.client_auth,
        skip_verify: false,
    };
    let rustls_config = ferrotunnel_core::transport::tls::create_server_config(&tls_config)?;
    let mut rustls_config = (*rustls_config).clone();
    rustls_config.alpn_protocols = vec![H3_ALPN.to_vec()];

    let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(rustls_config)
        .map_err(|e| TunnelError::Tls(format!("HTTP/3 QUIC server config: {e}")))?;

    let mut transport = TransportConfig::default();
    // Section 2: idle_timeout is connection-scoped; response_timeout is request-scoped only.
    transport.max_idle_timeout(Some(
        config
            .idle_timeout
            .try_into()
            .map_err(|e| TunnelError::Config(format!("idle_timeout: {e}")))?,
    ));
    transport.keep_alive_interval(Some(config.keep_alive_interval));
    // Section 7: configurable bidi stream limit
    transport.max_concurrent_bidi_streams(VarInt::from_u32(config.max_concurrent_bidi_streams));

    let mut server_config = ServerConfig::with_crypto(Arc::new(crypto));
    server_config.transport_config(Arc::new(transport));
    Endpoint::server(server_config, bind_addr).map_err(Into::into)
}

async fn handle_connection(
    connection: quinn::Connection,
    sessions: SessionStoreBackend,
    registry: Arc<PluginRegistry>,
    peer_addr: SocketAddr,
    config: Arc<Http3IngressConfig>,
) -> std::result::Result<(), String> {
    let h3_connection = h3_quinn::Connection::new(connection);
    let mut h3_connection = h3::server::builder()
        .build(h3_connection)
        .await
        .map_err(|e| format!("HTTP/3 connection build failed: {e}"))?;

    loop {
        match h3_connection.accept().await {
            Ok(Some(resolver)) => {
                let sessions = sessions.clone();
                let registry = registry.clone();
                let config = Arc::clone(&config);
                tokio::spawn(async move {
                    let resolved = resolver.resolve_request().await;
                    let (req, stream) = match resolved {
                        Ok(resolved) => resolved,
                        Err(e) => {
                            error!("Failed to resolve HTTP/3 request: {e}");
                            return;
                        }
                    };
                    if let Err(e) =
                        handle_request(req, stream, sessions, registry, peer_addr, &config).await
                    {
                        error!("HTTP/3 request handling failed: {e}");
                    }
                });
            }
            Ok(None) => return Ok(()),
            Err(e) => return Err(format!("HTTP/3 accept failed: {e}")),
        }
    }
}

// ---------------------------------------------------------------------------
// Request handling (Section 12.4: split into forward_via_h1 / forward_via_h2)
// ---------------------------------------------------------------------------

/// Result of running plugin request hooks. `Ok(req)` means proceed; `Err` means
/// the plugin already sent a response on the stream.
enum PluginResult {
    Continue(Box<Request<()>>),
    Handled,
}

async fn run_request_plugins<S>(
    mut req: Request<()>,
    h3_stream: &mut h3::server::RequestStream<S, Bytes>,
    registry: &PluginRegistry,
    ctx: &RequestContext,
) -> std::result::Result<PluginResult, String>
where
    S: BidiStream<Bytes> + Send + 'static,
    S::RecvStream: RecvStream + Send,
    S::SendStream: SendStream<Bytes> + Send,
{
    match registry.execute_request_hooks(&mut req, ctx).await {
        Ok(PluginAction::Continue) => Ok(PluginResult::Continue(Box::new(req))),
        Ok(PluginAction::Modify { .. }) => Ok(PluginResult::Continue(Box::new(req))),
        Ok(PluginAction::Reject { status, reason }) => {
            let status_code = match StatusCode::from_u16(status) {
                Ok(s) => s,
                Err(_) => {
                    warn!(status, "invalid plugin reject status, falling back to 403");
                    StatusCode::FORBIDDEN
                }
            };
            send_simple_response(h3_stream, status_code, Bytes::from(reason)).await?;
            Ok(PluginResult::Handled)
        }
        Ok(PluginAction::Respond {
            status,
            headers,
            body,
        }) => {
            send_plugin_response(h3_stream, status, headers, Bytes::from(body)).await?;
            Ok(PluginResult::Handled)
        }
        Err(e) => {
            error!("HTTP/3 plugin error: {e}");
            send_simple_response(
                h3_stream,
                StatusCode::INTERNAL_SERVER_ERROR,
                Bytes::from_static(b"Plugin processing error"),
            )
            .await?;
            Ok(PluginResult::Handled)
        }
    }
}

async fn handle_request<S>(
    req: Request<()>,
    mut h3_stream: h3::server::RequestStream<S, Bytes>,
    sessions: SessionStoreBackend,
    registry: Arc<PluginRegistry>,
    peer_addr: SocketAddr,
    config: &Http3IngressConfig,
) -> std::result::Result<(), String>
where
    S: BidiStream<Bytes> + Send + 'static,
    S::RecvStream: RecvStream + Send,
    S::SendStream: SendStream<Bytes> + Send,
{
    if req.uri().path() == "/health" {
        return send_simple_response(&mut h3_stream, StatusCode::OK, Bytes::from_static(b"OK"))
            .await;
    }

    let host = host_header_value(&req).ok_or("Missing or invalid Host header")?;
    let tunnel_id = parse_and_normalize_host(Some(&host)).map_err(str::to_string)?;

    let ctx = RequestContext {
        tunnel_id: tunnel_id.clone(),
        session_id: uuid::Uuid::new_v4().to_string(),
        remote_addr: peer_addr,
        timestamp: SystemTime::now(),
    };

    let plugin_req = match run_request_plugins(req, &mut h3_stream, &registry, &ctx).await? {
        PluginResult::Continue(r) => *r,
        PluginResult::Handled => return Ok(()),
    };

    let multiplexer = match sessions.get_by_tunnel_id(&tunnel_id) {
        Some(session) => match &session.multiplexer {
            Some(m) => m.clone(),
            None => {
                return send_simple_response(
                    &mut h3_stream,
                    StatusCode::BAD_GATEWAY,
                    Bytes::from_static(b"Tunnel not ready"),
                )
                .await;
            }
        },
        None => {
            return send_simple_response(
                &mut h3_stream,
                StatusCode::NOT_FOUND,
                Bytes::from_static(b"Tunnel not found"),
            )
            .await;
        }
    };

    let is_grpc = is_grpc(plugin_req.headers());
    let protocol = if is_grpc {
        Protocol::GRPC
    } else {
        Protocol::HTTP
    };

    let (body_rx, stream_rx) = spawn_body_reader(h3_stream, config.max_request_body_size);
    let streaming_body = Http3RequestBody {
        receiver: body_rx,
        bytes_read: 0,
        max_size: config.max_request_body_size,
    };

    let forward_req = match build_forward_request(plugin_req, streaming_body, &host, is_grpc) {
        Ok(req) => req,
        Err(e) => {
            error!("HTTP/3 request conversion error: {e}");
            if let Ok(mut stream) = stream_rx.await {
                let _ = send_simple_response(&mut stream, StatusCode::BAD_REQUEST, Bytes::from(e))
                    .await;
            }
            return Ok(());
        }
    };

    let tunnel_stream = match multiplexer.open_stream(protocol).await {
        Ok(stream) => stream,
        Err(e) => {
            error!("Failed to open HTTP/3 tunnel stream: {e}");
            if let Ok(mut stream) = stream_rx.await {
                let _ = send_simple_response(
                    &mut stream,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Bytes::from_static(b"Failed to open stream"),
                )
                .await;
            }
            return Ok(());
        }
    };

    if is_grpc {
        forward_via_h2(forward_req, stream_rx, tunnel_stream, registry, ctx, config).await
    } else {
        forward_via_h1(forward_req, stream_rx, tunnel_stream, registry, ctx, config).await
    }
}

/// Forward request to upstream via HTTP/1.1 and relay the response back over H3.
async fn forward_via_h1<S>(
    forward_req: Request<BoxBody>,
    stream_rx: tokio::sync::oneshot::Receiver<h3::server::RequestStream<S, Bytes>>,
    tunnel_stream: ferrotunnel_core::transport::BoxedStream,
    registry: Arc<PluginRegistry>,
    ctx: RequestContext,
    config: &Http3IngressConfig,
) -> std::result::Result<(), String>
where
    S: BidiStream<Bytes> + Send + 'static,
    S::RecvStream: RecvStream + Send,
    S::SendStream: SendStream<Bytes> + Send,
{
    let io = TokioIo::new(tunnel_stream);

    let handshake_result = tokio::time::timeout(
        config.handshake_timeout,
        hyper::client::conn::http1::handshake(io),
    )
    .await;
    let (mut sender, conn) = match handshake_result {
        Ok(Ok(handshake)) => handshake,
        Ok(Err(e)) => {
            error!("HTTP/3 tunnel handshake failed: {e}");
            if let Ok(mut s) = stream_rx.await {
                let _ = send_simple_response(
                    &mut s,
                    StatusCode::BAD_GATEWAY,
                    Bytes::from_static(b"Tunnel handshake failed"),
                )
                .await;
            }
            return Ok(());
        }
        Err(_) => {
            error!("HTTP/3 tunnel handshake timeout");
            if let Ok(mut s) = stream_rx.await {
                let _ = send_simple_response(
                    &mut s,
                    StatusCode::GATEWAY_TIMEOUT,
                    Bytes::from_static(b"Tunnel handshake timeout"),
                )
                .await;
            }
            return Ok(());
        }
    };
    tokio::spawn(async move {
        if let Err(err) = conn.await {
            error!("HTTP/3 upstream connection failed: {err}");
        }
    });

    let response_result =
        tokio::time::timeout(config.response_timeout, sender.send_request(forward_req)).await;
    let response = match response_result {
        Ok(Ok(response)) => response,
        Ok(Err(e)) => {
            error!("HTTP/3 request failed: {e}");
            if let Ok(mut s) = stream_rx.await {
                let _ = send_simple_response(
                    &mut s,
                    StatusCode::BAD_GATEWAY,
                    Bytes::from_static(b"Failed to send request"),
                )
                .await;
            }
            return Ok(());
        }
        Err(_) => {
            error!("HTTP/3 upstream response timeout");
            if let Ok(mut s) = stream_rx.await {
                let _ = send_simple_response(
                    &mut s,
                    StatusCode::GATEWAY_TIMEOUT,
                    Bytes::from_static(b"Upstream response timeout"),
                )
                .await;
            }
            return Ok(());
        }
    };

    let mut h3_stream = stream_rx
        .await
        .map_err(|_| "body reader task dropped unexpectedly".to_string())?;
    send_upstream_response(response, &mut h3_stream, registry, ctx, config).await
}

/// Forward request to upstream via HTTP/2 (gRPC) and relay response back over H3.
async fn forward_via_h2<S>(
    forward_req: Request<BoxBody>,
    stream_rx: tokio::sync::oneshot::Receiver<h3::server::RequestStream<S, Bytes>>,
    tunnel_stream: ferrotunnel_core::transport::BoxedStream,
    registry: Arc<PluginRegistry>,
    ctx: RequestContext,
    config: &Http3IngressConfig,
) -> std::result::Result<(), String>
where
    S: BidiStream<Bytes> + Send + 'static,
    S::RecvStream: RecvStream + Send,
    S::SendStream: SendStream<Bytes> + Send,
{
    let io = TokioIo::new(tunnel_stream);

    let handshake_result = tokio::time::timeout(
        config.handshake_timeout,
        hyper::client::conn::http2::handshake(TokioExecutor::new(), io),
    )
    .await;
    let (mut sender, conn) = match handshake_result {
        Ok(Ok(handshake)) => handshake,
        Ok(Err(e)) => {
            error!("HTTP/3 gRPC tunnel handshake failed: {e}");
            if let Ok(mut s) = stream_rx.await {
                let _ = send_simple_response(
                    &mut s,
                    StatusCode::BAD_GATEWAY,
                    Bytes::from_static(b"gRPC tunnel handshake failed"),
                )
                .await;
            }
            return Ok(());
        }
        Err(_) => {
            error!("HTTP/3 gRPC tunnel handshake timeout");
            if let Ok(mut s) = stream_rx.await {
                let _ = send_simple_response(
                    &mut s,
                    StatusCode::GATEWAY_TIMEOUT,
                    Bytes::from_static(b"gRPC tunnel handshake timeout"),
                )
                .await;
            }
            return Ok(());
        }
    };
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let response_result =
        tokio::time::timeout(config.response_timeout, sender.send_request(forward_req)).await;
    let response = match response_result {
        Ok(Ok(response)) => response,
        Ok(Err(e)) => {
            error!("HTTP/3 gRPC request failed: {e}");
            if let Ok(mut s) = stream_rx.await {
                let _ = send_simple_response(
                    &mut s,
                    StatusCode::BAD_GATEWAY,
                    Bytes::from_static(b"gRPC request failed"),
                )
                .await;
            }
            return Ok(());
        }
        Err(_) => {
            error!("HTTP/3 gRPC upstream response timeout");
            if let Ok(mut s) = stream_rx.await {
                let _ = send_simple_response(
                    &mut s,
                    StatusCode::GATEWAY_TIMEOUT,
                    Bytes::from_static(b"gRPC upstream response timeout"),
                )
                .await;
            }
            return Ok(());
        }
    };

    let mut h3_stream = stream_rx
        .await
        .map_err(|_| "body reader task dropped unexpectedly".to_string())?;
    send_upstream_response(response, &mut h3_stream, registry, ctx, config).await
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn host_header_value(req: &Request<()>) -> Option<HeaderValue> {
    req.headers().get(HOST).cloned().or_else(|| {
        req.uri()
            .authority()
            .and_then(|authority| HeaderValue::from_str(authority.as_str()).ok())
    })
}

fn is_grpc(headers: &hyper::HeaderMap) -> bool {
    headers
        .get(hyper::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("application/grpc"))
}

/// Build the forward request with a streaming body (Section 1).
fn build_forward_request(
    req: Request<()>,
    body: Http3RequestBody,
    host: &HeaderValue,
    is_grpc: bool,
) -> std::result::Result<Request<BoxBody>, String> {
    let (mut parts, ()) = req.into_parts();
    // Sanitize hop-by-hop headers first, then insert the trusted canonical Host
    // so a client-supplied `Connection: host` cannot drop it.
    crate::ingress::strip_hop_by_hop_headers(&mut parts.headers);
    parts.headers.insert(HOST, host.clone());

    if is_grpc {
        parts.version = hyper::Version::HTTP_2;
        // hyper's H2 client does not add `TE: trailers`, which gRPC upstreams
        // need to return trailing metadata (grpc-status).
        parts
            .headers
            .insert(hyper::header::TE, HeaderValue::from_static("trailers"));
        if parts.uri.authority().is_none() {
            let path_and_query = parts
                .uri
                .path_and_query()
                .map_or("/", hyper::http::uri::PathAndQuery::as_str);
            let host = host
                .to_str()
                .map_err(|_| "Invalid Host header for gRPC request".to_string())?;
            // Section 4: scheme reflects client-facing protocol (QUIC+TLS = https).
            // The actual tunnel transport is plaintext over the multiplexed stream.
            parts.uri = format!("https://{host}{path_and_query}")
                .parse::<Uri>()
                .map_err(|e| format!("Invalid gRPC URI: {e}"))?;
        }
    } else if let Some(path_and_query) = parts.uri.path_and_query().cloned() {
        parts.version = hyper::Version::HTTP_11;
        parts.uri = Uri::builder()
            .path_and_query(path_and_query)
            .build()
            .map_err(|e| format!("Invalid HTTP/1.1 URI: {e}"))?;
    }

    Ok(Request::from_parts(parts, streaming_body_to_boxbody(body)))
}

/// Convert our streaming Http3RequestBody into the project's BoxBody type.
fn streaming_body_to_boxbody(body: Http3RequestBody) -> BoxBody {
    use futures::stream::poll_fn;
    use http_body_util::StreamBody;
    use hyper::body::Body as _;

    let mut body = body;
    let frame_stream = poll_fn(
        move |cx| -> Poll<Option<std::result::Result<Frame<Bytes>, io::Error>>> {
            match Pin::new(&mut body).poll_frame(cx) {
                Poll::Ready(Some(Ok(frame))) => Poll::Ready(Some(Ok(frame))),
                // Propagate the read error instead of converting it to a clean
                // EOF, so the upstream sees a failed request body rather than a
                // silently truncated one.
                Poll::Ready(Some(Err(e))) => {
                    error!("HTTP/3 request body error: {e}");
                    Poll::Ready(Some(Err(e)))
                }
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            }
        },
    );

    StreamBody::new(frame_stream).boxed()
}

// ---------------------------------------------------------------------------
// Response relay
// ---------------------------------------------------------------------------

async fn send_upstream_response<S>(
    response: Response<hyper::body::Incoming>,
    h3_stream: &mut h3::server::RequestStream<S, Bytes>,
    registry: Arc<PluginRegistry>,
    ctx: RequestContext,
    config: &Http3IngressConfig,
) -> std::result::Result<(), String>
where
    S: SendStream<Bytes>,
{
    if registry.needs_response_buffering().await {
        send_buffered_response(response, h3_stream, registry, ctx, config).await
    } else {
        send_streaming_response(response, h3_stream, config.response_timeout).await
    }
}

/// Section 5: per-frame timeout on response body streaming.
async fn send_streaming_response<S>(
    response: Response<hyper::body::Incoming>,
    h3_stream: &mut h3::server::RequestStream<S, Bytes>,
    frame_timeout: Duration,
) -> std::result::Result<(), String>
where
    S: SendStream<Bytes>,
{
    let (parts, mut body) = response.into_parts();
    let response = Response::from_parts(sanitize_response_parts(parts), ());
    h3_stream
        .send_response(response)
        .await
        .map_err(|e| format!("Failed to send HTTP/3 response headers: {e}"))?;

    loop {
        let frame = match tokio::time::timeout(frame_timeout, body.frame()).await {
            Ok(Some(frame)) => {
                frame.map_err(|e| format!("Failed to read upstream response body: {e}"))?
            }
            Ok(None) => break,
            Err(_) => {
                error!("HTTP/3 upstream body inactivity timeout");
                let _ = h3_stream.finish().await;
                return Err("upstream body inactivity timeout".into());
            }
        };

        if let Some(data) = frame.data_ref() {
            h3_stream
                .send_data(data.clone())
                .await
                .map_err(|e| format!("Failed to send HTTP/3 response data: {e}"))?;
        }
        if let Some(trailers) = frame.trailers_ref() {
            h3_stream
                .send_trailers(trailers.clone())
                .await
                .map_err(|e| format!("Failed to send HTTP/3 response trailers: {e}"))?;
        }
    }

    h3_stream
        .finish()
        .await
        .map_err(|e| format!("Failed to finish HTTP/3 response: {e}"))
}

/// Section 9: avoid redundant Bytes → Vec → Bytes round-trip.
async fn send_buffered_response<S>(
    response: Response<hyper::body::Incoming>,
    h3_stream: &mut h3::server::RequestStream<S, Bytes>,
    registry: Arc<PluginRegistry>,
    ctx: RequestContext,
    config: &Http3IngressConfig,
) -> std::result::Result<(), String>
where
    S: SendStream<Bytes>,
{
    let (parts, body) = response.into_parts();
    // Section 5: per-frame timeout also applies to buffered collection
    let body =
        collect_upstream_body(body, config.max_response_size, config.response_timeout).await?;
    let status = parts.status;

    // Plugin API requires Vec<u8> — convert once at the boundary
    let mut plugin_response = Response::from_parts(parts, body.to_vec());

    let response_ctx = ResponseContext {
        tunnel_id: ctx.tunnel_id,
        session_id: ctx.session_id,
        status_code: status.as_u16(),
        duration_ms: u64::try_from(ctx.timestamp.elapsed().unwrap_or_default().as_millis())
            .unwrap_or(u64::MAX),
    };

    if let Err(e) = registry
        .execute_response_hooks(&mut plugin_response, &response_ctx)
        .await
    {
        error!("HTTP/3 plugin response hook error: {e}");
    }

    let (parts, plugin_body) = plugin_response.into_parts();
    let response = Response::from_parts(sanitize_response_parts(parts), ());
    h3_stream
        .send_response(response)
        .await
        .map_err(|e| format!("Failed to send HTTP/3 response headers: {e}"))?;
    if !plugin_body.is_empty() {
        // Convert once from Vec<u8> back to Bytes after plugin processing
        h3_stream
            .send_data(Bytes::from(plugin_body))
            .await
            .map_err(|e| format!("Failed to send HTTP/3 response data: {e}"))?;
    }
    h3_stream
        .finish()
        .await
        .map_err(|e| format!("Failed to finish HTTP/3 response: {e}"))
}

/// Section 5: collect with per-frame timeout.
async fn collect_upstream_body(
    mut body: hyper::body::Incoming,
    max_size: usize,
    frame_timeout: Duration,
) -> std::result::Result<Bytes, String> {
    let mut collected = Vec::new();
    loop {
        let frame = match tokio::time::timeout(frame_timeout, body.frame()).await {
            Ok(Some(frame)) => {
                frame.map_err(|e| format!("Failed to read upstream response body: {e}"))?
            }
            Ok(None) => break,
            Err(_) => {
                return Err("upstream body collection inactivity timeout".into());
            }
        };
        if let Some(data) = frame.data_ref() {
            if collected.len() + data.len() > max_size {
                return Err("HTTP/3 upstream response body too large".into());
            }
            collected.extend_from_slice(data);
        }
    }
    Ok(Bytes::from(collected))
}

fn sanitize_response_parts(
    mut parts: hyper::http::response::Parts,
) -> hyper::http::response::Parts {
    crate::ingress::strip_hop_by_hop_headers(&mut parts.headers);
    parts
}

async fn send_simple_response<S>(
    h3_stream: &mut h3::server::RequestStream<S, Bytes>,
    status: StatusCode,
    body: Bytes,
) -> std::result::Result<(), String>
where
    S: SendStream<Bytes>,
{
    let response = Response::builder()
        .status(status)
        .body(())
        .map_err(|e| format!("Failed to build HTTP/3 response: {e}"))?;
    h3_stream
        .send_response(response)
        .await
        .map_err(|e| format!("Failed to send HTTP/3 response headers: {e}"))?;
    if !body.is_empty() {
        h3_stream
            .send_data(body)
            .await
            .map_err(|e| format!("Failed to send HTTP/3 response data: {e}"))?;
    }
    h3_stream
        .finish()
        .await
        .map_err(|e| format!("Failed to finish HTTP/3 response: {e}"))
}

/// Section 12.2: warn on invalid plugin status codes.
async fn send_plugin_response<S>(
    h3_stream: &mut h3::server::RequestStream<S, Bytes>,
    status: u16,
    headers: Vec<(String, String)>,
    body: Bytes,
) -> std::result::Result<(), String>
where
    S: SendStream<Bytes>,
{
    let status_code = match StatusCode::from_u16(status) {
        Ok(s) => s,
        Err(_) => {
            warn!(status, "invalid plugin respond status, falling back to 200");
            StatusCode::OK
        }
    };
    let mut response = Response::builder().status(status_code);
    for (name, value) in headers {
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(&value),
        ) {
            response = response.header(name, value);
        }
    }
    let response = response
        .body(())
        .map_err(|e| format!("Failed to build plugin HTTP/3 response: {e}"))?;
    h3_stream
        .send_response(response)
        .await
        .map_err(|e| format!("Failed to send plugin HTTP/3 response headers: {e}"))?;
    if !body.is_empty() {
        h3_stream
            .send_data(body)
            .await
            .map_err(|e| format!("Failed to send plugin HTTP/3 response data: {e}"))?;
    }
    h3_stream
        .finish()
        .await
        .map_err(|e| format!("Failed to finish plugin HTTP/3 response: {e}"))
}
