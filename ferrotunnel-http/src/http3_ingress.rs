use crate::ingress::parse_and_normalize_host;
use bytes::{Buf, Bytes};
use ferrotunnel_common::{Result, TunnelError};
use ferrotunnel_core::transport::tls::TlsTransportConfig;
use ferrotunnel_core::tunnel::session::SessionStoreBackend;
use ferrotunnel_plugin::{PluginAction, PluginRegistry, RequestContext, ResponseContext};
use ferrotunnel_protocol::frame::Protocol;
use h3::quic::{BidiStream, RecvStream, SendStream};
use h3_quinn::quinn::{Endpoint, ServerConfig, TransportConfig, VarInt};
use http_body_util::{BodyExt, Empty};
use hyper::body::Incoming;
use hyper::header::{HeaderName, HeaderValue, CONNECTION, HOST, TRANSFER_ENCODING, UPGRADE};
use hyper::{Request, Response, StatusCode, Uri};
use hyper_util::rt::{TokioExecutor, TokioIo};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::Semaphore;
use tracing::{error, info, warn};

const H3_ALPN: &[u8] = b"h3";
const DEFAULT_MAX_REQUEST_BODY_BYTES: usize = 100 * 1024 * 1024;

type BoxBody = http_body_util::combinators::BoxBody<Bytes, hyper::Error>;

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
}

impl Default for Http3IngressConfig {
    fn default() -> Self {
        Self {
            cert_path: String::new(),
            key_path: String::new(),
            ca_cert_path: None,
            client_auth: false,
            max_connections: 10_000,
            max_request_body_size: DEFAULT_MAX_REQUEST_BODY_BYTES,
            max_response_size: 100 * 1024 * 1024,
            handshake_timeout: Duration::from_secs(10),
            response_timeout: Duration::from_secs(60),
            alt_svc_max_age: 86_400,
        }
    }
}

impl Http3IngressConfig {
    pub fn alt_svc_header_value(
        &self,
        addr: SocketAddr,
    ) -> std::result::Result<HeaderValue, String> {
        HeaderValue::from_str(&format!(
            "h3=\":{}\"; ma={}",
            addr.port(),
            self.alt_svc_max_age
        ))
        .map_err(|e| format!("invalid Alt-Svc value: {e}"))
    }
}

pub struct Http3Ingress {
    addr: SocketAddr,
    sessions: SessionStoreBackend,
    registry: Arc<PluginRegistry>,
    config: Http3IngressConfig,
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
            config,
            connection_semaphore,
        }
    }

    pub async fn start(self) -> Result<()> {
        let endpoint = create_http3_endpoint(&self.config, self.addr)?;
        info!("HTTP/3 Ingress listening on {} (UDP)", self.addr);

        loop {
            let Some(incoming) = endpoint.accept().await else {
                return Err(TunnelError::Connection("HTTP/3 endpoint closed".into()));
            };
            let peer_addr = incoming.remote_address();

            let Ok(permit) = self.connection_semaphore.clone().try_acquire_owned() else {
                warn!(
                    "Max HTTP/3 connections reached, rejecting connection from {}",
                    peer_addr
                );
                continue;
            };

            let sessions = self.sessions.clone();
            let registry = self.registry.clone();
            let config = self.config.clone();

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
                    handle_connection(connection, sessions, registry, peer_addr, config).await
                {
                    error!("HTTP/3 connection error from {peer_addr}: {e}");
                }
            });
        }
    }
}

fn create_http3_endpoint(config: &Http3IngressConfig, bind_addr: SocketAddr) -> Result<Endpoint> {
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
    transport.max_idle_timeout(Some(
        config
            .response_timeout
            .try_into()
            .map_err(|e| TunnelError::Config(format!("HTTP/3 idle timeout: {e}")))?,
    ));
    transport.max_concurrent_bidi_streams(VarInt::from_u32(256));

    let mut server_config = ServerConfig::with_crypto(Arc::new(crypto));
    server_config.transport_config(Arc::new(transport));
    Endpoint::server(server_config, bind_addr).map_err(Into::into)
}

async fn handle_connection(
    connection: quinn::Connection,
    sessions: SessionStoreBackend,
    registry: Arc<PluginRegistry>,
    peer_addr: SocketAddr,
    config: Http3IngressConfig,
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
                let config = config.clone();
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
                        handle_request(req, stream, sessions, registry, peer_addr, config).await
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

#[allow(clippy::too_many_lines)]
async fn handle_request<S>(
    req: Request<()>,
    mut h3_stream: h3::server::RequestStream<S, Bytes>,
    sessions: SessionStoreBackend,
    registry: Arc<PluginRegistry>,
    peer_addr: SocketAddr,
    config: Http3IngressConfig,
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

    let mut plugin_req = req;
    match registry.execute_request_hooks(&mut plugin_req, &ctx).await {
        Ok(PluginAction::Continue | PluginAction::Modify { .. }) => {}
        Ok(PluginAction::Reject { status, reason }) => {
            return send_simple_response(
                &mut h3_stream,
                StatusCode::from_u16(status).unwrap_or(StatusCode::FORBIDDEN),
                Bytes::from(reason),
            )
            .await;
        }
        Ok(PluginAction::Respond {
            status,
            headers,
            body,
        }) => {
            return send_plugin_response(&mut h3_stream, status, headers, Bytes::from(body)).await;
        }
        Err(e) => {
            error!("HTTP/3 plugin error: {e}");
            return send_simple_response(
                &mut h3_stream,
                StatusCode::INTERNAL_SERVER_ERROR,
                Bytes::from_static(b"Plugin processing error"),
            )
            .await;
        }
    }

    let multiplexer = if let Some(session) = sessions.get_by_tunnel_id(&tunnel_id) {
        if let Some(multiplexer) = &session.multiplexer {
            multiplexer.clone()
        } else {
            return send_simple_response(
                &mut h3_stream,
                StatusCode::BAD_GATEWAY,
                Bytes::from_static(b"Tunnel not ready"),
            )
            .await;
        }
    } else {
        return send_simple_response(
            &mut h3_stream,
            StatusCode::NOT_FOUND,
            Bytes::from_static(b"Tunnel not found"),
        )
        .await;
    };

    let is_grpc = is_grpc(plugin_req.headers());
    let protocol = if is_grpc {
        Protocol::GRPC
    } else {
        Protocol::HTTP
    };

    let request_body = match collect_h3_request_body(&mut h3_stream, config.max_request_body_size)
        .await
    {
        Ok(body) => body,
        Err(e) if e.contains("too large") => {
            return send_simple_response(
                &mut h3_stream,
                StatusCode::PAYLOAD_TOO_LARGE,
                Bytes::from(e),
            )
            .await;
        }
        Err(e) => {
            error!("HTTP/3 request body error: {e}");
            return send_simple_response(&mut h3_stream, StatusCode::BAD_REQUEST, Bytes::from(e))
                .await;
        }
    };
    let forward_req = match build_forward_request(plugin_req, request_body, &host, is_grpc) {
        Ok(req) => req,
        Err(e) => {
            error!("HTTP/3 request conversion error: {e}");
            return send_simple_response(&mut h3_stream, StatusCode::BAD_REQUEST, Bytes::from(e))
                .await;
        }
    };

    let stream = match multiplexer.open_stream(protocol).await {
        Ok(stream) => stream,
        Err(e) => {
            error!("Failed to open HTTP/3 tunnel stream: {e}");
            return send_simple_response(
                &mut h3_stream,
                StatusCode::INTERNAL_SERVER_ERROR,
                Bytes::from_static(b"Failed to open stream"),
            )
            .await;
        }
    };
    let io = TokioIo::new(stream);

    if is_grpc {
        let handshake_result = tokio::time::timeout(
            config.handshake_timeout,
            hyper::client::conn::http2::handshake(TokioExecutor::new(), io),
        )
        .await;
        let (mut sender, conn) = match handshake_result {
            Ok(Ok(handshake)) => handshake,
            Ok(Err(e)) => {
                error!("HTTP/3 gRPC tunnel handshake failed: {e}");
                return send_simple_response(
                    &mut h3_stream,
                    StatusCode::BAD_GATEWAY,
                    Bytes::from_static(b"gRPC tunnel handshake failed"),
                )
                .await;
            }
            Err(_) => {
                error!("HTTP/3 gRPC tunnel handshake timeout");
                return send_simple_response(
                    &mut h3_stream,
                    StatusCode::GATEWAY_TIMEOUT,
                    Bytes::from_static(b"gRPC tunnel handshake timeout"),
                )
                .await;
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
                return send_simple_response(
                    &mut h3_stream,
                    StatusCode::BAD_GATEWAY,
                    Bytes::from_static(b"gRPC request failed"),
                )
                .await;
            }
            Err(_) => {
                error!("HTTP/3 gRPC upstream response timeout");
                return send_simple_response(
                    &mut h3_stream,
                    StatusCode::GATEWAY_TIMEOUT,
                    Bytes::from_static(b"gRPC upstream response timeout"),
                )
                .await;
            }
        };
        return send_upstream_response(response, h3_stream, registry, ctx, config).await;
    }

    let handshake_result = tokio::time::timeout(
        config.handshake_timeout,
        hyper::client::conn::http1::handshake(io),
    )
    .await;
    let (mut sender, conn) = match handshake_result {
        Ok(Ok(handshake)) => handshake,
        Ok(Err(e)) => {
            error!("HTTP/3 tunnel handshake failed: {e}");
            return send_simple_response(
                &mut h3_stream,
                StatusCode::BAD_GATEWAY,
                Bytes::from_static(b"Tunnel handshake failed"),
            )
            .await;
        }
        Err(_) => {
            error!("HTTP/3 tunnel handshake timeout");
            return send_simple_response(
                &mut h3_stream,
                StatusCode::GATEWAY_TIMEOUT,
                Bytes::from_static(b"Tunnel handshake timeout"),
            )
            .await;
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
            return send_simple_response(
                &mut h3_stream,
                StatusCode::BAD_GATEWAY,
                Bytes::from_static(b"Failed to send request"),
            )
            .await;
        }
        Err(_) => {
            error!("HTTP/3 upstream response timeout");
            return send_simple_response(
                &mut h3_stream,
                StatusCode::GATEWAY_TIMEOUT,
                Bytes::from_static(b"Upstream response timeout"),
            )
            .await;
        }
    };
    send_upstream_response(response, h3_stream, registry, ctx, config).await
}

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

async fn collect_h3_request_body<S>(
    h3_stream: &mut h3::server::RequestStream<S, Bytes>,
    max_size: usize,
) -> std::result::Result<Bytes, String>
where
    S: RecvStream,
{
    let mut collected = Vec::new();
    while let Some(mut chunk) = h3_stream
        .recv_data()
        .await
        .map_err(|e| format!("Failed to read HTTP/3 request body: {e}"))?
    {
        let len = chunk.remaining();
        if collected.len() + len > max_size {
            return Err("HTTP/3 request body too large".into());
        }
        let bytes = chunk.copy_to_bytes(len);
        collected.extend_from_slice(&bytes);
    }
    Ok(Bytes::from(collected))
}

fn build_forward_request(
    req: Request<()>,
    body: Bytes,
    host: &HeaderValue,
    is_grpc: bool,
) -> std::result::Result<Request<BoxBody>, String> {
    let (mut parts, ()) = req.into_parts();
    parts.headers.insert(HOST, host.clone());
    remove_connection_specific_headers(&mut parts.headers);

    if is_grpc {
        parts.version = hyper::Version::HTTP_2;
        if parts.uri.authority().is_none() {
            let path_and_query = parts
                .uri
                .path_and_query()
                .map_or("/", hyper::http::uri::PathAndQuery::as_str);
            let host = host
                .to_str()
                .map_err(|_| "Invalid Host header for gRPC request".to_string())?;
            parts.uri = format!("http://{host}{path_and_query}")
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

    Ok(Request::from_parts(parts, full_body(body)))
}

fn remove_connection_specific_headers(headers: &mut hyper::HeaderMap) {
    headers.remove(CONNECTION);
    headers.remove(TRANSFER_ENCODING);
    headers.remove(UPGRADE);
}

async fn send_upstream_response<S>(
    response: Response<Incoming>,
    mut h3_stream: h3::server::RequestStream<S, Bytes>,
    registry: Arc<PluginRegistry>,
    ctx: RequestContext,
    config: Http3IngressConfig,
) -> std::result::Result<(), String>
where
    S: BidiStream<Bytes> + Send + 'static,
    S::RecvStream: RecvStream + Send,
    S::SendStream: SendStream<Bytes> + Send,
{
    if registry.needs_response_buffering().await {
        send_buffered_response(response, &mut h3_stream, registry, ctx, config).await
    } else {
        send_streaming_response(response, &mut h3_stream).await
    }
}

async fn send_streaming_response<S>(
    response: Response<Incoming>,
    h3_stream: &mut h3::server::RequestStream<S, Bytes>,
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

    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|e| format!("Failed to read upstream response body: {e}"))?;
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

async fn send_buffered_response<S>(
    response: Response<Incoming>,
    h3_stream: &mut h3::server::RequestStream<S, Bytes>,
    registry: Arc<PluginRegistry>,
    ctx: RequestContext,
    config: Http3IngressConfig,
) -> std::result::Result<(), String>
where
    S: SendStream<Bytes>,
{
    let (parts, body) = response.into_parts();
    let body = collect_upstream_body(body, config.max_response_size).await?;
    let status = parts.status;
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

    let (parts, body) = plugin_response.into_parts();
    let response = Response::from_parts(sanitize_response_parts(parts), ());
    h3_stream
        .send_response(response)
        .await
        .map_err(|e| format!("Failed to send HTTP/3 response headers: {e}"))?;
    if !body.is_empty() {
        h3_stream
            .send_data(Bytes::from(body))
            .await
            .map_err(|e| format!("Failed to send HTTP/3 response data: {e}"))?;
    }
    h3_stream
        .finish()
        .await
        .map_err(|e| format!("Failed to finish HTTP/3 response: {e}"))
}

async fn collect_upstream_body(
    mut body: Incoming,
    max_size: usize,
) -> std::result::Result<Bytes, String> {
    let mut collected = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|e| format!("Failed to read upstream response body: {e}"))?;
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
    remove_connection_specific_headers(&mut parts.headers);
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

async fn send_plugin_response<S>(
    h3_stream: &mut h3::server::RequestStream<S, Bytes>,
    status: u16,
    headers: Vec<(String, String)>,
    body: Bytes,
) -> std::result::Result<(), String>
where
    S: SendStream<Bytes>,
{
    let mut response =
        Response::builder().status(StatusCode::from_u16(status).unwrap_or(StatusCode::OK));
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

fn full_body(bytes: Bytes) -> BoxBody {
    http_body_util::Full::new(bytes)
        .map_err(|never| match never {})
        .boxed()
}

#[allow(dead_code)]
fn empty_body() -> BoxBody {
    Empty::<Bytes>::new()
        .map_err(|never| match never {})
        .boxed()
}
