use crate::accept_errors::{is_transient_accept_error, AcceptBackoff};
use ferrotunnel_common::Result;
use ferrotunnel_core::tunnel::session::SessionStoreBackend;
use ferrotunnel_plugin::{PluginAction, PluginRegistry, RequestContext, ResponseContext};
use ferrotunnel_protocol::frame::Protocol;
use http_body_util::{BodyExt, Empty};
use hyper::body::Bytes;
use hyper::header::HeaderValue;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as AutoBuilder;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tracing::{error, info, warn};

/// Configuration for HTTP ingress limits and timeouts
#[derive(Debug, Clone)]
pub struct IngressConfig {
    /// Maximum concurrent connections (default: 10000)
    pub max_connections: usize,
    /// Maximum request body size in bytes (default: 100MB)
    pub max_request_body_size: usize,
    /// Maximum response body size in bytes (default: 100MB)
    pub max_response_size: usize,
    /// Timeout for upstream handshake (default: 10s)
    pub handshake_timeout: Duration,
    /// Timeout for upstream response (default: 60s)
    pub response_timeout: Duration,
    /// Optional Alt-Svc header value to advertise HTTP/3 ingress.
    pub alt_svc_header: Option<HeaderValue>,
}

impl Default for IngressConfig {
    fn default() -> Self {
        Self {
            max_connections: 10000,
            max_request_body_size: 100 * 1024 * 1024, // 100MB
            max_response_size: 100 * 1024 * 1024,     // 100MB
            handshake_timeout: Duration::from_secs(10),
            response_timeout: Duration::from_mins(1),
            alt_svc_header: None,
        }
    }
}

pub struct HttpIngress {
    addr: SocketAddr,
    sessions: SessionStoreBackend,
    registry: Arc<PluginRegistry>,
    config: IngressConfig,
    connection_semaphore: Arc<Semaphore>,
}

type BoxBody = http_body_util::combinators::BoxBody<Bytes, hyper::Error>;

impl HttpIngress {
    pub fn new(
        addr: SocketAddr,
        sessions: SessionStoreBackend,
        registry: Arc<PluginRegistry>,
    ) -> Self {
        Self::with_config(addr, sessions, registry, IngressConfig::default())
    }

    pub fn with_config(
        addr: SocketAddr,
        sessions: SessionStoreBackend,
        registry: Arc<PluginRegistry>,
        config: IngressConfig,
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

    #[must_use]
    pub fn with_alt_svc_header(mut self, value: HeaderValue) -> Self {
        self.config.alt_svc_header = Some(value);
        self
    }

    pub async fn start(self) -> Result<()> {
        let listener = TcpListener::bind(self.addr).await?;
        info!(
            "HTTP Ingress listening on {} (HTTP/1.1 + HTTP/2)",
            self.addr
        );

        let mut accept_backoff = AcceptBackoff::default();
        loop {
            let (stream, peer_addr) = match listener.accept().await {
                Ok(connection) => {
                    accept_backoff.reset();
                    connection
                }
                Err(error) if is_transient_accept_error(&error) => {
                    let (delay, should_log) = accept_backoff.record_failure();
                    if should_log {
                        warn!(
                            bind_addr = %self.addr,
                            error = %error,
                            backoff_ms = delay.as_millis(),
                            "Transient HTTP ingress accept error; backing off"
                        );
                    }
                    tokio::time::sleep(delay).await;
                    continue;
                }
                Err(error) => return Err(error.into()),
            };

            // Acquire connection permit (limit concurrent connections)
            let Ok(permit) = self.connection_semaphore.clone().try_acquire_owned() else {
                warn!(
                    "Max connections reached, rejecting connection from {}",
                    peer_addr
                );
                drop(stream);
                continue;
            };

            let io = TokioIo::new(stream);
            let registry = self.registry.clone();
            let sessions = self.sessions.clone();
            let config = self.config.clone();

            tokio::spawn(async move {
                let _permit = permit; // Hold permit until connection closes

                let service = service_fn(move |req| {
                    handle_request(
                        req,
                        sessions.clone(),
                        registry.clone(),
                        peer_addr,
                        config.clone(),
                    )
                });

                let builder = AutoBuilder::new(TokioExecutor::new());
                if let Err(err) = builder.serve_connection_with_upgrades(io, service).await {
                    if !is_connection_close_error(&err) {
                        error!("Error serving connection: {:?}", err);
                    }
                }
            });
        }
    }
}

#[allow(clippy::too_many_lines)]
async fn handle_request(
    mut req: Request<hyper::body::Incoming>,
    sessions: SessionStoreBackend,
    registry: Arc<PluginRegistry>,
    peer_addr: SocketAddr,
    config: IngressConfig,
) -> std::result::Result<Response<BoxBody>, hyper::Error> {
    // 0. Global Health Check
    if req.uri().path() == "/health" {
        return Ok(full_response(StatusCode::OK, "OK"));
    }

    // 1. Parse and normalize Host header
    let tunnel_id = match parse_and_normalize_host(req.headers().get("host")) {
        Ok(host) => host,
        Err(msg) => {
            return Ok(full_response(StatusCode::BAD_REQUEST, msg));
        }
    };

    let ctx = RequestContext {
        tunnel_id: tunnel_id.clone(),
        session_id: uuid::Uuid::new_v4().to_string(),
        remote_addr: peer_addr,
        timestamp: SystemTime::now(),
    };

    let is_ws = is_websocket_upgrade(req.headers());

    let client_upgrade = if is_ws {
        Some(hyper::upgrade::on(&mut req))
    } else {
        None
    };

    // 1. Run Request Hooks (On Headers Only - No Body Buffering)
    let (mut parts, body) = req.into_parts();

    // Create a temporary request with empty body for plugins to inspect headers
    let mut plugin_req = Request::from_parts(parts.clone(), ());

    match registry.execute_request_hooks(&mut plugin_req, &ctx).await {
        Ok(PluginAction::Continue | PluginAction::Modify { .. }) => {
            // If modified, update parts (headers/uri/method)
            // Note: Body modification is not supported in streaming mode yet
            let (new_parts, ()) = plugin_req.into_parts();
            parts = new_parts;
        }
        Ok(PluginAction::Reject { status, reason }) => {
            return Ok(full_response(
                StatusCode::from_u16(status).unwrap_or(StatusCode::FORBIDDEN),
                &reason,
            ));
        }
        Ok(PluginAction::Respond {
            status,
            headers,
            body,
        }) => {
            let mut res =
                Response::builder().status(StatusCode::from_u16(status).unwrap_or(StatusCode::OK));
            for (k, v) in headers {
                res = res.header(k, v);
            }
            return Ok(res.body(full_body(Bytes::from(body))).unwrap_or_else(|_| {
                full_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to build plugin response",
                )
            }));
        }

        Err(e) => {
            error!("Plugin error: {}", e);
            return Ok(full_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Plugin processing error",
            ));
        }
    }

    // 2. Identify Target Session (Routing Fix)
    // FIX #27: Use get_by_tunnel_id instead of find_multiplexer
    // We try to find by exact match of host (tunnel_id).
    // If not found, we could fallback to find_multiplexer() ONLY for local dev/testing if needed,
    // but for security we should be strict.
    // However, for verify plan "Routing Fix", strict lookup is key.

    // We need to clone multiplexer from the Ref
    let multiplexer = if let Some(session) = sessions.get_by_tunnel_id(&tunnel_id) {
        if let Some(m) = &session.multiplexer {
            m.clone()
        } else {
            return Ok(full_response(StatusCode::BAD_GATEWAY, "Tunnel not ready"));
        }
    } else {
        // Fallback for "unknown" host or direct IP access (development mode?)
        // If we want to support default tunnel for testing, we can keep find_multiplexer logic
        // BUT strict multi-tenancy requires us to fail.
        // Let's assume strict for now as per issue description "Insecure Global Routing".
        return Ok(full_response(StatusCode::NOT_FOUND, "Tunnel not found"));
    };

    // Reconstruct request for forwarding using the ORIGINAL streaming body
    // FIX #28: No body buffering here.
    // Recompute gRPC after plugin hooks: plugins may add or remove Content-Type.
    let is_grpc = is_grpc(&parts.headers);
    let protocol = if is_ws {
        Protocol::WebSocket
    } else if is_grpc {
        Protocol::GRPC
    } else {
        Protocol::HTTP
    };

    // Reject oversized request bodies before forwarding upstream.
    // A declared Content-Length over the limit is rejected up front with 413.
    // Bodies without a Content-Length (e.g. chunked) are hard-capped by the
    // `Limited` wrapper below so they cannot pin a permit + memory unboundedly.
    if content_length_exceeds(&parts.headers, config.max_request_body_size) {
        warn!("Request body exceeds max_request_body_size; rejecting with 413");
        return Ok(full_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            "Request body too large",
        ));
    }

    let limited_body = http_body_util::Limited::new(body, config.max_request_body_size).boxed();
    let mut forward_req = Request::from_parts(parts, limited_body);

    // HTTP/2 (gRPC) requires an absolute URI (scheme + authority).
    // Callers often send requests with a path-only URI and a Host header;
    // reconstruct the absolute form so the h2 client can set :scheme/:authority.
    if is_grpc && forward_req.uri().authority().is_none() {
        let canonical_uri = forward_req
            .headers()
            .get(hyper::header::HOST)
            .and_then(|h| h.to_str().ok())
            .and_then(|host| {
                let path_and_query = forward_req
                    .uri()
                    .path_and_query()
                    .map_or("/", hyper::http::uri::PathAndQuery::as_str);
                format!("http://{host}{path_and_query}")
                    .parse::<hyper::Uri>()
                    .ok()
            });
        if let Some(uri) = canonical_uri {
            *forward_req.uri_mut() = uri;
        }
    }

    // 3. Open Stream
    let stream = match multiplexer.open_stream(protocol).await {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to open stream: {}", e);
            return Ok(full_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to open stream",
            ));
        }
    };

    // 4. Handshake and Send Request (with timeout)
    let io = TokioIo::new(stream);

    // gRPC: forward over HTTP/2, preserving trailers (grpc-status, grpc-message)
    if is_grpc {
        let handshake_result = tokio::time::timeout(
            config.handshake_timeout,
            hyper::client::conn::http2::handshake(TokioExecutor::new(), io),
        )
        .await;

        let (mut sender, conn) = match handshake_result {
            Ok(Ok(res)) => res,
            Ok(Err(e)) => {
                error!("gRPC tunnel handshake failed: {}", e);
                return Ok(full_response(
                    StatusCode::BAD_GATEWAY,
                    "gRPC tunnel handshake failed",
                ));
            }
            Err(_) => {
                error!("gRPC tunnel handshake timeout");
                return Ok(full_response(
                    StatusCode::GATEWAY_TIMEOUT,
                    "gRPC tunnel handshake timeout",
                ));
            }
        };

        tokio::spawn(async move {
            let _ = conn.await;
        });

        let response_result =
            tokio::time::timeout(config.response_timeout, sender.send_request(forward_req)).await;

        let res = match response_result {
            Ok(Ok(res)) => res,
            Ok(Err(e)) => {
                error!("gRPC request failed: {}", e);
                return Ok(full_response(
                    StatusCode::BAD_GATEWAY,
                    "gRPC request failed",
                ));
            }
            Err(_) => {
                error!("gRPC response timeout");
                return Ok(full_response(
                    StatusCode::GATEWAY_TIMEOUT,
                    "gRPC upstream response timeout",
                ));
            }
        };

        let (parts, body) = res.into_parts();
        // Stream the response body directly to preserve HTTP/2 trailers
        return Ok(with_alt_svc_header(
            Response::from_parts(parts, body.boxed()),
            &config,
        ));
    }

    let handshake_result = tokio::time::timeout(
        config.handshake_timeout,
        hyper::client::conn::http1::handshake(io),
    )
    .await;

    let (mut sender, conn) = match handshake_result {
        Ok(Ok(res)) => res,
        Ok(Err(e)) => {
            error!("Handshake failed: {}", e);
            return Ok(full_response(
                StatusCode::BAD_GATEWAY,
                "Tunnel handshake failed",
            ));
        }
        Err(_) => {
            error!("Handshake timeout");
            return Ok(full_response(
                StatusCode::GATEWAY_TIMEOUT,
                "Tunnel handshake timeout",
            ));
        }
    };

    tokio::spawn(async move {
        if let Err(err) = conn.with_upgrades().await {
            error!("Connection failed: {:?}", err);
        }
    });

    // 5. Send Request and receive response (with timeout)
    let response_result =
        tokio::time::timeout(config.response_timeout, sender.send_request(forward_req)).await;

    let res = match response_result {
        Ok(Ok(res)) => res,
        Ok(Err(e)) => {
            error!("Failed to send request: {}", e);
            return Ok(full_response(
                StatusCode::BAD_GATEWAY,
                "Failed to send request",
            ));
        }
        Err(_) => {
            error!("Response timeout");
            return Ok(full_response(
                StatusCode::GATEWAY_TIMEOUT,
                "Upstream response timeout",
            ));
        }
    };

    if is_ws && res.status() == StatusCode::SWITCHING_PROTOCOLS {
        let upstream_headers = res.headers().clone();
        let tunnel_upgrade = hyper::upgrade::on(res);

        if let Some(client_upgrade) = client_upgrade {
            tokio::spawn(async move {
                let (tunnel_result, client_result) = tokio::join!(tunnel_upgrade, client_upgrade);

                let tunnel_upgraded = match tunnel_result {
                    Ok(u) => u,
                    Err(e) => {
                        error!("Tunnel upgrade failed: {e}");
                        return;
                    }
                };
                let client_upgraded = match client_result {
                    Ok(u) => u,
                    Err(e) => {
                        error!("Client upgrade failed: {e}");
                        return;
                    }
                };

                let mut tunnel_io = TokioIo::new(tunnel_upgraded);
                let mut client_io = TokioIo::new(client_upgraded);
                if let Err(e) = tokio::io::copy_bidirectional(&mut tunnel_io, &mut client_io).await
                {
                    error!("WebSocket copy error: {e}");
                }
            });
        }

        let mut client_res = Response::builder().status(StatusCode::SWITCHING_PROTOCOLS);
        for (key, value) in &upstream_headers {
            client_res = client_res.header(key, value);
        }
        let client_res = client_res
            .body(
                Empty::<Bytes>::new()
                    .map_err(|never| match never {})
                    .boxed(),
            )
            .unwrap_or_else(|_| {
                full_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to build upgrade response",
                )
            });
        return Ok(client_res);
    }

    let (parts, body) = res.into_parts();

    if !registry.needs_response_buffering().await {
        let streaming_body = body.boxed();
        return Ok(with_alt_svc_header(
            Response::from_parts(parts, streaming_body),
            &config,
        ));
    }

    // Buffer response for plugin processing
    let body_bytes = match collect_body_with_limit(
        body,
        config.max_response_size,
        config.response_timeout,
    )
    .await
    {
        Ok(bytes) => bytes,
        Err(err) => {
            error!("Response body error: {}", err.message());
            return Ok(full_response(err.status(), err.message()));
        }
    };

    let mut proxy_res = Response::from_parts(parts, body_bytes.to_vec());

    let response_ctx = ResponseContext {
        tunnel_id: ctx.tunnel_id.clone(),
        session_id: ctx.session_id.clone(),
        status_code: proxy_res.status().as_u16(),
        duration_ms: u64::try_from(ctx.timestamp.elapsed().unwrap_or_default().as_millis())
            .unwrap_or(u64::MAX),
    };

    // Run Response Hooks
    match registry
        .execute_response_hooks(&mut proxy_res, &response_ctx)
        .await
    {
        Ok(PluginAction::Continue | _) => {}
        // A hook error (including an isolated plugin panic, see #138) leaves
        // `proxy_res` holding the empty placeholder swapped in by the registry,
        // so we must not fall through and ship it. Return a clean 500 instead of
        // a silent empty 200.
        Err(e) => {
            error!("Plugin response hook error: {}", e);
            return Ok(full_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Plugin processing error",
            ));
        }
    }

    let (final_parts, final_body) = proxy_res.into_parts();
    let boxed_body = http_body_util::Full::new(Bytes::from(final_body))
        .map_err(|never| match never {})
        .boxed();

    Ok(with_alt_svc_header(
        Response::from_parts(final_parts, boxed_body),
        &config,
    ))
}

/// Returns `true` if the request declares a `Content-Length` larger than `max_size`.
///
/// Used to reject oversized request bodies up front with `413 Payload Too Large`
/// before any upstream stream is opened.
fn content_length_exceeds(headers: &hyper::HeaderMap, max_size: usize) -> bool {
    headers
        .get(hyper::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .is_some_and(|len| len > max_size as u64)
}

fn is_grpc(headers: &hyper::HeaderMap) -> bool {
    headers
        .get(hyper::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("application/grpc"))
}

fn is_websocket_upgrade(headers: &hyper::HeaderMap) -> bool {
    let upgrade = headers
        .get(hyper::header::UPGRADE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("websocket"));
    let connection = headers
        .get(hyper::header::CONNECTION)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| {
            v.split(',')
                .any(|s| s.trim().eq_ignore_ascii_case("upgrade"))
        });
    upgrade && connection
}

/// Parse and normalize the Host header for secure multi-tenant routing.
/// Handles IPv6 addresses, port stripping, and case normalization.
pub(crate) fn parse_and_normalize_host(
    host_header: Option<&HeaderValue>,
) -> std::result::Result<String, &'static str> {
    let host_str = host_header
        .and_then(|h| h.to_str().ok())
        .ok_or("Missing or invalid Host header")?;

    if host_str.is_empty() {
        return Err("Empty Host header");
    }

    let host = host_str.trim();

    // Handle IPv6 addresses in bracket notation: [::1]:8080 -> ::1
    let normalized = if host.starts_with('[') {
        // IPv6 with brackets
        if let Some(bracket_end) = host.find(']') {
            // Extract the IPv6 address without brackets
            &host[1..bracket_end]
        } else {
            return Err("Invalid IPv6 Host header format");
        }
    } else if host.contains(':') && host.matches(':').count() > 1 {
        // IPv6 without brackets (unusual but possible)
        host
    } else {
        // IPv4 or hostname - strip port if present
        host.split(':').next().unwrap_or(host)
    };

    // Normalize: lowercase and strip trailing dot (FQDN format)
    let normalized = normalized.to_lowercase();
    let normalized = normalized.strip_suffix('.').unwrap_or(&normalized);

    if normalized.is_empty() {
        return Err("Empty host after normalization");
    }

    Ok(normalized.to_string())
}

/// Initial reserve for response body collection to reduce reallocations.
const BODY_COLLECT_RESERVE: usize = 64 * 1024;

/// Reason a buffered response body could not be collected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BodyCollectError {
    /// A frame did not arrive within the per-frame timeout (stalled upstream).
    Timeout,
    /// The collected body exceeded `max_size`.
    TooLarge,
    /// The upstream body stream returned a read error.
    Read,
}

impl BodyCollectError {
    /// HTTP status to return to the downstream client for this failure.
    fn status(self) -> StatusCode {
        match self {
            BodyCollectError::Timeout => StatusCode::GATEWAY_TIMEOUT,
            BodyCollectError::TooLarge | BodyCollectError::Read => StatusCode::BAD_GATEWAY,
        }
    }

    /// Human-readable message for the response body and logs.
    fn message(self) -> &'static str {
        match self {
            BodyCollectError::Timeout => "Upstream response body timeout",
            BodyCollectError::TooLarge => "Response body too large",
            BodyCollectError::Read => "Error reading response body",
        }
    }
}

/// Collect a response body with a size limit and a per-frame read timeout.
///
/// The size limit prevents unbounded memory use; the per-frame timeout ensures a
/// stalled upstream cannot pin a connection permit and buffered memory
/// indefinitely (mirrors the HTTP/3 ingress `collect_upstream_body`).
async fn collect_body_with_limit<B>(
    mut body: B,
    max_size: usize,
    frame_timeout: Duration,
) -> std::result::Result<Bytes, BodyCollectError>
where
    B: hyper::body::Body<Data = Bytes> + Unpin,
{
    let mut collected = Vec::with_capacity(BODY_COLLECT_RESERVE.min(max_size));

    loop {
        let frame = match tokio::time::timeout(frame_timeout, body.frame()).await {
            Ok(Some(Ok(frame))) => frame,
            Ok(Some(Err(_))) => return Err(BodyCollectError::Read),
            Ok(None) => break,
            Err(_) => return Err(BodyCollectError::Timeout),
        };

        if let Some(data) = frame.data_ref() {
            if collected.len() + data.len() > max_size {
                return Err(BodyCollectError::TooLarge);
            }
            collected.extend_from_slice(data);
        }
    }

    Ok(Bytes::from(collected))
}

/// Static bytes for common response bodies to avoid allocation.
const BODY_OK: &[u8] = b"OK";
const BODY_TUNNEL_NOT_READY: &[u8] = b"Tunnel not ready";
const BODY_TUNNEL_NOT_FOUND: &[u8] = b"Tunnel not found";

fn full_response(status: StatusCode, body: &str) -> Response<BoxBody> {
    let bytes = match body {
        "OK" => Bytes::from_static(BODY_OK),
        "Tunnel not ready" => Bytes::from_static(BODY_TUNNEL_NOT_READY),
        "Tunnel not found" => Bytes::from_static(BODY_TUNNEL_NOT_FOUND),
        "Internal error"
        | "Failed to build plugin response"
        | "Plugin processing error"
        | "Failed to open stream"
        | "Tunnel handshake failed"
        | "Tunnel handshake timeout"
        | "Failed to send request"
        | "Upstream response timeout"
        | "Response timeout" => Bytes::copy_from_slice(body.as_bytes()),
        _ => Bytes::copy_from_slice(body.as_bytes()),
    };
    // Response::builder() with valid status and body should never fail
    Response::builder()
        .status(status)
        .body(
            http_body_util::Full::new(bytes)
                .map_err(|never| match never {})
                .boxed(),
        )
        .unwrap_or_else(|_| {
            Response::new(
                http_body_util::Full::new(Bytes::new())
                    .map_err(|never| match never {})
                    .boxed(),
            )
        })
}

fn full_body(bytes: Bytes) -> BoxBody {
    http_body_util::Full::new(bytes)
        .map_err(|never| match never {})
        .boxed()
}

fn with_alt_svc_header(
    mut response: Response<BoxBody>,
    config: &IngressConfig,
) -> Response<BoxBody> {
    if let Some(value) = &config.alt_svc_header {
        response.headers_mut().insert(
            hyper::header::HeaderName::from_static("alt-svc"),
            value.clone(),
        );
    }
    response
}

fn _empty_response(status: StatusCode) -> Response<BoxBody> {
    Response::builder()
        .status(status)
        .body(Empty::new().map_err(|never| match never {}).boxed())
        .unwrap_or_else(|_| Response::new(Empty::new().map_err(|never| match never {}).boxed()))
}

/// Check if error is a benign connection close to reduce log noise
#[allow(clippy::borrowed_box)]
fn is_connection_close_error(err: &Box<dyn std::error::Error + Send + Sync>) -> bool {
    let msg = err.to_string();
    msg.contains("connection closed")
        || msg.contains("reset by peer")
        || msg.contains("broken pipe")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_host_simple() {
        let hv = HeaderValue::from_static("example.com");
        assert_eq!(parse_and_normalize_host(Some(&hv)).unwrap(), "example.com");
    }

    #[test]
    fn test_parse_host_with_port() {
        let hv = HeaderValue::from_static("example.com:8080");
        assert_eq!(parse_and_normalize_host(Some(&hv)).unwrap(), "example.com");
    }

    #[test]
    fn test_parse_host_uppercase() {
        let hv = HeaderValue::from_static("EXAMPLE.COM");
        assert_eq!(parse_and_normalize_host(Some(&hv)).unwrap(), "example.com");
    }

    #[test]
    fn test_parse_host_trailing_dot() {
        let hv = HeaderValue::from_static("example.com.");
        assert_eq!(parse_and_normalize_host(Some(&hv)).unwrap(), "example.com");
    }

    #[test]
    fn test_parse_host_ipv6() {
        let hv = HeaderValue::from_static("[::1]:8080");
        assert_eq!(parse_and_normalize_host(Some(&hv)).unwrap(), "::1");
    }

    #[test]
    fn test_parse_host_ipv4() {
        let hv = HeaderValue::from_static("192.168.1.1:3000");
        assert_eq!(parse_and_normalize_host(Some(&hv)).unwrap(), "192.168.1.1");
    }

    #[test]
    fn test_parse_host_empty() {
        let hv = HeaderValue::from_static("");
        assert!(parse_and_normalize_host(Some(&hv)).is_err());
    }

    #[test]
    fn test_parse_host_missing() {
        assert!(parse_and_normalize_host(None).is_err());
    }

    #[test]
    fn test_websocket_upgrade_detected() {
        let mut headers = hyper::HeaderMap::new();
        headers.insert(hyper::header::UPGRADE, "websocket".parse().unwrap());
        headers.insert(hyper::header::CONNECTION, "Upgrade".parse().unwrap());
        assert!(is_websocket_upgrade(&headers));
    }

    #[test]
    fn test_websocket_upgrade_case_insensitive() {
        let mut headers = hyper::HeaderMap::new();
        headers.insert(hyper::header::UPGRADE, "WebSocket".parse().unwrap());
        headers.insert(
            hyper::header::CONNECTION,
            "keep-alive, Upgrade".parse().unwrap(),
        );
        assert!(is_websocket_upgrade(&headers));
    }

    #[test]
    fn test_not_websocket_without_upgrade_header() {
        let mut headers = hyper::HeaderMap::new();
        headers.insert(hyper::header::CONNECTION, "Upgrade".parse().unwrap());
        assert!(!is_websocket_upgrade(&headers));
    }

    #[test]
    fn test_not_websocket_without_connection_header() {
        let mut headers = hyper::HeaderMap::new();
        headers.insert(hyper::header::UPGRADE, "websocket".parse().unwrap());
        assert!(!is_websocket_upgrade(&headers));
    }

    #[test]
    fn test_not_websocket_regular_request() {
        let headers = hyper::HeaderMap::new();
        assert!(!is_websocket_upgrade(&headers));
    }

    #[test]
    fn content_length_exceeds_when_over_limit() {
        let mut headers = hyper::HeaderMap::new();
        headers.insert(hyper::header::CONTENT_LENGTH, "2048".parse().unwrap());
        assert!(content_length_exceeds(&headers, 1024));
    }

    #[test]
    fn content_length_within_limit_is_allowed() {
        let mut headers = hyper::HeaderMap::new();
        headers.insert(hyper::header::CONTENT_LENGTH, "512".parse().unwrap());
        assert!(!content_length_exceeds(&headers, 1024));
    }

    #[test]
    fn content_length_missing_is_allowed() {
        let headers = hyper::HeaderMap::new();
        assert!(!content_length_exceeds(&headers, 1024));
    }

    #[test]
    fn content_length_non_numeric_is_allowed() {
        let mut headers = hyper::HeaderMap::new();
        headers.insert(
            hyper::header::CONTENT_LENGTH,
            "not-a-number".parse().unwrap(),
        );
        assert!(!content_length_exceeds(&headers, 1024));
    }

    // Acceptance (1): an oversized request body is rejected. The `Limited`
    // wrapper used on the forwarded request body errors once the cap is passed,
    // hard-capping bodies that have no declared Content-Length.
    #[tokio::test]
    async fn oversized_request_body_is_rejected_by_limited() {
        let body = http_body_util::Full::new(Bytes::from(vec![0u8; 2048]));
        let limited = http_body_util::Limited::new(body, 1024);
        let result = limited.collect().await;
        assert!(result.is_err(), "body over the limit must be rejected");
    }

    #[tokio::test]
    async fn within_limit_request_body_is_accepted_by_limited() {
        let body = http_body_util::Full::new(Bytes::from(vec![0u8; 512]));
        let limited = http_body_util::Limited::new(body, 1024);
        let result = limited.collect().await;
        assert!(result.is_ok(), "body within the limit must be accepted");
    }

    /// A response body that never yields a frame, used to exercise the
    /// per-frame read timeout in `collect_body_with_limit`.
    struct StalledBody;

    impl hyper::body::Body for StalledBody {
        type Data = Bytes;
        type Error = std::convert::Infallible;

        fn poll_frame(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<std::result::Result<hyper::body::Frame<Self::Data>, Self::Error>>>
        {
            std::task::Poll::Pending
        }
    }

    // Acceptance (2): a stalled upstream response body times out instead of
    // pinning a permit + memory indefinitely, surfacing as a 504.
    #[tokio::test]
    async fn stalled_response_body_times_out() {
        let result = collect_body_with_limit(StalledBody, 1024, Duration::from_millis(50)).await;
        assert_eq!(result, Err(BodyCollectError::Timeout));
        assert_eq!(
            BodyCollectError::Timeout.status(),
            StatusCode::GATEWAY_TIMEOUT
        );
    }

    #[tokio::test]
    async fn response_body_over_limit_is_rejected() {
        let body = http_body_util::Full::new(Bytes::from(vec![0u8; 2048]));
        let result = collect_body_with_limit(body, 1024, Duration::from_secs(5)).await;
        assert_eq!(result, Err(BodyCollectError::TooLarge));
        assert_eq!(BodyCollectError::TooLarge.status(), StatusCode::BAD_GATEWAY);
    }

    #[tokio::test]
    async fn response_body_within_limit_is_collected() {
        let body = http_body_util::Full::new(Bytes::from_static(b"hello"));
        let result = collect_body_with_limit(body, 1024, Duration::from_secs(5)).await;
        assert_eq!(result, Ok(Bytes::from_static(b"hello")));
    }
}
