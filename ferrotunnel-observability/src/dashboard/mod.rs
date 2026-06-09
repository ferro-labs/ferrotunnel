//! Dashboard API module for tunnel inspection and monitoring.
//!
//! This module provides a REST API and SSE endpoint for the FerroTunnel dashboard.
//!
//! # Usage
//!
//! ```ignore
//! use ferrotunnel_observability::dashboard::{create_router, DashboardState, EventBroadcaster};
//! use std::sync::Arc;
//! use tokio::sync::RwLock;
//!
//! // Create shared state
//! let state = Arc::new(RwLock::new(DashboardState::new(1000)));
//! let broadcaster = Arc::new(EventBroadcaster::new(100));
//!
//! // Create the router
//! let app = create_router(state, broadcaster, None);
//!
//! // Run the server
//! let listener = tokio::net::TcpListener::bind("127.0.0.1:4040").await?;
//! axum::serve(listener, app).await?;
//! ```

pub mod events;
pub mod handlers;
pub mod models;

pub use events::{DashboardEvent, EventBroadcaster};
pub use models::{
    ApiError, DashboardState, DashboardTunnelInfo, HealthResponse, RequestDetails, RequestLogEntry,
    SharedDashboardState, TunnelStatus,
};

use std::sync::Arc;

use axum::{
    body::Body,
    extract::State,
    http::{header, HeaderMap, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use tower_http::{compression::CompressionLayer, trace::TraceLayer};

const DASHBOARD_AUTH_HEADER: &str = "x-dashboard-token";
const DASHBOARD_AUTH_COOKIE: &str = "ferrotunnel_dashboard_token";

#[derive(Clone)]
struct DashboardAuth {
    token: Arc<str>,
}

/// Creates the dashboard API router with all endpoints.
///
/// # Arguments
///
/// * `state` - Shared dashboard state for tunnel and request data.
/// * `broadcaster` - Event broadcaster for SSE streaming.
/// * `auth_token` - Optional token required for all `/api/v1/*` endpoints.
///
/// # Endpoints
///
/// - `GET /api/v1/health` - Health check
/// - `GET /api/v1/tunnels` - List all tunnels
/// - `GET /api/v1/tunnels/:id` - Get tunnel by ID
/// - `GET /api/v1/requests` - List recent requests
/// - `GET /api/v1/requests/:id` - Get request details
/// - `GET /api/v1/requests/:id/replay` - Replay a request
/// - `GET /api/v1/metrics` - Prometheus metrics
/// - `GET /api/v1/events` - SSE event stream
// Embedded assets
#[derive(rust_embed::RustEmbed)]
#[folder = "src/dashboard/static/"]
struct Assets;

async fn static_handler(uri: axum::http::Uri) -> impl IntoResponse {
    let path = uri.path().trim_start_matches('/');
    let path = if path.is_empty() { "index.html" } else { path };

    match Assets::get(path) {
        Some(content) => {
            let mime = mime_guess::from_path(path).first_or_octet_stream();
            ([(header::CONTENT_TYPE, mime.as_ref())], content.data).into_response()
        }
        None => (StatusCode::NOT_FOUND, "404 Not Found").into_response(),
    }
}

pub fn create_router(
    state: SharedDashboardState,
    broadcaster: Arc<EventBroadcaster>,
    auth_token: Option<String>,
) -> Router {
    let data_routes = Router::new()
        .route("/health", get(handlers::health_handler))
        .route("/tunnels", get(handlers::list_tunnels_handler))
        .route("/tunnels/{id}", get(handlers::get_tunnel_handler))
        .route("/requests", get(handlers::list_requests_handler))
        .route("/requests/{id}", get(handlers::get_request_handler))
        .route(
            "/requests/{id}/replay",
            post(handlers::replay_request_handler),
        )
        .route("/metrics", get(handlers::metrics_handler))
        .with_state(state);

    let event_routes = Router::new()
        .route("/events", get(events::events_handler))
        .with_state(broadcaster);

    let api_routes = match auth_token {
        Some(token) => data_routes
            .merge(event_routes)
            .layer(middleware::from_fn_with_state(
                DashboardAuth {
                    token: Arc::from(token),
                },
                dashboard_auth_middleware,
            )),
        None => data_routes.merge(event_routes),
    };

    Router::new()
        .nest("/api/v1", api_routes)
        .fallback(static_handler)
        .layer(CompressionLayer::new())
        .layer(TraceLayer::new_for_http())
}

async fn dashboard_auth_middleware(
    State(auth): State<DashboardAuth>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let provided = request_auth_token(req.headers(), req.uri().query());
    if let Some(provided) = provided {
        if constant_time_eq(provided.as_bytes(), auth.token.as_bytes()) {
            return next.run(req).await;
        }
    }

    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, "Bearer")],
        "Unauthorized",
    )
        .into_response()
}

fn request_auth_token(headers: &HeaderMap, query: Option<&str>) -> Option<String> {
    bearer_token(headers)
        .or_else(|| header_token(headers))
        .or_else(|| cookie_token(headers))
        .or_else(|| query_token(query))
}

fn bearer_token(headers: &HeaderMap) -> Option<String> {
    let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let (scheme, token) = value.split_once(' ')?;
    if scheme.eq_ignore_ascii_case("bearer") {
        Some(token.to_string())
    } else {
        None
    }
}

fn header_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get(DASHBOARD_AUTH_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(ToString::to_string)
}

fn cookie_token(headers: &HeaderMap) -> Option<String> {
    let value = headers.get(header::COOKIE)?.to_str().ok()?;
    value.split(';').find_map(|part| {
        let (name, token) = part.trim().split_once('=')?;
        if name == DASHBOARD_AUTH_COOKIE {
            Some(percent_decode(token, false))
        } else {
            None
        }
    })
}

fn query_token(query: Option<&str>) -> Option<String> {
    query?.split('&').find_map(|part| {
        let (name, token) = part.split_once('=').unwrap_or((part, ""));
        if percent_decode(name, true) == "token" {
            Some(percent_decode(token, true))
        } else {
            None
        }
    })
}

fn percent_decode(value: &str, plus_as_space: bool) -> String {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;

    while index < bytes.len() {
        match bytes[index] {
            b'%' if index + 2 < bytes.len() => {
                if let (Some(high), Some(low)) =
                    (hex_value(bytes[index + 1]), hex_value(bytes[index + 2]))
                {
                    decoded.push((high << 4) | low);
                    index += 3;
                    continue;
                }
                decoded.push(bytes[index]);
                index += 1;
            }
            b'+' if plus_as_space => {
                decoded.push(b' ');
                index += 1;
            }
            byte => {
                decoded.push(byte);
                index += 1;
            }
        }
    }

    String::from_utf8_lossy(&decoded).into_owned()
}

const fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let max_len = left.len().max(right.len());
    let mut diff = left.len() ^ right.len();

    for index in 0..max_len {
        let left_byte = left.get(index).copied().unwrap_or(0);
        let right_byte = right.get(index).copied().unwrap_or(0);
        diff |= usize::from(left_byte ^ right_byte);
    }

    diff == 0
}

/// Configuration for the dashboard server.
#[derive(Debug, Clone)]
pub struct DashboardConfig {
    /// Address to bind the dashboard server.
    pub bind_addr: std::net::SocketAddr,
    /// Maximum number of requests to keep in history.
    pub max_requests: usize,
    /// Optional authentication token.
    pub auth_token: Option<String>,
}

impl Default for DashboardConfig {
    fn default() -> Self {
        Self {
            bind_addr: ([127, 0, 0, 1], 4040).into(),
            max_requests: 1000,
            auth_token: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::RwLock;

    async fn spawn_dashboard(auth_token: Option<String>) -> String {
        let state = Arc::new(RwLock::new(DashboardState::new(10)));
        let broadcaster = Arc::new(EventBroadcaster::new(10));
        let app = create_router(state, broadcaster, auth_token);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        format!("http://{addr}")
    }

    #[test]
    fn constant_time_compare_matches_equal_tokens() {
        assert!(constant_time_eq(b"secret-token", b"secret-token"));
        assert!(!constant_time_eq(b"secret-token", b"secret-tokem"));
        assert!(!constant_time_eq(b"secret-token", b"secret-token-longer"));
    }

    #[tokio::test]
    async fn api_auth_rejects_missing_token() {
        let base_url = spawn_dashboard(Some("secret-token".to_string())).await;
        let response = reqwest::get(format!("{base_url}/api/v1/health"))
            .await
            .unwrap();

        assert_eq!(
            response.status().as_u16(),
            StatusCode::UNAUTHORIZED.as_u16()
        );
    }

    #[tokio::test]
    async fn api_auth_accepts_bearer_token() {
        let base_url = spawn_dashboard(Some("secret-token".to_string())).await;
        let client = reqwest::Client::new();
        let response = client
            .get(format!("{base_url}/api/v1/health"))
            .bearer_auth("secret-token")
            .send()
            .await
            .unwrap();

        assert_eq!(response.status().as_u16(), StatusCode::OK.as_u16());
    }
}
