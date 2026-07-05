//! Dashboard capture middleware for traffic inspection

use bytes::Bytes;
use chrono::Utc;
use ferrotunnel_http::proxy::{error_response, ProxyError};
use ferrotunnel_observability::dashboard::{
    DashboardEvent, EventBroadcaster, RequestDetails, SharedDashboardState,
};
use http_body_util::{BodyExt, Full};
use hyper::body::Body;
use hyper::{Request, Response, StatusCode};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tower::{Layer, Service};
use uuid::Uuid;

type BoxBody = http_body_util::combinators::BoxBody<Bytes, ProxyError>;

#[derive(Clone)]
pub struct DashboardCaptureLayer {
    pub state: SharedDashboardState,
    pub broadcaster: Arc<EventBroadcaster>,
    pub tunnel_id: Uuid,
}

impl<S> Layer<S> for DashboardCaptureLayer {
    type Service = DashboardCaptureService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        DashboardCaptureService {
            inner,
            state: self.state.clone(),
            broadcaster: self.broadcaster.clone(),
            tunnel_id: self.tunnel_id,
        }
    }
}

#[derive(Clone)]
pub struct DashboardCaptureService<S> {
    inner: S,
    state: SharedDashboardState,
    broadcaster: Arc<EventBroadcaster>,
    tunnel_id: Uuid,
}

impl<S, B> Service<Request<B>> for DashboardCaptureService<S>
where
    S: Service<Request<BoxBody>, Response = Response<BoxBody>, Error = hyper::Error>
        + Send
        + Clone
        + 'static,
    S::Future: Send + 'static,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<ProxyError>,
{
    type Response = Response<BoxBody>;
    type Error = hyper::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx) // Assuming inner service manages backpressure
    }

    fn call(&mut self, req: Request<B>) -> Self::Future {
        let mut inner = self.inner.clone();
        let state = self.state.clone();
        let broadcaster = self.broadcaster.clone();
        let tunnel_id = self.tunnel_id;

        Box::pin(async move {
            let start_time = Utc::now();
            let request_id = Uuid::new_v4();

            // 1. Buffer Request
            let (parts, body) = req.into_parts();

            // Capture request headers
            let mut request_headers = HashMap::new();
            for (k, v) in &parts.headers {
                if let Ok(val) = v.to_str() {
                    request_headers.insert(k.to_string(), val.to_string());
                }
            }

            let request_method = parts.method.to_string();
            // Capture the full path and query so a replayed request matches the
            // original; `uri.path()` alone would drop the query string.
            let request_path = parts.uri.path_and_query().map_or_else(
                || parts.uri.path().to_string(),
                |pq| pq.as_str().to_string(),
            );

            let request_bytes = match body.collect().await {
                Ok(c) => c.to_bytes(),
                Err(_) => {
                    return Ok(error_response(
                        StatusCode::BAD_REQUEST,
                        "Failed to read request body",
                    ))
                }
            };

            let request_body_str = if request_bytes.len() < 1024 * 1024 {
                // Cap capture at 1MB
                String::from_utf8(request_bytes.to_vec()).ok()
            } else {
                Some("<Body too large>".to_string())
            };

            // Reconstruct Request with Box<dyn Error>
            let inner_req = Request::from_parts(
                parts,
                Full::new(request_bytes)
                    .map_err(|_| ProxyError::Custom("Request body error".into()))
                    .boxed(),
            );

            // 2. Call Inner Service
            let res = inner.call(inner_req).await;

            // 3. Process Response
            match res {
                Ok(response) => {
                    let (parts, body) = response.into_parts();

                    // Capture response headers
                    let mut response_headers = HashMap::new();
                    for (k, v) in &parts.headers {
                        if let Ok(val) = v.to_str() {
                            response_headers.insert(k.to_string(), val.to_string());
                        }
                    }
                    let status = parts.status.as_u16();

                    let response_bytes = match body.collect().await {
                        Ok(c) => c.to_bytes(),
                        Err(e) => {
                            return Ok(error_response(
                                StatusCode::BAD_GATEWAY,
                                &format!("Failed to read upstream response: {e}"),
                            ))
                        }
                    };

                    let response_body_str = if response_bytes.len() < 1024 * 1024 {
                        String::from_utf8(response_bytes.to_vec()).ok()
                    } else {
                        Some("<Body too large>".to_string())
                    };

                    let duration_ms = Utc::now()
                        .signed_duration_since(start_time)
                        .num_milliseconds();
                    let duration: u64 = duration_ms.max(0).try_into().unwrap_or_default();

                    // Record to Dashboard State
                    let details = RequestDetails {
                        id: request_id,
                        tunnel_id,
                        method: request_method,
                        path: request_path,
                        request_headers,
                        request_body: request_body_str,
                        status,
                        response_headers,
                        response_body: response_body_str,
                        duration_ms: duration,
                        timestamp: start_time,
                    };

                    {
                        let mut guard = state.write().await;
                        guard.add_request(details.clone());
                    }

                    // Broadcast Event
                    let log_entry =
                        ferrotunnel_observability::dashboard::RequestLogEntry::from(&details);
                    broadcaster.send(DashboardEvent::NewRequest(log_entry));

                    // Reconstruct Response
                    let inner_res = Response::from_parts(
                        parts,
                        Full::new(response_bytes)
                            .map_err(|_| ProxyError::Custom("Response body error".into()))
                            .boxed(),
                    );

                    Ok(inner_res)
                }
                Err(e) => Err(e),
            }
        })
    }
}
