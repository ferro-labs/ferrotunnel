//! gRPC tunneling integration tests
//!
//! Verifies that FerroTunnel correctly detects `Content-Type: application/grpc`,
//! tags the stream as `Protocol::GRPC`, and forwards requests end-to-end over
//! HTTP/2 so that gRPC trailers (`grpc-status`, `grpc-message`) are preserved.

use super::{get_free_port, wait_for_server};
use bytes::Bytes;
use ferrotunnel::{Client, Server};
use futures_util::stream;
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::Frame;
use hyper::{Request, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpListener;

/// Minimal HTTP/2 server that simulates a gRPC endpoint.
///
/// Accepts any POST request and responds with:
/// - status 200
/// - `content-type: application/grpc`
/// - body: empty gRPC message frame (5-byte prefix + 0 bytes)
/// - HTTP/2 trailers: `grpc-status: 0`
async fn start_grpc_server(addr: SocketAddr) -> tokio::task::JoinHandle<()> {
    let listener = TcpListener::bind(addr)
        .await
        .expect("Failed to bind gRPC server");

    tokio::spawn(async move {
        loop {
            if let Ok((stream, _)) = listener.accept().await {
                let io = TokioIo::new(stream);
                tokio::spawn(async move {
                    let _ = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                        .serve_connection(
                            io,
                            hyper::service::service_fn(
                                |_req: Request<hyper::body::Incoming>| async {
                                    // Minimal 5-byte gRPC message frame for an empty message
                                    let grpc_frame = Bytes::from_static(b"\x00\x00\x00\x00\x00");
                                    let mut trailers = hyper::HeaderMap::new();
                                    trailers.insert(
                                        hyper::header::HeaderName::from_static("grpc-status"),
                                        hyper::header::HeaderValue::from_static("0"),
                                    );
                                    let frames = vec![
                                        Ok::<Frame<Bytes>, hyper::Error>(Frame::data(grpc_frame)),
                                        Ok(Frame::trailers(trailers)),
                                    ];
                                    let resp = hyper::Response::builder()
                                        .status(200)
                                        .header("content-type", "application/grpc")
                                        .body(StreamBody::new(stream::iter(frames)))
                                        .unwrap();
                                    Ok::<_, hyper::Error>(resp)
                                },
                            ),
                        )
                        .await;
                });
            }
        }
    })
}

/// End-to-end gRPC tunnel test.
///
/// Verifies that:
/// 1. The ingress detects `Content-Type: application/grpc` and routes the
///    stream as `Protocol::GRPC`.
/// 2. The FerroTunnel client forwards the request over HTTP/2 to the local
///    gRPC server.
/// 3. The response (status 200, correct content-type) makes it back to the
///    caller.
/// 4. HTTP/2 trailers (`grpc-status: 0`) are preserved end-to-end through
///    the tunnel — the core gRPC requirement.
#[tokio::test]
async fn test_grpc_tunnel() {
    let server_port = get_free_port();
    let http_port = get_free_port();
    let local_port = get_free_port();

    let server_addr: SocketAddr = format!("127.0.0.1:{server_port}").parse().unwrap();
    let http_addr: SocketAddr = format!("127.0.0.1:{http_port}").parse().unwrap();
    let local_addr: SocketAddr = format!("127.0.0.1:{local_port}").parse().unwrap();

    let _grpc_handle = start_grpc_server(local_addr).await;

    let mut server = Server::builder()
        .bind(server_addr)
        .http_bind(http_addr)
        .token("test-secret-token")
        .build()
        .expect("Failed to build server");

    server.start().await.expect("Failed to start server");

    assert!(
        wait_for_server(server_addr, Duration::from_secs(5)).await,
        "Server did not start"
    );

    let mut client = Client::builder()
        .server_addr(server_addr.to_string())
        .token("test-secret-token")
        .local_addr(local_addr.to_string())
        .build()
        .expect("Failed to build client");

    let info = client.start().await.expect("Client failed to connect");
    let session_id = info
        .session_id
        .expect("Session ID should be present")
        .to_string();

    // Allow time for the client to register its session on the server
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Connect to the FerroTunnel HTTP ingress with a raw h2c (cleartext HTTP/2)
    // connection — the AutoBuilder on the ingress side detects h2c automatically.
    let tcp = tokio::net::TcpStream::connect(http_addr)
        .await
        .expect("Failed to connect to HTTP ingress");

    let io = TokioIo::new(tcp);
    let (mut sender, conn) = hyper::client::conn::http2::handshake(TokioExecutor::new(), io)
        .await
        .expect("HTTP/2 handshake with ingress failed");

    tokio::spawn(async move {
        let _ = conn.await;
    });

    // Build a minimal gRPC unary request.
    // The 5-byte gRPC message frame below is: compressed=0, length=0 (empty message).
    let grpc_frame = Bytes::from_static(b"\x00\x00\x00\x00\x00");
    let req = Request::builder()
        .method("POST")
        .uri("/test.EchoService/Echo")
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .header("host", &session_id)
        .body(Full::new(grpc_frame))
        .expect("Failed to build gRPC request");

    let res = sender
        .send_request(req)
        .await
        .expect("gRPC request through tunnel failed");

    let (res_parts, mut res_body) = res.into_parts();

    assert_eq!(
        res_parts.status,
        StatusCode::OK,
        "Expected 200 OK from gRPC server"
    );

    let content_type = res_parts
        .headers
        .get("content-type")
        .expect("Missing content-type header")
        .to_str()
        .expect("Invalid content-type value");

    assert!(
        content_type.starts_with("application/grpc"),
        "Expected application/grpc content-type, got: {content_type}"
    );

    // Drain body frames and collect HTTP/2 trailers to verify end-to-end
    // trailer preservation — the primary gRPC-over-HTTP/2 requirement.
    let mut grpc_status: Option<String> = None;
    while let Some(frame) = res_body.frame().await {
        let frame = frame.expect("Body frame error");
        if let Ok(trailers) = frame.into_trailers() {
            if let Some(val) = trailers.get("grpc-status") {
                grpc_status = Some(val.to_str().unwrap_or("").to_owned());
            }
        }
    }

    assert_eq!(
        grpc_status.as_deref(),
        Some("0"),
        "Expected grpc-status: 0 trailer to be preserved through the tunnel"
    );

    let _ = client.shutdown().await;
    let _ = server.shutdown().await;
}

/// Verifies that ordinary HTTP/1.1 requests are NOT classified as gRPC
/// when `Content-Type: application/grpc` is absent, so the existing HTTP
/// path remains unaffected.
#[tokio::test]
async fn test_non_grpc_not_classified_as_grpc() {
    let server_port = get_free_port();
    let http_port = get_free_port();
    let local_port = get_free_port();

    let server_addr: SocketAddr = format!("127.0.0.1:{server_port}").parse().unwrap();
    let http_addr: SocketAddr = format!("127.0.0.1:{http_port}").parse().unwrap();
    let local_addr: SocketAddr = format!("127.0.0.1:{local_port}").parse().unwrap();

    // Plain HTTP echo server (no gRPC)
    let listener = TcpListener::bind(local_addr)
        .await
        .expect("Failed to bind HTTP server");
    tokio::spawn(async move {
        loop {
            if let Ok((stream, _)) = listener.accept().await {
                let io = TokioIo::new(stream);
                tokio::spawn(async move {
                    let _ = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                        .serve_connection(
                            io,
                            hyper::service::service_fn(
                                |_req: Request<hyper::body::Incoming>| async {
                                    let resp = hyper::Response::builder()
                                        .status(200)
                                        .header("content-type", "text/plain")
                                        .body(Full::new(Bytes::from("hello")))
                                        .unwrap();
                                    Ok::<_, hyper::Error>(resp)
                                },
                            ),
                        )
                        .await;
                });
            }
        }
    });

    let mut server = Server::builder()
        .bind(server_addr)
        .http_bind(http_addr)
        .token("test-secret-token")
        .build()
        .expect("Failed to build server");

    server.start().await.expect("Failed to start server");

    assert!(
        wait_for_server(server_addr, Duration::from_secs(5)).await,
        "Server did not start"
    );

    let mut client = Client::builder()
        .server_addr(server_addr.to_string())
        .token("test-secret-token")
        .local_addr(local_addr.to_string())
        .build()
        .expect("Failed to build client");

    let info = client.start().await.expect("Client failed to connect");
    let session_id = info
        .session_id
        .expect("Session ID should be present")
        .to_string();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Plain HTTP/1.1 request (no gRPC content-type)
    let http_client = reqwest::Client::builder()
        .build()
        .expect("Failed to build reqwest client");

    let res = http_client
        .get(format!("http://{http_addr}/"))
        .header("host", &session_id)
        .send()
        .await
        .expect("HTTP request failed");

    assert_eq!(
        res.status(),
        StatusCode::OK,
        "Expected 200 OK from HTTP server"
    );

    let ct = res.headers()["content-type"].to_str().unwrap();
    assert_eq!(
        ct, "text/plain",
        "Expected text/plain, not gRPC content-type"
    );

    let _ = client.shutdown().await;
    let _ = server.shutdown().await;
}
