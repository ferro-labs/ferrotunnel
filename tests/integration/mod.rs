#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Integration tests for `FerroTunnel`
//!
//! These tests verify end-to-end functionality of the tunnel system.

mod concurrent_test;
mod error_test;
mod grpc_test;
#[cfg(feature = "http3")]
mod http3_test;
mod multi_client_test;
mod plugin_test;
#[cfg(feature = "quic")]
mod quic_test;
mod tcp_test;
mod tls_test;
mod tunnel_test;
mod websocket_test;

use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;

/// Test configuration with high ports to avoid conflicts
pub struct TestConfig {
    pub server_addr: SocketAddr,
    pub http_addr: SocketAddr,
    pub local_service_addr: SocketAddr,
    pub token: &'static str,
}

static NEXT_PORT: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(30000);

pub fn get_free_port() -> u16 {
    use std::sync::atomic::Ordering;
    loop {
        let port = NEXT_PORT.fetch_add(1, Ordering::Relaxed);
        if std::net::TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
}

impl Default for TestConfig {
    fn default() -> Self {
        let server_port = get_free_port();
        let http_port = get_free_port();
        let local_port = get_free_port();

        Self {
            server_addr: format!("127.0.0.1:{server_port}").parse().unwrap(),
            http_addr: format!("127.0.0.1:{http_port}").parse().unwrap(),
            local_service_addr: format!("127.0.0.1:{local_port}").parse().unwrap(),
            token: "test-secret-token",
        }
    }
}

/// Wait for a server to start listening
pub async fn wait_for_server(addr: SocketAddr, timeout: Duration) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return true;
        }
        sleep(Duration::from_millis(50)).await;
    }
    false
}

pub async fn start_test_server(config: &TestConfig) -> ferrotunnel::Server {
    let mut server = ferrotunnel::Server::builder()
        .bind(config.server_addr)
        .http_bind(config.http_addr)
        .token(config.token)
        .build()
        .expect("Failed to build server");

    server.start().await.expect("Failed to start server");
    assert!(
        wait_for_server(config.server_addr, Duration::from_secs(5)).await,
        "Server did not start in time"
    );

    server
}

/// Start a simple HTTP server that echoes requests
pub async fn start_echo_server(addr: SocketAddr) -> tokio::task::JoinHandle<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let listener = TcpListener::bind(addr)
        .await
        .expect("Failed to bind echo server");

    tokio::spawn(async move {
        loop {
            if let Ok((mut socket, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 4096];
                    let n = socket.read(&mut buf).await.unwrap_or(0);
                    if n > 0 {
                        let response = "HTTP/1.1 200 OK\r\n\
                             Content-Type: text/plain\r\n\
                             Content-Length: 13\r\n\
                             \r\n\
                             Hello, World!"
                            .to_string();
                        let _ = socket.write_all(response.as_bytes()).await;
                    }
                });
            }
        }
    })
}

/// Create a reqwest client configured for testing (no proxy, direct connection)
pub fn make_client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("Failed to build reqwest client")
}

/// Generate a self-signed certificate for testing
pub fn generate_self_signed_cert(subject_alt_names: Vec<String>) -> (String, String) {
    let mut params =
        rcgen::CertificateParams::new(subject_alt_names).expect("Failed to create params");
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "localhost");

    let key_pair = rcgen::KeyPair::generate().expect("Failed to generate key pair");
    let cert = params
        .self_signed(&key_pair)
        .expect("Failed to generate cert");
    let cert_pem = cert.pem();
    let key_pem = key_pair.serialize_pem();

    (cert_pem, key_pem)
}

/// Start a server that reads the body and returns 200 OK
pub async fn start_sink_server(addr: SocketAddr) -> tokio::task::JoinHandle<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let listener = TcpListener::bind(addr)
        .await
        .expect("Failed to bind sink server");

    tokio::spawn(async move {
        loop {
            if let Ok((mut socket, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 32 * 1024];
                    loop {
                        let n = socket.read(&mut buf).await.unwrap_or(0);
                        if n == 0 {
                            break;
                        }

                        let request = String::from_utf8_lossy(&buf[..n]);
                        if request.contains("\r\n\r\n") {
                            // Headers received.
                            // Send Response.
                            let response = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
                            let _ = socket.write_all(response.as_bytes()).await;
                            // For testing purposes, we assume 'large payload' is sent in one go or we just ack it.
                            // If we want to drain, we should loop.
                            // But for now let's just reply.
                        }
                    }
                });
            }
        }
    })
}
