//! QUIC transport integration tests

use super::{generate_self_signed_cert, get_free_port, make_client, start_echo_server};
use ferrotunnel::{Client, Server};
use ferrotunnel_common::QuicConfig;
use ferrotunnel_core::stream::QuicVirtualStream;
use ferrotunnel_core::transport::quic::QuicTransportConfig;
use ferrotunnel_core::transport::TransportConfig;
use ferrotunnel_core::TunnelClient;
use ferrotunnel_core::TunnelServer;
use std::io::Write;
use std::time::Duration;
use tokio::net::TcpStream;
use tracing::error;

/// Wait for a QUIC server to become available by attempting a QUIC connection.
async fn wait_for_quic_server(
    addr: std::net::SocketAddr,
    cert_path: &std::path::Path,
    timeout: Duration,
) -> bool {
    let config = QuicTransportConfig {
        ca_cert_path: Some(cert_path.to_string_lossy().to_string()),
        skip_verify: true,
        ..Default::default()
    };

    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        match ferrotunnel_core::transport::quic::connect(&addr.to_string(), &config).await {
            Ok((endpoint, conn)) => {
                conn.close(quinn::VarInt::from_u32(0), b"probe");
                endpoint.wait_idle().await;
                return true;
            }
            Err(_) => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    false
}

/// Test basic QUIC connection: client connects to server, handshake succeeds
#[tokio::test]
async fn test_quic_connection() {
    let _ = rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    let quic_port = get_free_port();
    let local_port = get_free_port();
    let quic_addr: std::net::SocketAddr = format!("127.0.0.1:{quic_port}").parse().unwrap();
    let token = "test-quic-token";

    // Generate self-signed certs
    let temp_dir =
        std::env::temp_dir().join(format!("ferrotunnel_test_quic_{}", uuid::Uuid::new_v4()));
    let _ = std::fs::create_dir_all(&temp_dir);

    let (cert_pem, key_pem) =
        generate_self_signed_cert(vec!["localhost".to_string(), "127.0.0.1".to_string()]);

    let cert_path = temp_dir.join("server.crt");
    let key_path = temp_dir.join("server.key");

    std::fs::File::create(&cert_path)
        .unwrap()
        .write_all(cert_pem.as_bytes())
        .unwrap();
    std::fs::File::create(&key_path)
        .unwrap()
        .write_all(key_pem.as_bytes())
        .unwrap();

    // Start local echo service
    let _echo = start_echo_server(format!("127.0.0.1:{local_port}").parse().unwrap()).await;

    // Configure QUIC transport
    let quic_config = QuicTransportConfig {
        cert_path: cert_path.to_string_lossy().to_string(),
        key_path: key_path.to_string_lossy().to_string(),
        skip_verify: false,
        ..Default::default()
    };

    // Start QUIC server
    let server = TunnelServer::new(
        format!("127.0.0.1:{}", get_free_port()).parse().unwrap(),
        token.to_string(),
    )
    .with_transport(TransportConfig::Quic(quic_config.clone()));

    let _server_handle = tokio::spawn(async move {
        let _ = server.run_quic(quic_addr).await;
    });

    // Wait for QUIC server to be ready
    assert!(
        wait_for_quic_server(quic_addr, &cert_path, Duration::from_secs(5)).await,
        "QUIC server did not start in time"
    );

    // Create QUIC client config (skip verify for self-signed cert)
    let client_quic_config = QuicTransportConfig {
        ca_cert_path: Some(cert_path.to_string_lossy().to_string()),
        skip_verify: true,
        server_name: Some("localhost".to_string()),
        ..Default::default()
    };

    let mut client = TunnelClient::new(quic_addr.to_string(), token.to_string())
        .with_transport(TransportConfig::Quic(client_quic_config))
        .with_tunnel_id("test-quic-tunnel");

    // Use a channel to signal successful connection
    let (connected_tx, connected_rx) = tokio::sync::oneshot::channel();
    let connected_tx = std::sync::Arc::new(std::sync::Mutex::new(Some(connected_tx)));

    let local_addr = format!("127.0.0.1:{local_port}");
    let client_handle = tokio::spawn(async move {
        client
            .connect_and_run_quic_with_callback(
                move |stream: QuicVirtualStream| {
                    let local_addr = local_addr.clone();
                    async move {
                        // Forward QUIC stream to local service
                        tokio::spawn(async move {
                            match TcpStream::connect(&local_addr).await {
                                Ok(mut local_stream) => {
                                    let mut tunnel_stream = stream;
                                    let _ = tokio::io::copy_bidirectional(
                                        &mut tunnel_stream,
                                        &mut local_stream,
                                    )
                                    .await;
                                }
                                Err(e) => {
                                    error!("Failed to connect to local service: {}", e);
                                }
                            }
                        });
                    }
                },
                move |session_id| {
                    if let Ok(mut guard) = connected_tx.lock() {
                        if let Some(tx) = guard.take() {
                            let _ = tx.send(session_id);
                        }
                    }
                },
            )
            .await
    });

    // Wait for connection with timeout
    let session_id = tokio::time::timeout(Duration::from_secs(10), connected_rx)
        .await
        .expect("Timed out waiting for QUIC connection")
        .expect("Connection channel closed");

    assert!(!session_id.is_nil(), "Session ID should not be nil");

    // Abort client (cleanup)
    client_handle.abort();
    let _ = std::fs::remove_dir_all(temp_dir);
}

/// Test the public builder APIs over QUIC with HTTP ingress routing.
#[tokio::test]
async fn test_public_api_quic_routing() {
    let _ = rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    let quic_port = get_free_port();
    let http_port = get_free_port();
    let local_port = get_free_port();
    let quic_addr: std::net::SocketAddr = format!("127.0.0.1:{quic_port}").parse().unwrap();
    let http_addr: std::net::SocketAddr = format!("127.0.0.1:{http_port}").parse().unwrap();
    let token = "test-public-quic-token";
    let tunnel_id = "test-public-quic";

    let temp_dir = std::env::temp_dir().join(format!(
        "ferrotunnel_test_public_quic_{}",
        uuid::Uuid::new_v4()
    ));
    let _ = std::fs::create_dir_all(&temp_dir);

    let (cert_pem, key_pem) =
        generate_self_signed_cert(vec!["localhost".to_string(), "127.0.0.1".to_string()]);

    let cert_path = temp_dir.join("server.crt");
    let key_path = temp_dir.join("server.key");

    std::fs::File::create(&cert_path)
        .unwrap()
        .write_all(cert_pem.as_bytes())
        .unwrap();
    std::fs::File::create(&key_path)
        .unwrap()
        .write_all(key_pem.as_bytes())
        .unwrap();

    let _echo = start_echo_server(format!("127.0.0.1:{local_port}").parse().unwrap()).await;

    let server_quic = QuicConfig {
        enabled: true,
        cert_path: Some(cert_path.clone()),
        key_path: Some(key_path.clone()),
        ..Default::default()
    };

    let mut server = Server::builder()
        .bind(quic_addr)
        .http_bind(http_addr)
        .token(token)
        .quic(&server_quic)
        .build()
        .expect("Failed to build QUIC server");

    server.start().await.expect("Failed to start server");

    assert!(
        wait_for_quic_server(quic_addr, &cert_path, Duration::from_secs(5)).await,
        "QUIC server did not start in time"
    );

    let client_quic = QuicConfig {
        enabled: true,
        ca_cert_path: Some(cert_path.clone()),
        server_name: Some("localhost".to_string()),
        skip_verify: true,
        ..Default::default()
    };

    let mut client = Client::builder()
        .server_addr(quic_addr.to_string())
        .token(token)
        .local_addr(format!("127.0.0.1:{local_port}"))
        .tunnel_id(tunnel_id)
        .quic(&client_quic)
        .build()
        .expect("Failed to build QUIC client");

    client
        .start()
        .await
        .expect("Client failed to connect over QUIC");

    tokio::time::sleep(Duration::from_millis(500)).await;

    let response = make_client()
        .get(format!("http://{http_addr}/"))
        .header("Host", tunnel_id)
        .send()
        .await
        .expect("Failed to send HTTP request through QUIC tunnel");

    assert_eq!(response.status(), 200);
    let text = response.text().await.expect("Failed to read response body");
    assert!(text.contains("Hello, World!"));

    let _ = client.shutdown().await;
    let _ = server.shutdown().await;
    let _ = std::fs::remove_dir_all(temp_dir);
}
