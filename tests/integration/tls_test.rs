//! TLS integration tests

use super::{wait_for_server, TestConfig};
use ferrotunnel::{Client, Server};
use ferrotunnel_common::config::TlsConfig;
use std::io::Write;
use std::time::Duration;

/// Test client connecting to server over TLS
#[tokio::test]
async fn test_tls_connection() {
    let _ = rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
    let config = TestConfig::default();

    // Create temp directory for certs
    let temp_dir =
        std::env::temp_dir().join(format!("ferrotunnel_test_tls_{}", uuid::Uuid::new_v4()));
    let _ = std::fs::create_dir_all(&temp_dir);

    // Generate certs
    let (cert_pem, key_pem) =
        super::generate_self_signed_cert(vec!["localhost".to_string(), "127.0.0.1".to_string()]);

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

    // Start local echo
    let _echo = super::start_echo_server(config.local_service_addr).await;

    // Configure Server TLS
    let server_tls = TlsConfig {
        enabled: true,
        cert_path: Some(cert_path.clone()),
        key_path: Some(key_path.clone()),
        ..Default::default()
    };

    // Start server with TLS
    let mut server = Server::builder()
        .bind(config.server_addr)
        .http_bind(config.http_addr)
        .token(config.token)
        .tls(&server_tls)
        .build()
        .expect("Failed to build server");

    server.start().await.expect("Failed to start server");

    // Wait for server? (Should we check TCP connect?)
    // But plain TCP connect might hang on handshake if we don't speak TLS?
    // wait_for_server does pure TCP connect. It should succeed regardless of TLS handshake.
    assert!(wait_for_server(config.server_addr, Duration::from_secs(5)).await);

    // Configure Client TLS
    // We treat the self-signed cert as the CA for the client
    let client_tls = TlsConfig {
        enabled: true,
        ca_cert_path: Some(cert_path.clone()),
        server_name: Some("localhost".to_string()),
        ..Default::default()
    };

    // Start client with TLS
    let mut client = Client::builder()
        .server_addr(config.server_addr.to_string())
        .token(config.token)
        .local_addr(config.local_service_addr.to_string())
        .tls(&client_tls)
        .build()
        .expect("Failed to build client");

    let info = client.start().await;
    assert!(
        info.is_ok(),
        "Client failed to connect via TLS: {:?}",
        info.err()
    );

    // Clean up
    let _ = client.shutdown().await;
    let _ = server.shutdown().await;
    let _ = std::fs::remove_dir_all(temp_dir);
}
