//! End-to-end tunnel tests
//!
//! Tests basic tunnel functionality: server, client, HTTP routing

use super::{start_echo_server, start_test_server, TestConfig};
use ferrotunnel::{Client, Server};
use std::time::Duration;

/// Test that server starts and accepts connections
#[tokio::test]
async fn test_server_starts() {
    let config = TestConfig::default();

    let mut server = Server::builder()
        .bind(config.server_addr)
        .http_bind(config.http_addr)
        .token(config.token)
        .build()
        .expect("Failed to build server");

    server.start().await.expect("Failed to start server");
    assert!(server.is_running());
    server.shutdown().await.expect("Failed to stop server");
    assert!(!server.is_running());
}

/// Test that client connects to server
#[tokio::test]
async fn test_client_connects() {
    let config = TestConfig::default();

    // Start local service
    let _echo_handle = start_echo_server(config.local_service_addr).await;

    let mut server = start_test_server(&config).await;

    // Build and start client
    let mut client = Client::builder()
        .server_addr(config.server_addr.to_string())
        .token(config.token)
        .local_addr(config.local_service_addr.to_string())
        .build()
        .expect("Failed to build client");

    let info = client.start().await;
    assert!(info.is_ok(), "Client failed to connect: {:?}", info.err());

    // Verify client got a session
    let tunnel_info = info.unwrap();
    assert!(
        tunnel_info.session_id().is_some(),
        "Session ID should not be empty"
    );

    // Shutdown
    let _ = client.shutdown().await;
    let _ = server.shutdown().await;
}

/// Test full HTTP request through tunnel
#[tokio::test]
async fn test_http_through_tunnel() {
    let config = TestConfig::default();

    // Start local echo service
    let _echo_handle = start_echo_server(config.local_service_addr).await;

    let mut server = start_test_server(&config).await;

    // Start client
    let mut client = Client::builder()
        .server_addr(config.server_addr.to_string())
        .token(config.token)
        .local_addr(config.local_service_addr.to_string())
        .build()
        .expect("Failed to build client");

    let info = client.start().await.expect("Client failed to connect");
    let session_id = info
        .session_id()
        .expect("Session ID should be present")
        .to_string();

    // Give time for tunnel to be registered
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Make HTTP request through the tunnel
    let http_client = super::make_client();
    let url = format!("http://{}/", config.http_addr);

    let response = http_client
        .get(&url)
        .header("Host", session_id)
        .send()
        .await
        .expect("Failed to send HTTP request");

    assert_eq!(
        response.status(),
        200,
        "Expected 200 OK, got {}",
        response.status()
    );
    let text = response.text().await.expect("Failed to read response body");
    assert!(
        text.contains("Hello, World!"),
        "Response should contain echo message"
    );

    let _ = client.shutdown().await;
    let _ = server.shutdown().await;
}

/// Test large payload handling
#[tokio::test]
async fn test_large_payload() {
    let config = TestConfig::default();

    // Start local sink service that reads everything and replies OK
    let _sink_handle = super::start_sink_server(config.local_service_addr).await;

    let mut server = start_test_server(&config).await;

    // Start client
    let mut client = Client::builder()
        .server_addr(config.server_addr.to_string())
        .token(config.token)
        .local_addr(config.local_service_addr.to_string())
        .build()
        .expect("Failed to build client");

    let info = client.start().await.expect("Client failed to connect");
    let session_id = info
        .session_id()
        .expect("Session ID should be present")
        .to_string();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Create 1MB payload
    let payload = vec![b'x'; 1024 * 1024];

    let http_client = super::make_client();
    let url = format!("http://{}/", config.http_addr);

    let response = http_client
        .post(&url)
        .header("Host", session_id)
        .body(payload)
        .send()
        .await
        .expect("Failed to send large payload request");

    assert_eq!(
        response.status(),
        200,
        "Expected 200 OK, got {}",
        response.status()
    );

    let _ = client.shutdown().await;
    let _ = server.shutdown().await;
}
