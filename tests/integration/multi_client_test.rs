//! Multi-client integration tests
//!
//! Tests scenarios with multiple concurrent tunnel clients

use super::{start_echo_server, start_test_server, TestConfig};
use ferrotunnel::Client;
use std::time::Duration;

/// Test multiple clients connecting to same server
#[tokio::test]
async fn test_multiple_clients() {
    let config = TestConfig::default();

    // Allocate extra port for second client
    let local_port2 = super::get_free_port();
    let local_addr2 = format!("127.0.0.1:{local_port2}");

    // Start local services for each client
    let _echo1 = start_echo_server(config.local_service_addr).await;
    let _echo2 = start_echo_server(local_addr2.parse().unwrap()).await;

    let mut server = start_test_server(&config).await;

    // Start first client
    let mut client1 = Client::builder()
        .server_addr(format!("{}", config.server_addr))
        .token(config.token)
        .local_addr(format!("{}", config.local_service_addr))
        .build()
        .expect("Failed to build client1");

    let info1 = client1.start().await.expect("Client1 failed to connect");

    // Start second client
    let mut client2 = Client::builder()
        .server_addr(format!("{}", config.server_addr))
        .token(config.token)
        .local_addr(&local_addr2)
        .build()
        .expect("Failed to build client2");

    let info2 = client2.start().await.expect("Client2 failed to connect");

    // Verify both clients have unique session IDs
    assert_ne!(
        info1.session_id(),
        info2.session_id(),
        "Clients should have different session IDs"
    );

    // Cleanup
    let _ = client1.shutdown().await;
    let _ = client2.shutdown().await;
    let _ = server.shutdown().await;
}

/// Test client reconnection behavior
#[tokio::test]
async fn test_client_reconnect() {
    let config = TestConfig::default();

    // Start local service
    let _echo = start_echo_server(config.local_service_addr).await;

    let mut server = start_test_server(&config).await;

    // First connection
    let mut client = Client::builder()
        .server_addr(format!("{}", config.server_addr))
        .token(config.token)
        .local_addr(format!("{}", config.local_service_addr))
        .build()
        .expect("Failed to build client");

    let info1 = client.start().await.expect("First connect failed");
    let session1 = info1.session_id();

    // Disconnect
    let _ = client.shutdown().await;

    // Small delay
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Reconnect - build a new client
    let mut client2 = Client::builder()
        .server_addr(format!("{}", config.server_addr))
        .token(config.token)
        .local_addr(format!("{}", config.local_service_addr))
        .build()
        .expect("Failed to build client");

    let info2 = client2.start().await.expect("Reconnect failed");

    // New connection should have different session
    assert_ne!(
        session1,
        info2.session_id(),
        "Reconnect should get new session"
    );

    let _ = client2.shutdown().await;
    let _ = server.shutdown().await;
}
