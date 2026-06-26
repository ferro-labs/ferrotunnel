//! Concurrency integration tests

use super::{start_echo_server, start_test_server, TestConfig};
use ferrotunnel::Client;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

/// Test multiple concurrent requests through a single tunnel
#[tokio::test]
async fn test_concurrent_requests() {
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
    let session_id = info.session_id.expect("Session ID required").to_string();

    tokio::time::sleep(Duration::from_millis(500)).await;

    let success_count = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    // Spawn 50 concurrent requests
    for i in 0..50 {
        let http_addr = config.http_addr;
        let tunnel_id = session_id.clone();
        let counter = success_count.clone();

        handles.push(tokio::spawn(async move {
            let http_client = super::make_client();
            let url = format!("http://{http_addr}/?req={i}");

            match http_client.get(&url).header("Host", tunnel_id).send().await {
                Ok(resp) if resp.status() == 200 => {
                    if let Ok(text) = resp.text().await {
                        if text.contains("Hello, World!") {
                            counter.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                _ => {}
            }
        }));
    }

    // Wait for all requests
    for h in handles {
        let _ = h.await;
    }

    assert_eq!(
        success_count.load(Ordering::Relaxed),
        50,
        "All 50 requests should succeed"
    );

    let _ = client.shutdown().await;
    let _ = server.shutdown().await;
}
