//! HTTP/3 ingress integration tests

use super::{
    generate_self_signed_cert, get_free_port, make_client, start_echo_server, wait_for_server,
};
use bytes::Buf;
use ferrotunnel::{Client, Server};
use futures_util::future;
use h3_quinn::quinn::{self, ClientConfig, Endpoint};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

struct Http3TestContext {
    server_addr: std::net::SocketAddr,
    http_addr: std::net::SocketAddr,
    http3_addr: std::net::SocketAddr,
    cert_path: PathBuf,
    key_path: PathBuf,
    cert_pem: String,
    temp_dir: PathBuf,
}

impl Drop for Http3TestContext {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.temp_dir);
    }
}

fn http3_context() -> Http3TestContext {
    let server_port = get_free_port();
    let http_port = get_free_port();
    let http3_port = get_free_port();
    let temp_dir =
        std::env::temp_dir().join(format!("ferrotunnel_test_http3_{}", uuid::Uuid::new_v4()));
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

    Http3TestContext {
        server_addr: format!("127.0.0.1:{server_port}").parse().unwrap(),
        http_addr: format!("127.0.0.1:{http_port}").parse().unwrap(),
        http3_addr: format!("127.0.0.1:{http3_port}").parse().unwrap(),
        cert_path,
        key_path,
        cert_pem,
        temp_dir,
    }
}

#[tokio::test]
async fn test_http3_request_through_tunnel_and_alt_svc_advertised() {
    let context = http3_context();
    let local_port = get_free_port();
    let local_addr = format!("127.0.0.1:{local_port}");
    let token = "test-http3-token";
    let tunnel_id = "test-http3-ingress";
    let _echo = start_echo_server(format!("127.0.0.1:{local_port}").parse().unwrap()).await;

    let mut server = Server::builder()
        .bind(context.server_addr)
        .http_bind(context.http_addr)
        .http3(
            context.http3_addr,
            context.cert_path.clone(),
            context.key_path.clone(),
        )
        .token(token)
        .build()
        .expect("Failed to build HTTP/3 server");
    server.start().await.expect("Failed to start server");

    assert!(
        wait_for_server(context.server_addr, Duration::from_secs(5)).await,
        "Server did not start in time"
    );

    let mut client = Client::builder()
        .server_addr(context.server_addr.to_string())
        .token(token)
        .local_addr(local_addr)
        .tunnel_id(tunnel_id)
        .build()
        .expect("Failed to build client");
    client.start().await.expect("Client failed to connect");
    tokio::time::sleep(Duration::from_millis(500)).await;

    let http_response = make_client()
        .get(format!("http://{}/", context.http_addr))
        .header("Host", tunnel_id)
        .send()
        .await
        .expect("Failed to send HTTP request");

    assert_eq!(http_response.status(), 200);
    assert_eq!(
        http_response
            .headers()
            .get("alt-svc")
            .and_then(|v| v.to_str().ok()),
        Some(
            format!(
                "h3=\":{}\"; ma=86400, h3-29=\":{}\"; ma=86400",
                context.http3_addr.port(),
                context.http3_addr.port()
            )
            .as_str(),
        )
    );

    let (status, body) = send_h3_get(context.http3_addr, &context.cert_pem, tunnel_id).await;
    assert_eq!(status, http::StatusCode::OK);
    assert!(body.contains("Hello, World!"));

    let _ = client.shutdown().await;
    let _ = server.shutdown().await;
}

#[tokio::test]
async fn test_http3_unknown_host_returns_not_found() {
    let context = http3_context();
    let mut server = Server::builder()
        .bind(context.server_addr)
        .http_bind(context.http_addr)
        .http3(
            context.http3_addr,
            context.cert_path.clone(),
            context.key_path.clone(),
        )
        .token("test-http3-token")
        .build()
        .expect("Failed to build HTTP/3 server");
    server.start().await.expect("Failed to start server");

    assert!(
        wait_for_server(context.server_addr, Duration::from_secs(5)).await,
        "Server did not start in time"
    );

    let (status, body) =
        send_h3_get(context.http3_addr, &context.cert_pem, "unknown.example").await;
    assert_eq!(status, http::StatusCode::NOT_FOUND);
    assert_eq!(body, "Tunnel not found");

    let _ = server.shutdown().await;
}

async fn send_h3_get(
    http3_addr: std::net::SocketAddr,
    cert_pem: &str,
    host: &str,
) -> (http::StatusCode, String) {
    let (endpoint, connection) = connect_h3_client(http3_addr, cert_pem).await;
    let h3_connection = h3_quinn::Connection::new(connection);
    let (mut driver, mut send_request) = h3::client::new(h3_connection)
        .await
        .expect("Failed to create HTTP/3 client");
    let driver_handle = tokio::spawn(async move {
        let _ = future::poll_fn(|cx| driver.poll_close(cx)).await;
    });

    let request = http::Request::builder()
        .method("GET")
        .uri(format!("https://{host}/"))
        .body(())
        .expect("Failed to build HTTP/3 request");
    let mut request_stream = send_request
        .send_request(request)
        .await
        .expect("Failed to send HTTP/3 request");
    request_stream
        .finish()
        .await
        .expect("Failed to finish HTTP/3 request");

    let h3_response = request_stream
        .recv_response()
        .await
        .expect("Failed to receive HTTP/3 response");
    let status = h3_response.status();

    let mut body = Vec::new();
    while let Some(mut chunk) = request_stream
        .recv_data()
        .await
        .expect("Failed to receive HTTP/3 body")
    {
        let len = chunk.remaining();
        body.extend_from_slice(&chunk.copy_to_bytes(len));
    }

    endpoint.close(0u32.into(), b"done");
    endpoint.wait_idle().await;
    driver_handle.abort();

    (status, String::from_utf8_lossy(&body).to_string())
}

async fn connect_h3_client(
    http3_addr: std::net::SocketAddr,
    cert_pem: &str,
) -> (Endpoint, quinn::Connection) {
    use rustls::pki_types::pem::PemObject;

    let mut roots = rustls::RootCertStore::empty();
    let cert = rustls::pki_types::CertificateDer::from_pem_slice(cert_pem.as_bytes())
        .expect("Failed to parse test cert");
    roots.add(cert).expect("Failed to add test root cert");

    let mut tls_config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    tls_config.alpn_protocols = vec![b"h3".to_vec()];

    let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(tls_config)
        .expect("Failed to create HTTP/3 QUIC client crypto config");
    let client_config = ClientConfig::new(Arc::new(crypto));

    let mut endpoint = Endpoint::client("127.0.0.1:0".parse().unwrap())
        .expect("Failed to create HTTP/3 client endpoint");
    endpoint.set_default_client_config(client_config);
    let connection = endpoint
        .connect(http3_addr, "localhost")
        .expect("Failed to start HTTP/3 connection")
        .await
        .expect("Failed to connect HTTP/3 client");

    (endpoint, connection)
}
