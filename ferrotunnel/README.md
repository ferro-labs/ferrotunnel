# ferrotunnel

[![Crates.io](https://img.shields.io/crates/v/ferrotunnel)](https://crates.io/crates/ferrotunnel)
[![Documentation](https://docs.rs/ferrotunnel/badge.svg)](https://docs.rs/ferrotunnel)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue)](LICENSE)

Reverse tunnel library for Rust applications.

## Quick Start

```toml
[dependencies]
ferrotunnel = "0.1"
```

```rust
use ferrotunnel::Client;

#[tokio::main]
async fn main() -> ferrotunnel::Result<()> {
    let mut client = Client::builder()
        .server_addr("tunnel.example.com:7835")
        .token("my-token")
        .local_addr("127.0.0.1:8080")
        .build()?;

    client.start().await?;
    Ok(())
}
```

## Features

- TLS 1.3 encryption with rustls
- QUIC transport with native stream multiplexing (feature: `quic`)
- HTTP/3 ingress with Alt-Svc advertising (feature: `http3`)
- Token-based authentication
- HTTP, WebSocket, gRPC, and TCP tunneling
- Automatic reconnection with backoff
- Prometheus metrics and tracing

## HTTP/3 Ingress

Build with the `http3` feature to accept browser-facing HTTP/3 traffic on a UDP
ingress port:

```toml
[dependencies]
ferrotunnel = { version = "1.0", features = ["http3"] }
```

```rust
use ferrotunnel::Server;

#[tokio::main]
async fn main() -> ferrotunnel::Result<()> {
    let mut server = Server::builder()
        .bind("0.0.0.0:7835".parse().unwrap())
        .http_bind("0.0.0.0:8080".parse().unwrap())
        .http3(
            "0.0.0.0:8443".parse().unwrap(),
            "server.crt",
            "server.key",
        )
        .token("my-token")
        .build()?;

    server.start().await
}
```

The regular HTTP ingress advertises HTTP/3 with `Alt-Svc` when `.http3(...)` is
configured. HTTP/3 routing uses the same strict `Host` matching as HTTP/1.1 and
HTTP/2 ingress.

## Documentation

See [docs.rs/ferrotunnel](https://docs.rs/ferrotunnel) for API documentation.

## License

MIT OR Apache-2.0
