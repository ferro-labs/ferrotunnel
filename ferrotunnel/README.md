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
- Token-based authentication
- HTTP, WebSocket, gRPC, and TCP tunneling
- Automatic reconnection with backoff
- Prometheus metrics and tracing

## Documentation

See [docs.rs/ferrotunnel](https://docs.rs/ferrotunnel) for API documentation.

## License

MIT OR Apache-2.0
