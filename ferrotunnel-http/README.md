# ferrotunnel-http

[![Crates.io](https://img.shields.io/crates/v/ferrotunnel-http)](https://crates.io/crates/ferrotunnel-http)
[![Documentation](https://docs.rs/ferrotunnel-http/badge.svg)](https://docs.rs/ferrotunnel-http)

HTTP ingress and proxy for [FerroTunnel](https://github.com/ferro-labs/ferrotunnel).

## Overview

This crate provides HTTP handling for the tunnel:

- `HttpIngress` - Server-side HTTP listener that routes requests through tunnels
- `Http3Ingress` - Feature-gated HTTP/3 listener for browser-facing QUIC traffic
- `TcpIngress` - Server-side TCP listener for raw socket tunneling
- `HttpProxy` - Client-side proxy that forwards tunneled requests to local services

## Features

- **HTTP/2 Support** - Automatic protocol detection for HTTP/1.1 and HTTP/2
- **HTTP/3 Ingress** - Optional `http3` feature using `h3` + `h3-quinn`
- **Connection Pooling** - Efficient connection reuse to local services
- **WebSocket Tunneling** - Transparent WebSocket upgrade handling
- **Configurable Limits** - Max connections, timeouts, response size limits

## Components

### HttpIngress (Server)
Hyper-based HTTP server that:
- Listens for incoming HTTP requests
- Routes requests to connected tunnel clients
- Supports HTTP/1.1, HTTP/2, and WebSocket upgrades
- Automatic protocol detection via `hyper-util::AutoBuilder`
- Can advertise HTTP/3 via `Alt-Svc` when paired with `Http3Ingress`

### Http3Ingress (Server, feature: `http3`)
QUIC/HTTP3 server that:
- Listens for browser-facing HTTP/3 requests over UDP
- Uses TLS 1.3 ALPN `h3` through `h3` and `h3-quinn`
- Routes requests by the same normalized `Host` lookup as `HttpIngress`
- Forwards HTTP/3 request bodies to the existing tunnel multiplexer
- Streams upstream responses back as HTTP/3 responses

### TcpIngress (Server)
Socket listener that:
- Accepts raw TCP connections
- Routes traffic through the tunnel protocol
- Protocol-agnostic (supports Database, SSH, etc.)

### HttpProxy (Client)
Handles tunneled HTTP requests by:
- Receiving virtual streams from the multiplexer
- Connecting to local services via connection pool
- Proxying data bidirectionally
- Reusing HTTP/1.1 and HTTP/2 connections for performance

## Usage

### Basic Usage

```rust
use ferrotunnel_http::{HttpIngress, TcpIngress, HttpProxy};

// Server-side: Create ingress (supports HTTP/1.1 and HTTP/2 automatically)
let ingress = HttpIngress::new("0.0.0.0:8080".parse()?, sessions.clone(), registry);
let tcp_ingress = TcpIngress::new("0.0.0.0:5000".parse()?, sessions);

tokio::try_join!(ingress.start(), tcp_ingress.start())?;

// Client-side: Create proxy with default connection pooling
let proxy = HttpProxy::new("127.0.0.1:3000".into());
proxy.handle_stream(virtual_stream);
```

### HTTP/3 Ingress

```toml
[dependencies]
ferrotunnel-http = { version = "1.0", features = ["http3"] }
```

```rust
use ferrotunnel_http::{Http3Ingress, Http3IngressConfig, HttpIngress};

let http3_config = Http3IngressConfig {
    cert_path: "server.crt".into(),
    key_path: "server.key".into(),
    ..Default::default()
};
let alt_svc = http3_config.alt_svc_header_value("0.0.0.0:8443".parse()?)?;

let http = HttpIngress::new("0.0.0.0:8080".parse()?, sessions.clone(), registry.clone())
    .with_alt_svc_header(alt_svc);
let http3 = Http3Ingress::new(
    "0.0.0.0:8443".parse()?,
    sessions,
    registry,
    http3_config,
);

tokio::try_join!(http.start(), http3.start())?;
```

HTTP/3 ingress is intentionally separate from the QUIC tunnel transport. It
accepts public HTTP/3 requests, then forwards them through the existing
`AnyMultiplexer` session selected by normalized `Host`.

### Advanced: Custom Connection Pool

```rust
use ferrotunnel_http::{HttpProxy, PoolConfig};
use std::time::Duration;

// Configure connection pool for optimal performance
let pool_config = PoolConfig {
    max_idle_per_host: 32,                // Max idle connections (default: 32)
    idle_timeout: Duration::from_secs(90), // Idle timeout (default: 90s)
    prefer_h2: false,                      // Prefer HTTP/2 (default: false)
};

let proxy = HttpProxy::with_pool_config("127.0.0.1:3000".into(), pool_config);
proxy.handle_stream(virtual_stream);
```

**Connection Pooling Benefits:**
- Eliminates TCP handshake overhead per request
- Reuses HTTP/1.1 connections via LIFO queue
- Shares HTTP/2 connections via multiplexing
- Background eviction prevents resource leaks

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your option.
