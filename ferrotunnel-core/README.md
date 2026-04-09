# ferrotunnel-core

[![Crates.io](https://img.shields.io/crates/v/ferrotunnel-core)](https://crates.io/crates/ferrotunnel-core)
[![Documentation](https://docs.rs/ferrotunnel-core/badge.svg)](https://docs.rs/ferrotunnel-core)

Core tunnel implementation for [FerroTunnel](https://github.com/ferro-labs/ferrotunnel).

## Overview

This crate provides the core tunnel logic:

- `TunnelClient` - Client-side tunnel connection
- `TunnelServer` - Server-side tunnel listener
- `Multiplexer` - Stream multiplexing over a single connection
- `Session` - Session management with heartbeat tracking
- `TlsTransport` - Encrypted transport support
- `QuicMultiplexer` - QUIC-native stream multiplexing (feature: `quic`)

## Components

### Tunnel
- **TunnelClient** - Connects to server, handles handshake, runs message loop
- **TunnelServer** - Listens for connections, authenticates clients, manages sessions

### Stream
- **Multiplexer** - Multiplexes virtual streams over one TCP connection; frames are sent in priority order (Critical → High → Normal → Low).
- **VirtualStream** - AsyncRead/AsyncWrite implementation for tunneled data; created with a stream priority for QoS.
- **open_stream** / **open_stream_with_priority** - Open streams with default or specified priority.
- **Bytes pool** - Thread-local buffer pooling for zero-copy transfers (configurable via env).

### Transport
- **TCP transport** - Standard TCP connection
- **TLS support** - Native TLS 1.3 encryption (rustls)
- **QUIC transport** - UDP-based transport with native stream multiplexing (feature: `quic`, uses quinn 0.11)

## Usage

```rust
use ferrotunnel_core::{TunnelClient, TunnelServer};

// Server
let server = TunnelServer::new("0.0.0.0:7835".parse()?, "secret".into());
server.run().await?;

// Client
let mut client = TunnelClient::new("localhost:7835".into(), "secret".into());
client.connect_and_run(|stream| async { /* handle stream */ }).await?;
```

## Configuration

Buffer pool limits (used by the stream layer) can be tuned via environment variables. Values are read once at first use.

| Variable | Default | Description |
|----------|---------|-------------|
| `FERROTUNNEL_POOL_MAX_SIZE` | 32 | Max number of buffers per thread in the pool |
| `FERROTUNNEL_POOL_MAX_CAPACITY_BYTES` | 65536 | Max capacity (bytes) of a buffer that is pooled; larger buffers are not reused |
| `FERROTUNNEL_POOL_DEFAULT_CAPACITY_BYTES` | 4096 | Default capacity for newly allocated buffers |

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your option.
