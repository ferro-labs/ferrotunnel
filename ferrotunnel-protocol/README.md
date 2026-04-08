# ferrotunnel-protocol

[![Crates.io](https://img.shields.io/crates/v/ferrotunnel-protocol)](https://crates.io/crates/ferrotunnel-protocol)
[![Documentation](https://docs.rs/ferrotunnel-protocol/badge.svg)](https://docs.rs/ferrotunnel-protocol)

Wire protocol definitions and codec for [FerroTunnel](https://github.com/ferro-labs/ferrotunnel).

## Overview

This crate defines the binary protocol used for tunnel communication:

- **12 frame types** for control, data, and keepalive (7 protocol variants: HTTP, HTTPS, HTTP2, WebSocket, gRPC, TCP, QUIC)
- **Length-prefixed codec** (4-byte length + 1-byte type) with bincode control frames
- **16MB max frame size** with validation

## Frame Types

### Control Frames
- `Handshake` / `HandshakeAck` - Connection establishment
- `Register` / `RegisterAck` - Tunnel registration

### Stream Frames
- `OpenStream` / `StreamAck` - Stream creation
- `Data` - Payload transfer
- `CloseStream` - Stream termination

### Keepalive
- `Heartbeat` / `HeartbeatAck` - Connection health

### Other
- `Error` - Error reporting
- `PluginData` - Plugin communication

## Usage

```rust
use ferrotunnel_protocol::{Frame, TunnelCodec};
use tokio_util::codec::{Framed, Decoder};

// Create a codec for framing
let codec = TunnelCodec::new();

// Create frames
let frame = Frame::Heartbeat { 
    timestamp: 1234567890 
};
```

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your option.
