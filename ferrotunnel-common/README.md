# ferrotunnel-common

[![Crates.io](https://img.shields.io/crates/v/ferrotunnel-common)](https://crates.io/crates/ferrotunnel-common)
[![Documentation](https://docs.rs/ferrotunnel-common/badge.svg)](https://docs.rs/ferrotunnel-common)

Shared types and error handling for [FerroTunnel](https://github.com/ferro-labs/ferrotunnel).

## Overview

This crate provides common utilities shared across all FerroTunnel crates:

- **Error handling**: `TunnelError`, `Result<T>`
- **Configuration**: `TlsConfig`, `QuicConfig`, `LimitsConfig`, `RateLimitConfig` (serializable for config files)
- **Default constants**: `DEFAULT_TUNNEL_PORT` (7835), `DEFAULT_HTTP_PORT` (8080), `DEFAULT_METRICS_PORT` (9090), `DEFAULT_DASHBOARD_PORT` (4040), `DEFAULT_TUNNEL_BIND`, `DEFAULT_HTTP_BIND`, `DEFAULT_LOCAL_ADDR`

## Usage

```rust
use ferrotunnel_common::{Result, TunnelError};

fn example() -> Result<()> {
    // Your tunnel logic here
    Ok(())
}
```

```rust
use ferrotunnel_common::{DEFAULT_HTTP_PORT, DEFAULT_TUNNEL_PORT, TlsConfig};

// Use default ports when building bind addresses
let tunnel_bind = format!("0.0.0.0:{}", DEFAULT_TUNNEL_PORT);
let _tls = TlsConfig::default();
```

## Error Types

- `Io` - I/O errors
- `Protocol` - Protocol violations
- `Authentication` - Auth failures
- `SessionNotFound` - Invalid session
- `StreamNotFound` - Invalid stream
- `Timeout` - Operation timeout
- `Config` - Configuration errors
- `Connection` - Connection failures
- `Tls` - TLS handshake or certificate errors

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your option.
