# FerroTunnel 🦀

[![CI](https://github.com/ferro-labs/ferrotunnel/workflows/CI/badge.svg)](https://github.com/ferro-labs/ferrotunnel/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/ferrotunnel)](https://crates.io/crates/ferrotunnel)
[![Documentation](https://docs.rs/ferrotunnel/badge.svg)](https://docs.rs/ferrotunnel)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE)
[![Rust Version](https://img.shields.io/badge/rust-1.91%2B-orange.svg)](https://www.rust-lang.org)
[![Discord](https://img.shields.io/badge/Discord-Join-5865F2?logo=discord&logoColor=white)](https://discord.gg/yCAeYvJeDV)

**High-performance reverse tunnel you can embed in your Rust applications.**

FerroTunnel multiplexes streams over a single connection and ships as a library-first Rust crate. Expose local services behind NAT, route HTTP by hostname, and run it through the CLI or the `Client::builder()` API.


## Prerequisites

- **Rust 1.91+**: FerroTunnel uses modern Rust features for performance and safety.
- **Cargo**: Required for building and installing from source.
- **Git**: For cloning the repository during development.

## Installation

### Linux / macOS (recommended)

```bash
curl -fsSL https://tunnel.ferrolabs.ai/install.sh | bash
```

### Cargo

```bash
cargo install ferrotunnel-cli
```

## Quick Start

```bash

# Start server; enter the token at the prompt or set FERROTUNNEL_TOKEN
ferrotunnel server

# Start client in another terminal; use the same token via env or prompt
ferrotunnel client --server localhost:7835 --local-addr 127.0.0.1:8080 --tunnel-id my-app
```

### Library

```toml
[dependencies]
ferrotunnel = "1.5"
tokio = { version = "1", features = ["full"] }
```

```rust
use ferrotunnel::Client;

#[tokio::main]
async fn main() -> ferrotunnel::Result<()> {
    let mut client = Client::builder()
        .server_addr("tunnel.example.com:7835")
        .token("my-secret-token")
        .local_addr("127.0.0.1:8080")
        .tunnel_id("my-app")
        .build()?;

    client.start().await?;

    tokio::signal::ctrl_c().await?;
    client.shutdown().await
}
```

### HTTP/2 and Connection Pooling

FerroTunnel v1.0.3+ includes automatic HTTP/2 support and connection pooling for improved performance:

**Server-side**: The HTTP ingress automatically detects and handles both HTTP/1.1 and HTTP/2 connections from clients.

**Client-side**: Connection pooling reuses HTTP connections to local services, eliminating per-request TCP handshake overhead:

```rust
use ferrotunnel_http::{HttpProxy, PoolConfig};
use std::time::Duration;

// Create proxy with custom pool configuration
let pool_config = PoolConfig {
    max_idle_per_host: 32,           // Max idle connections per host (default: 32)
    idle_timeout: Duration::from_secs(90), // Connection idle timeout (default: 90s)
    prefer_h2: false,                 // Prefer HTTP/2 when available (default: false)
};

let proxy = HttpProxy::with_pool_config("127.0.0.1:8080".into(), pool_config);
```

**CLI**: Use default pool settings (no flags needed) or customize via the library API.

**Benefits**:
- 🚀 Eliminates TCP handshake overhead per request
- 🔄 HTTP/2 multiplexing reduces connection count
- 🧹 Background eviction prevents resource leaks
- 📈 Significantly improves throughput (target: 800-1000 MB/s)

### gRPC Tunneling

FerroTunnel v1.0.6+ natively tunnels gRPC traffic over HTTP/2 with zero configuration.

**How it works**: The server-side ingress automatically detects gRPC requests by inspecting the `Content-Type: application/grpc` header. Detected gRPC streams are forwarded over a dedicated HTTP/2 connection to the local service, preserving HTTP/2 trailers (including `grpc-status` and `grpc-message`) end-to-end.

**CLI** — no special flags needed; detection is automatic:

```bash
# Expose a local gRPC server running on port 50051
ferrotunnel client --server tunnel.example.com:7835 --local-addr 127.0.0.1:50051 --tunnel-id my-grpc-service
```

**Library**:

```rust
use ferrotunnel::Client;

#[tokio::main]
async fn main() -> ferrotunnel::Result<()> {
    let mut client = Client::builder()
        .server_addr("tunnel.example.com:7835")
        .token("my-secret-token")
        .local_addr("127.0.0.1:50051")  // gRPC server port
        .tunnel_id("my-grpc-service")
        .build()?;

    client.start().await?;
    tokio::signal::ctrl_c().await?;
    client.shutdown().await
}
```

**What is preserved end-to-end**:
- HTTP/2 stream multiplexing
- gRPC trailers (`grpc-status`, `grpc-message`, custom metadata)
- Streaming RPCs (server-streaming, client-streaming, bidirectional)
- Standard gRPC status codes and error propagation

### QUIC Transport

FerroTunnel v1.0.7+ supports QUIC as an alternative transport for the tunnel control plane, providing built-in TLS 1.3 encryption, native stream multiplexing (no head-of-line blocking), and lower connection latency.

**CLI** — enable with the `quic` feature flag:

```bash
# Build with QUIC support
cargo build --features quic

# Server: keep the TCP control plane on :7835 and add a shared-state QUIC listener on :7836
# Token is read from FERROTUNNEL_TOKEN, --token-file, or the secure prompt.
ferrotunnel server --quic-bind 0.0.0.0:7836 --tls-cert server.crt --tls-key server.key

# Client: connect via QUIC
ferrotunnel client --server 127.0.0.1:7836 --quic --tls-skip-verify
```

`--tls-skip-verify` is explicit insecure mode for local or self-signed testing only.

**Library**:

```rust
use ferrotunnel::Client;
use ferrotunnel::common::QuicConfig;

#[tokio::main]
async fn main() -> ferrotunnel::Result<()> {
    let quic = QuicConfig {
        enabled: true,
        cert_path: Some("client.crt".into()),
        key_path: Some("client.key".into()),
        skip_verify: true,
        ..Default::default()
    };

    let mut client = Client::builder()
        .server_addr("tunnel.example.com:7836")
        .token("my-secret-token")
        .local_addr("127.0.0.1:8080")
        .quic(&quic)
        .build()?;

    client.start().await?;
    tokio::signal::ctrl_c().await?;
    client.shutdown().await
}
```

**Key benefits**:
- No head-of-line blocking — each tunnel stream uses a native QUIC stream
- Built-in TLS 1.3 encryption (mandatory in QUIC)
- `--quic-0rtt` is reserved for future 0-RTT support; current clients fall back to a full handshake
- UDP-based — works better on lossy networks

### HTTP/3 Ingress

FerroTunnel v1.0.8+ can accept browser-facing HTTP/3 traffic on a UDP ingress
port while preserving the existing HTTP/1.1, HTTP/2, WebSocket, and gRPC paths.
HTTP/3 ingress is separate from QUIC tunnel transport: it uses `h3` +
`h3-quinn` for public client requests, then forwards through the same strict
`Host`-based tunnel routing as the TCP HTTP ingress.

**CLI** — enable with the `http3` feature flag:

```bash
# Build with HTTP/3 ingress support
cargo build -p ferrotunnel-cli --features http3

# Server: HTTP/1.1+HTTP/2 on TCP :8080, HTTP/3 on UDP :8443
# Token is read from FERROTUNNEL_TOKEN, --token-file, or the secure prompt.
ferrotunnel server \
  --http-bind 0.0.0.0:8080 \
  --http3-bind 0.0.0.0:8443 \
  --tls-cert server.crt \
  --tls-key server.key
```

When HTTP/3 is enabled, the TCP HTTP ingress advertises it with `Alt-Svc`, for
example `Alt-Svc: h3=":8443"; ma=86400`.

**Library**:

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
        .token("my-secret-token")
        .build()?;

    server.start().await?;
    tokio::signal::ctrl_c().await?;
    server.shutdown().await
}
```

**Deployment notes**:
- Requires TLS certificate and private key because HTTP/3 runs over QUIC/TLS 1.3
- Requires UDP reachability to the HTTP/3 bind port
- Keeps strict `Host` header routing; unknown hosts return `404 Tunnel not found`

## Features

| Feature | Description |
|---------|-------------|
| **Embeddable** | Use as a library with builder APIs |
| **HTTP/2** | Automatic HTTP/1.1 and HTTP/2 protocol detection |
| **Connection Pooling** | Efficient connection reuse for improved performance |
| **Plugin System** | Auth, rate limiting, logging, circuit breaker |
| **Dashboard** | Real-time WebUI at `localhost:4040` |
| **TLS** | Optional verified TLS connections with rustls |
| **Mutual TLS** | Client certificate authentication |
| **Observability** | Prometheus metrics + OpenTelemetry tracing |
| **WebSocket** | Transparent WebSocket upgrade tunneling |
| **gRPC** | Native gRPC tunneling over HTTP/2 with trailer preservation |
| **QUIC** | Optional QUIC transport with native stream multiplexing |
| **HTTP/3** | Optional browser-facing HTTP/3 ingress with Alt-Svc advertising |
| **TCP & HTTP** | Forward both HTTP and raw TCP traffic |



**Choose FerroTunnel when**: You need many services over a single connection, HTTP routing, plugins, or resource efficiency.

See [Architecture](docs/ARCHITECTURE.md) for detailed analysis of the multiplexing trade-off.

## Security: Why Rust Matters

FerroTunnel is built in Rust, and every workspace-owned crate forbids `unsafe` code. In that
code, Rust's ownership and type system prevent common memory-corruption defects, including
use-after-free, double-free, and data races, at compile time. This narrows the attack surface of
a network-facing service without replacing dependency review, secure configuration, or
operational controls.

**Security features:**

- **Safe project code** - `unsafe_code = "forbid"` is enforced across workspace-owned crates.
- **Protected transports** - Verified TLS is available for TCP, mutual TLS is supported, and
  QUIC always uses TLS 1.3.
- **Hardened authentication** - Shared tokens are format-validated and compared in constant
  time.
- **Bounded traffic** - Session ceilings, protocol frame-size validation, and session rate
  limits are enforced.
- **Protected dashboard** - Dashboard access defaults to loopback, with authentication required
  for non-loopback access.
- **Automated checks** - CI runs RustSec advisory checks, cargo-deny policy checks, tests, lints,
  documentation builds, and fuzz smoke tests.

**Why it matters:** Memory-corruption bugs can turn malformed network input into crashes or code
execution. Rust prevents many of those failure modes in FerroTunnel's safe project code, while
the project continues to test and audit the risks the language does not eliminate.

See [SECURITY.md](SECURITY.md) for vulnerability reporting and
[docs/security.md](docs/security.md) for deployment guidance and security boundaries.

## Ideal For

FerroTunnel is a strong fit when its embeddable Rust API, multiplexed streams, and deployment
controls match the workload:

- 🔐 **Crypto and blockchain infrastructure** - Rust-based services that need authenticated
  tunnels, mutual TLS, plugins, and observability.
- 📡 **IoT gateways** - Native deployments that benefit from no managed garbage collector,
  configurable resource limits, and a small measured connection footprint.
- ⚡ **Edge computing** - Hostname-based HTTP routing and multiple logical streams over one tunnel
  connection, with optional QUIC transport.
- 🖥️ **Embedded and appliance-style systems** - Deployments that value safe concurrency and
  predictable native execution; validate target support and capacity on the intended hardware.
- 🏢 **Enterprise and internal platforms** - Verified TLS, mutual TLS, token authentication, rate
  limits, Prometheus metrics, and OpenTelemetry tracing.

Published FerroTunnel 1.0.0 loopback benchmarks measured 0.078 ms median latency and 47.3 MB
peak memory at 1,000 concurrent connections. Treat these as reproducible reference points, not
deployment guarantees, and benchmark the intended hardware and configuration. See
[docs/benchmark.md](docs/benchmark.md) for the test environment and methodology.

## CLI Reference


### Server

```bash
ferrotunnel server [OPTIONS]
```

| Option | Env Variable | Default | Description |
|--------|--------------|---------|-------------|
| _(env only)_ | `FERROTUNNEL_TOKEN` | optional | Auth token; not a CLI flag so it can't leak via argv |
| `--token-file` | `FERROTUNNEL_TOKEN_FILE` | - | Read auth token from a file |
| `--bind` | `FERROTUNNEL_BIND` | `0.0.0.0:7835` | Control plane |
| `--http-bind` | `FERROTUNNEL_HTTP_BIND` | `0.0.0.0:8080` | HTTP ingress |
| `--tcp-bind` | `FERROTUNNEL_TCP_BIND` | - | TCP ingress |
| `--tls-cert` | `FERROTUNNEL_TLS_CERT` | - | TLS certificate |
| `--tls-key` | `FERROTUNNEL_TLS_KEY` | - | TLS private key |
| `--quic-bind`* | `FERROTUNNEL_QUIC_BIND` | - | QUIC endpoint (UDP) |
| `--http3-bind`** | `FERROTUNNEL_HTTP3_BIND` | - | HTTP/3 ingress endpoint (UDP) |

### Client

```bash
ferrotunnel client [OPTIONS]
```

| Option | Env Variable | Default | Description |
|--------|--------------|---------|-------------|
| `--server` | `FERROTUNNEL_SERVER` | required | Server address |
| `--token` | `FERROTUNNEL_TOKEN` | optional | Auth token; if omitted, uses env or prompts securely |
| `--local-addr` | `FERROTUNNEL_LOCAL_ADDR` | `127.0.0.1:8000` | Local service |
| `--tunnel-id` | `FERROTUNNEL_TUNNEL_ID` | (auto) | Tunnel ID for HTTP routing |
| `--dashboard-port` | `FERROTUNNEL_DASHBOARD_PORT` | `4040` | Dashboard port |
| `--dashboard-bind` | `FERROTUNNEL_DASHBOARD_BIND` | `127.0.0.1` | Dashboard bind address |
| `--dashboard-allow-non-loopback` | `FERROTUNNEL_DASHBOARD_ALLOW_NON_LOOPBACK` | `false` | Allow exposed dashboard bind; requires auth token |
| `--dashboard-auth-token` | `FERROTUNNEL_DASHBOARD_AUTH_TOKEN` | generated | Dashboard API auth token |
| `--tls` | `FERROTUNNEL_TLS` | false | Enable TLS; requires `--tls-ca` unless `--tls-skip-verify` is explicit |
| `--tls-ca` | `FERROTUNNEL_TLS_CA` | - | CA certificate for verified TLS |
| `--quic`* | `FERROTUNNEL_QUIC` | false | Use QUIC transport |
| `--quic-0rtt`* | `FERROTUNNEL_QUIC_0RTT` | false | Reserved; currently uses a full handshake |

For TCP/TLS clients, `--tls` requires `--tls-ca` unless `--tls-skip-verify` is explicitly set.

*\* Requires `--features quic` at build time.*
*\*\* Requires `--features http3` at build time.*

See [ferrotunnel-cli/README.md](ferrotunnel-cli/README.md) for all options.

## Crates

| Crate | Description |
|-------|-------------|
| [`ferrotunnel`](ferrotunnel/) | Main library with builder APIs |
| [`ferrotunnel-cli`](ferrotunnel-cli/) | Unified CLI binary |
| [`ferrotunnel-core`](ferrotunnel-core/) | Tunnel logic and transport |
| [`ferrotunnel-protocol`](ferrotunnel-protocol/) | Wire protocol and codec |
| [`ferrotunnel-http`](ferrotunnel-http/) | HTTP/TCP ingress and proxy |
| [`ferrotunnel-plugin`](ferrotunnel-plugin/) | Plugin system |
| [`ferrotunnel-observability`](ferrotunnel-observability/) | Metrics and dashboard |
| [`ferrotunnel-common`](ferrotunnel-common/) | Shared types |

## Installation

### Pre-built Binaries

Download from [GitHub Releases](https://github.com/ferro-labs/ferrotunnel/releases).

### From Source

```bash
cargo install ferrotunnel-cli
```

### macOS (Homebrew)

```bash
brew tap ferro-labs/ferrotunnel
brew install ferrotunnel
```

### Docker

#### Using Pull

You can pull the official image from GitHub Container Registry:

```bash
# Pull the latest image
docker pull ghcr.io/ferro-labs/ferrotunnel:latest

# Run as a server using FERROTUNNEL_TOKEN from the host environment
export FERROTUNNEL_TOKEN=secret
docker run -e FERROTUNNEL_TOKEN -p 7835:7835 -p 8080:8080 ghcr.io/ferro-labs/ferrotunnel:latest server
```

#### Using Docker Compose

For more complex setups, use the provided `docker-compose.yml`:

```bash
docker-compose up --build
```

## Examples

Ready-to-run examples are maintained in a separate repository:

**[https://github.com/ferro-labs/tunnel-examples](https://github.com/ferro-labs/tunnel-examples)**

## Documentation

- [CLI Reference](ferrotunnel-cli/README.md)
- [Contributing](CONTRIBUTING.md) & [Code of Conduct](CODE_OF_CONDUCT.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Benchmark & Performance](docs/benchmark.md)
- [Deployment Guide](docs/deployment.md)
- [Plugin crate guide](ferrotunnel-plugin/README.md)
- [Security](docs/security.md)

## Development

```bash
# Build
cargo build --workspace

# Test
cargo test --workspace

# Lint
cargo clippy --workspace --all-targets -- -D warnings

# Benchmark
cargo bench --workspace
```

### Developer Tools

- [`tools/loadgen`](tools/loadgen/) - Load testing
- [`tools/soak`](tools/soak/) - Stability testing
- [`tools/profiler`](tools/profiler/) - Performance profiling

## Benchmark

FerroTunnel is benchmarked against [rathole](https://github.com/rapiz1/rathole) and [frp](https://github.com/fatedier/frp). Unlike rathole/frp which use 1:1 TCP forwarding, FerroTunnel uses **multiplexed streams over a single connection** the same architecture used by [ngrok](https://ngrok.com/docs/http/) and [Cloudflare Tunnel](https://developers.cloudflare.com/speed/optimization/protocol/http2-to-origin/) (HTTP/2 multiplexing). This enables HTTP routing, plugins, and multi-service tunnels.

<p align="center">
  <img src="docs/static/server_heap_graph.png" alt="Server Heap Graph" width="45%">
  <img src="docs/static/top_allocations.png" alt="Top Allocations" width="45%">
</p>
<p align="center"><em>Memory profile: flat heap usage, minimal allocations under load</em></p>

See [docs/benchmark.md](docs/benchmark.md) for detailed analysis of the architectural trade-offs.

## License

Licensed under either of [Apache License 2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT) at your option.
