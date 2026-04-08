# ferrotunnel-cli

[![Crates.io](https://img.shields.io/crates/v/ferrotunnel-cli)](https://crates.io/crates/ferrotunnel-cli)

Unified CLI for [FerroTunnel](https://github.com/ferro-labs/ferrotunnel) - a reverse tunnel system in Rust.

## Installation

```bash
cargo install ferrotunnel-cli
```

## Usage

```bash
ferrotunnel <COMMAND> [OPTIONS]
```

## Commands

### Server

Run the tunnel server:

```bash
ferrotunnel server --token my-secret-token
```

**Options:**

| Option | Env Variable | Default | Description |
|--------|--------------|---------|-------------|
| `--bind` | `FERROTUNNEL_BIND` | `0.0.0.0:7835` | Tunnel control plane address |
| `--http-bind` | `FERROTUNNEL_HTTP_BIND` | `0.0.0.0:8080` | HTTP ingress address |
| `--tcp-bind` | `FERROTUNNEL_TCP_BIND` | - | TCP ingress address (optional) |
| `--token` | `FERROTUNNEL_TOKEN` | (required) | Authentication token |
| `--log-level` | `RUST_LOG` | `info` | Log level |
| `--metrics-bind` | `FERROTUNNEL_METRICS_BIND` | `0.0.0.0:9090` | Prometheus metrics address |
| `--observability` | `FERROTUNNEL_OBSERVABILITY` | `false` | Enable tracing |
| `--metrics` | `FERROTUNNEL_METRICS` | `false` | Enable Prometheus metrics endpoint |
| `--tls-cert` | `FERROTUNNEL_TLS_CERT` | - | TLS certificate file path |
| `--tls-key` | `FERROTUNNEL_TLS_KEY` | - | TLS private key file path |
| `--tls-ca` | `FERROTUNNEL_TLS_CA` | - | CA certificate for client auth |
| `--tls-client-auth` | `FERROTUNNEL_TLS_CLIENT_AUTH` | `false` | Require client certificates |
| `--quic-bind`* | `FERROTUNNEL_QUIC_BIND` | - | QUIC endpoint address (UDP) |
| `--quic-cert`* | `FERROTUNNEL_QUIC_CERT` | - | TLS cert for QUIC (falls back to `--tls-cert`) |
| `--quic-key`* | `FERROTUNNEL_QUIC_KEY` | - | TLS key for QUIC (falls back to `--tls-key`) |

*\* Requires `--features quic` at build time.*

### Client

Run the tunnel client:

```bash
# With token on command line
ferrotunnel client --server tunnel.example.com:7835 --token my-secret-token

# Token from environment (recommended for scripts)
export FERROTUNNEL_TOKEN=my-secret-token
ferrotunnel client --server tunnel.example.com:7835

# Token prompted securely (when omitted and not in env)
ferrotunnel client --server tunnel.example.com:7835
# → Prompts: "Token: " (input is not echoed)
```

**Options:**

| Option | Env Variable | Default | Description |
|--------|--------------|---------|-------------|
| `--server` | `FERROTUNNEL_SERVER` | (required) | Server address (`host:port`) |
| `--token` | `FERROTUNNEL_TOKEN` | (optional) | Authentication token; if omitted, uses env or prompts securely |
| `--local-addr` | `FERROTUNNEL_LOCAL_ADDR` | `127.0.0.1:8000` | Local service to forward |
| `--tunnel-id` | `FERROTUNNEL_TUNNEL_ID` | (auto) | Tunnel ID for HTTP routing (matched against Host header) |
| `--dashboard-port` | `FERROTUNNEL_DASHBOARD_PORT` | `4040` | Dashboard port |
| `--no-dashboard` | - | `false` | Disable dashboard |
| `--log-level` | `RUST_LOG` | `info` | Log level |
| `--observability` | `FERROTUNNEL_OBSERVABILITY` | `false` | Enable tracing |
| `--metrics` | `FERROTUNNEL_METRICS` | `false` | Enable metrics collection |
| `--tls` | `FERROTUNNEL_TLS` | `false` | Enable TLS |
| `--tls-skip-verify` | `FERROTUNNEL_TLS_SKIP_VERIFY` | `false` | Skip certificate verification |
| `--tls-ca` | `FERROTUNNEL_TLS_CA` | - | CA certificate path |
| `--tls-server-name` | `FERROTUNNEL_TLS_SERVER_NAME` | - | SNI hostname |
| `--tls-cert` | `FERROTUNNEL_TLS_CERT` | - | Client certificate (mTLS) |
| `--tls-key` | `FERROTUNNEL_TLS_KEY` | - | Client private key (mTLS) |
| `--quic`* | `FERROTUNNEL_QUIC` | `false` | Use QUIC transport |
| `--quic-0rtt`* | `FERROTUNNEL_QUIC_0RTT` | `false` | Enable 0-RTT reconnection |

*\* Requires `--features quic` at build time.*

### Version

Show version information:

```bash
ferrotunnel version
```

## Examples

### Quick Start

```bash
# Terminal 1: Start server
ferrotunnel server --token secret

# Terminal 2: Start client (token from env or prompt if omitted)
ferrotunnel client --server localhost:7835 --local-addr 127.0.0.1:8080

# Terminal 3: Start local service
python3 -m http.server 8080
```

### With TLS

```bash
# Server with TLS
ferrotunnel server --token secret \
  --tls-cert server.crt --tls-key server.key

# Client with TLS
ferrotunnel client --server tunnel.example.com:7835 --token secret \
  --tls --tls-ca ca.crt
```

### With QUIC Transport

```bash
# Build with QUIC support
cargo install ferrotunnel-cli --features quic

# Server with QUIC endpoint
ferrotunnel server --token secret \
  --tls-cert server.crt --tls-key server.key \
  --quic-bind 0.0.0.0:7836

# Client connecting via QUIC
ferrotunnel client --server 127.0.0.1:7836 --token secret \
  --quic --tls-skip-verify
```

### Using Environment Variables

Avoid putting the token on the command line (visible in process list and shell history). Use the environment or a secure prompt instead:

```bash
export FERROTUNNEL_TOKEN=my-secret-token
export FERROTUNNEL_SERVER=tunnel.example.com:7835

ferrotunnel client --local-addr 127.0.0.1:3000
```

If `--token` and `FERROTUNNEL_TOKEN` are both unset, the client prompts for the token on the TTY (input is not echoed).

## Developer Tools

For load testing and soak testing, see the separate tools:

- [tools/loadgen](../tools/loadgen) - Load generator
- [tools/soak](../tools/soak) - Long-duration stability testing

## Library Usage

For embedding in your application, use the main `ferrotunnel` crate instead:

```rust
use ferrotunnel::Client;

let mut client = Client::builder()
    .server_addr("tunnel.example.com:7835")
    .token("secret")
    .local_addr("127.0.0.1:8000")
    .build()?;

client.start().await?;
```

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your option.
