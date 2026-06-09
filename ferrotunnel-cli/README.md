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

Run the tunnel server. Prefer the environment, a token file, or the secure prompt so the token is not exposed in process arguments.

```bash
export FERROTUNNEL_TOKEN=my-secret-token
ferrotunnel server

# Or read the token from a secret file
ferrotunnel server --token-file /run/secrets/ferrotunnel-token

# Or omit both to enter the token securely at the prompt
ferrotunnel server
```

**Options:**

| Option | Env Variable | Default | Description |
|--------|--------------|---------|-------------|
| `--bind` | `FERROTUNNEL_BIND` | `0.0.0.0:7835` | Tunnel control plane address |
| `--http-bind` | `FERROTUNNEL_HTTP_BIND` | `0.0.0.0:8080` | HTTP ingress address |
| `--tcp-bind` | `FERROTUNNEL_TCP_BIND` | - | TCP ingress address (optional) |
| `--token` | `FERROTUNNEL_TOKEN` | (optional) | Authentication token; avoid for shared systems because argv can expose it |
| `--token-file` | `FERROTUNNEL_TOKEN_FILE` | - | Read authentication token from a file |
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
| `--http3-bind`** | `FERROTUNNEL_HTTP3_BIND` | - | HTTP/3 ingress address (UDP) |
| `--http3-cert`** | `FERROTUNNEL_HTTP3_CERT` | - | TLS cert for HTTP/3 (falls back to `--tls-cert`) |
| `--http3-key`** | `FERROTUNNEL_HTTP3_KEY` | - | TLS key for HTTP/3 (falls back to `--tls-key`) |

*\* Requires `--features quic` at build time.*
*\*\* Requires `--features http3` at build time.*

### Client

Run the tunnel client:

```bash
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
| `--dashboard-bind` | `FERROTUNNEL_DASHBOARD_BIND` | `127.0.0.1` | Dashboard bind address |
| `--dashboard-allow-non-loopback` | `FERROTUNNEL_DASHBOARD_ALLOW_NON_LOOPBACK` | `false` | Allow exposed dashboard bind; requires auth token |
| `--dashboard-auth-token` | `FERROTUNNEL_DASHBOARD_AUTH_TOKEN` | generated | Dashboard API auth token |
| `--no-dashboard` | - | `false` | Disable dashboard |
| `--log-level` | `RUST_LOG` | `info` | Log level |
| `--observability` | `FERROTUNNEL_OBSERVABILITY` | `false` | Enable tracing |
| `--metrics` | `FERROTUNNEL_METRICS` | `false` | Enable metrics collection |
| `--tls` | `FERROTUNNEL_TLS` | `false` | Enable TLS; requires `--tls-ca` unless `--tls-skip-verify` is explicit |
| `--tls-skip-verify` | `FERROTUNNEL_TLS_SKIP_VERIFY` | `false` | Explicit insecure mode; skip certificate verification |
| `--tls-ca` | `FERROTUNNEL_TLS_CA` | - | CA certificate path for verified TLS |
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
# Terminal 1: Start server (token from env or prompt if omitted)
export FERROTUNNEL_TOKEN=secret
ferrotunnel server

# Terminal 2: Start client (use the same token via env or prompt)
export FERROTUNNEL_TOKEN=secret
ferrotunnel client --server localhost:7835 --local-addr 127.0.0.1:8080

# Terminal 3: Start local service
python3 -m http.server 8080
```

### With TLS

```bash
# Server with TLS (token from env or prompt if omitted)
ferrotunnel server \
  --tls-cert server.crt --tls-key server.key

# Client with TLS (token from env or prompt if omitted)
ferrotunnel client --server tunnel.example.com:7835 \
  --tls --tls-ca ca.crt
```

`--tls` requires `--tls-ca` for certificate verification. Use `--tls-skip-verify` only when explicitly accepting insecure self-signed testing mode.

### With QUIC Transport

```bash
# Build with QUIC support
cargo install ferrotunnel-cli --features quic

# Server with QUIC endpoint (token from env or prompt if omitted)
ferrotunnel server \
  --tls-cert server.crt --tls-key server.key \
  --quic-bind 0.0.0.0:7836

# Client connecting via QUIC (token from env or prompt if omitted)
ferrotunnel client --server 127.0.0.1:7836 \
  --quic --tls-skip-verify
```

### With HTTP/3 Ingress

```bash
# Build with HTTP/3 ingress support
cargo install ferrotunnel-cli --features http3

# HTTP/1.1 + HTTP/2 ingress on TCP :8080; HTTP/3 ingress on UDP :8443
# Token is read from FERROTUNNEL_TOKEN, --token-file, or the secure prompt.
ferrotunnel server \
  --http-bind 0.0.0.0:8080 \
  --http3-bind 0.0.0.0:8443 \
  --tls-cert server.crt \
  --tls-key server.key
```

When enabled, FerroTunnel adds `Alt-Svc` to HTTP ingress responses so compatible
clients can discover the HTTP/3 UDP endpoint. The HTTP/3 ingress uses the same
strict `Host`-based tunnel routing as the regular HTTP ingress.

### Using Environment Variables

Avoid putting the token on the command line: `--token` can be visible in process listings and shell history. Use the environment, a server token file, or the secure prompt instead:

```bash
export FERROTUNNEL_TOKEN=my-secret-token

# Server reads FERROTUNNEL_TOKEN or prompts when omitted
ferrotunnel server

# Server can also read from a file managed by your secret store
ferrotunnel server --token-file /run/secrets/ferrotunnel-token

# Client reads FERROTUNNEL_TOKEN or prompts when omitted
export FERROTUNNEL_SERVER=tunnel.example.com:7835
ferrotunnel client --local-addr 127.0.0.1:3000
```

For the server, if `--token`, `FERROTUNNEL_TOKEN`, and `--token-file` are unset, the CLI prompts for the token on the TTY (input is not echoed). The client prompts when `--token` and `FERROTUNNEL_TOKEN` are unset.

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
