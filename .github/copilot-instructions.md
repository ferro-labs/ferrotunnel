# Copilot instructions for FerroTunnel

## Build, test, and lint commands

Use the workspace-level Cargo commands that match CI:

```bash
# Build
cargo build --workspace
cargo build --workspace --all-features --profile ci
cargo build --workspace --features quic

# Lint / format
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
make check

# Full test suite
cargo test --workspace --all-features
make test

# Run one integration test from the dedicated integration-test crate
cargo test -p ferrotunnel-tests test_server_starts -- --exact

# Run one QUIC integration test
cargo test -p ferrotunnel-tests --features quic test_quic_connection -- --exact

# Run tests for one area or one crate
cargo test -p ferrotunnel-tests websocket
cargo test -p ferrotunnel-protocol codec
```

The integration tests do **not** live under the main `ferrotunnel` crate; they are in the separate `ferrotunnel-tests` package under `tests/`.

## High-level architecture

FerroTunnel is a Rust workspace with a library-first public API and a thin CLI on top:

- `ferrotunnel` is the main entry point for embedders. It exposes `Client::builder()` and `Server::builder()` and re-exports the common protocol/core types.
- `ferrotunnel-core` owns the control-plane connection, handshake/authentication, session store, multiplexing, transport layer, reconnect logic, rate limits, and heartbeat handling.
- `ferrotunnel-http` is the ingress/data-plane layer. Server-side `HttpIngress` accepts HTTP/1.1 and HTTP/2 traffic, runs plugins, resolves a session by normalized `Host`, then opens a virtual stream through the session multiplexer. Client-side `HttpProxy` forwards that stream to the local service and handles HTTP, WebSocket, and gRPC forwarding.
- `ferrotunnel-protocol` defines the wire format (`Frame`, `TunnelCodec`, protocol enums, validation).
- `ferrotunnel-plugin` provides the trait-based plugin system used by ingress hooks.
- `ferrotunnel-common` holds shared config and the workspace error/result types.
- `ferrotunnel-observability` contains metrics, tracing, and dashboard support.
- `ferrotunnel-cli` is a unified binary with `server` and `client` subcommands; it mostly wires CLI args into the library builders.

The core runtime path is:

```text
Internet request -> HttpIngress -> PluginRegistry -> SessionStore lookup by tunnel_id
-> AnyMultiplexer (TCP or QUIC) -> tunneled stream/frame transport
-> TunnelClient on the private side -> HttpProxy/Tcp handling -> local service
```

Transport selection is intentionally abstracted. The control plane uses `TransportConfig`, transport-agnostic frame I/O traits (`FrameSender` / `FrameReceiver`), and `AnyMultiplexer` so ingress logic can stay the same whether the tunnel is backed by TCP/TLS or QUIC.

## Key conventions

- Prefer the public `ferrotunnel` crate when changing API-facing behavior. It is the stable surface for embedders and re-exports the important pieces from the lower-level crates.
- Keep transport-specific logic behind the existing abstractions. QUIC is feature-gated and should be wired through `TransportConfig`, `AnyMultiplexer`, and the `quic` feature path instead of introducing transport checks into ingress/business logic.
- HTTP routing is strict and host-based: ingress normalizes the `Host` header and resolves sessions with `get_by_tunnel_id()`. Do not switch request routing back to “first available session” behavior.
- Library crates use `ferrotunnel_common::TunnelError` / `Result<T>`; the CLI uses `anyhow::Result` at the application boundary.
- Any executable or test that sets up TLS or QUIC must install the rustls ring provider before creating configs or connections: `let _ = rustls::crypto::ring::default_provider().install_default();`
- The workspace enforces `unsafe_code = "forbid"` and runs clippy pedantic warnings with `-D warnings` in CI, so code that compiles locally still needs to satisfy the workspace lint profile.
