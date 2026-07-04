# AGENTS.md

This file provides guidance to AI agents (Claude Code, Copilot, Cursor, etc.) when working with code in this repository.

## Build & Test Commands

```bash
# Build
cargo build --workspace                    # Debug build
cargo build --workspace --features quic    # With QUIC transport

# Test
cargo test --workspace --all-features      # All tests (or: make test)
cargo test -p ferrotunnel-protocol codec   # Single crate/test
cargo test -p ferrotunnel-tests --features quic -- quic_test  # QUIC integration test

# Lint & Format
cargo clippy --workspace --all-targets --all-features -- -D warnings  # (or: make lint)
cargo fmt --all                            # (or: make fmt)
make check                                 # fmt check + clippy

# Benchmark
cargo bench --workspace                    # All benchmarks
cargo bench --bench tcp_throughput         # Specific benchmark
./scripts/benchmark.sh save               # Save baseline
./scripts/benchmark.sh main full_stack    # Compare against baseline

# Fuzzing (requires nightly)
cd ferrotunnel-protocol && cargo +nightly fuzz run codec_decode -- -max_total_time=300

# Security audit
make audit                                 # cargo-audit + cargo-deny
```

## Architecture

Rust workspace (tokio-style) with 8 published crates + 3 internal tools:

- **ferrotunnel** — Public API with `Client::builder()` / `Server::builder()` pattern
- **ferrotunnel-core** — Tunnel logic: `TunnelClient`, `TunnelServer`, `Multiplexer`, `QuicMultiplexer`, transport layer (TCP/TLS/QUIC), session management, auth, rate limiting
- **ferrotunnel-protocol** — Wire protocol: 12 frame types, length-prefixed codec (`TunnelCodec`), bincode serialization. `Protocol` enum: HTTP, HTTPS, HTTP2, WebSocket, GRPC, TCP, QUIC
- **ferrotunnel-http** — HTTP/TCP ingress (`HttpIngress`, `TcpIngress`), `HttpProxy` with connection pooling (H1 + H2), gRPC detection
- **ferrotunnel-plugin** — `Plugin` trait + `PluginRegistry`, built-in: logger, token auth, rate limit
- **ferrotunnel-observability** — Prometheus metrics, OpenTelemetry tracing, web dashboard
- **ferrotunnel-common** — `TunnelError`, `Result<T>`, config types (`TlsConfig`, `QuicConfig`, `LimitsConfig`)
- **ferrotunnel-cli** — Unified binary with `server` and `client` subcommands (clap derive)

### Data Flow

```
Internet → HttpIngress(:8080) → SessionStore → AnyMultiplexer → [TCP: Multiplexer/VirtualStream | QUIC: QuicMultiplexer/QuicVirtualStream] → BatchedSender → Transport(TCP/TLS/QUIC) → TunnelClient → LocalService
```

### Key Abstractions

- **`TransportConfig`** enum (`Tcp | Tls | Quic`) — transport selection; QUIC behind `#[cfg(feature = "quic")]`
- **`FrameSender` / `FrameReceiver`** traits — transport-agnostic frame I/O (TCP reuses `TcpFrameSender`; QUIC uses control stream directly)
- **`AnyMultiplexer`** enum (`Tcp(Multiplexer) | Quic(QuicMultiplexer)`) — stored in `Session`, used by ingress to open streams
- **`BoxedStream`** (`Pin<Box<dyn AsyncStream>>`) — returned by `AnyMultiplexer::open_stream()` for transport-agnostic stream handling

### Feature Flags

- `quic` — QUIC transport via quinn 0.11 (propagated: core → http → ferrotunnel → cli → tests)
- `metrics` — Prometheus/OpenTelemetry metrics in ferrotunnel-core
- `dashboard` — Web dashboard in ferrotunnel-observability

## Code Style

- **`unsafe_code = "forbid"`** at workspace level — no unsafe code
- **Clippy pedantic** enabled; `unwrap_used` and `expect_used` warn (allowed in tests)
- `dbg!`, `todo!`, `unimplemented!` are warnings
- Edition 2021, MSRV 1.91, max line width 100, 4-space indent
- Use `thiserror` for library errors, `anyhow` for CLI/application errors
- Prefer `Bytes` for zero-copy buffers, `kanal` for async channels, `DashMap` for concurrent maps
- TLS uses `rustls` 0.23 + ring crypto provider (must call `install_default()` before any TLS config)

## Public-Facing Wording

Keep all public-facing text — commit messages, rustdoc (`///` doc comments),
`CHANGELOG.md`, `ROADMAP.md`, and GitHub issues/PRs — **neutral and
outcome-focused**. Do **not** reference internal tooling, code-review services,
AI assistants, private decisions, or how the change was produced; describe
*what* changed and *why* it matters to users. Commit messages stay short and
imperative; rustdoc stays brief with no meta-commentary or disclaimers.

## Publishing

Crates must be published in dependency order:
```
ferrotunnel-common → ferrotunnel-protocol → ferrotunnel-plugin → ferrotunnel-core → ferrotunnel-observability → ferrotunnel-http → ferrotunnel → ferrotunnel-cli
```
Use `make publish-dry-run` to validate, `make publish` (runs `scripts/publish.sh`) for production.
