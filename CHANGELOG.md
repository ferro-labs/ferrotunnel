# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.5.2] - 2026-08-17

Reliability release covering per-session rate limiting, frame delivery under
cancellation, and ingress timeout configuration.

### Added

- Expose the ingress upstream response timeout as `--http-response-timeout`
  (`FERROTUNNEL_HTTP_RESPONSE_TIMEOUT`), so deployments of the shipped binary can
  serve upstreams that legitimately take longer than the 60s default. The timeout
  bounds both the response head and stalls between streamed body frames
  ([#172](https://github.com/ferro-labs/ferrotunnel/issues/172),
  [`9202301`](https://github.com/ferro-labs/ferrotunnel/commit/92023013d7f78299b8b99b3dda1529d5e3ff2c37)).
- Configure the same timeout from the library through
  `ServerBuilder::http_response_timeout`, and from `Http3IngressConfig` through a
  matching builder method. Both ingress construction paths now read it, where they
  previously used a hardcoded 60s
  ([#172](https://github.com/ferro-labs/ferrotunnel/issues/172),
  [`9202301`](https://github.com/ferro-labs/ferrotunnel/commit/92023013d7f78299b8b99b3dda1529d5e3ff2c37)).
- Apply per-session rate limits configured through `Server::builder()`. The
  `rate_limits` setting was previously accepted and never reached the tunnel
  server ([#167](https://github.com/ferro-labs/ferrotunnel/issues/167),
  [`e2e57de`](https://github.com/ferro-labs/ferrotunnel/commit/e2e57dee55dbc0dd53e82db3bfb0487f2f3a69f4)).

### Fixed

- Throttle over-budget sessions instead of dropping their data. Frames exceeding
  the byte-rate quota are now delayed and delivered, waiting for quota in bounded
  chunks with the session heartbeat refreshed between them, so a slow session is
  never evicted as stale
  ([#167](https://github.com/ferro-labs/ferrotunnel/issues/167),
  [`e2e57de`](https://github.com/ferro-labs/ferrotunnel/commit/e2e57dee55dbc0dd53e82db3bfb0487f2f3a69f4)).
- Stop losing frames when a send or receive is cancelled by a timeout, which
  could corrupt a stream mid-transfer
  ([#168](https://github.com/ferro-labs/ferrotunnel/issues/168),
  [`d852b28`](https://github.com/ferro-labs/ferrotunnel/commit/d852b280a1fba7f56a6c7e7698cfd01930814a7b)).
- Reject zero-valued rate limits at `build()` instead of silently treating them
  as `1`, the most restrictive setting available, for a value that reads as
  disabled ([#171](https://github.com/ferro-labs/ferrotunnel/issues/171),
  [`d59f14f`](https://github.com/ferro-labs/ferrotunnel/commit/d59f14fe6d7f0d656c90bb6f5876843903c0f4fc)).
- Reject a `bytes_per_sec` above `u32::MAX` rather than clamping it to roughly
  4.29 GB/s. The limiter counts in `u32` cells, so an out-of-range rate now fails
  at startup instead of throttling sessions below the configured value. Both
  `ServerBuilder::build` and `TunnelServer::run` validate, closing the path that
  bypassed validation entirely
  ([#176](https://github.com/ferro-labs/ferrotunnel/issues/176)).
- Complete the `multiplexer_round_trip` benchmark group, which could not finish
  and left `cargo bench --bench full_stack` hanging
  ([#174](https://github.com/ferro-labs/ferrotunnel/issues/174)).

### Changed

- Name the library setting `ServerBuilder::http_response_timeout` to match the
  `--http-response-timeout` flag and reflect that it applies to the HTTP and
  HTTP/3 ingress rather than the tunnel control plane
  ([#177](https://github.com/ferro-labs/ferrotunnel/issues/177)).
- Update workspace and internal dependency versions to 1.5.2.

## [1.5.1] - 2026-07-10

Maintenance release restoring reproducible dependency and release gates and aligning public documentation with the 1.5.x runtime.

### Security

- Update `crossbeam-epoch` to 0.9.20 to resolve RUSTSEC-2026-0204.
- Update `anyhow` to 1.0.103 and `rand` to 0.9.3 to clear the remaining RustSec unsoundness notices.
- Track `Cargo.lock` and build, test, and publish with `--locked` so the committed lockfile is the audited dependency set.
- Run all-feature RustSec advisory, license, source, and ban policy checks in the required CI compliance gate.

### Changed

- Require the Rust 1.91 test job in the aggregate CI gate while retaining beta as an allowed signal.
- Build and verify publish archives for all eight published crates before release.
- Run dependency security checks before the production publish script uploads crates, and wait for each crate to appear on crates.io before publishing its dependents.
- Preserve the underlying error chain when server plugin initialization fails, instead of flattening it to a single message.
- Publish release assets as full GitHub releases rather than pre-releases. Releases are still created as drafts.
- Correct roadmap, security, plugin, soak, installer, workflow, and QUIC 0-RTT documentation.
- Update workspace and internal dependency versions to 1.5.1.

## [1.5.0] - 2026-07-05

Public-API and audit hardening release closing the v1.5.0 milestone and part of
the v1.5.x audit backlog.

### ⚠ Breaking changes

- Configuration types are now encapsulated. `ClientConfig`, `ServerConfig`, and
  `TunnelInfo` fields are private with public getters: construct them through
  `Client::builder()` / `Server::builder()` and read them back with the
  accessors (for example `config.token()`, `info.session_id()`) instead of
  field access. This prevents mutation after `build()` and keeps the
  authentication token out of `Debug` output ([#140](https://github.com/ferro-labs/ferrotunnel/issues/140)).

### Security

- Redact the authentication token in the `Debug` output of `ClientConfig` and
  `ServerConfig` ([#140](https://github.com/ferro-labs/ferrotunnel/issues/140)).
- Return a generic 401 reason from the token-auth plugin so a missing token and
  a wrong token are indistinguishable and the expected header name is not
  disclosed ([#145](https://github.com/ferro-labs/ferrotunnel/issues/145)).

### Fixed

- Make builder TLS configuration fail at `build()` time: a disabled config or
  empty/incomplete certificate, key, or CA paths are now rejected instead of
  silently leaving the transport unchanged ([#141](https://github.com/ferro-labs/ferrotunnel/issues/141)).
- Propagate the CLI `--tls-client-auth`/`--tls-ca` validation error through
  `main` instead of calling `std::process::exit`, so tracing and shutdown run
  ([#142](https://github.com/ferro-labs/ferrotunnel/issues/142)).
- Honor response-plugin `Reject`/`Respond` decisions on the HTTP/1 ingress path,
  which previously discarded them ([#145](https://github.com/ferro-labs/ferrotunnel/issues/145)).
- Strip hop-by-hop headers on the HTTP/1 forward path, matching the HTTP/3 path,
  while preserving them for WebSocket upgrades ([#145](https://github.com/ferro-labs/ferrotunnel/issues/145)).
- Propagate HTTP/3 request-body read errors instead of converting them to a
  clean EOF that silently truncates the upstream body ([#145](https://github.com/ferro-labs/ferrotunnel/issues/145)).
- Fail stream-ID allocation when the ID space is exhausted instead of wrapping
  and aliasing a live stream ([#145](https://github.com/ferro-labs/ferrotunnel/issues/145)).
- Capture the full request path and query for request replay so a replayed
  request matches the original ([#145](https://github.com/ferro-labs/ferrotunnel/issues/145)).
- Report tracing/OTLP initialization failures instead of discarding them
  ([#145](https://github.com/ferro-labs/ferrotunnel/issues/145)).
- Enforce the `HandshakeFrame` `min_version <= max_version` invariant during
  frame validation ([#145](https://github.com/ferro-labs/ferrotunnel/issues/145)).

### Changed

- Hide the `core`, `http`, and `protocol` implementation-crate re-exports from
  the public API documentation ([#140](https://github.com/ferro-labs/ferrotunnel/issues/140)).
- Unify the duplicate `rand` dependency on 0.9 and document the `cargo-deny`
  duplicate-version policy ([#122](https://github.com/ferro-labs/ferrotunnel/issues/122)).
- Align the advertised MSRV (README, CONTRIBUTING, CLI, Dockerfile) with the
  1.91 toolchain used by the manifest and CI ([#123](https://github.com/ferro-labs/ferrotunnel/issues/123)).
- Mark `Client::builder()` / `Server::builder()` `#[must_use]` and document the
  `burst_factor` bound ([#145](https://github.com/ferro-labs/ferrotunnel/issues/145)).

## [1.4.0] - 2026-07-03

Protocol and configuration hardening release closing the v1.4.0 milestone.

### Security

- Validate every wire-frame variant in the decode path so oversized or
  high-cardinality control-frame contents are rejected before use, closing an
  allocation-amplification vector reachable by an authenticated peer ([#139](https://github.com/ferro-labs/ferrotunnel/issues/139)).
- Bound the control-frame decode allocation to the frame size, so a small frame
  can no longer force a large allocation while decoding, and large allocations
  require a proportionally large frame ([#163](https://github.com/ferro-labs/ferrotunnel/issues/163)).

### Fixed

- Wire the configured frame/limit settings through to the codec so
  `max_frame_bytes` is enforced by client and server, and reject a zero or
  oversized configured limit ([#144](https://github.com/ferro-labs/ferrotunnel/issues/144)).
- Install the rustls crypto provider automatically for embedded library TLS
  users, so `Client`/`Server` builders no longer require a manual provider
  install ([#121](https://github.com/ferro-labs/ferrotunnel/issues/121)).

## [1.3.0] - 2026-06-26

Bug-fix and reliability release closing the v1.3.0 milestone.

### Fixed

- Supervise the server lifecycle task so `is_running()`, `shutdown()`, and the double-start guard work ([#120](https://github.com/ferro-labs/ferrotunnel/issues/120)).
- Bound `Client::start()` with a configurable `startup_timeout` so it no longer hangs on initial connection failure under auto-reconnect ([#117](https://github.com/ferro-labs/ferrotunnel/issues/117)).
- Make duplicate `tunnel_id` registration atomic, closing a TOCTOU race ([#118](https://github.com/ferro-labs/ferrotunnel/issues/118)).
- Spawn TCP virtual-stream handlers concurrently instead of serializing multiplexed streams ([#129](https://github.com/ferro-labs/ferrotunnel/issues/129)).
- Don't hold the HTTP/2 connection-pool mutex across connect/handshake ([#133](https://github.com/ferro-labs/ferrotunnel/issues/133)).
- Reuse the shared connection pool for gRPC streams instead of one pool per stream ([#134](https://github.com/ferro-labs/ferrotunnel/issues/134)).
- Ingress: return 413 for chunked oversized request bodies and apply a per-frame timeout on the streaming response path ([#159](https://github.com/ferro-labs/ferrotunnel/issues/159)).

## [1.2.0] - 2026-06-19

Hardening release closing the v1.2.0 audit milestone.

### Security

- Bound and time-limit HTTP/1.1+HTTP/2 ingress bodies: `max_request_body_size` cap (oversized → 413) plus a per-frame response read timeout (stalled upstream → 504) ([#132](https://github.com/ferro-labs/ferrotunnel/issues/132)).
- Enforce the previously dead per-session rate limiter: stream-open limits and byte-accurate data limits ([#119](https://github.com/ferro-labs/ferrotunnel/issues/119)).

### Fixed

- Isolate plugin hook panics so a panicking plugin yields a clean 500 instead of dropping the connection ([#138](https://github.com/ferro-labs/ferrotunnel/issues/138)).
- Make metrics initialization idempotent and `/metrics` scraping panic-free ([#137](https://github.com/ferro-labs/ferrotunnel/issues/137)).
- Survive transient `accept()` errors in HTTP/TCP ingress instead of terminating the accept loop ([#131](https://github.com/ferro-labs/ferrotunnel/issues/131)).
- Back off on accept-loop errors (core TLS/QUIC and ingress) to avoid busy-spinning a core and flooding logs ([#130](https://github.com/ferro-labs/ferrotunnel/issues/130)).
- Add send timeouts to the frame channel so teardown cannot hang under network partition ([#136](https://github.com/ferro-labs/ferrotunnel/issues/136)).
- Track and abort the session-cleanup task on shutdown; guarantee one cleanup loop per server ([#135](https://github.com/ferro-labs/ferrotunnel/issues/135)).

## [1.1.0] - 2026-06-11

### Security

- Fixed token authentication timing side-channel by replacing `HashSet<String>::contains` with constant-time token comparison in `TokenAuthPlugin` ([#124](https://github.com/ferro-labs/ferrotunnel/issues/124)).
- Hardened the observability dashboard: it now binds to loopback by default, requires explicit opt-in plus authentication for non-loopback binds, removes wildcard CORS, and enforces dashboard API authentication with constant-time token comparison ([#125](https://github.com/ferro-labs/ferrotunnel/issues/125)).
- Fixed stored XSS risks in the dashboard by rendering captured request paths, headers, tunnel metadata, and public URLs through safe text/escaping paths, and by only using `http` or `https` public URLs as links ([#126](https://github.com/ferro-labs/ferrotunnel/issues/126)).
- Added client and server handshake read timeouts for TCP, TLS, and QUIC paths so silent peers cannot hold session permits indefinitely ([#127](https://github.com/ferro-labs/ferrotunnel/issues/127)).
- Stopped requiring server tokens on the command line; the server now supports `FERROTUNNEL_TOKEN`, `--token-file`, or secure prompt fallback, and hides token environment values in help output ([#128](https://github.com/ferro-labs/ferrotunnel/issues/128)).
- Made `ferrotunnel client --tls` secure by default: verified TLS now requires `--tls-ca` unless `--tls-skip-verify` is explicitly set ([#116](https://github.com/ferro-labs/ferrotunnel/issues/116)).
- Added warnings and symmetric config handling for TLS/QUIC paths that disable certificate verification ([#143](https://github.com/ferro-labs/ferrotunnel/issues/143)).

### Added

- Added dashboard CLI controls for `--dashboard-bind`, `--dashboard-allow-non-loopback`, and `--dashboard-auth-token`, with a generated dashboard token URL when no token is supplied.
- Added regression coverage for dashboard bind validation, dashboard API authentication, token-file loading, constant-time plugin auth, TLS argument validation, and handshake timeout behavior.

### Changed

- Updated CLI, deployment, observability, and release-planning docs for the safer token, dashboard, and TLS defaults.

## [1.0.8] - 2026-04-26

### Added

#### HTTP/3 Ingress
- **HTTP/3 ingress**: Optional browser-facing HTTP/3 listener over QUIC/TLS 1.3 using `h3` and `h3-quinn`, gated behind the `http3` feature flag
- **`Http3Ingress` / `Http3IngressConfig`**: New server-side HTTP/3 ingress in `ferrotunnel-http` that accepts public HTTP/3 requests, normalizes `Host`/authority, and routes through the existing tunnel session store
- **Strict host-based routing for HTTP/3**: HTTP/3 ingress uses the same normalized `Host` lookup behavior as HTTP/1.1 and HTTP/2 ingress; unknown hosts return `404 Tunnel not found`
- **Alt-Svc advertisement**: TCP HTTP ingress can advertise the configured HTTP/3 UDP endpoint with `Alt-Svc` when HTTP/3 is enabled
- **Public server API**: Added feature-gated `.http3(bind_addr, cert_path, key_path)` support on `ServerBuilder`
- **CLI flags** (gated behind `--features http3`):
  - Server: `--http3-bind`, `--http3-cert`, `--http3-key`
- **Deployment and README docs**: Added HTTP/3 usage, feature, CLI, firewall/UDP, TLS certificate, and architecture notes across the root, crate, CLI, HTTP, and deployment documentation
- **Integration tests**: Added end-to-end HTTP/3 tunnel coverage plus strict unknown-host routing coverage in `ferrotunnel-tests`

### Security
- **aws-lc-sys** 0.37.0 → 0.40.0 — fixes RUSTSEC-2026-0044/0045/0046/0047/0048 (PKCS7 validation bypass, CRL scope logic error, AES-CCM timing side-channel, X.509 name constraints bypass)
- **rustls-webpki** 0.103.9 → 0.103.13 — fixes RUSTSEC-2026-0098/0099/0104 (URI name constraint bypass, IP name constraint bypass, CRL parsing panic)
- **time** 0.3.46 → 0.3.47 — fixes RUSTSEC-2026-0009 (stack exhaustion DoS)
- **quinn-proto** 0.11.13 → 0.11.14

#### Dependencies
- **h3** 0.0.8 and **h3-quinn** 0.0.10 (optional, `http3` feature): HTTP/3 protocol support on top of the existing Quinn/rustls stack

## [1.0.7] - 2026-04-09

### Added

#### QUIC Transport
- **QUIC tunnel transport**: Optional QUIC-based control plane using [quinn](https://crates.io/crates/quinn) 0.11 (behind the `quic` feature flag). Provides built-in TLS 1.3 encryption, native stream multiplexing (no head-of-line blocking), and lower connection latency over UDP
- **`QuicConfig`**: New configuration struct in `ferrotunnel-common` for QUIC transport settings (cert paths, 0-RTT, idle timeout, keep-alive)
- **`QuicTransportConfig`**: Core transport config in `ferrotunnel-core` that mirrors `TlsTransportConfig` with QUIC-specific fields, reusing existing rustls + ring crypto stack (zero new crypto dependencies)
- **`QuicMultiplexer` / `QuicVirtualStream`**: QUIC-native stream multiplexer that maps each tunnel stream 1:1 to a QUIC bidirectional stream, eliminating head-of-line blocking. `QuicVirtualStream` implements `AsyncRead + AsyncWrite`
- **`AnyMultiplexer`**: Unified enum (`Tcp`/`Quic`) so the HTTP ingress can open streams without knowing the underlying transport
- **`TunnelServer::run_quic()`**: New method to run the QUIC server on a UDP endpoint alongside (or instead of) the TCP server
- **`TunnelClient::connect_and_run_quic()`**: New method for QUIC-based client connections with dedicated control stream for handshake/heartbeat
- **Builder API**: Added `.quic(&QuicConfig)` method on both `ServerBuilder` and `ClientBuilder`
- **CLI flags** (gated behind `--features quic`):
  - Server: `--quic-bind`, `--quic-cert`, `--quic-key`
  - Client: `--quic`, `--quic-0rtt` (`--quic-0rtt` currently falls back to a full handshake)
- **`Protocol::QUIC`**: New variant in the protocol enum
- **Integration test**: `test_quic_connection` verifying end-to-end QUIC handshake

#### Dependencies
- **quinn** 0.11 (optional, `quic` feature): QUIC implementation using rustls 0.23 + ring (already existing dependencies)

## [1.0.6] - 2026-03-13

### Fixed

#### TLS
- **CLI TLS panic on first connection** ([#98](https://github.com/ferro-labs/ferrotunnel/issues/98)): The `ferrotunnel` binary panicked with `Could not automatically determine the process-level CryptoProvider` when TLS was enabled. rustls 0.23 requires `rustls::crypto::ring::default_provider().install_default()` to be called before any `ClientConfig`/`ServerConfig` is built. Added this call at the very start of `main()` in `ferrotunnel-cli` and added `rustls` as a direct dependency.

### Added

#### gRPC Support
- **gRPC tunnel support**: Transparent gRPC tunneling over HTTP/2 — no tonic or protobuf knowledge required at the tunnel layer. FerroTunnel acts as a pure HTTP/2 proxy, preserving gRPC trailers (`grpc-status`, `grpc-message`) end-to-end
- **Automatic gRPC detection**: Server-side ingress detects `Content-Type: application/grpc*` and tags the stream as `Protocol::GRPC` (the enum variant was already reserved; this release adds the full implementation)
- **HTTP/2 forwarding path in ingress**: When a gRPC stream is detected, the ingress uses `hyper::client::conn::http2::handshake` over the `VirtualStream` instead of the HTTP/1.1 path, ensuring HTTP/2 framing and trailer semantics are preserved through the tunnel
- **`LocalProxyService` h2 mode**: Added `use_h2` field and `with_pool_h2()` constructor; the service now uses `ConnectionPool::acquire_h2()` for gRPC streams, routing requests over a shared HTTP/2 connection to the local gRPC server
- **`HttpProxy::handle_grpc_stream()`**: New method on `HttpProxy<L>` that serves a `VirtualStream` as HTTP/2 using `hyper::server::conn::http2::Builder`, with a dedicated HTTP/2 connection pool (always acquiring via `acquire_h2()`) for local forwarding
- **Automatic CLI dispatch**: The CLI client dispatches `Protocol::GRPC` streams to `handle_grpc_stream()` automatically — no new flags required
- **gRPC example**: New `examples/basic/grpc_tunnel.rs` demonstrating how to tunnel any local gRPC server
- **Integration tests**: `test_grpc_tunnel` (end-to-end raw HTTP/2+gRPC through the full tunnel stack) and `test_non_grpc_not_classified_as_grpc` (regression guard for the HTTP path)
## [1.0.4] - 2026-03-09

### Changed

#### Project Ownership
- **Repository transfer**: Project ownership transferred from MitulShah1 to the [ferro-labs](https://github.com/ferro-labs) organization. Repository is now at <https://github.com/ferro-labs/ferrotunnel>
- **Updated all references**: GitHub URLs, GHCR image paths (`ghcr.io/ferro-labs/ferrotunnel`), and Homebrew tap (`brew tap ferro-labs/ferrotunnel`) updated throughout the codebase

#### Code Quality
- **Refactor `ClientFeatureArgs` struct**: Replaced excessive boolean fields with nested configuration structs (`DashboardConfig`, `TlsConfig`, `TelemetryConfig`) using `#[command(flatten)]`, removing the `#[allow(clippy::struct_excessive_bools)]` suppression ([#76](https://github.com/ferro-labs/ferrotunnel/issues/76))
- **Remove `unnecessary_literal_bound` allow directives**: Cleaned up redundant `#[allow(clippy::unnecessary_literal_bound)]` suppressions across plugin modules (`auth.rs`, `rate_limit.rs`, `logger.rs`, `circuit_breaker.rs`) ([#77](https://github.com/ferro-labs/ferrotunnel/issues/77))

### Fixed

#### Safety
- **Safe integer truncation in timestamp conversions**: Replaced `as_millis() as u64` casts in tunnel client and server with `.min(u64::MAX as u128) as u64`, removing `#[allow(clippy::cast_possible_truncation)]` suppressions and making truncation behaviour explicit ([#78](https://github.com/ferro-labs/ferrotunnel/issues/78))

## [1.0.3] - 2026-02-16

### Added

#### HTTP/2 Support
- **HTTP/2 ingress**: Server-side ingress now supports both HTTP/1.1 and HTTP/2 via automatic protocol detection using `hyper-util`'s `AutoBuilder`
- **HTTP/2 protocol variant**: Added `HTTP2` variant to the `Protocol` enum for future protocol-specific handling
- **Connection-close error filtering**: Added helper function to reduce log noise from benign connection close errors

#### Connection Pooling
- **Connection pool module**: New `pool` module (`ferrotunnel-http/src/pool.rs`) for efficient connection reuse
- **HTTP/1.1 pooling**: Idle HTTP/1.1 connections are stored in a LIFO queue (VecDeque) for cache warmth, with configurable limits (default: 32 per host, 90s timeout)
- **HTTP/2 multiplexing**: Single shared HTTP/2 connection per target with automatic clone-cheap multiplexing
- **Background eviction**: Automatic cleanup of expired idle connections every 30 seconds
- **Pool configuration**: `PoolConfig` struct with `max_idle_per_host`, `idle_timeout`, and `prefer_h2` options
- **`HttpProxy::with_pool_config()`**: New constructor for custom pool configuration

### Changed

#### Performance
- **Client proxy connection reuse**: `LocalProxyService` now acquires connections from the pool instead of creating new TCP connections per request, significantly reducing connection overhead
- **Connection lifecycle management**: Connections are returned to the pool after successful requests, but not for upgraded (WebSocket) connections or failed requests

#### Dependencies
- **hyper**: Added `http2` feature flag
- **hyper-util**: Added `server-auto` and `tokio` features for HTTP/2 auto-detection
- **thiserror**: Added for connection pool error types

### Fixed
- **Test compatibility**: Connection pool constructor now checks for tokio runtime availability before spawning background tasks, preventing test failures

## [1.0.2] - 2026-02-11

### Added

#### WebSocket Tunneling
- **Full WebSocket tunnel support**: Transparent WebSocket upgrade handling through the tunnel — real-time applications (chat, dashboards, gaming) now work out of the box
- **Automatic upgrade detection**: HTTP ingress detects `Connection: Upgrade` + `Upgrade: websocket` headers and opens streams with `Protocol::WebSocket`
- **Bidirectional bridging**: After the 101 handshake, upgraded connections are bridged with zero-copy `copy_bidirectional` for minimal overhead
- **End-to-end integration tests**: Two new WebSocket integration tests (`test_websocket_upgrade_through_tunnel`, `test_websocket_raw_upgrade_101`)

#### Graceful Shutdown
- **CLI signal handling**: Both `ferrotunnel server` and `ferrotunnel client` now handle Ctrl-C / SIGTERM gracefully, logging shutdown and cleaning up resources before exit
- **Server shutdown**: Server `tokio::select!` races all services against `ctrl_c()` for clean process termination
- **Client shutdown**: Client reconnection loop exits cleanly on signal, calling `shutdown_tracing()` before exit

### Changed

#### HTTP Proxy
- **Upgrade support**: HTTP/1 connections in both ingress and proxy now use `.with_upgrades()` for hyper upgrade protocol compatibility

## [1.0.1] - 2026-02-07

### Added

#### Installation
- **Homebrew Formula**: Introduce `brew install ferrotunnel` command for macOS users via [ferro-labs/homebrew-ferrotunnel](https://github.com/ferro-labs/homebrew-ferrotunnel) tap

#### Tunnel Routing
- **`--tunnel-id` CLI flag**: New `--tunnel-id` option for `ferrotunnel client` to set the tunnel ID used for HTTP Host-header routing (`FERROTUNNEL_TUNNEL_ID` env var supported)
- **`.tunnel_id()` builder method**: New method on `Client::builder()` for setting the tunnel ID when using the library API

### Fixed

#### Tunnel Routing
- **HTTP ingress routing**: Fixed "Tunnel not found" error when accessing tunnels via direct IP. The client now registers a `tunnel_id` that matches the Host header used by incoming HTTP requests

#### Docker Verification
- **Metrics Endpoint**: Fixed issue where the metrics server was not enabled by default in the Docker environment, causing verification scripts to report missing data.

### Improved

#### Docker Optimization
- **Optimized Docker image size**: Reduced from 34.8 MB to **13.4 MB** (61.6% smaller)
- **Faster build times**: Build time reduced from 6.5 minutes to **2.5 minutes** (62% faster)
- **Minimal base image**: Switched to Google's `distroless/cc-debian12` for minimal attack surface
- **Aggressive compiler optimizations**: Size-focused compile flags (`-C opt-level=z`, single codegen unit, panic=abort)
- **Enhanced caching**: cargo-chef for faster incremental builds
- **Binary stripping**: Comprehensive symbol removal for smaller binaries

#### Documentation
- Enhanced README with security comparisons and CVE analysis
- Updated ROADMAP to prioritize user adoption (WebSocket, HTTP/2, gRPC)
- Improved architecture diagrams

## [1.0.0] - 2026-02-05

### Highlights

FerroTunnel v1.0.0 is the first stable release.

### Features

#### Core Tunnel System
- **Protocol**: Custom binary protocol with length-prefixed frames, heartbeats, and multiplexing
- **Multiplexer**: Multiple concurrent virtual streams over a single TCP connection
- **Transport**: TCP and TLS 1.3 support with mutual TLS (mTLS) authentication
- **Reconnection**: Automatic reconnection with exponential backoff

#### HTTP & TCP Ingress
- **HTTP Ingress**: Hyper-based HTTP server for receiving public requests
- **TCP Ingress**: Raw TCP forwarding support
- **HTTP Proxy**: Client-side proxy to local services

#### Plugin System
- **Plugin Trait**: Async trait with `on_request` and `on_response` hooks
- **Plugin Registry**: Chain multiple plugins with control flow actions
- **Built-in Plugins**:
  - `LoggerPlugin` - Structured request logging
  - `TokenAuthPlugin` - Header-based token authentication
  - `RateLimitPlugin` - IP-based rate limiting
  - `CircuitBreakerPlugin` - Failure isolation

#### Observability
- **Prometheus Metrics**: Counters, gauges, and histograms
- **OpenTelemetry**: Distributed tracing with OTLP exporter support
- **Real-Time Dashboard**: Web UI at `http://localhost:4040` with:
  - Live traffic charts
  - Request/response inspector
  - Request replay functionality
  - SSE-based real-time updates

#### Unified CLI
- Single `ferrotunnel` binary with subcommands:
  - `ferrotunnel server` - Run the tunnel server
  - `ferrotunnel client` - Run the tunnel client
  - `ferrotunnel version` - Show version information
- Full TLS support via CLI flags and environment variables
- Optional observability (disabled by default for lower latency)

#### Library API
- **Embeddable**: Use as a library in your Rust applications
- **Builder Pattern**: `Client::builder()` and `Server::builder()` APIs
- **Lifecycle Management**: `start()`, `shutdown()`, `stop()` methods

#### Performance
- Zero-copy frame decoding with `Bytes`
- Batched I/O to reduce syscall overhead
- Lock-free concurrency with `DashMap`
- `mimalloc` allocator for improved performance
- TCP_NODELAY and optimized buffer sizes

#### Security
- TLS 1.3 with rustls
- Mutual TLS (mTLS) client authentication
- Token-based authentication
- Rate limiting and circuit breakers
- Protocol fuzzing test suite

#### Developer Tools
- `tools/loadgen` - Load generator for benchmarking
- `tools/soak` - Long-duration stability testing
- `tools/profiler` - CPU and memory profiling scripts

### Crates

| Crate | Description |
|-------|-------------|
| `ferrotunnel` | Main library with builder APIs |
| `ferrotunnel-cli` | Unified CLI binary |
| `ferrotunnel-core` | Core tunnel logic and transport |
| `ferrotunnel-protocol` | Wire protocol and codec |
| `ferrotunnel-http` | HTTP/TCP ingress and proxy |
| `ferrotunnel-plugin` | Plugin system and built-ins |
| `ferrotunnel-observability` | Metrics, tracing, and dashboard |
| `ferrotunnel-common` | Shared types and errors |

[Unreleased]: https://github.com/ferro-labs/ferrotunnel/compare/v1.5.2...HEAD
[1.5.2]: https://github.com/ferro-labs/ferrotunnel/compare/v1.5.1...v1.5.2
[1.5.1]: https://github.com/ferro-labs/ferrotunnel/compare/v1.5.0...v1.5.1
[1.5.0]: https://github.com/ferro-labs/ferrotunnel/compare/v1.4.0...v1.5.0
[1.4.0]: https://github.com/ferro-labs/ferrotunnel/compare/v1.3.0...v1.4.0
[1.3.0]: https://github.com/ferro-labs/ferrotunnel/compare/v1.2.0...v1.3.0
[1.2.0]: https://github.com/ferro-labs/ferrotunnel/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/ferro-labs/ferrotunnel/compare/v1.0.8...v1.1.0
[1.0.8]: https://github.com/ferro-labs/ferrotunnel/compare/v1.0.7...v1.0.8
[1.0.7]: https://github.com/ferro-labs/ferrotunnel/compare/v1.0.6...v1.0.7
[1.0.6]: https://github.com/ferro-labs/ferrotunnel/compare/v1.0.4...v1.0.6
[1.0.4]: https://github.com/ferro-labs/ferrotunnel/compare/v1.0.3...v1.0.4
[1.0.3]: https://github.com/ferro-labs/ferrotunnel/compare/v1.0.2...v1.0.3
[1.0.2]: https://github.com/ferro-labs/ferrotunnel/releases/tag/v1.0.2
[1.0.1]: https://github.com/ferro-labs/ferrotunnel/releases/tag/v1.0.1
[1.0.0]: https://github.com/ferro-labs/ferrotunnel/releases/tag/v1.0.0
