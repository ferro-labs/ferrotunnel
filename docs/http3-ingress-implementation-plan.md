# HTTP/3 Ingress Implementation Plan

## Goal

Implement v1.0.8 HTTP/3 ingress so FerroTunnel can accept browser/client HTTP/3
traffic over QUIC, route it by normalized `Host`, and forward it through the
existing tunnel multiplexer to connected clients.

This must stay separate from the existing QUIC tunnel transport. The current
QUIC tunnel path uses custom tunnel framing; HTTP/3 ingress must use HTTP/3
semantics through `h3` and `h3-quinn`.

## Sub-agent brief

Ask the implementation sub-agent to make the change end-to-end, not just sketch
it. The sub-agent should preserve current HTTP/1.1, HTTP/2, WebSocket, gRPC,
TCP ingress, and QUIC tunnel behavior while adding HTTP/3 behind a feature flag.

Recommended prompt:

```text
Implement HTTP/3 ingress for FerroTunnel according to docs/http3-ingress-implementation-plan.md.
Preserve strict Host-based tunnel routing, keep browser-facing HTTP/3 separate from the existing QUIC tunnel transport, add feature-gated API/CLI wiring, add Alt-Svc injection, and add integration tests. Validate with fmt, clippy, and relevant cargo tests.
```

## Implementation phases

### 1. Dependencies and feature flags

- Add workspace dependencies for `h3` and `h3-quinn`.
- Add an `http3` feature to `ferrotunnel-http`.
- Propagate `http3` through `ferrotunnel`, `ferrotunnel-cli`, and
  `ferrotunnel-tests`.
- Keep QUIC tunnel support feature-gated as it is today; do not make HTTP/3
  imply any behavior change unless the new HTTP/3 bind/config is enabled.

Primary files:

- `Cargo.toml`
- `ferrotunnel-http/Cargo.toml`
- `ferrotunnel/Cargo.toml`
- `ferrotunnel-cli/Cargo.toml`
- `tests/Cargo.toml`

### 2. Shared ingress routing helpers

- Extract or expose reusable request-routing logic from
  `ferrotunnel-http/src/ingress.rs` where appropriate:
  - Host header parsing and normalization.
  - Plugin request hook execution.
  - Strict `sessions.get_by_tunnel_id()` lookup.
  - Backend stream opening through `AnyMultiplexer`.
- Do not reintroduce "first available tunnel" fallback routing.
- Keep response plugin behavior equivalent where HTTP/3 response body handling
  makes it practical.

Primary files:

- `ferrotunnel-http/src/ingress.rs`
- New helper module if needed, for example `ferrotunnel-http/src/routing.rs`

### 3. HTTP/3 ingress module

- Add a new feature-gated `Http3Ingress` rather than mixing UDP/QUIC handling
  into the existing TCP `HttpIngress`.
- Use `h3` + `h3-quinn` to accept HTTP/3 requests on a UDP socket.
- Reuse rustls/quinn certificate handling patterns where possible, but do not
  reuse `QuicMultiplexer` because it is tunnel-protocol-specific.
- For each HTTP/3 request:
  - Accept/decode request headers and body.
  - Normalize authority/host and route to the matching tunnel session.
  - Open an upstream tunnel stream with the right `Protocol`.
  - Forward to the existing client-side local service path.
  - Return status, headers, body, and trailers where supported.
- Add explicit logging for bind/startup, accept failures, routing failures, and
  upstream forwarding errors.

Primary files:

- New `ferrotunnel-http/src/http3_ingress.rs`
- `ferrotunnel-http/src/lib.rs`

### 4. Alt-Svc support

- Add optional `Alt-Svc` header injection to HTTP/1.1 and HTTP/2 ingress
  responses when HTTP/3 is configured.
- Make the advertised authority/port configurable enough for local tests and
  production deployment.
- Do not inject `Alt-Svc` on error paths where doing so would be misleading
  unless the configured HTTP/3 listener is actually enabled.

Primary files:

- `ferrotunnel-http/src/ingress.rs`
- Any new HTTP/3 config type in `ferrotunnel-http`

### 5. Public API and CLI wiring

- Extend the embeddable server config/builder to support optional HTTP/3 ingress
  configuration.
- Add CLI flags/env vars for HTTP/3 ingress, likely:
  - `--http3-bind` / `FERROTUNNEL_HTTP3_BIND`
  - HTTP/3 cert/key options, or documented reuse of existing TLS/QUIC cert/key
    options.
- Start HTTP/3 ingress alongside the existing control plane and HTTP ingress
  only when configured and compiled with the `http3` feature.
- Ensure rustls ring provider installation still happens before any TLS/QUIC
  config creation in executables and tests.

Primary files:

- `ferrotunnel/src/config.rs`
- `ferrotunnel/src/server.rs`
- `ferrotunnel-cli/src/commands/server.rs`
- `ferrotunnel-cli/src/main.rs`

### 6. Tests

- Add integration tests in the `ferrotunnel-tests` crate, gated behind
  `http3`.
- Cover:
  - Basic HTTP/3 request/response through a tunnel.
  - Strict Host-based routing and unknown-host rejection.
  - Alt-Svc header injection from HTTP/1.1 or HTTP/2 ingress.
  - Coexistence with existing HTTP ingress and QUIC tunnel transport.
  - Certificate/provider setup for HTTP/3 tests.
- Keep existing QUIC transport tests separate; HTTP/3 tests should exercise
  browser-facing ingress behavior, not the tunnel QUIC framing path.

Primary files:

- New `tests/integration/http3_test.rs`
- `tests/integration/mod.rs`
- `tests/Cargo.toml`

### 7. Documentation and release notes

- Update user-facing docs once implementation behavior is finalized:
  - CLI flags/env vars.
  - TLS/certificate requirements.
  - HTTP/3 deployment notes for UDP firewall/load balancer configuration.
  - Alt-Svc behavior.
- Update `ROADMAP.md` and `CHANGELOG.md` when v1.0.8 is ready.

Primary files:

- `docs/deployment.md`
- `ROADMAP.md`
- `CHANGELOG.md`

## Validation checklist

Run the existing workspace checks that match CI:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo test -p ferrotunnel-tests --features http3
```

If HTTP/3 is implemented as part of `quic`/`http3` feature combinations, also
run:

```bash
cargo test -p ferrotunnel-tests --features "quic http3"
cargo build --workspace --features "quic http3"
```

## Non-goals

- Do not replace the existing HTTP/1.1 or HTTP/2 ingress.
- Do not merge browser-facing HTTP/3 ingress with the private QUIC tunnel
  transport.
- Do not change strict Host-based routing.
- Do not make HTTP/3 required for users who only need TCP/TLS tunnel transport.
