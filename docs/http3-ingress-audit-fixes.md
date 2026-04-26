# HTTP/3 Ingress — Audit Remediation Plan (v1.0.8)

Branch: `feature/v1.0.8-http3-ingress`
Target file (primary): `ferrotunnel-http/src/http3_ingress.rs`
Other touched files: `ferrotunnel-http/src/ingress.rs`, `ferrotunnel/src/server.rs`,
`ferrotunnel-http/Cargo.toml` (only if a builder type is added).

This document is the single source of truth for the audit fixes that must land
before merging the HTTP/3 ingress. A subagent will execute every section in
order. The Iron Law: **every fix must compile (`cargo build --workspace
--all-features`) and pass clippy (`cargo clippy --workspace --all-targets
--all-features -- -D warnings`) before moving on**.

---

## Verification gate (run after EACH section)

```bash
cargo build  -p ferrotunnel-http --features http3
cargo clippy -p ferrotunnel-http --features http3 --all-targets -- -D warnings
```

After the final section also run:

```bash
cargo build  --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test   -p ferrotunnel-http --features http3
cargo fmt --all
```

If any step fails, STOP and fix before continuing. Do not suppress warnings
with `#[allow(...)]` to make them pass — the project workspace forbids that
pattern (see AGENTS.md).

---

## 🔴 Section 1 — Stream the request body (do not buffer)

**Problem**: `collect_h3_request_body` reads the entire request body into a
`Vec<u8>` (up to 100 MB) before opening the upstream tunnel stream. This
defeats HTTP/3's streaming benefits and breaks gRPC client-streaming and
chunked uploads.

**Fix**:

1. Delete `collect_h3_request_body`.
2. Introduce a `Http3RequestBody` type that implements `hyper::body::Body<Data
   = Bytes, Error = std::io::Error>`. Internally it holds the
   `RequestStream<S::RecvStream, Bytes>` half (use `h3_stream.split()` to
   separate send + recv halves) and pulls frames via `recv_data().await` in
   `poll_frame`.
3. Enforce `max_request_body_size` *inside* `poll_frame` by tracking bytes
   read so far; return `io::Error::new(InvalidData, "body too large")` when
   exceeded.
4. Pre-validate `chunk.remaining()` against the remaining budget *before*
   calling `copy_to_bytes` (prevents adversarial-length OOM via Buf reservation).
5. `build_forward_request` must take this streaming body instead of `Bytes` and
   produce a `BoxBody<Bytes, hyper::Error>`. Convert the `io::Error` to
   `hyper::Error` via `BodyExt::map_err`.
6. Update both the gRPC (HTTP/2) and HTTP/1 forwarding branches to use the new
   streaming body.

Acceptance: a 10 MB POST sent in 8 KB chunks must be forwarded with no buffer
larger than ~64 KB resident at any time, and the existing integration tests
must still pass.

---

## 🔴 Section 2 — Separate `idle_timeout` from `response_timeout`

**Problem**: `transport.max_idle_timeout(Some(self.response_timeout.try_into()?))`
sets the QUIC connection idle timeout to the per-response timeout (default 60 s),
killing long-lived H3 connections between requests.

**Fix**:

1. Add to `Http3IngressConfig`:
   ```rust
   pub idle_timeout: Duration,        // default Duration::from_secs(30)
   pub keep_alive_interval: Duration, // default Duration::from_secs(10)
   ```
2. In `create_http3_endpoint`:
   ```rust
   transport.max_idle_timeout(Some(config.idle_timeout.try_into()
       .map_err(|e| TunnelError::Config(format!("idle_timeout: {e}")))?));
   transport.keep_alive_interval(Some(config.keep_alive_interval));
   ```
3. Update `Default` impl accordingly.
4. Document in the rustdoc that `response_timeout` is request-scoped only.

---

## 🔴 Section 3 — Install rustls crypto provider defensively

**Problem**: `QuicServerConfig::try_from(rustls_config)` panics at runtime if
no rustls crypto provider has been installed. The CLI installs it but library
embedders calling `Server::start` directly do not.

**Fix**:

1. Inside `create_http3_endpoint` (top of function), call:
   ```rust
   use std::sync::Once;
   static INSTALL: Once = Once::new();
   INSTALL.call_once(|| {
       let _ = rustls::crypto::ring::default_provider().install_default();
   });
   ```
   The `let _ =` is intentional — installation is a no-op if already done.
2. Add a one-line comment explaining why: "rustls 0.23 requires an explicit
   crypto provider; ferrotunnel standardizes on ring (see AGENTS.md)."

---

## 🔴 Section 4 — Use `https://` scheme for tunneled gRPC URI

**Problem**: `build_forward_request` constructs `format!("http://{host}{path_and_query}")`
for gRPC requests. Downstream observers expect the scheme to reflect the
client-facing protocol.

**Fix**:

1. Change the gRPC URI construction to:
   ```rust
   parts.uri = format!("https://{host}{path_and_query}").parse::<Uri>()
       .map_err(|e| format!("Invalid gRPC URI: {e}"))?;
   ```
2. Add a brief comment: "scheme reflects the client-facing protocol; the
   actual tunnel transport is plaintext over the multiplexed stream".
3. Verify the H1 ingress (`ferrotunnel-http/src/ingress.rs`) does the same;
   if it uses `http://`, leave that alone — but note it in the commit message.

---

## 🔴 Section 5 — Per-frame timeout on response body streaming

**Problem**: `response_timeout` only covers waiting for the upstream response
headers. The body streaming loop in `send_streaming_response` has no deadline
— a slow upstream pins a QUIC stream + permit indefinitely.

**Fix**:

1. Pass `config.response_timeout` into `send_streaming_response` (and
   `send_buffered_response` already takes the full `config`).
2. Wrap each `body.frame().await` in `tokio::time::timeout(config.response_timeout, ...)`.
   On timeout: log `error!`, attempt `h3_stream.finish().await` (best effort),
   and return `Err("upstream body inactivity timeout".into())`.
3. Same treatment for `collect_upstream_body` in the buffered path.

---

## 🟠 Section 6 — Fix `PluginAction::Modify` arm

**Problem**:
```rust
Ok(PluginAction::Continue | PluginAction::Modify { .. }) => {}
```
collapses `Modify` into `Continue`, silently discarding plugin modifications.

**Fix**:

1. Inspect how `ferrotunnel-http/src/ingress.rs` handles `PluginAction::Modify`
   (likely it applies the returned headers/body to `plugin_req`).
2. Mirror that handling exactly in the H3 path — destructure `Modify`, apply
   header/body changes to `plugin_req` before continuing.
3. If the H1 ingress also no-ops `Modify`, leave a `// TODO(plugins)` and
   raise this in the PR description rather than silently diverging.

---

## 🟠 Section 7 — Make `max_concurrent_bidi_streams` configurable

**Fix**:

1. Add `pub max_concurrent_bidi_streams: u32` to `Http3IngressConfig` (default
   `256`).
2. Use it in `create_http3_endpoint`:
   ```rust
   transport.max_concurrent_bidi_streams(VarInt::from_u32(config.max_concurrent_bidi_streams));
   ```

---

## 🟠 Section 8 — Connection rejection should emit a metric / structured warn

**Problem**: `try_acquire_owned` failure logs a single `warn!` with no
counter. Under burst load this is invisible.

**Fix**:

1. Keep the `warn!` but include `peer_addr` and the configured limit:
   `warn!(peer = %peer_addr, max = self.config.max_connections, "HTTP/3 max_connections reached, rejecting");`
2. If the `metrics` feature is enabled, increment a counter
   `ferrotunnel_http3_rejected_connections_total`. Use a `#[cfg(feature = "metrics")]`
   block. Skip if no metrics infrastructure exists in the http crate yet —
   note in the commit message.

---

## 🟠 Section 9 — Eliminate redundant allocation in `send_buffered_response`

**Problem**:
```rust
let mut plugin_response = Response::from_parts(parts, body.to_vec());
...
h3_stream.send_data(Bytes::from(body)).await
```
`body` is already `Bytes`. `to_vec()` then `Bytes::from(Vec)` is a pointless
clone of every response body.

**Fix**:

1. Change `ResponseContext`/plugin-response handling so the body stays as
   `Bytes` end-to-end. If the plugin API requires `Vec<u8>` (check
   `ferrotunnel-plugin`), use `body.to_vec()` only at the plugin boundary and
   re-wrap with `Bytes::from(plugin_response.into_body())` once after.
2. Avoid double-copy: one `Bytes -> Vec` for plugin in, one `Vec -> Bytes`
   back out. No extra clones.

---

## 🟠 Section 10 — Graceful shutdown via `Endpoint::wait_idle()`

**Problem**: H3 endpoint is dropped on shutdown without draining open streams.

**Fix**:

1. Change `Http3Ingress::start` to accept an optional shutdown signal:
   ```rust
   pub async fn start(self) -> Result<()>            // existing
   pub async fn start_with_shutdown(self, mut shutdown: tokio::sync::watch::Receiver<bool>) -> Result<()>
   ```
   Implement `start` in terms of `start_with_shutdown` with a never-firing
   channel.
2. The accept loop becomes:
   ```rust
   loop {
       tokio::select! {
           biased;
           _ = shutdown.changed() => {
               info!("HTTP/3 ingress shutting down, draining...");
               endpoint.close(VarInt::from_u32(0), b"server shutdown");
               endpoint.wait_idle().await;
               return Ok(());
           }
           accepted = endpoint.accept() => {
               let Some(incoming) = accepted else {
                   return Err(TunnelError::Connection("HTTP/3 endpoint closed".into()));
               };
               // ... existing per-connection spawn logic
           }
       }
   }
   ```
3. In `ferrotunnel/src/server.rs`, wire the existing shutdown `watch::Receiver`
   into `start_with_shutdown` for the H3 task.

---

## 🟠 Section 11 — Cheaper request id

**Problem**: `uuid::Uuid::new_v4().to_string()` per request allocates 36 bytes
+ heap.

**Fix**:

1. Replace with the same id source the H1 ingress uses (`grep` for `session_id`
   in `ferrotunnel-http/src/ingress.rs`). If H1 also uses `Uuid::new_v4()`,
   keep parity — do *not* diverge unilaterally — but switch both to a shared
   `next_session_id()` helper in a follow-up. Note that follow-up in the PR.

---

## 🟡 Section 12 — Cleanups (apply together)

1. Remove `empty_body()` and its `#[allow(dead_code)]`.
2. Replace `unwrap_or(StatusCode::FORBIDDEN)` in the plugin `Reject` arm with
   `match StatusCode::from_u16(status) { Ok(s) => s, Err(_) => { warn!(status, "invalid plugin reject status"); StatusCode::FORBIDDEN } }`.
   Same treatment for `unwrap_or(StatusCode::OK)` in `send_plugin_response`.
3. `host_header_value`: change return type to `Option<&HeaderValue>` to avoid
   the clone; clone once at the call site only when needed.
4. Split `handle_request` into:
   - `forward_via_h1(...)` (HTTP/1.1 upstream branch)
   - `forward_via_h2(...)` (gRPC upstream branch)
   Then remove `#[allow(clippy::too_many_lines)]`.
5. In `Http3IngressConfig`, mark the struct `#[non_exhaustive]` so future
   fields are non-breaking. Document in CHANGELOG that construction must use
   `Http3IngressConfig::default()` + struct update syntax.
6. `Alt-Svc`: change the format string to advertise both:
   `format!("h3=\":{port}\"; ma={ma}, h3-29=\":{port}\"; ma={ma}", port = addr.port(), ma = self.alt_svc_max_age)`.
   Adjust the existing alt-svc unit test if any.
7. Run `cargo fmt --all` last.

---

## CHANGELOG.md

Append under the existing v1.0.8 entry:

```
### Fixed (post-audit)
- HTTP/3 ingress now streams request bodies instead of buffering up to 100 MB
- HTTP/3 idle timeout decoupled from response timeout; defaults to 30 s with 10 s keepalive
- HTTP/3 endpoint installs default rustls crypto provider for library embedders
- HTTP/3 gRPC forwarded URIs now use https:// scheme to match client-facing protocol
- HTTP/3 streaming responses enforce per-frame timeout (no more indefinitely pinned streams)
- HTTP/3 plugin Modify action is now applied (was silently dropped)
- HTTP/3 ingress drains open streams on graceful shutdown via Endpoint::wait_idle()
- HTTP/3 max_concurrent_bidi_streams is now configurable
- Alt-Svc advertises both h3 and h3-29 for broader client compatibility

### Notes
- h3 0.0.8 / h3-quinn 0.0.10 are pre-1.0; HTTP/3 ingress is marked experimental
```

---

## Out of scope (track as follow-ups, do NOT do here)

- Sharing `next_session_id()` between H1 and H3 ingress (Section 11 follow-up).
- Adding a `Http3IngressConfigBuilder`. `#[non_exhaustive]` (Section 12.5)
  unblocks evolving the type without it.
- Metrics infrastructure for the http crate if not already present (Section 8
  fallback).

---

## Final commit message template

```
fix(http3): address audit findings for v1.0.8 ingress

- Stream request bodies via Http3RequestBody (was buffered)
- Decouple idle_timeout from response_timeout; add keep_alive_interval
- Install rustls ring provider defensively for library embedders
- Apply per-frame response timeout in streaming + buffered paths
- Use https:// scheme for tunneled gRPC URIs
- Honor PluginAction::Modify in H3 path (parity with H1 ingress)
- Make max_concurrent_bidi_streams configurable
- Graceful shutdown drains open H3 streams via Endpoint::wait_idle
- Remove dead empty_body(); split handle_request; mark config non_exhaustive
- Advertise h3-29 alongside h3 in Alt-Svc

Refs: docs/http3-ingress-audit-fixes.md
```
