# FerroTunnel Security Guide

This guide describes the security properties and deployment controls available in FerroTunnel 1.5.x.

## Why Rust Matters: Memory-Safety Comparison

Tunnel servers parse untrusted network input and maintain many concurrent connections.
Memory-corruption defects in this class of software can turn malformed input or state-management
errors into crashes or arbitrary code execution.

### Historical examples

Illustrative vulnerabilities in C-based tunnel projects and related components include:

| Project or component | Advisory | Reported issue |
| --- | --- | --- |
| OpenSSH `sshd` 9.1 | [CVE-2023-25136](https://nvd.nist.gov/vuln/detail/CVE-2023-25136) | An unauthenticated attacker could trigger a double-free; code execution was considered theoretically possible. |
| OpenVPN TAP-Windows6 driver | [CVE-2024-1305](https://nvd.nist.gov/vuln/detail/CVE-2024-1305) | An unchecked write size could overflow kernel buffers, causing a system crash or potentially arbitrary code execution. |
| stunnel 4.40 and 4.41 | [CVE-2011-2940](https://nvd.nist.gov/vuln/detail/CVE-2011-2940) | Heap memory corruption could cause denial of service or potentially arbitrary code execution. |
| OpenSSH `sshd` | [CVE-2024-6387](https://nvd.nist.gov/vuln/detail/CVE-2024-6387) | A signal-handler race could allow an unauthenticated remote attacker to trigger unsafe signal handling. |

These examples are illustrative, not an exhaustive CVE count. Each advisory has its own affected
component, configuration, and exploitability. They show why memory safety matters, but they do
not prove that choosing Rust alone would prevent every exact root cause.

Advisory summaries were checked on 2026-07-10. Follow the linked records for current scope and
status.

### Safe Rust guarantees and remaining risks

| Risk class | What Safe Rust provides | What remains |
| --- | --- | --- |
| Out-of-bounds memory access | Array and slice indexing is bounds-checked. Constant indices may be rejected at compile time; dynamic indices are checked at runtime. | A failed runtime check can still panic, so parsers need explicit validation and error handling. |
| Use-after-free and double-free | Ownership and lifetimes prevent access after a value is dropped and prevent ordinary double-free in safe code. | This guarantee does not extend to unsound `unsafe` code or defects in dependencies. |
| Data races | Borrowing rules together with `Send` and `Sync` prevent data races in safe code. | Rust does not prevent logical race conditions, deadlocks, starvation, or incorrect synchronization. |
| Null and dangling references | Safe references must be valid and non-null; `Option<T>` represents absence explicitly. | Raw pointers and foreign interfaces remain outside this safe-reference guarantee. |
| Integer overflow | Integer overflow does not produce C-style undefined behavior, and checked or saturating operations are available. | Optimized builds may wrap unless checks are enabled or explicit checked operations are used; logic bugs remain possible. |
| Memory and resource leaks | Ownership and `Drop` provide deterministic cleanup on ordinary ownership paths. | Safe Rust permits leaks, reference cycles, deadlocks, and processes that never reach cleanup. |

The [Rust Reference](https://doc.rust-lang.org/reference/expressions/array-expr.html#array-and-slice-indexing-expressions)
documents compile-time versus runtime bounds checks. The
[Rustonomicon](https://doc.rust-lang.org/nomicon/races.html) distinguishes data races from
general race conditions, and the
[Rust Reference](https://doc.rust-lang.org/reference/behavior-not-considered-unsafe.html)
documents integer-overflow behavior and permitted resource leaks.

### FerroTunnel's unsafe-code boundary

The workspace enforces:

```toml
[workspace.lints.rust]
unsafe_code = "forbid"
```

Every workspace-owned crate either inherits this lint or declares the same prohibition. This
rejects unsafe syntax in project-owned source; it does not inspect third-party dependencies or
claim that the full dependency graph contains no unsafe or native code.

FerroTunnel uses [rustls](https://github.com/rustls/rustls) instead of OpenSSL for tunnel TLS and
explicitly installs rustls's `ring` cryptography provider. This describes the selected stack; it
is not a claim that cryptography is vulnerability-free or that every dependency is pure Rust.

**Why it matters:** Safe Rust removes major memory-corruption paths from FerroTunnel's
project-owned code before deployment. Protocol validation, resource limits, dependency audits,
secure transport configuration, and operational monitoring remain necessary for the risks the
language does not eliminate.

## Security model

FerroTunnel combines the memory-safety boundary above with authenticated sessions, configurable
encrypted transports, protocol validation, resource controls, observability, and dependency
checks. The sections below distinguish enforced controls from reserved configuration.

FerroTunnel authenticates tunnel sessions with a shared token. Token comparison is constant time after format validation. The configured token itself is used for authentication; `hash_token` is a utility function and is not a hashed-token storage mode.

## Transport security

The control-plane transport is selected explicitly:

| Mode | Encryption | Notes |
| --- | --- | --- |
| TCP | None | Default. Use only on a trusted network. |
| TLS over TCP | rustls TLS | Requires server certificates and client CA verification. |
| QUIC | TLS 1.3 | Requires certificates. The 0-RTT option currently uses a full handshake. |

### TLS server and client

```bash
# Server
export FERROTUNNEL_TOKEN="$(cat /run/secrets/ferrotunnel-token)"
ferrotunnel server \
  --bind 0.0.0.0:7835 \
  --tls-cert /run/secrets/server.crt \
  --tls-key /run/secrets/server.key

# Client
export FERROTUNNEL_TOKEN="$(cat /run/secrets/ferrotunnel-token)"
ferrotunnel client \
  --server tunnel.example.com:7835 \
  --tls \
  --tls-ca /etc/ssl/ferrotunnel-ca.crt \
  --local-addr 127.0.0.1:8080
```

For mutual TLS, add `--tls-client-auth --tls-ca <client-ca>` on the server and `--tls-cert <client-cert> --tls-key <client-key>` on the client.

`--tls-skip-verify` disables peer authentication and is only appropriate for controlled testing. It must be selected explicitly and emits a warning.

### QUIC

Build with the `quic` feature, configure the server certificate and key, and give clients a trusted CA. QUIC always encrypts transport data, but an encrypted connection without certificate verification is still vulnerable to interception.

## Token management

FerroTunnel accepts non-empty printable tokens up to 256 bytes. Use at least 32 random bytes even though that recommendation is not enforced.

```bash
openssl rand -base64 32
```

Server tokens should come from `FERROTUNNEL_TOKEN`, `--token-file`, or the secure prompt. The server deliberately does not accept `--token`, which keeps the secret out of process listings and shell history. Restrict token-file permissions and rotate tokens after exposure.

Tokens sent over the default TCP transport are plaintext on the network. Use TLS or QUIC outside a trusted network.

## Dashboard security

The dashboard binds to loopback by default. A non-loopback bind requires `--dashboard-allow-non-loopback` and an authentication token. Keep the dashboard behind a firewall or authenticated reverse proxy, and avoid placing its token in URLs or logs.

## Resource controls

The public `LimitsConfig` contains both enforced and reserved controls:

| Control | 1.5.x status |
| --- | --- |
| `max_sessions` | Enforced when accepting tunnel sessions. |
| `max_frame_bytes` | Enforced by protocol codecs. |
| Token and capability bounds | Enforced during frame validation. |
| Session rate limits | Enforced for stream and byte budgets. |
| `max_streams_per_session` | Validated but not yet wired into stream admission. |
| `max_inflight_frames` | Validated but not yet wired into frame admission. |

Configure the enforced limits and use firewall, ingress, and operating-system controls for the remaining concurrency boundaries. Do not treat the reserved fields as isolation controls in 1.5.x.

```rust
use ferrotunnel::{common::LimitsConfig, Server};

# fn build() -> ferrotunnel::Result<Server> {
let limits = LimitsConfig {
    max_frame_bytes: 4 * 1024 * 1024,
    max_sessions: 500,
    ..LimitsConfig::default()
};

Server::builder()
    .bind("0.0.0.0:7835".parse().expect("valid tunnel bind"))
    .http_bind("0.0.0.0:8080".parse().expect("valid HTTP bind"))
    .token("replace-with-a-random-token")
    .limits(&limits)
    .build()
# }
```

Raw TCP ingress in 1.5.x selects an eligible TCP session and has no tenant routing key. Run it only in a single-tenant trust boundary. Host-based HTTP ingress has explicit routing.

## Network controls

- Expose only the required control-plane and ingress ports.
- Keep metrics and dashboard listeners on private or loopback addresses.
- Apply connection limits at the firewall or load balancer.
- Protect certificate private keys with restrictive file permissions.
- Monitor structured tracing for repeated authentication and connection failures.

## Dependency checks

Run the same security gate used by release validation:

```bash
cargo install cargo-audit cargo-deny --locked
make audit
```

`cargo audit` checks the lockfile against RustSec advisories. `cargo deny check` applies advisory, license, source, and duplicate-dependency policy across all features.

## Deployment checklist

- [ ] Upgrade to the latest 1.5.x patch.
- [ ] Enable verified TLS or QUIC outside a trusted network.
- [ ] Generate a random token of at least 32 bytes and store it outside process arguments.
- [ ] Bind the dashboard and metrics endpoints to loopback or a private interface.
- [ ] Set an enforced session and frame-size ceiling appropriate for the host.
- [ ] Add external connection limits for stream and in-flight concurrency.
- [ ] Restrict raw TCP ingress to a single-tenant boundary.
- [ ] Run `make audit` before deployment and after dependency updates.

Report vulnerabilities privately using [SECURITY.md](../SECURITY.md).
