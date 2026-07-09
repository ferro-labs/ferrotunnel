# FerroTunnel Development Roadmap

FerroTunnel is an embeddable reverse tunnel for Rust applications, with a CLI, hostname-based HTTP routing, raw TCP ingress, plugins, and optional observability.

This roadmap records shipped capability and the intended order of future releases. It does not promise delivery dates.

## Shipped baseline

| Release | Outcome |
| --- | --- |
| v1.0.0 | Stable protocol, tunnel runtime, HTTP and TCP ingress, plugin primitives, dashboard, observability, and unified CLI. |
| v1.0.1 | Packaging, tunnel identifiers, ingress routing fixes, and benchmark tooling. |
| v1.0.2 | WebSocket tunneling and graceful CLI shutdown. |
| v1.0.3 | HTTP/2 ingress and HTTP/1.1 and HTTP/2 connection pooling. |
| v1.0.6 | gRPC tunneling over HTTP/2 with trailer preservation. |
| v1.0.7 | Optional QUIC tunnel transport with native stream multiplexing and TLS 1.3. The 0-RTT option is reserved and currently uses a full handshake. |
| v1.0.8 | Optional browser-facing HTTP/3 ingress with Alt-Svc discovery. |
| v1.1.0-v1.5.0 | Security, resource, lifecycle, concurrency, protocol, configuration, and public-API hardening. |

## Current release

### v1.5.1 - Maintenance

- Update vulnerable and unsound dependency versions recorded in the lockfile.
- Run both RustSec and cargo-deny checks in the required CI gate.
- Reconcile the remaining audit checklist and move behavior changes to their owning future release.
- Correct security, plugin, soak, QUIC 0-RTT, release, and workflow documentation.
- Validate all published crate packages and release metadata.

Exit gate: `make check`, `cargo test --workspace --all-features`, documentation with warnings denied, `make audit`, and `make publish-dry-run`.

## Planned releases

### v1.6.x - Runtime correctness and operations

- Add explicit tenant routing for raw TCP ingress.
- Enforce configured per-session stream and in-flight frame ceilings.
- Wire reconnect backoff, heartbeat deadlines, and resource cleanup through runtime paths.
- Expose custom plugin registration and complete plugin lifecycle integration.
- Add certificate reload, CLI logging configuration, and real through-tunnel soak coverage.

### v1.7.x - AI workload connectivity

- Add streaming-safe plugin hooks and long-running stream presets.
- Support ephemeral SDK tunnels and common local model and MCP workflows.
- Add per-tunnel authorization and stream-focused telemetry.

### v1.8.x - Edge deployments

- Publish client-focused ARM artifacts and service packaging.
- Add device identity, authorization, and fleet-safe credential handling.
- Improve recovery across roaming and intermittent links.

### v1.9.x - High availability and custom routing

- Add shared session ownership and cross-node stream forwarding.
- Support graceful node drain and health-aware routing.
- Add verified custom-domain aliases after distributed ownership is available.

### v2.0.x - Protocol evolution

- Add forward-compatible frame extensions.
- Add QUIC datagrams and UDP relay.
- Add routing groups and cross-stream flow control where wire changes require a major release.

## Release policy

- `main` contains stable, reviewed code and release tags.
- `release/X.Y.Z` branches prepare a release candidate before merging to `main`.
- `feature/*` and `fix/*` branches submit focused changes through pull requests.
- Patch releases remain API-compatible and avoid new runtime behavior.
- User-visible changes are recorded in [CHANGELOG.md](CHANGELOG.md).

## Required quality gates

The required CI path covers:

- formatting and Clippy with warnings denied;
- release-profile builds on Linux, macOS, and Windows;
- all-feature tests on stable Rust and the minimum supported Rust version;
- optional beta Rust signal;
- RustSec and cargo-deny dependency policy;
- rustdoc with warnings denied;
- protocol fuzz smoke tests.

Scheduled CodeQL analysis and longer fuzz runs provide additional coverage. Release candidates also validate the package contents for every published crate.

## Technical targets

These are measurement targets, not guarantees:

- less than 5 ms tunnel overhead in the reference benchmark;
- bounded memory under the documented session and frame limits;
- no crashes during a seven-day through-tunnel soak run;
- predictable recovery after connection loss without a reconnect surge.

Benchmark and soak results must identify the version, host, workload, transport, and configuration used.
