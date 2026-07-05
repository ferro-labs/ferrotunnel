# FerroTunnel Development Roadmap

Embeddable, extensible, and observable reverse tunnel for Rust developers.

---

## Vision

**FerroTunnel** is not just another tunnel - it's the **embeddable**, **extensible**, and **observable** reverse tunnel for Rust developers.

### Core Differentiators

🎯 **Library-First** - Published to crates.io, embedded in your apps
🎯 **Plugin System** - Trait-based extensibility for custom behavior
🎯 **Built-in Dashboard** - Real-time WebUI for monitoring

---

## Version Strategy

### Stable

- **v1.0.0** - First stable release ✅
  - Protocol, tunnel, HTTP/TCP ingress, plugin system, observability, dashboard, unified CLI
  - Published to crates.io

- **v1.0.1** - Stability & Developer Experience ✅
  - Homebrew formula, Docker optimization (13.4 MB image)
  - `--tunnel-id` CLI flag and `.tunnel_id()` builder method
  - HTTP ingress routing fix, performance benchmarks vs. alternatives
  - **Goal**: Convert evaluators → users → advocates

- **v1.0.2** - WebSocket Tunneling & Graceful Shutdown ✅
  - Full WebSocket tunnel support (upgrade detection, bidirectional bridging)
  - Graceful shutdown for CLI server and client (Ctrl-C / SIGTERM handling)
  - Real-time application compatibility (chat, dashboards, gaming)
  - **Market Impact**: Opens to entire real-time application developer segment

- **v1.0.3** - HTTP/2 Support & Connection Pooling ✅
  - HTTP/2 ingress and client proxy with automatic protocol detection
  - Connection pooling for both HTTP/1.1 and HTTP/2
  - Background connection eviction for resource efficiency
  - **Value**: Modern web baseline, enterprise credibility, improved performance

- **v1.0.6** - gRPC Support ✅
  - Native gRPC tunneling over HTTP/2 with automatic detection and trailer preservation
  - **Target Audience**: Enterprise and microservices developers

- **v1.0.7** - QUIC Transport ✅
  - QUIC tunnel transport using quinn 0.11 (behind `quic` feature flag)
  - Native QUIC stream multiplexing (no head-of-line blocking)
  - 0-RTT reconnection support, built-in TLS 1.3
  - CLI flags: `--quic-bind` (server), `--quic` / `--quic-0rtt` (client)
  - **Differentiator**: Next-gen transport for competitive advantage

- **v1.1.0 – v1.5.0** - Security & Reliability Hardening ✅
  - Audit-driven hardening across dashboard/TLS auth (v1.1.0), ingress and
    session-resource limits (v1.2.0), concurrency and lifecycle correctness
    (v1.3.0), wire-protocol and configuration hardening (v1.4.0), and public-API
    encapsulation with authentication-token redaction (v1.5.0)
  - **Value**: Production-grade robustness ahead of the API-stabilization window

### Planned

> **Strategy**: Prioritize features that maximize user adoption and "time to first success"

- **v1.0.8** - HTTP/3 Ingress
  - HTTP/3 ingress using h3 + h3-quinn (accept HTTP/3 from browsers)
  - Alt-Svc header injection for automatic HTTP/3 upgrade
  - **Value**: Complete HTTP/3 story (QUIC transport + HTTP/3 ingress)

- **v1.0.9** - Multi-region Support
  - Geographic load balancing
  - Regional failover capabilities

- **v1.0.10** - Custom Domains
  - Custom domain mapping for white-label deployments

- **v2.0.0** - Breaking Changes (if needed)
  - Protocol improvements based on v1.x learnings

---

## Comparison with Alternatives

| Feature | Rathole | frp | FerroTunnel |
|---------|---------|-----|-------------|
| Language | Rust | Go | Rust |
| Embeddable | ❌ | ❌ | ✅ crates.io library |
| Plugin System | ❌ | Limited | ✅ Trait-based |
| Dashboard | ❌ | Basic | ✅ Built-in WebUI |
| Request Inspector | ❌ | ❌ | ✅ Built-in |
| OpenTelemetry | ❌ | ❌ | ✅ Built-in |
| Memory Efficiency | — | ~300MB/1k tunnels | ~100MB/1k tunnels |
| License | Apache-2.0 | Apache-2.0 | MIT OR Apache-2.0 |

---

## Success Metrics

### Technical Targets

- **Performance**: < 5ms latency overhead vs raw TCP
- **Scalability**: 10k concurrent streams per server
- **Efficiency**: < 100MB memory for 1000 tunnels
- **Reliability**: Zero crashes in 7-day soak test

### Differentiation Validation

- ✅ **Only embeddable** Rust tunnel (crates.io)
- ✅ **Most extensible** via plugin system
- ✅ **Best observability** with built-in dashboard

---

## Development Workflow

### Branch Strategy

- `main` - Stable, tagged releases
- `develop` - Integration branch
- `feature/*` - Feature branches
- `fix/*` - Bug fix branches

### Release Process

1. Development on `feature/*` branches
2. Merge to `develop` via PR
3. Integration testing on `develop`
4. Tag release from `develop` → `main`
5. Publish to crates.io
6. Create GitHub release

### CI/CD Pipeline

```yaml
# .github/workflows/ci.yml
- Cargo check
- Cargo test (all features)
- Cargo clippy (deny warnings)
- Cargo fmt --check
- Cargo audit (dependency security)
- Cargo doc (documentation build)
- Coverage report (codecov)
```
