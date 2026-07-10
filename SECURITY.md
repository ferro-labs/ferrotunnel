# Security Policy

## Supported versions

Security fixes are released for the current stable minor line. Users should upgrade to the latest patch in that line.

| Version | Supported |
| --- | --- |
| 1.5.x | Yes |
| < 1.5.0 | No |

## Report a vulnerability

Do not report security vulnerabilities through public GitHub issues. Use one of these private channels:

- Email [hello@ferrolabs.ai](mailto:hello@ferrolabs.ai).
- Use [GitHub private vulnerability reporting](https://github.com/ferro-labs/ferrotunnel/security/advisories/new).

Include the affected version or commit, reproduction steps, required configuration, impact, and a proof of concept when one is available. Avoid including secrets or third-party data.

## Coordination

The maintainers use GitHub Security Advisories to coordinate fixes with reporters and affected downstream maintainers. Participation requests may be sent to [hello@ferrolabs.ai](mailto:hello@ferrolabs.ai) with a GitHub username and a brief description of the affected deployment.

Security fixes are disclosed through:

1. [GitHub Security Advisories](https://github.com/ferro-labs/ferrotunnel/security/advisories)
2. [GitHub Releases](https://github.com/ferro-labs/ferrotunnel/releases)
3. [CHANGELOG.md](CHANGELOG.md)

Dependency vulnerabilities may also be present in the [RustSec Advisory Database](https://rustsec.org/advisories/), which is consumed by the project audit gate.

## Response targets

- Initial response: within 48 hours
- Status update: within 7 days
- Fix timeline: based on severity and exploitability
- Public disclosure: coordinated after a fix is available

These are targets rather than service-level guarantees.

## Automated checks

Every pull request and push to `main` runs:

- `cargo deny check advisories bans licenses sources` for RustSec advisories, licenses, sources, and dependency policy
- formatting, lint, test, build, documentation, and fuzz smoke-test gates

Builds and tests run with `--locked` so the committed lockfile is the audited dependency set.

CodeQL and longer fuzz jobs run on their own schedules.

Run the local security gate with:

```bash
cargo install cargo-audit cargo-deny --locked
make audit
```

## Deployment security

### Transport selection

| Transport | Encryption | Production guidance |
| --- | --- | --- |
| TCP | None | Use only on a trusted network or replace it with TLS. |
| TLS over TCP | rustls TLS | Configure a trusted CA and hostname verification. |
| QUIC | TLS 1.3 | Configure certificates and keep peer verification enabled. |

`--tls-skip-verify` and the corresponding QUIC option disable peer authentication. They are intended only for controlled testing and emit a warning.

### Authentication tokens

- Generate a random token of at least 32 bytes. FerroTunnel validates non-empty printable tokens up to 256 bytes, but it does not enforce the 32-byte recommendation.
- Supply server tokens through `FERROTUNNEL_TOKEN`, `--token-file`, or the secure prompt. The server does not accept a token in process arguments.
- Store tokens in a secret manager, restrict file permissions, and rotate them after suspected exposure.
- Use TLS or QUIC so tokens are not sent over plaintext TCP.

### Dashboard and ingress

- Keep the dashboard on its loopback default. Non-loopback dashboard binds require explicit opt-in and an authentication token.
- Restrict tunnel, HTTP, TCP, HTTP/3, metrics, and dashboard ports with host or network firewall rules.
- Treat raw TCP ingress as single-tenant in the 1.5.x line; it selects an eligible TCP session without a tenant routing key.

### Resource limits

The 1.5.x runtime enforces the configured session ceiling and frame-size validation. Rate limits are enforced on session traffic. `max_streams_per_session` and `max_inflight_frames` are validated configuration fields but are not yet wired into stream admission, so deployments must also use ingress and network-level concurrency controls.

## Security properties and limits

- Workspace-owned Rust source is built with `unsafe_code = "forbid"`. This does not make claims about unsafe code inside third-party dependencies.
- Safe Rust prevents common memory corruption and data races, but it does not prevent logical races, denial of service, misconfiguration, or vulnerable dependencies.
- Authentication uses constant-time comparison after token-format validation.
- Structured tracing and request logging are available; FerroTunnel does not provide a tamper-resistant audit-log store.
- TLS is opt-in for TCP. QUIC includes encryption by protocol design.

## Safe harbor

The project will not pursue legal action against researchers who act in good faith, test only systems they own or are authorized to test, avoid privacy violations and service disruption, and disclose only the minimum data required to demonstrate the issue. Contact [hello@ferrolabs.ai](mailto:hello@ferrolabs.ai) if unexpected user data is encountered.

FerroTunnel does not currently offer a paid bug bounty.
