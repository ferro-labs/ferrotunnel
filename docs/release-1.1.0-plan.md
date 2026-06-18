# Release 1.1.0 Fix Plan

Branch: `feature/1.1.0`
Milestone: `v1.1.0`
Snapshot date: 2026-06-09

## Milestone Scope

The `v1.1.0` milestone shows 7 open issues and 0 closed issues in GitHub, but
all 7 fixes are now implemented on `feature/1.1.0`. GitHub will keep the issues
open until the release branch reaches the default branch or the issue-closing PR
is merged.

| Issue | Severity | Area | Branch status | Fix summary |
| --- | --- | --- | --- | --- |
| [#143](https://github.com/ferro-labs/ferrotunnel/issues/143) | Medium | TLS/QUIC transport | Covered by merged PR [#150](https://github.com/ferro-labs/ferrotunnel/pull/150) at `b706551` | Warn on TLS/QUIC skip-verify and make TLS/QUIC config symmetric. |
| [#124](https://github.com/ferro-labs/ferrotunnel/issues/124) | Critical | Plugin auth | Implemented | Token auth now scans tokens with constant-time byte comparison and focused tests. |
| [#125](https://github.com/ferro-labs/ferrotunnel/issues/125) | Critical | Observability dashboard | Implemented | Dashboard binds loopback by default, non-loopback needs explicit opt-in plus auth, API routes enforce bearer/header/cookie/query token auth, and wildcard CORS is removed. |
| [#126](https://github.com/ferro-labs/ferrotunnel/issues/126) | Critical | Dashboard UI | Implemented | Dashboard renders captured traffic through text/escaping paths and only uses safe `http` or `https` public URLs as links. |
| [#127](https://github.com/ferro-labs/ferrotunnel/issues/127) | Critical | Core tunnel handshake | Implemented | Client/server TCP, TLS, and QUIC handshake reads now use a default 10s timeout with regression coverage. |
| [#128](https://github.com/ferro-labs/ferrotunnel/issues/128) | Critical | CLI server secrets | Implemented | Server token is optional on argv, supports env/token-file/prompt, hides env values, and docs avoid argv secrets. |
| [#116](https://github.com/ferro-labs/ferrotunnel/issues/116) | High | CLI TLS | Implemented | `ferrotunnel client --tls` now requires `--tls-ca` unless `--tls-skip-verify` is explicitly set, with CLI tests and docs. |

## Validation Gates

Completed locally on this branch:

```bash
node --check ferrotunnel-observability/src/dashboard/static/app.js
make check
cargo test --workspace --all-features
make audit
```

`make audit` completed successfully. It reported allowed `rand` advisories and existing duplicate/license warnings through `cargo deny`, and the command exited 0.
