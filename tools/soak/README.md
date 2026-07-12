# FerroTunnel Soak Testing Tool

The soak tool repeatedly opens authenticated tunnel-client sessions, holds them for five minutes, shuts them down, and records process metrics. It is useful for connection-lifecycle and long-running memory checks.

It does not yet send application data through the tunnel. The `total_bytes` field is a synthetic progress counter, `active_tunnels` is the configured worker count, and RSS measures the soak process itself. Through-tunnel traffic coverage is planned separately.

## Usage

### 1. Start the server

```bash
export FERROTUNNEL_TOKEN=my-secret-token
cargo run --release --bin ferrotunnel -- server --bind 127.0.0.1:7835
```

### 2. Run the soak tool

In another terminal:

```bash
cargo run -p ferrotunnel-soak -- \
    --tunnel-addr 127.0.0.1:7835 \
    --token my-secret-token \
    --concurrency 50 \
    --duration 60
```

Any failed session cycle increments the error count and is retried after five seconds. Server
unavailability is one such failure.

## Arguments

- `--tunnel-addr <TUNNEL_ADDR>`: FerroTunnel server address (default: `127.0.0.1:7835`).
- `--token <TOKEN>`: Client authentication token (default: `my-secret-token`).
- `--concurrency <CONCURRENCY>`: Number of session workers (default: `10`).
- `--duration <DURATION>`: Duration in minutes; `0` runs until interrupted (default: `0`).
- `--output <OUTPUT>`: JSONL metrics path (default: `soak_metrics.jsonl`).
- `--target <TARGET>`: Reserved for future through-tunnel traffic (default: `127.0.0.1:9999`).

## Interpreting output

- `rss_mb`: resident memory of the soak runner. Look for sustained growth after the worker count stabilizes.
- `errors`: failed session cycles. A healthy local run should remain at zero.
- `total_bytes`: synthetic progress only; do not use it as a throughput measurement.
- `active_tunnels`: configured worker count, not a live connection gauge.
