# FerroTunnel Deployment Guide

## Quick Start

### Server

```bash
# The server reads its token from FERROTUNNEL_TOKEN (or --token-file, or a
# secure TTY prompt). It is never accepted as a CLI flag, so it cannot leak
# via the process list.
export FERROTUNNEL_TOKEN="your-secret-token"

# Basic server (no TLS)
ferrotunnel server --bind 0.0.0.0:8080

# With TLS
ferrotunnel server --bind 0.0.0.0:8443 \
  --tls-cert /path/to/server.crt \
  --tls-key /path/to/server.key
```

### Client

```bash
# Connect to server
ferrotunnel client --server tunnel.example.com:8443 \
  --token "your-secret-token" \
  --local-addr 127.0.0.1:3000 \
  --tls --tls-ca /path/to/ca.crt
```

## System Requirements

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| CPU | 1 core | 2+ cores |
| Memory | 128MB | 512MB+ |
| Disk | 50MB | 100MB |
| Network | 10 Mbps | 100+ Mbps |

## TLS Setup

### Development (Self-Signed)

```bash
# Generate CA
openssl genrsa -out ca.key 4096
openssl req -new -x509 -days 365 -key ca.key -out ca.crt \
  -subj "/CN=FerroTunnel CA"

# Generate server certificate
openssl genrsa -out server.key 2048
openssl req -new -key server.key -out server.csr \
  -subj "/CN=tunnel.example.com"
openssl x509 -req -days 365 -in server.csr -CA ca.crt -CAkey ca.key \
  -CAcreateserial -out server.crt \
  -extfile <(echo "subjectAltName=DNS:tunnel.example.com,DNS:localhost,IP:127.0.0.1")
```

### Production (Let's Encrypt)

```bash
# Using certbot
certbot certonly --standalone -d tunnel.example.com

# Certificate locations
# /etc/letsencrypt/live/tunnel.example.com/fullchain.pem
# /etc/letsencrypt/live/tunnel.example.com/privkey.pem
```

## Configuration

FerroTunnel is configured via CLI arguments or environment variables. All options can be set either way.

### Server Environment Variables

```bash
# Required
export FERROTUNNEL_TOKEN="your-secret-token"

# Optional (with defaults)
export FERROTUNNEL_BIND="0.0.0.0:7835"           # Tunnel control plane
export FERROTUNNEL_HTTP_BIND="0.0.0.0:8080"      # HTTP ingress
export FERROTUNNEL_METRICS_BIND="0.0.0.0:9090"   # Prometheus metrics
export RUST_LOG="info"                            # Log level

# TLS (optional)
export FERROTUNNEL_TLS_CERT="/etc/ferrotunnel/server.crt"
export FERROTUNNEL_TLS_KEY="/etc/ferrotunnel/server.key"
export FERROTUNNEL_TLS_CA="/etc/ferrotunnel/ca.crt"       # For client auth
export FERROTUNNEL_TLS_CLIENT_AUTH="true"                  # Require client certs

# HTTP/3 ingress (optional, requires the http3 feature and UDP reachability)
export FERROTUNNEL_HTTP3_BIND="0.0.0.0:8443"
export FERROTUNNEL_HTTP3_CERT="/etc/ferrotunnel/server.crt" # Defaults to TLS cert
export FERROTUNNEL_HTTP3_KEY="/etc/ferrotunnel/server.key"  # Defaults to TLS key

# Performance
export FERROTUNNEL_OBSERVABILITY="true"          # Enable metrics/tracing
```

### HTTP/3 Ingress

HTTP/3 ingress is available when the binary is built with the `http3` feature.
It listens on UDP and runs alongside the existing HTTP/1.1 and HTTP/2 TCP
ingress. When enabled, the TCP ingress advertises the HTTP/3 endpoint with an
`Alt-Svc` header so compatible clients can upgrade automatically.

```bash
cargo build -p ferrotunnel-cli --features http3

export FERROTUNNEL_TOKEN="your-secret-token"
ferrotunnel server \
  --bind 0.0.0.0:7835 \
  --http-bind 0.0.0.0:8080 \
  --http3-bind 0.0.0.0:8443 \
  --tls-cert /etc/ferrotunnel/server.crt \
  --tls-key /etc/ferrotunnel/server.key
```

Allow UDP traffic to the HTTP/3 bind port in firewalls and load balancers. The
HTTP/3 listener uses the same strict `Host`-based tunnel routing as the existing
HTTP ingress.

### Client Environment Variables

```bash
# Required
export FERROTUNNEL_SERVER="tunnel.example.com:7835"
export FERROTUNNEL_TOKEN="your-secret-token"

# Optional (with defaults)
export FERROTUNNEL_LOCAL_ADDR="127.0.0.1:8000"   # Local service to forward
export FERROTUNNEL_DASHBOARD_PORT="4040"          # Dashboard port
export RUST_LOG="info"                            # Log level

# TLS (optional)
export FERROTUNNEL_TLS="true"                     # Enable TLS
export FERROTUNNEL_TLS_CA="/path/to/ca.crt"      # Required for verified TLS
export FERROTUNNEL_TLS_SERVER_NAME="tunnel.example.com"  # SNI hostname
export FERROTUNNEL_TLS_SKIP_VERIFY="false"       # Explicit insecure mode only

# Mutual TLS (optional)
export FERROTUNNEL_TLS_CERT="/path/to/client.crt"
export FERROTUNNEL_TLS_KEY="/path/to/client.key"

# Performance
export FERROTUNNEL_OBSERVABILITY="true"          # Enable metrics/tracing
```

## Running as a Service

### systemd (Linux)

Create `/etc/systemd/system/ferrotunnel.service`:

```ini
[Unit]
Description=FerroTunnel Server
After=network.target

[Service]
Type=simple
User=ferrotunnel
Group=ferrotunnel
ExecStart=/usr/local/bin/ferrotunnel server
Restart=always
RestartSec=5

# Configuration via environment variables
Environment=FERROTUNNEL_TOKEN=your-secret-token
Environment=FERROTUNNEL_BIND=0.0.0.0:7835
Environment=FERROTUNNEL_HTTP_BIND=0.0.0.0:8080
Environment=FERROTUNNEL_TLS_CERT=/etc/ferrotunnel/server.crt
Environment=FERROTUNNEL_TLS_KEY=/etc/ferrotunnel/server.key

# Security hardening
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
ReadOnlyPaths=/etc/ferrotunnel

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable ferrotunnel
sudo systemctl start ferrotunnel
```

### Docker

```dockerfile
# Dockerfile
FROM rust:1.91-slim as builder
WORKDIR /app
COPY . .
RUN cargo build --release -p ferrotunnel-cli

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/ferrotunnel /usr/local/bin/
EXPOSE 7835 8080
ENTRYPOINT ["ferrotunnel"]
CMD ["server"]
```

```yaml
# docker-compose.yml
version: '3.8'
services:
  ferrotunnel:
    build: .
    ports:
      - "7835:7835"
      - "8080:8080"
    environment:
      - FERROTUNNEL_TOKEN=${FERROTUNNEL_TOKEN}
    volumes:
      - ./certs:/etc/ferrotunnel:ro
    command: >
      server
      --bind 0.0.0.0:7835
      --tls-cert /etc/ferrotunnel/server.crt
      --tls-key /etc/ferrotunnel/server.key
    restart: unless-stopped
```

## Scaling

### Resource Limits Tuning

| Workload | max_sessions | max_streams | Memory |
|----------|--------------|-------------|--------|
| Light | 100 | 50 | ~50MB |
| Medium | 500 | 100 | ~100MB |
| Heavy | 1000 | 100 | ~200MB |

### Load Balancing

For high availability, run multiple server instances behind a load balancer:

```
                    ┌─────────────────┐
                    │  Load Balancer  │
                    │   (TCP/TLS)     │
                    └────────┬────────┘
                             │
            ┌────────────────┼────────────────┐
            ▼                ▼                ▼
    ┌───────────────┐ ┌───────────────┐ ┌───────────────┐
    │ FerroTunnel   │ │ FerroTunnel   │ │ FerroTunnel   │
    │   Server 1    │ │   Server 2    │ │   Server 3    │
    └───────────────┘ └───────────────┘ └───────────────┘
```

## Monitoring

### Health Check Endpoint

```bash
# Check if server is healthy
curl -f http://localhost:9090/health
```

### Metrics

Prometheus metrics available at `/metrics`:

- `ferrotunnel_sessions_active` - Current active sessions
- `ferrotunnel_streams_active` - Current active streams
- `ferrotunnel_bytes_transferred_total` - Total bytes transferred
- `ferrotunnel_errors_total` - Total errors by type

### Logging

Set log level via environment:

```bash
RUST_LOG=info ferrotunnel server ...
RUST_LOG=debug ferrotunnel server ...  # Verbose
RUST_LOG=ferrotunnel=trace ferrotunnel server ...  # Very verbose
```
