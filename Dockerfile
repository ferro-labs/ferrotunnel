# Build stage with cargo-chef for dependency caching
FROM rust:1.91-slim-bookworm AS chef
RUN apt-get update && apt-get install -y \
    build-essential \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*
RUN cargo install cargo-chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json

# Build dependencies with aggressive size optimizations
ENV RUSTFLAGS="-C link-arg=-s -C opt-level=z -C codegen-units=1 -C panic=abort -C strip=symbols"
RUN cargo chef cook --release --recipe-path recipe.json

# Build application
COPY . .
RUN cargo build --release -p ferrotunnel-cli && \
    strip --strip-all /app/target/release/ferrotunnel

# Runtime stage with Google distroless (minimal Debian)
FROM gcr.io/distroless/cc-debian12:nonroot AS runtime

COPY --from=builder --chown=nonroot:nonroot /app/target/release/ferrotunnel /usr/local/bin/

USER nonroot
EXPOSE 7835 8080 9090 4040

# Default to server, but allows easy override for client
ENTRYPOINT ["ferrotunnel"]
CMD ["server", "--bind", "0.0.0.0:7835"]
