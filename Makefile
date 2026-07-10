.PHONY: help fmt check lint test test-integration build clean all

# Default target
all: fmt check test build

# Show help
help:
	@echo "FerroTunnel - Development Commands"
	@echo ""
	@echo "Usage:"
	@echo "  make fmt          - Format code with rustfmt"
	@echo "  make check        - Run formatter and linter checks"
	@echo "  make lint         - Run clippy linter"
	@echo "  make test         - Run all tests"
	@echo "  make test-integration - Run integration tests only"
	@echo "  make build        - Build the project"
	@echo "  make bench        - Run performance benchmarks"
	@echo "  make audit        - Run security audit"
	@echo "  make fuzz         - Run fuzz tests (5 min smoke test)"
	@echo "  make soak         - Build soak testing tool"
	@echo "  make loadgen      - Build load generator"
	@echo "  make clean        - Clean build artifacts"
	@echo "  make all          - Run fmt, check, test, and build"
	@echo ""

# Format code
fmt:
	@echo "Running rustfmt..."
	cargo fmt --all

# Check formatting and linting
check:
	@echo "Checking formatting..."
	cargo fmt --all -- --check
	@echo "Running clippy..."
	cargo clippy --workspace --all-targets --all-features -- -D warnings

# Run clippy linter
lint:
	@echo "Running clippy..."
	cargo clippy --workspace --all-targets --all-features -- -D warnings

# Run tests
test:
	@echo "Running tests..."
	cargo test --workspace --all-features

# Run integration tests
test-integration:
	@echo "Running integration tests..."
	cargo test -p ferrotunnel --test integration

# Run benchmarks
bench:
	@echo "Running benchmarks..."
	cargo bench --workspace

# Run security audit
audit:
	@echo "Running security audit..."
	cargo audit
	cargo deny check advisories bans licenses sources

# Run fuzz tests (smoke test)
fuzz:
	@echo "Running fuzz tests (5 min)..."
	cd ferrotunnel-protocol && cargo +nightly fuzz run codec_decode --sanitizer=none -- -max_total_time=300

# Build the project
build:
	@echo "Building project..."
	cargo build --workspace

# Build release
release:
	@echo "Building release..."
	cargo build --workspace --release

# Build soak testing tool
soak:
	@echo "Building soak testing tool..."
	cargo build -p ferrotunnel-soak

# Build load generator
loadgen:
	@echo "Building load generator..."
	cargo build -p ferrotunnel-loadgen

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	cargo clean

# Install development tools
install-tools:
	@echo "Installing development tools..."
	rustup component add rustfmt clippy
	cargo install cargo-audit cargo-deny cargo-fuzz

# Build package archives and verify their extracted publish-form sources.
publish-dry-run:
	@echo "Validating publishable crate packages..."
	./scripts/verify-packages.sh

# Publish release manually
publish:
	@echo "Starting manual publish..."
	./scripts/publish.sh
