#!/usr/bin/env bash
# FerroTunnel installer script
# Usage: curl -fsSL https://raw.githubusercontent.com/ferro-labs/ferrotunnel/main/scripts/install.sh | bash
#
# Environment variables:
#   FERROTUNNEL_VERSION  - specific version to install (default: latest)
#   FERROTUNNEL_INSTALL  - install directory (default: /usr/local/bin or ~/.local/bin)

set -euo pipefail

REPO="ferro-labs/ferrotunnel"
BINARY_NAME="ferrotunnel"

# --- Colors (disabled if not a terminal) ---
if [ -t 1 ]; then
    BOLD='\033[1m'
    GREEN='\033[0;32m'
    YELLOW='\033[0;33m'
    RED='\033[0;31m'
    RESET='\033[0m'
else
    BOLD='' GREEN='' YELLOW='' RED='' RESET=''
fi

info()  { echo -e "${GREEN}[info]${RESET}  $*"; }
warn()  { echo -e "${YELLOW}[warn]${RESET}  $*"; }
error() { echo -e "${RED}[error]${RESET} $*" >&2; }
bold()  { echo -e "${BOLD}$*${RESET}"; }

# --- Detect OS ---
detect_os() {
    local os
    os="$(uname -s)"
    case "$os" in
        Linux*)  echo "linux" ;;
        Darwin*) echo "darwin" ;;
        *)       error "Unsupported OS: $os"; exit 1 ;;
    esac
}

# --- Detect architecture ---
detect_arch() {
    local arch
    arch="$(uname -m)"
    case "$arch" in
        x86_64|amd64)  echo "amd64" ;;
        aarch64|arm64) echo "arm64" ;;
        *)             error "Unsupported architecture: $arch"; exit 1 ;;
    esac
}

# --- Detect musl vs glibc on Linux ---
detect_libc() {
    if [ "$(detect_os)" != "linux" ]; then
        echo "gnu"
        return
    fi
    # Check if running on musl-based system (Alpine, Void musl, etc.)
    if command -v ldd >/dev/null 2>&1 && ldd --version 2>&1 | grep -qi musl; then
        echo "musl"
    elif [ -f /etc/alpine-release ]; then
        echo "musl"
    else
        echo "gnu"
    fi
}

# --- Resolve latest version from GitHub ---
get_latest_version() {
    local url="https://api.github.com/repos/${REPO}/releases/latest"
    local version

    if command -v curl >/dev/null 2>&1; then
        version=$(curl -fsSL "$url" | grep '"tag_name"' | head -1 | sed -E 's/.*"tag_name":\s*"([^"]+)".*/\1/')
    elif command -v wget >/dev/null 2>&1; then
        version=$(wget -qO- "$url" | grep '"tag_name"' | head -1 | sed -E 's/.*"tag_name":\s*"([^"]+)".*/\1/')
    else
        error "Neither curl nor wget found. Please install one of them."
        exit 1
    fi

    if [ -z "$version" ]; then
        error "Failed to determine the latest version. Set FERROTUNNEL_VERSION manually."
        exit 1
    fi
    echo "$version"
}

# --- Download a URL to a file ---
download() {
    local url="$1" dest="$2"
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL -o "$dest" "$url"
    elif command -v wget >/dev/null 2>&1; then
        wget -qO "$dest" "$url"
    else
        error "Neither curl nor wget found."
        exit 1
    fi
}

# --- Determine install directory ---
get_install_dir() {
    if [ -n "${FERROTUNNEL_INSTALL:-}" ]; then
        echo "$FERROTUNNEL_INSTALL"
        return
    fi

    # Prefer /usr/local/bin if writable, else fall back to ~/.local/bin
    if [ -w "/usr/local/bin" ]; then
        echo "/usr/local/bin"
    else
        echo "${HOME}/.local/bin"
    fi
}

# --- Main ---
main() {
    bold "FerroTunnel Installer"
    echo ""

    local os arch libc version asset_name install_dir tmpdir

    os="$(detect_os)"
    arch="$(detect_arch)"
    libc="$(detect_libc)"

    # Resolve version
    if [ -n "${FERROTUNNEL_VERSION:-}" ]; then
        version="$FERROTUNNEL_VERSION"
        # Ensure version starts with 'v'
        case "$version" in
            v*) ;;
            *)  version="v${version}" ;;
        esac
    else
        info "Fetching latest version..."
        version="$(get_latest_version)"
    fi

    info "Version:      $version"
    info "OS:           $os"
    info "Architecture: $arch"

    # Build asset name
    case "$os" in
        linux)
            if [ "$libc" = "musl" ]; then
                asset_name="ferrotunnel-linux-musl-${arch}"
                info "Libc:         musl"
            else
                asset_name="ferrotunnel-linux-${arch}"
                info "Libc:         glibc"
            fi
            ;;
        darwin)
            asset_name="ferrotunnel-darwin-${arch}"
            ;;
    esac

    # Currently arm64 Linux is not in the release matrix
    if [ "$os" = "linux" ] && [ "$arch" = "arm64" ]; then
        error "Pre-built arm64 Linux binaries are not yet available."
        error "Install from source: cargo install ferrotunnel-cli"
        exit 1
    fi

    local download_url="https://github.com/${REPO}/releases/download/${version}/${asset_name}.tar.gz"
    info "Downloading:  $download_url"

    # Download to temp directory
    tmpdir="$(mktemp -d)"
    trap 'rm -rf "$tmpdir"' EXIT

    download "$download_url" "${tmpdir}/${asset_name}.tar.gz"

    # Extract
    tar -xzf "${tmpdir}/${asset_name}.tar.gz" -C "$tmpdir"

    # Verify binary exists
    if [ ! -f "${tmpdir}/${BINARY_NAME}" ]; then
        error "Binary '${BINARY_NAME}' not found in archive."
        exit 1
    fi

    chmod +x "${tmpdir}/${BINARY_NAME}"

    # Install
    install_dir="$(get_install_dir)"
    mkdir -p "$install_dir"

    if [ -w "$install_dir" ]; then
        mv "${tmpdir}/${BINARY_NAME}" "${install_dir}/${BINARY_NAME}"
    else
        info "Elevated permissions required to install to ${install_dir}"
        sudo mv "${tmpdir}/${BINARY_NAME}" "${install_dir}/${BINARY_NAME}"
    fi

    info "Installed to: ${install_dir}/${BINARY_NAME}"

    # Verify installation
    if command -v "$BINARY_NAME" >/dev/null 2>&1; then
        echo ""
        bold "FerroTunnel ${version} installed successfully!"
        echo ""
        echo "  Get started:"
        echo "    ferrotunnel --help"
        echo "    ferrotunnel server --token <secret>"
        echo "    ferrotunnel client --server <host>:7835 --local-addr 127.0.0.1:8080 --tunnel-id my-app"
        echo ""
    else
        warn "Installed to ${install_dir}, but it is not in your PATH."
        echo ""
        echo "  Add it to your PATH:"
        echo "    export PATH=\"${install_dir}:\$PATH\""
        echo ""
        echo "  Or add this line to your shell profile (~/.bashrc, ~/.zshrc, etc.):"
        echo "    echo 'export PATH=\"${install_dir}:\$PATH\"' >> ~/.bashrc"
        echo ""
    fi
}

main "$@"
