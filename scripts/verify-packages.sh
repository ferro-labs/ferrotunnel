#!/usr/bin/env bash
set -euo pipefail

readonly -a CRATES=(
    ferrotunnel-common
    ferrotunnel-protocol
    ferrotunnel-plugin
    ferrotunnel-observability
    ferrotunnel-core
    ferrotunnel-http
    ferrotunnel
    ferrotunnel-cli
)

package_args=()
for crate in "${CRATES[@]}"; do
    package_args+=(--package "${crate}")
done

# scripts/publish.sh refuses to run on a dirty tree, so the gate that validates
# it is strict by default. Set ALLOW_DIRTY=1 to package uncommitted work while
# iterating locally.
if [ "${ALLOW_DIRTY:-0}" = "1" ]; then
    package_args+=(--allow-dirty)
fi

# Packaging all crates together lets Cargo stage their unpublished versions in
# a temporary local registry before verifying each archive.
cargo package \
    --locked \
    --all-features \
    "${package_args[@]}"
