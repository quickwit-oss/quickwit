#!/usr/bin/env bash
#
# Discovers quickwit-metrics reverse dependencies, patches Cargo.toml and
# src/main.rs, builds the inventory binary with all features of the
# discovered crates enabled, runs it, then restores
# Cargo.toml, Cargo.lock, and src/main.rs from temporary backups.
# Files are always restored — even on Ctrl-C or failure.
#
# Usage:
#   ./scripts/run.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CRATE_DIR="$(dirname "$SCRIPT_DIR")"
WORKSPACE_DIR="$(dirname "$CRATE_DIR")"
CARGO_TOML="$CRATE_DIR/Cargo.toml"
CARGO_LOCK="$WORKSPACE_DIR/Cargo.lock"
MAIN_RS="$CRATE_DIR/src/main.rs"

# Back up instead of `git restore` so that local edits are preserved.
BACKUP_DIR=$(mktemp -d)
cp "$CARGO_TOML" "$BACKUP_DIR/Cargo.toml"
cp "$CARGO_LOCK" "$BACKUP_DIR/Cargo.lock"
cp "$MAIN_RS" "$BACKUP_DIR/main.rs"
restore_files() {
    cp "$BACKUP_DIR/Cargo.toml" "$CARGO_TOML"
    cp "$BACKUP_DIR/Cargo.lock" "$CARGO_LOCK"
    cp "$BACKUP_DIR/main.rs" "$MAIN_RS"
    rm -rf "$BACKUP_DIR"
}
trap restore_files EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# --format '{lib}' outputs the Rust crate name (underscores, no version/path).
# --prefix none removes tree decorators. tail skips the root (quickwit-metrics itself).
# Binary-only crates (such as this one) print an empty line, which the loop skips.
REVERSE_DEPS=$(cargo tree --manifest-path "$WORKSPACE_DIR/Cargo.toml" \
    --workspace --all-features --depth 1 --invert quickwit-metrics \
    --prefix none --format '{lib}' \
    | tail -n +2)

PACKAGE_ARGS=(--package quickwit-metrics-inventory)
for rust_name in $REVERSE_DEPS; do
    pkg_name=$(echo "$rust_name" | tr '_' '-')
    echo "$pkg_name = { workspace = true }" >> "$CARGO_TOML"
    echo "extern crate $rust_name;" >> "$MAIN_RS"
    PACKAGE_ARGS+=(--package "$pkg_name")
done

# Selecting every discovered package with --all-features unifies their
# features into the inventory binary, so feature-gated metrics (e.g. the
# PostgreSQL metastore metrics) are included.
# Read the binary path from Cargo instead of assuming the target directory
# layout (which `build.target-dir` or `--target` would change). Diagnostics are
# rendered to stderr, so stdout only carries JSON messages.
BUILD_MESSAGES=$(cargo build --manifest-path "$WORKSPACE_DIR/Cargo.toml" --all-features \
    --message-format=json-render-diagnostics "${PACKAGE_ARGS[@]}")
INVENTORY_BIN=$(echo "$BUILD_MESSAGES" \
    | sed -nE 's/.*"executable":"([^"]*\/quickwit-metrics-inventory(\.exe)?)".*/\1/p' \
    | tail -n 1)
if [[ -z "$INVENTORY_BIN" ]]; then
    echo "error: could not find the quickwit-metrics-inventory binary in cargo build output" >&2
    exit 1
fi
"$INVENTORY_BIN"
