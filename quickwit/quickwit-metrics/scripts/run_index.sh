#!/usr/bin/env bash
#
# Discovers quickwit-metrics reverse dependencies, patches Cargo.toml and
# a temporary main.rs, builds and runs the metrics index, then restores both
# files via git. Files are always restored — even on Ctrl-C or failure.
#
# Usage:
#   ./scripts/run_index.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CRATE_DIR="$(dirname "$SCRIPT_DIR")"
WORKSPACE_DIR="$(dirname "$CRATE_DIR")"
CARGO_TOML="$CRATE_DIR/Cargo.toml"
INVENTORY_RS="$CRATE_DIR/bin/inventory.rs"

trap 'git restore "$CARGO_TOML" "$INVENTORY_RS"' EXIT

# --format '{lib}' outputs the Rust crate name (underscores, no version/path).
# --prefix none removes tree decorators. tail skips the root (quickwit-metrics itself).
REVERSE_DEPS=$(cargo tree --manifest-path "$WORKSPACE_DIR/Cargo.toml" \
    --workspace --all-features --depth 1 --invert quickwit-metrics \
    --prefix none --format '{lib}' 2>/dev/null \
    | tail -n +2)

for rust_name in $REVERSE_DEPS; do
    pkg_name=$(echo "$rust_name" | tr '_' '-')
    echo "$pkg_name = { workspace = true }" >> "$CARGO_TOML"
    echo "extern crate $rust_name;" >> "$INVENTORY_RS"
done

cargo run --manifest-path "$CARGO_TOML" --bin inventory
