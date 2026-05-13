#!/usr/bin/env bash
# Run N local CloudPrem searchers for the batching experiment (CLOUDPREM-416).
#
# Each searcher connects to the staging gateway via reverse WebSocket.
# Goal A: confirm uniform routing across searchers.
# Goal B: confirm same-query bursts get batched on the same searcher.
#
# Prerequisites:
#   - Built binary: cd quickwit && cargo build --release -p quickwit-cli
#   - DD_API_KEY env var (staging API key)
#
# Usage:
#   DD_API_KEY=xxx ./scripts/run-local-searchers.sh [N]
#   DD_API_KEY=xxx CP_BATCH_WINDOW_MS=200 ./scripts/run-local-searchers.sh 10

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
N="${1:-${N_SEARCHERS:-5}}"
BASE_PORT="${BASE_PORT:-7280}"
PORT_STEP=10

BINARY="${BINARY:-$REPO_ROOT/quickwit/target/release/quickwit}"
WORK_DIR="/tmp/pomsky-local-searchers"

DD_API_KEY="${DD_API_KEY:?set DD_API_KEY to a staging API key}"
CP_SITE="${CP_SITE:-app.datad0g.com}"
QW_CLUSTER_ID="${QW_CLUSTER_ID:-local-batching-experiment}"
CP_BATCH_WINDOW_MS="${CP_BATCH_WINDOW_MS:-200}"
RUST_LOG="${RUST_LOG:-info,quickwit_search::batch=debug}"

die()  { echo "[ERROR] $*" >&2; exit 1; }
log()  { echo "[INFO]  $*"; }

[[ -x "$BINARY" ]] || die "binary not found at $BINARY — run: cd $REPO_ROOT/quickwit && cargo build --release -p quickwit-cli"

mkdir -p "$WORK_DIR/logs"

pids=()
cleanup() {
    log "Shutting down searchers..."
    for pid in "${pids[@]:-}"; do kill "$pid" 2>/dev/null || true; done
}
trap cleanup EXIT INT TERM

log "Starting $N searchers — cluster=$QW_CLUSTER_ID site=$CP_SITE batch_window=${CP_BATCH_WINDOW_MS}ms"
log "Logs → $WORK_DIR/logs/"
echo ""

for i in $(seq 1 "$N"); do
    port=$(( BASE_PORT + (i - 1) * PORT_STEP ))
    node_id="local-searcher-$i"
    data_dir="$WORK_DIR/data-$i"
    log_file="$WORK_DIR/logs/searcher-$i.log"
    cfg_file="$WORK_DIR/config-$i.yaml"

    mkdir -p "$data_dir"

    cat > "$cfg_file" <<YAML
version: 0.8
node_id: $node_id
listen_address: 127.0.0.1
rest:
  listen_port: $port
data_dir: $data_dir
cloudprem:
  enable_reverse_connection: true
  site: $CP_SITE
  dd_api_key: $DD_API_KEY
YAML

    CP_DISABLE_CERTIFICATE_VERIFICATION=true \
    CP_BATCH_WINDOW_MS="$CP_BATCH_WINDOW_MS" \
    QW_CLUSTER_ID="$QW_CLUSTER_ID" \
    RUST_LOG="$RUST_LOG" \
      "$BINARY" run \
        --config "$cfg_file" \
        --service searcher,metastore \
      > "$log_file" 2>&1 &

    pid=$!
    pids+=("$pid")
    log "searcher-$i  port=$port  pid=$pid  log=$log_file"
done

echo ""
log "All $N searchers started."
log ""
log "Experiment hints:"
log "  Goal A (uniform): each searcher log should show ~equal query counts"
log "  Goal B (batching): grep 'dispatching expired batch' — look for batch_size>1"
log "  Key log lines:"
log "    starting new batch      — first request of a group"
log "    appending request       — same-query request joined existing batch"
log "    dispatching expired batch batch_size=N — N requests dispatched together"
log "    executing combined batch search        — confirms merge happened"
echo ""
log "Ctrl-C to stop all searchers."
echo ""

# Tail all logs with searcher-N prefix
for i in $(seq 1 "$N"); do
    tail -f "$WORK_DIR/logs/searcher-$i.log" | sed "s/^/[searcher-$i] /" &
done
wait
