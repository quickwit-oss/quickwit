#!/bin/bash
# Cardinality Aggregation Test Script for Staging
# Sends logs with known distinct values on multiple field types,
# then queries cardinality via the Datadog UI timeseries API.

set -e

NAMESPACE="logs-cloudprem"
SERVICE="pomsky-staging-cloudprem-indexer"
LOCAL_PORT=7280
REMOTE_PORT=7280
INGEST_URL="http://localhost:${LOCAL_PORT}/api/v2/logs"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_highlight() { echo -e "${CYAN}[TEST]${NC} $1"; }

check_port_forward() {
    curl -s "http://localhost:${LOCAL_PORT}/health/livez" > /dev/null 2>&1
}

start_port_forward() {
    if check_port_forward; then
        log_info "Port-forward already active on port ${LOCAL_PORT}"
        return 0
    fi

    log_info "Starting port-forward to ${SERVICE}..."
    kubectl port-forward "svc/${SERVICE}" "${LOCAL_PORT}:${REMOTE_PORT}" -n "${NAMESPACE}" &
    PF_PID=$!

    for i in {1..10}; do
        sleep 1
        if check_port_forward; then
            log_info "Port-forward ready (PID: ${PF_PID})"
            return 0
        fi
    done

    log_error "Failed to establish port-forward"
    exit 1
}

send_logs() {
    local logs="$1"
    local description="$2"

    response=$(echo "$logs" | curl -s -w "\n%{http_code}" -X POST "${INGEST_URL}" \
        -H "Content-Type: application/json" \
        --data-binary @-)

    http_code=$(echo "$response" | tail -n1)

    if [ "$http_code" = "200" ]; then
        log_info "✓ Sent: ${description}"
    else
        log_error "✗ Failed (HTTP ${http_code}): ${description}"
        echo "$response" | head -5
    fi
}

minutes_ago() {
    local mins=$1
    date -u -v-${mins}M "+%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || \
        date -u -d "${mins} minutes ago" "+%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || \
        date -u "+%Y-%m-%dT%H:%M:%SZ"
}

# ============================================================================
# Generate logs with KNOWN distinct values for cardinality testing
# ============================================================================

generate_cardinality_test_logs() {
    log_highlight "=== Cardinality Test: Generating logs with known distinct values ==="

    # Batch 1: 20 logs with 10 distinct "user_id" values (text field)
    # and 10 distinct "response_time_ms" values (numeric field)
    # Each value appears exactly twice to ensure indexing.
    local logs="["
    local user_ids=("alice" "bob" "charlie" "diana" "eve" "frank" "grace" "heidi" "ivan" "judy")
    local response_times=(100 200 300 400 500 600 700 800 900 1000)

    for round in 1 2; do
        for i in {0..9}; do
            local idx=$((round * 10 + i))
            local user="${user_ids[$i]}"
            local rt="${response_times[$i]}"
            local ts=$(minutes_ago $((idx + 1)))
            local host="cardinality-host-$((i % 3 + 1))"

            [ $idx -gt 10 ] || [ $i -gt 0 ] && logs+=","
            local msg_json="{\\\"log_message\\\": \\\"cardinality test entry\\\", \\\"user_id\\\": \\\"${user}\\\", \\\"response_time_ms\\\": ${rt}, \\\"request_id\\\": \\\"req-${round}-${i}\\\", \\\"env\\\": \\\"staging\\\"}"
            logs+="{\"message\": \"${msg_json}\", \"service\": \"cardinality-test-svc\", \"hostname\": \"${host}\", \"ddsource\": \"cardinality-test\", \"status\": \"info\", \"ddtags\": \"env:staging,test:cardinality\"}"
        done
    done
    logs+="]"

    send_logs "$logs" "cardinality test batch 1: 20 logs, 10 distinct user_ids, 10 distinct response_times"

    # Batch 2: 15 logs with 5 distinct "error_code" values (numeric)
    # and 5 distinct "region" values (text)
    logs="["
    local error_codes=(400 401 403 404 500)
    local regions=("us-east-1" "us-west-2" "eu-west-1" "ap-southeast-1" "sa-east-1")

    for round in 1 2 3; do
        for i in {0..4}; do
            local idx=$((round * 5 + i))
            local ec="${error_codes[$i]}"
            local region="${regions[$i]}"
            local ts=$(minutes_ago $((idx + 1)))

            [ $idx -gt 5 ] || [ $i -gt 0 ] && logs+=","
            local msg_json="{\\\"log_message\\\": \\\"cardinality error test\\\", \\\"error_code\\\": ${ec}, \\\"region\\\": \\\"${region}\\\", \\\"env\\\": \\\"staging\\\"}"
            logs+="{\"message\": \"${msg_json}\", \"service\": \"cardinality-test-svc\", \"hostname\": \"cardinality-host-1\", \"ddsource\": \"cardinality-test\", \"status\": \"error\", \"ddtags\": \"env:staging,test:cardinality\"}"
        done
    done
    logs+="]"

    send_logs "$logs" "cardinality test batch 2: 15 logs, 5 distinct error_codes, 5 distinct regions"

    # Batch 3: Test with top-level fields that don't need JSON message parsing
    # These use 'hostname' and 'service' which are top-level indexed fields
    logs="["
    local services=("svc-alpha" "svc-beta" "svc-gamma" "svc-delta" "svc-epsilon" "svc-zeta" "svc-eta" "svc-theta")
    local hostnames=("host-aaa" "host-bbb" "host-ccc" "host-ddd" "host-eee" "host-fff")

    for round in 1 2; do
        for i in {0..7}; do
            local idx=$((round * 8 + i))
            local svc="${services[$i]}"
            local host="${hostnames[$((i % 6))]}"
            local ts=$(minutes_ago $((idx + 1)))

            [ $idx -gt 8 ] || [ $i -gt 0 ] && logs+=","
            logs+="{\"message\": \"top-level cardinality test entry ${idx}\", \"service\": \"${svc}\", \"hostname\": \"${host}\", \"ddsource\": \"cardinality-test\", \"status\": \"info\", \"ddtags\": \"env:staging,test:cardinality\"}"
        done
    done
    logs+="]"

    send_logs "$logs" "cardinality test batch 3: 16 logs, 8 distinct services, 6 distinct hostnames"

    echo ""
    log_highlight "=== Cardinality Test Data Summary ==="
    log_highlight "Total logs sent: 51"
    log_highlight "Expected cardinality values:"
    log_highlight "  - @user_id (text):          10 distinct"
    log_highlight "  - @response_time_ms (num):   10 distinct"
    log_highlight "  - @error_code (num):          5 distinct"
    log_highlight "  - @region (text):             5 distinct"
    log_highlight "  - service (top-level):        10 distinct (8 new + cardinality-test-svc from batches 1&2)"
    log_highlight "  - host (top-level):            8 distinct (6 new + 3 from batch 1)"
    echo ""
    log_highlight "Query filter: source:cardinality-test"
    log_highlight "  OR for service-only: source:cardinality-test service:svc-*"
}

print_query_examples() {
    echo ""
    log_highlight "=== Example Datadog UI Queries ==="
    echo ""
    log_info "In Datadog Logs UI (https://app.datad0g.com/logs), use:"
    log_info "  index: cloudprem-staging"
    log_info "  query: source:cardinality-test"
    log_info "  storage: cloud_prem (Hot)"
    echo ""
    log_info "Test cardinality aggregations:"
    log_info "  1. Timeseries: agg_t=cardinality, agg_m=host         → expect ~8"
    log_info "  2. Timeseries: agg_t=cardinality, agg_m=service      → expect ~10"
    log_info "  3. Timeseries: agg_t=cardinality, agg_m=@user_id     → expect ~10"
    log_info "  4. Timeseries: agg_t=cardinality, agg_m=@response_time_ms → expect ~10"
    log_info "  5. Timeseries: agg_t=cardinality, agg_m=@error_code  → expect ~5"
    log_info "  6. Timeseries: agg_t=cardinality, agg_m=@region      → expect ~5"
    echo ""
    log_info "Compare with working aggregations:"
    log_info "  7. Timeseries: agg_t=avg, agg_m=@response_time_ms    → expect ~550"
    log_info "  8. Timeseries: agg_t=count                            → expect 51"
    echo ""
    log_warn "If cardinality returns 0 on ALL fields (including host/service),"
    log_warn "the issue is in the Pomsky→Event-Query response path."
    log_warn "If cardinality works on host/service but not @user_id/@response_time_ms,"
    log_warn "the issue is field indexing/schema for nested JSON fields."
}

main() {
    echo ""
    log_highlight "========================================"
    log_highlight "  Cardinality Aggregation Staging Test"
    log_highlight "========================================"
    echo ""

    start_port_forward

    generate_cardinality_test_logs

    log_info "Waiting 20s for indexing..."
    sleep 20
    log_info "Data should now be searchable."

    print_query_examples
}

main "$@"
