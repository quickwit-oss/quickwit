#!/bin/bash
# Staging Log Ingest Script
# This script port-forwards to the staging cluster and ingests test logs
# for testing various search and aggregation queries.

set -e

NAMESPACE="logs-cloudprem"
SERVICE="pomsky-staging-cloudprem-indexer"
LOCAL_PORT=7280
REMOTE_PORT=7280
INGEST_URL="http://localhost:${LOCAL_PORT}/api/v2/logs"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Check if port-forward is already running
check_port_forward() {
    if curl -s "http://localhost:${LOCAL_PORT}/health/livez" > /dev/null 2>&1; then
        return 0
    fi
    return 1
}

# Start port-forward in background
start_port_forward() {
    if check_port_forward; then
        log_info "Port-forward already active on port ${LOCAL_PORT}"
        return 0
    fi

    log_info "Starting port-forward to ${SERVICE}..."
    kubectl port-forward "svc/${SERVICE}" "${LOCAL_PORT}:${REMOTE_PORT}" -n "${NAMESPACE}" &
    PF_PID=$!

    # Wait for port-forward to be ready
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

# Send logs to the Datadog API endpoint
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
        log_error "✗ Failed to send: ${description} (HTTP ${http_code})"
    fi
}

# Generate timestamp for N minutes ago
minutes_ago() {
    local mins=$1
    # macOS uses -v flag, Linux uses -d flag
    date -u -v-${mins}M "+%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || \
        date -u -d "${mins} minutes ago" "+%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || \
        date -u "+%Y-%m-%dT%H:%M:%SZ"
}

# =============================================================================
# LOG GENERATORS - Add your custom log patterns here
# =============================================================================

generate_basic_logs() {
    log_info "Generating basic logs with different statuses..."

    local now=$(date -u "+%Y-%m-%dT%H:%M:%SZ")

    send_logs '[
        {"message": "Application started successfully", "service": "api-gateway", "hostname": "server-01", "ddsource": "staging-test", "status": "info", "ddtags": "env:staging,version:1.0.0"},
        {"message": "User authentication successful", "service": "auth-service", "hostname": "server-02", "ddsource": "staging-test", "status": "info", "ddtags": "env:staging,version:1.0.0"},
        {"message": "Database connection established", "service": "db-service", "hostname": "server-03", "ddsource": "staging-test", "status": "info", "ddtags": "env:staging,version:1.0.0"},
        {"message": "Cache miss for key user:123", "service": "cache-service", "hostname": "server-01", "ddsource": "staging-test", "status": "warn", "ddtags": "env:staging,version:1.0.0"},
        {"message": "Request timeout after 30s", "service": "api-gateway", "hostname": "server-02", "ddsource": "staging-test", "status": "error", "ddtags": "env:staging,version:1.0.0"},
        {"message": "Memory usage at 85%", "service": "monitor-service", "hostname": "server-03", "ddsource": "staging-test", "status": "warn", "ddtags": "env:staging,version:1.0.0"},
        {"message": "Failed to connect to external API", "service": "integration-service", "hostname": "server-01", "ddsource": "staging-test", "status": "error", "ddtags": "env:staging,version:1.0.0"},
        {"message": "Debug: Processing request ID 12345", "service": "api-gateway", "hostname": "server-02", "ddsource": "staging-test", "status": "debug", "ddtags": "env:staging,version:1.0.0"}
    ]' "basic logs (8 entries)"
}

generate_service_logs() {
    log_info "Generating logs for multiple services (for service aggregation)..."

    # Different services with varying log counts
    local services=("payment-service" "order-service" "inventory-service" "notification-service" "user-service")
    local statuses=("info" "info" "info" "warn" "error")
    local counts=(10 8 6 4 2)

    for i in "${!services[@]}"; do
        local svc="${services[$i]}"
        local count="${counts[$i]}"
        local logs="["

        for j in $(seq 1 $count); do
            local status="${statuses[$((j % 5))]}"
            local ts=$(minutes_ago $((j * 2)))
            [ $j -gt 1 ] && logs+=","
            logs+="{\"message\": \"Log entry $j from ${svc}\", \"service\": \"${svc}\", \"hostname\": \"server-0$((j % 3 + 1))\", \"ddsource\": \"staging-test\", \"status\": \"${status}\", \"ddtags\": \"env:staging,team:platform\"}"
        done
        logs+="]"

        send_logs "$logs" "${svc} (${count} entries)"
    done
}

generate_error_logs() {
    log_info "Generating error logs with different error types..."

    # Message is a JSON string that becomes the 'custom' field after parsing
    send_logs '[
        {"message": "{\"log_message\": \"NullPointerException in UserController.getUser()\", \"error_type\": \"NullPointerException\", \"error_code\": 500, \"env\": \"staging\"}", "service": "user-service", "hostname": "server-01", "ddsource": "staging-test", "status": "error"},
        {"message": "{\"log_message\": \"SQLException: Connection refused to database\", \"error_type\": \"SQLException\", \"error_code\": 503, \"env\": \"staging\"}", "service": "db-service", "hostname": "server-02", "ddsource": "staging-test", "status": "error"},
        {"message": "{\"log_message\": \"TimeoutException: Request timed out after 30000ms\", \"error_type\": \"TimeoutException\", \"error_code\": 504, \"timeout_ms\": 30000, \"env\": \"staging\"}", "service": "api-gateway", "hostname": "server-03", "ddsource": "staging-test", "status": "error"},
        {"message": "{\"log_message\": \"OutOfMemoryError: Java heap space\", \"error_type\": \"OutOfMemoryError\", \"error_code\": 500, \"memory_used_mb\": 8192, \"env\": \"staging\"}", "service": "batch-processor", "hostname": "server-01", "ddsource": "staging-test", "status": "error"},
        {"message": "{\"log_message\": \"IOException: Failed to read file /data/config.json\", \"error_type\": \"IOException\", \"error_code\": 500, \"env\": \"staging\"}", "service": "config-service", "hostname": "server-02", "ddsource": "staging-test", "status": "error"},
        {"message": "{\"log_message\": \"AuthenticationException: Invalid credentials\", \"error_type\": \"AuthenticationException\", \"error_code\": 401, \"env\": \"staging\"}", "service": "auth-service", "hostname": "server-03", "ddsource": "staging-test", "status": "error"},
        {"message": "{\"log_message\": \"RateLimitException: Too many requests\", \"error_type\": \"RateLimitException\", \"error_code\": 429, \"requests_per_sec\": 1500, \"env\": \"staging\"}", "service": "api-gateway", "hostname": "server-01", "ddsource": "staging-test", "status": "error"},
        {"message": "{\"log_message\": \"ValidationException: Invalid email format\", \"error_type\": \"ValidationException\", \"error_code\": 400, \"env\": \"staging\"}", "service": "user-service", "hostname": "server-02", "ddsource": "staging-test", "status": "error"}
    ]' "error logs with types (8 entries)"
}

generate_http_logs() {
    log_info "Generating HTTP request logs (for status code aggregation)..."

    local endpoints=("/api/users" "/api/orders" "/api/products" "/api/payments" "/api/health")
    local methods=("GET" "POST" "PUT" "DELETE" "GET")
    local status_codes=(200 201 400 404 500 200 200 200 204 401)
    local logs="["

    for i in {1..30}; do
        local endpoint="${endpoints[$((i % 5))]}"
        local method="${methods[$((i % 5))]}"
        local code="${status_codes[$((i % 10))]}"
        local duration=$((RANDOM % 1000 + 50))
        local request_size=$((RANDOM % 10000 + 100))
        local response_size=$((RANDOM % 50000 + 500))
        local ts=$(minutes_ago $i)

        local log_status="info"
        [ $code -ge 400 ] && log_status="warn"
        [ $code -ge 500 ] && log_status="error"

        [ $i -gt 1 ] && logs+=","
        # Message is a JSON string that becomes the 'custom' field after parsing
        local msg_json="{\\\"log_message\\\": \\\"${method} ${endpoint} - ${code} - ${duration}ms\\\", \\\"http_method\\\": \\\"${method}\\\", \\\"http_status\\\": ${code}, \\\"endpoint\\\": \\\"${endpoint}\\\", \\\"response_time_ms\\\": ${duration}, \\\"request_size_bytes\\\": ${request_size}, \\\"response_size_bytes\\\": ${response_size}, \\\"env\\\": \\\"staging\\\"}"
        logs+="{\"message\": \"${msg_json}\", \"service\": \"api-gateway\", \"hostname\": \"lb-0$((i % 2 + 1))\", \"ddsource\": \"http-access\", \"status\": \"${log_status}\"}"
    done
    logs+="]"

    send_logs "$logs" "HTTP access logs (30 entries)"
}

generate_metric_logs() {
    log_info "Generating metric-style logs (for numeric aggregation: min/max/avg/percentile)..."

    local logs="["
    local hostnames=("server-01" "server-02" "server-03" "server-04")
    local regions=("us-east-1" "us-west-2" "eu-west-1" "ap-northeast-1")

    # Generate 50 logs with varied numeric values for better aggregation testing
    # IMPORTANT: The 'message' field must be a JSON STRING - it gets parsed and stored in 'custom'
    for i in {1..50}; do
        local cpu=$((RANDOM % 100))
        local memory=$((RANDOM % 100))
        local latency=$((RANDOM % 500 + 10))
        local requests=$((RANDOM % 1000 + 100))
        local error_count=$((RANDOM % 10))
        local queue_depth=$((RANDOM % 100))
        local connections=$((RANDOM % 500 + 10))
        local disk_usage=$((RANDOM % 100))
        local hostname="${hostnames[$((i % 4))]}"
        local region="${regions[$((i % 4))]}"
        local ts=$(minutes_ago $i)

        [ $i -gt 1 ] && logs+=","
        # Message is a JSON string that becomes the 'custom' field after parsing
        local msg_json="{\\\"log_message\\\": \\\"System metrics report\\\", \\\"cpu_percent\\\": ${cpu}, \\\"memory_percent\\\": ${memory}, \\\"latency_ms\\\": ${latency}, \\\"request_count\\\": ${requests}, \\\"error_count\\\": ${error_count}, \\\"queue_depth\\\": ${queue_depth}, \\\"active_connections\\\": ${connections}, \\\"disk_usage_percent\\\": ${disk_usage}, \\\"region\\\": \\\"${region}\\\", \\\"env\\\": \\\"staging\\\"}"
        logs+="{\"message\": \"${msg_json}\", \"service\": \"metrics-collector\", \"hostname\": \"${hostname}\", \"ddsource\": \"metrics\", \"status\": \"info\"}"
    done
    logs+="]"

    send_logs "$logs" "metric logs (50 entries)"
}

generate_user_activity_logs() {
    log_info "Generating user activity logs (for user/action aggregation)..."

    local users=("user_001" "user_002" "user_003" "user_004" "user_005")
    local actions=("login" "logout" "view_page" "click_button" "submit_form" "api_call")
    local pages=("/home" "/dashboard" "/settings" "/profile" "/checkout")
    local logs="["

    for i in {1..25}; do
        local user="${users[$((RANDOM % 5))]}"
        local action="${actions[$((RANDOM % 6))]}"
        local page="${pages[$((RANDOM % 5))]}"
        local session_duration=$((RANDOM % 3600 + 60))
        local page_load_time=$((RANDOM % 5000 + 100))
        local ts=$(minutes_ago $((i * 2)))

        [ $i -gt 1 ] && logs+=","
        # Message is a JSON string that becomes the 'custom' field after parsing
        local msg_json="{\\\"log_message\\\": \\\"User ${user} performed ${action}\\\", \\\"user_id\\\": \\\"${user}\\\", \\\"action\\\": \\\"${action}\\\", \\\"page\\\": \\\"${page}\\\", \\\"session_duration_sec\\\": ${session_duration}, \\\"page_load_time_ms\\\": ${page_load_time}, \\\"env\\\": \\\"staging\\\"}"
        logs+="{\"message\": \"${msg_json}\", \"service\": \"activity-tracker\", \"hostname\": \"web-0$((i % 2 + 1))\", \"ddsource\": \"user-activity\", \"status\": \"info\"}"
    done
    logs+="]"

    send_logs "$logs" "user activity logs (25 entries)"
}

generate_distributed_trace_logs() {
    log_info "Generating distributed trace logs (for trace aggregation)..."

    local trace_ids=("trace_abc123" "trace_def456" "trace_ghi789")
    local services=("frontend" "api-gateway" "user-service" "db-service" "cache-service")
    local logs="["
    local count=1

    for trace_id in "${trace_ids[@]}"; do
        for svc in "${services[@]}"; do
            local span_id="span_$(printf '%06d' $RANDOM)"
            local duration=$((RANDOM % 100 + 5))
            local db_queries=$((RANDOM % 20))
            local cache_hits=$((RANDOM % 50))
            local ts=$(minutes_ago $count)

            [ $count -gt 1 ] && logs+=","
            # Message is a JSON string that becomes the 'custom' field after parsing
            local msg_json="{\\\"log_message\\\": \\\"[${trace_id}] ${svc} processed request\\\", \\\"trace_id\\\": \\\"${trace_id}\\\", \\\"span_id\\\": \\\"${span_id}\\\", \\\"duration_ms\\\": ${duration}, \\\"db_query_count\\\": ${db_queries}, \\\"cache_hit_count\\\": ${cache_hits}, \\\"env\\\": \\\"staging\\\"}"
            logs+="{\"message\": \"${msg_json}\", \"service\": \"${svc}\", \"hostname\": \"k8s-node-0$((count % 3 + 1))\", \"ddsource\": \"distributed-trace\", \"status\": \"info\"}"
            ((count++))
        done
    done
    logs+="]"

    send_logs "$logs" "distributed trace logs (15 entries)"
}

# =============================================================================
# CUSTOM LOG FUNCTION - Use this to add your own logs
# =============================================================================

send_custom_logs() {
    if [ -z "$1" ]; then
        log_error "Usage: $0 custom '<json_array_of_logs>'"
        log_info "Example: $0 custom '[{\"message\": \"test\", \"service\": \"my-svc\", \"hostname\": \"host1\", \"ddsource\": \"test\", \"status\": \"info\"}]'"
        exit 1
    fi

    send_logs "$1" "custom logs"
}

# =============================================================================
# EXAMPLE QUERIES - Print example aggregation queries
# =============================================================================

print_example_queries() {
    echo ""
    log_info "=============================================="
    log_info "Example Search & Aggregation Queries"
    log_info "=============================================="
    echo ""
    echo "=== In Datadog UI (index:cloudprem-staging) ==="
    echo ""
    echo "1. Filter by service:        service:api-gateway"
    echo "2. Filter by status:         status:error"
    echo "3. Filter by source:         source:metrics"
    echo "4. Combined filters:         service:api-gateway status:error"
    echo "5. Search custom fields:     custom.error_type:SQLException"
    echo "                             custom.http_status:500"
    echo "                             custom.latency_ms:>100"
    echo ""
    echo "=== Via curl (use POST for aggregations) ==="
    echo ""
    echo "# Search custom fields:"
    echo "curl -s 'http://localhost:7280/api/v1/datadog/search?query=custom.error_type:SQLException&max_hits=5' | jq"
    echo ""
    echo "# Terms aggregation (count by error_type):"
    cat << 'CURL_EOF'
curl -s -X POST 'http://localhost:7280/api/v1/datadog/search' \
  -H 'Content-Type: application/json' \
  -d '{"query": "custom.error_type:*", "max_hits": 0, "aggs": {"by_error_type": {"terms": {"field": "custom.error_type"}}}}' | jq '.aggregations'
CURL_EOF
    echo ""
    echo "# Average latency:"
    cat << 'CURL_EOF'
curl -s -X POST 'http://localhost:7280/api/v1/datadog/search' \
  -H 'Content-Type: application/json' \
  -d '{"query": "source:metrics", "max_hits": 0, "aggs": {"avg_latency": {"avg": {"field": "custom.latency_ms"}}}}' | jq '.aggregations'
CURL_EOF
    echo ""
    echo "# Percentiles (p50, p90, p95, p99):"
    cat << 'CURL_EOF'
curl -s -X POST 'http://localhost:7280/api/v1/datadog/search' \
  -H 'Content-Type: application/json' \
  -d '{"query": "source:metrics", "max_hits": 0, "aggs": {"latency_pct": {"percentiles": {"field": "custom.latency_ms", "percents": [50, 90, 95, 99]}}}}' | jq '.aggregations'
CURL_EOF
    echo ""
    echo "# Stats (count, min, max, avg, sum):"
    cat << 'CURL_EOF'
curl -s -X POST 'http://localhost:7280/api/v1/datadog/search' \
  -H 'Content-Type: application/json' \
  -d '{"query": "source:metrics", "max_hits": 0, "aggs": {"cpu_stats": {"stats": {"field": "custom.cpu_percent"}}}}' | jq '.aggregations'
CURL_EOF
    echo ""
}

# =============================================================================
# MAIN
# =============================================================================

show_help() {
    echo "Staging Log Ingest Script"
    echo ""
    echo "Usage: $0 [command]"
    echo ""
    echo "Commands:"
    echo "  all           Generate all test logs (default)"
    echo "  basic         Generate basic logs with different statuses"
    echo "  services      Generate logs for multiple services"
    echo "  errors        Generate error logs with different types"
    echo "  http          Generate HTTP request logs"
    echo "  metrics       Generate metric-style logs"
    echo "  users         Generate user activity logs"
    echo "  traces        Generate distributed trace logs"
    echo "  custom '<json>' Send custom logs (JSON array)"
    echo "  queries       Print example search/aggregation queries"
    echo "  port-forward  Only start port-forward (keep running)"
    echo "  help          Show this help message"
    echo ""
    echo "Prerequisites:"
    echo "  - kubectl configured with access to vaporeon-b.us1.staging.dog"
    echo "  - kubectx vaporeon-b.us1.staging.dog (run first)"
    echo ""
}

main() {
    local command="${1:-all}"

    case "$command" in
        help|--help|-h)
            show_help
            exit 0
            ;;
        queries)
            print_example_queries
            exit 0
            ;;
        port-forward)
            start_port_forward
            log_info "Port-forward running. Press Ctrl+C to stop."
            wait
            ;;
        custom)
            start_port_forward
            send_custom_logs "$2"
            ;;
        basic)
            start_port_forward
            generate_basic_logs
            ;;
        services)
            start_port_forward
            generate_service_logs
            ;;
        errors)
            start_port_forward
            generate_error_logs
            ;;
        http)
            start_port_forward
            generate_http_logs
            ;;
        metrics)
            start_port_forward
            generate_metric_logs
            ;;
        users)
            start_port_forward
            generate_user_activity_logs
            ;;
        traces)
            start_port_forward
            generate_distributed_trace_logs
            ;;
        all)
            start_port_forward
            echo ""
            log_info "=============================================="
            log_info "Generating all test logs..."
            log_info "=============================================="
            echo ""
            generate_basic_logs
            generate_service_logs
            generate_error_logs
            generate_http_logs
            generate_metric_logs
            generate_user_activity_logs
            generate_distributed_trace_logs
            echo ""
            log_info "=============================================="
            log_info "All logs sent! Waiting 15s for indexing..."
            log_info "=============================================="
            sleep 15
            log_info "Logs should now be searchable in Datadog UI"
            log_info "Use: index:cloudprem-staging source:staging-test"
            print_example_queries
            ;;
        *)
            log_error "Unknown command: $command"
            show_help
            exit 1
            ;;
    esac
}

main "$@"
