# pomsky-intake

Vector-based intake service for Pomsky. It receives observability data (logs, metrics, traces) via
Datadog Agent, HTTP, and OpenTelemetry sources, preprocesses them through custom Vector transforms,
and forwards the results to the Quickwit BYOC ingest API.

## Pipeline

```
SOURCES                      TRANSFORMS                                                     SINKS
───────────────────────      ──────────────────────────────────────────────────────────    ──────────────────

Logs:
  datadog_agent.logs ──┐
  http_server ─────────┼──► preprocess_log ────► add_host_tags ──────────────────────────► logs_out (HTTP/JSON)
  otlp.logs ───────────┘

Metrics:
  datadog_agent.metrics ─┐
  otlp.metrics ──────────┼──► preprocess_metric ──► add_host_tags ──► metric_metadata ───► metrics_out (Arrow IPC)
  connections ───────────┘  (via connections_to_apm_metrics)

Traces:
  datadog_agent.traces ───► preprocess_dd_trace ──► explode_trace_spans ─┐
  otlp.traces ───────────────────────────────────────────────────────────┴► preprocess_span ──► add_host_tags ──► traces_out (HTTP/JSON)
```

### Ports

| Port | Protocol | Source |
|------|----------|--------|
| 8181 | TCP | Datadog Agent (logs, metrics, traces) |
| 8282 | HTTP | Generic HTTP |
| 8383 | gRPC | OTLP |
| 8384 | HTTP | OTLP |
| 8585 | TCP | Agent CollectorConnections (APM metrics) |
| 8686 | HTTP | Vector API (health check) |

## Configuration

The config file is a YAML file with the following fields (all optional with defaults):

```yaml
data_dir: qwdata/intake          # default: qwdata/intake
logs_endpoint: http://127.0.0.1:7280/api/datadog/v1/byoc/logs
metrics_endpoint: http://127.0.0.1:7280/api/datadog/v1/byoc/metrics
traces_endpoint: http://127.0.0.1:7280/api/datadog/v1/byoc/traces

# Datadog credentials — shared across all Datadog-backed pollers.
site: datadoghq.com              # default: datadoghq.com; DD_SITE env var overrides
api_key: <your-api-key>          # DD_API_KEY env var overrides

# Organization identifier and metadata service URL — used by metric_metadata.
org_id: default                  # default: "default"
metadata_svc_url: http://localhost:9999  # default: http://localhost:9999

host_tags:
  poll_interval_secs: 15         # default: 15
  fetch_timeout_secs: 10         # default: 10; must be < poll_interval_secs
  ttl_min_secs: 900              # default: 900  (15 min)
  ttl_max_secs: 3600             # default: 3600 (60 min)
  cache_path: /var/lib/pomsky-intake/host_tags.ndjson  # optional
```

## Host-tags enrichment

A background task polls the Datadog metadata service for host tags and merges them into each
event:

- **Metrics**: tags are added as additional metric tags (hostname read from the `host` tag).
- **Logs**: tags are added under the `tags` object (hostname read from the `hostname` field).
- **Traces**: tags are added under the `meta` object (hostname read from `meta.host`).

Existing keys are never overwritten — the transform only fills in missing keys.

### Credentials

| Config key | Env var override | Default | Required |
|------------|-----------------|---------|----------|
| `site` | `DD_SITE` | `datadoghq.com` | no |
| `api_key` | `DD_API_KEY` | — | **yes** |

`api_key` is required — the service fails at startup if it cannot be resolved from either the
config file or `DD_API_KEY`. Both keys live at the top level of the intake config (not under
`host_tags:`) because they're shared across all Datadog-backed pollers.

The metadata service URL is built as `https://{dd_site}/api/unstable/byoc/ingest/metadata/host-tags`.

### Runtime behavior

- **Unknown hosts**: on a cache miss, the hostname is queued in an in-memory FIFO collector. The
  background poller drains the collector each cycle and fetches tags for those hosts.
- **TTL**: each host is re-fetched after a random TTL between `ttl_min_secs` and `ttl_max_secs`
  (jitter avoids thundering-herd refreshes). Expiries are tracked as unix-seconds, matching the
  on-disk format.
- **Batching**: up to 200 hostnames per request to the metadata service.
- **Fetch timeout**: each HTTP call is capped at `fetch_timeout_secs`. This must be strictly less
  than `poll_interval_secs` — startup panics if that invariant is violated — so a slow fetch can
  never overlap the next cycle.
- **Persistence**: when `cache_path` is set, the poller loads non-expired entries on startup and
  rewrites the file after each successful fetch via write-to-temp + `sync_data` + atomic rename +
  parent-dir `sync_all`. Expired entries are loaded as stale fallback and queued for immediate
  re-fetch. Crash-durable: if the process dies mid-write, the target path either holds the previous
  good state or the fully-written new state — never a truncated or empty file.
- **Lookups are lock-free**: the store uses `ArcSwap`, so the Vector hot path reads tags without
  blocking on the poller. Tag lists are shared via `Arc<[HostTag]>`, so `lookup` is one
  atomic-refcount bump with no string allocation.

## Metric metadata collection

The `metric_metadata` transform sits at the end of the metrics pipeline (after `add_host_tags`,
before the Arrow IPC sink). It tracks which `(metric_name, type)` pairs have been reported to
`byoc-ingest-metadata-svc` so that the service is notified about new metrics without re-sending
known ones every cycle.

### How it works

- **Deduplication**: each metric event is checked against an in-memory `KnownMetrics` set. Only
  metrics unseen (or whose TTL has expired) are queued for flushing.
- **Flush**: pending metrics are POSTed to `metadata_svc_url` in batches (default 200) every
  `flush_interval_secs` (default 15 s). An early flush fires when the pending count reaches
  `batch_size`.
- **TTL**: each known metric expires after a random duration between `ttl_min_hours` (default 12 h)
  and `ttl_max_hours` (default 36 h) — jitter prevents thundering-herd re-submissions.
- **Persistence**: the known-metrics set is written to a CSV at `persist_file_path` every
  `persist_interval_secs` (default 30 s) and loaded back on startup. Writes are crash-durable
  (write-to-temp + atomic rename). Expired entries loaded at startup are queued for immediate
  re-submission.
- **Startup validation**: `DD_API_KEY` and the parent directory of `persist_file_path` must be
  accessible at startup — misconfiguration fails fast with a descriptive error.

### Credentials

The transform reads `DD_API_KEY` directly from the environment; `org_id` and `metadata_svc_url`
are supplied from the intake config file.

## Local test tools

`pomsky-intake/local-test/` contains scripts for running a full end-to-end test against a local
Quickwit/Pomsky instance without deploying to any environment.

```
local-test/
├── test-pomsky.sh          # Orchestrator: starts Quickwit + pomsky-intake and drives traffic
├── intake-local.yaml       # pomsky-intake config for local testing
├── quickwit-local.yaml     # Quickwit config for local testing
├── generate-test-logs.py   # Sends synthetic log events
├── generate-test-metrics.py  # Sends synthetic metric events (protobuf)
├── generate-test-traces.py   # Sends synthetic trace payloads (protobuf)
├── upload-test-data.py     # Bulk-uploads pre-recorded test data
├── dd_metrics.proto        # Protobuf schema for DD metrics wire format
├── dd_metrics_pb2.py       # Generated bindings for dd_metrics.proto
└── dd_trace_pb2.py         # Generated bindings for DD trace proto
```

Usage:

```bash
# Run from the repo root — the script resolves paths from $GOPATH
export DD_API_KEY=<your-key>
cd quickwit/pomsky-intake/local-test
./test-pomsky.sh [-f <freq_secs>] [-c <count>] [-p] [-h] [-m]
  # -f  send signals every <freq_secs> seconds (default: 1)
  # -c  stop after <count> rounds (default: 10)
  # -p  use local Pomsky instead of the stub sink server
  # -h  start a local byoc-hosttags-mgr
  # -m  start a local byoc-metrics-metadata service
```

## Usage

```bash
cargo run -p pomsky-intake -- --config <path-to-config.yaml>
```

## Why a separate binary?

pomsky-intake is built as its own binary rather than being compiled into the main `quickwit` binary.
This is a deliberate choice driven by a dependency conflict.

Vector enables the `serde_json/preserve_order` feature, which replaces `BTreeMap` with `IndexMap`
as the backing store for `serde_json::Map`. Quickwit avoids `IndexMap` for performance reasons
and enforces this with a compile-time canary in `quickwit-storage`.

Because Cargo unifies features across all crates in a single build, compiling Vector and Quickwit
together would activate `preserve_order` workspace-wide, breaking the canary. Splitting intake into
a separate binary with its own `cargo build -p pomsky-intake` invocation keeps the feature sets
isolated.

For the same reason, `pomsky-intake` is listed in the workspace `members` but excluded from
`default-members`. This means:

- `cargo build` / `cargo run` builds Quickwit without Vector's features leaking in.
- `cargo build -p pomsky-intake` builds the intake binary explicitly.
