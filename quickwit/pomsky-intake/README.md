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
  connections ─────────────────► connections_to_apm_metrics ─┐
  datadog_agent.metrics ─────────────────────────────────────┼──► preprocess_metric ──► add_host_tags ──► metric_metadata ───► metrics_out (Arrow IPC)
  otlp.metrics ──────────────────────────────────────────────┘

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
sketches_endpoint: http://127.0.0.1:7280/api/datadog/v1/byoc/sketches
traces_endpoint: http://127.0.0.1:7280/api/datadog/v1/byoc/traces

# Datadog credentials — shared across all Datadog-backed pollers.
site: datadoghq.com              # default: datadoghq.com; DD_SITE env var overrides
api_key: <your-api-key>          # DD_API_KEY env var overrides

# Path to the CSV used by metric_metadata to persist its known-metrics set across restarts.
metric_metadata_persist_file_path: qwdata/intake/metric_metadata_known.csv  # default shown

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
- **Flush**: pending metrics are POSTed to the metadata service (URL derived from `site`) in
  batches (default 200) every `flush_interval_secs` (default 15 s). An early flush fires when
  the pending count reaches `batch_size`.
- **TTL**: each known metric expires after a random duration between `ttl_min_hours` (default 12 h)
  and `ttl_max_hours` (default 36 h) — jitter prevents thundering-herd re-submissions.
- **Persistence**: the known-metrics set is written to a CSV at `metric_metadata_persist_file_path`
  every `persist_interval_secs` (default 30 s) and loaded back on startup. Writes are crash-durable
  (write-to-temp + atomic rename). Expired entries loaded at startup are queued for immediate
  re-submission.
- **Startup validation**: `DD_API_KEY` and the parent directory of `metric_metadata_persist_file_path`
  must be accessible at startup — misconfiguration fails fast with a descriptive error.

### Credentials

The API key and metadata service URL (derived from `site`) are resolved by `pomsky-intake`
once at startup and interpolated into the transform's Vector config.

## USM APM metrics extraction

The `connections_to_apm_metrics` transform processes Datadog Agent `CollectorConnections` payloads
received on port 8585 and emits APM-style metrics into the metrics pipeline.

### What it does

- **Protocol parsers**: extracts per-connection protocol stats for HTTP, HTTP/2, gRPC, Kafka,
  Postgres, and Redis. Each parser yields `ProtoStat` records carrying service name, status code,
  latency sketch bytes, and hit counts.
- **Service resolution**: resolves a service name for each connection using a priority hierarchy
  (service tag from container tags → service tag from host tags → NSX-inferred name → container ID
  prefix → hostname fallback). Direction fixups mirror the NSX heuristics from the Go reference
  implementation.
- **Emitted metrics**:
  - `universal.<proto>.<dir>.hits` — count metric per protocol and direction (inbound/outbound)
  - `universal.<proto>.<dir>` — distribution sketch (DDSketch) per protocol and direction
  - `trace.services_by_operation` — service × operation family for service list discovery
- **Safety cap**: payloads with more than 1,000,000 connections are rejected before parsing to
  bound memory use. Incoming request bodies are capped at 64 MiB.

### Envelope stripping

The `connections` source accepts V3–V8 agent envelopes and handles both zstd and uncompressed
bodies. The agent timestamp is recovered from the envelope header and attached to each emitted
event; intake time is used as a fallback when the header carries no timestamp.

## Trace processing

### Chunk-level field propagation

`preprocess_dd_trace` runs before `explode_trace_spans` and copies chunk-level `host` and `env`
fields into each span's `meta` map (as `meta._dd.hostname` and `meta.env`) so they survive the
explode step, which only carries span-local data. Per-span values take precedence; chunk-level
values are only written when the span's meta key is absent.

### Span normalization (`preprocess_span`)

After `explode_trace_spans`, each Datadog agent span event is normalized by `preprocess_span`:

**Timestamps**

| Field | Type | Value |
|-------|------|-------|
| `start_time` | i64 (unix ns) | span start, full nanosecond precision |
| `timestamp` | string (RFC 3339, ms, Z) | span end = `floor((start + duration) / 1e6)` — the index's `timestamp_field` |
| `discovery_timestamp` | i64 (unix ms) | when intake observed the span |

The doc is dropped at indexing time if `timestamp` is absent.

**IDs**

All IDs are normalized to unsigned 64-bit decimal strings:

| Wire field | Emitted field(s) | Notes |
|-----------|-----------------|-------|
| `trace_id` (i64) | `trace_id`, `trace_id_low` | Both hold the same lower-64-bit decimal value. `trace_id_low` is kept for schema compatibility with SaaS docs where the upper 64 bits travel separately. |
| `span_id` (i64) | `span_id` | Unsigned decimal |
| `parent_id` (i64) | `parent_id` | Unsigned decimal |

### Schema remapping (`remap_dd_span_to_schema`)

After normalization, spans are remapped to the `datadog-spans` index schema:

| Operation | Detail |
|-----------|--------|
| Rename | `name` → `operation_name`, `resource` → `resource_name` |
| Status | `status` derived from wire `error` flag: 0 → `"ok"`, non-zero → `"error"` |
| Error type | `meta.error.type` lifted to top-level `error.type` |
| Host / env | `meta._dd.hostname` → `host`, `meta.env` → `env` |
| Resource hash | `resource_hash` = lower 64 bits of murmur3_x64_128 over the resource string, as hex |
| Fixed fields | `single_span` and `analytics_enabled` set to `false`; `tiebreaker` set to a random positive integer |
| Catch-all | `meta`, `metrics`, `meta_struct` (msgpack-decoded), `duration`, `span_links`, and `span_events` are folded into `custom`. The index declares `custom` with `expand_dots: true`, so dotted keys like `_dd.agent_version` are nested at indexing time. |
| Cleanup | The leftover `start` Timestamp field is dropped (already extracted into `start_time`) |

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
