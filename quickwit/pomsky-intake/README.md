# pomsky-intake

Vector-based intake service for Pomsky. It receives observability data (logs, metrics, traces) via
Datadog Agent, HTTP, and OpenTelemetry sources, preprocesses them through custom Vector transforms,
and forwards the results to the Quickwit BYOC ingest API.

## Pipeline

```
SOURCES                      TRANSFORMS                                    SINKS
───────────────────────      ───────────────────────────────────────       ──────────────────

Logs:
  datadog_agent.logs ──┐
  http_server ─────────┼──► preprocess_log ────► add_host_tags ─────────► logs_out (HTTP/JSON)
  otlp.logs ───────────┘

Metrics:
  datadog_agent.metrics ┐
  otlp.metrics ─────────┴─► preprocess_metric ──► add_host_tags ────────► metrics_out (Arrow IPC)

Traces:
  datadog_agent.traces ───► explode_trace_spans ─┐
  otlp.traces ──────────────────────────────────-┴► preprocess_trace ──► add_host_tags ──► traces_out (HTTP/JSON)
```

### Ports

| Port | Protocol | Source |
|------|----------|--------|
| 8181 | TCP | Datadog Agent |
| 8282 | HTTP | Generic HTTP |
| 8383 | gRPC | OTLP |
| 8384 | HTTP | OTLP |

## Configuration

The config file is a YAML file with the following fields (all optional with defaults):

```yaml
data_dir: /tmp/pomsky-intake
logs_endpoint: http://127.0.0.1:7280/api/datadog/v1/byoc/logs
metrics_endpoint: http://127.0.0.1:7280/api/datadog/v1/byoc/metrics
traces_endpoint: http://127.0.0.1:7280/api/datadog/v1/byoc/traces

# Datadog credentials — shared across all Datadog-backed pollers.
dd_site: datadoghq.com        # default: datadoghq.com; DD_SITE env var overrides
dd_api_key: <your-api-key>    # DD_API_KEY env var overrides

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

Both of these are **required** — the service panics at startup if either is unresolvable:

| Setting | Source | Default |
|---------|--------|---------|
| `dd_site` | config file (overridden by `DD_SITE` env var) | `datadoghq.com` |
| `dd_api_key` | config file (overridden by `DD_API_KEY` env var) | — (required) |

They live at the top level of the intake config — not under `host_tags:` — because they're shared
across all Datadog-backed pollers (host tags today, more to come).

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
