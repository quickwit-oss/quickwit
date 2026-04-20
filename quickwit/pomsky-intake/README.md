# pomsky-intake

Vector-based intake service for Pomsky. It receives observability data (logs, metrics, traces) via
Datadog Agent, HTTP, and OpenTelemetry sources, preprocesses them through custom Vector transforms,
and forwards the results to the Quickwit BYOC ingest API.

## Pipeline

```
SOURCES                      TRANSFORMS                 SINKS
───────────────────────      ──────────────────────     ──────────────────

Logs:
  datadog_agent.logs ──┐
  http_server ─────────┼──► preprocess_log ────────► logs_out (HTTP/JSON)
  otlp.logs ───────────┘

Metrics:
  datadog_agent.metrics ┐
  otlp.metrics ─────────┴─► preprocess_metric ────► metrics_out (Arrow IPC)

Traces:
  datadog_agent.traces ───► explode_trace_spans ─┐
  otlp.traces ──────────────────────────────────-┴► preprocess_trace ──► traces_out (HTTP/JSON)
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
