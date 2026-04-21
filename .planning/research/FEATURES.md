# Feature Landscape: Metric Metadata Tracking Transform

**Domain:** In-process Vector transform for tracking and submitting new metric metadata
**Researched:** 2026-04-16
**Confidence:** HIGH (primary source is the Go service being replaced, verified against Go service source and Vector YAML pipeline)

## Context

The Go service (byoc-metrics-metadata) being replaced has three cooperating processes:
1. Vector YAML pipeline tags metrics as new using an enrichment table lookup against known_metrics.csv
2. Vector writes new metric metadata to a file (NDJSON)
3. Go service polls that file, submits to SaaS, updates the CSV

The Rust transform collapses all three into a single in-process component. The feature set is therefore not speculative — it is derived directly from the Go service source and the five YAML nodes being retired.

---

## Feature Landscape

### Table Stakes (Users Expect These)

Features that must exist for the transform to function at all. Absence means the transform cannot be deployed in place of the Go service.

| # | Feature | Why Expected | Complexity | Notes |
|---|---------|-------------|------------|-------|
| TS-1 | **In-memory HashMap of known metric names with expiry** | The core novelty detection mechanism. Without this, every metric is "new" on every event. The Go service maintains `map[string]int64` (name -> expiresAt unix). | MEDIUM | Must be bounded by TTL — entries expire after 12-36h (randomized to prevent thundering herd). Cap at batchSize on overflow: discard excess, they will be re-detected next cycle. |
| TS-2 | **TTL-based expiry with randomized range** | Metrics must re-submit periodically so SaaS metadata stays current. Fixed TTL would cause synchronized re-submission storms for all known metrics. Go service uses `[12h, 36h)` randomized range. | LOW | TTL is assigned at submission time (when metric is acknowledged by SaaS), not at detection time. Configurable via `ttl_min` / `ttl_max` in transform config. |
| TS-3 | **Metric type mapping (counter/gauge/sketch → API types)** | The SaaS endpoint expects normalized metric types (`count`, `rate`, `gauge`, `ddsketch`), not raw Vector types. The mapping is non-trivial for counters: counter without `interval_ms` → `count` (interval=10), counter with `interval_ms` → `rate` (interval=interval_ms/1000). | LOW | Exactly as implemented in `metric_metadata_remap` YAML node. The interval_ms ambiguity (Datadog COUNT vs RATE both become `counter` in Vector) is a known limitation documented in the YAML. |
| TS-4 | **In-memory pending list for detected new metrics** | New metrics accumulate between flush cycles. Must hold `MetricEntry` structs (name, type, interval) until batch threshold or flush interval fires. | LOW | Distinct from the known-metrics HashMap. The pending list is candidates awaiting SaaS acknowledgement. Known metrics = already acknowledged. |
| TS-5 | **Batch flush to byoc-ingest-metadata-svc on interval** | The SaaS endpoint is not meant to be hit per-event. Go service flushes on configurable interval (default 5s in config, PROJECT.md says 15s for Rust). | MEDIUM | Flush is triggered by: (a) interval timer, or (b) size threshold (default 200 metrics). Must run asynchronously — the Vector event processing loop must not block on HTTP. |
| TS-6 | **HTTP POST with DD-API-KEY header and org_id body** | The SaaS API contract. Request body: `{org_id: string, records: [{metric_name, metric_type, interval}]}`. Response: `{succeeded_metrics: [string]}`. Header: `DD-API-KEY`. | LOW | Must be identical to Go service's HTTP shape. API endpoint: `POST {DD_METADATA_SVC_URL}/api/unstable/byoc/ingest/metadata/metric-metadata`. |
| TS-7 | **Update known-metrics map with SaaS-acknowledged names only** | Only metrics in `succeeded_metrics` response get added to the known-metrics map. Metrics not acknowledged (partial success, server error) are discarded from pending — they will be re-detected on next arrival. | LOW | The Go service's `AppendAndWrite(succeeded)` call exactly mirrors this. The `succeeded_metrics` response field drives what enters the CSV/map. |
| TS-8 | **Drop pending list on flush failure** | On HTTP error, the pending list is discarded (not retried). Simpler than retry queues; re-detection is the recovery mechanism. | LOW | This is an explicit design decision in PROJECT.md. Go service also drops on error (Step 4 in `pollOnce` returns without `AppendAndWrite`). |
| TS-9 | **Persist known metrics to file on configurable interval** | On restart, the transform would otherwise re-submit all metrics as new (overwhelming the SaaS with duplicate submissions). Persistence enables restart recovery. | MEDIUM | Go service writes CSV to disk on every successful poll cycle. Rust transform should write on a configurable interval (default 30s per PROJECT.md). Write must be atomic (temp file + rename) to avoid corrupt reads. |
| TS-10 | **Load known metrics from persistence file on startup** | The complement to TS-9. Go service reads CSV at construction time (`New()` calls `loadCSV`). Without this, every restart causes a re-submission storm. | LOW | Missing persistence file is OK (fresh start). Malformed rows should be skipped, not fatal. |
| TS-11 | **Pass-through: all metrics reach the downstream sink unchanged** | The transform sits in the metric pipeline; it must not alter or drop the events it classifies. The Go service architecture achieves this because the YAML pipeline sends metrics to both the file sink AND the Quickwit HTTP sink — the Rust transform must emit all input events to output. | LOW | The `strip_new_metric_tag` YAML node demonstrates this: internal `_new_metric` tag is removed before forwarding. In the Rust transform, internal state must be entirely invisible to downstream. |
| TS-12 | **Deduplication of metrics within a batch** | If the same metric name appears multiple times before a flush cycle, it should only appear once in the SaaS request. Go service deduplicates in `ReadAndClearInput` via `seen` map. | LOW | Also needed within the in-memory pending list: seeing the same metric name twice before flush should not double-submit. First occurrence wins (keep first, discard duplicates). |
| TS-13 | **Configurable flush interval and batch size threshold** | Go service exposes `DD_POLL_INTERVAL` and `DD_BATCH_SIZE`. Rust transform must expose equivalent config fields in YAML. | LOW | `flush_interval` (default 15s per PROJECT.md), `batch_size` (default 200 per Go service). Both are per-instance config, not env vars in the Rust model (env vars are for secrets only). |
| TS-14 | **Error classification for HTTP responses** | Go service distinguishes: timeout, 401/403 (auth failure), 4xx (bad request), 5xx (server error), unexpected status. Each has distinct log messaging. | LOW | Classification matters for observability — auth failures are operator errors, server errors are transient. The Rust implementation should mirror `errors.go` error taxonomy. |

### Differentiators (Improvements Over the Go Service)

Features that improve on the Go service. The Go service is the baseline; these are not required for functional parity but represent genuine improvements.

| # | Feature | Value Proposition | Complexity | Notes |
|---|---------|------------------|------------|-------|
| D-1 | **No file-based IPC** | Eliminates the TOCTOU window between Vector writing NDJSON and Go service reading/truncating it. The Go service acknowledges this race in `ReadAndClearInput` (TOCTOU comment). In-process removes the race entirely. | LOW | Not a feature to build — a structural benefit of the collapse. The Rust transform sees each metric event as it arrives, no file polling required. |
| D-2 | **No enrichment table overhead** | The YAML `find_new_metrics` node queries the Vector enrichment table (CSV) on every metric event. This is an O(1) HashMap lookup, but the enrichment table has a reload mechanism and memory overhead. The Rust transform holds the known-metrics set directly in memory. | LOW | Same structural benefit. The in-memory HashMap is simpler, has no reload latency, and is always consistent. |
| D-3 | **Atomic persistence with no K8s EXDEV risk** | Go service uses `os.CreateTemp(dir, ...)` explicitly in the same directory as the CSV to avoid cross-device rename errors on K8s volume mounts. The Rust transform should do the same: temp file in the same directory as the CSV, then rename. | LOW | This is about getting the implementation right, not a new feature. But it is a correctness improvement over naive `std::fs::write`. |
| D-4 | **Immediate first flush on startup** | Go service runs `runPoll()` immediately before starting the ticker, avoiding the first-interval delay. The Rust transform should flush immediately on first batch threshold hit and not wait for the first timer tick. | LOW | The timer-based flush should start ticking from startup, but also trigger immediately if `batch_size` threshold is hit during that first interval. |
| D-5 | **Structured observability via Vector metrics** | The Go service emits StatsD metrics (`poll.success`, `poll.error`, `metrics_read`, `metrics_submitted`). The Rust transform can emit Vector-native internal metrics (via `vector_lib` counters/gauges), which integrate with the existing Vector metrics pipeline without requiring a StatsD sidecar. | MEDIUM | Emit: `flush_attempts_total`, `flush_errors_total`, `metrics_detected_total`, `metrics_submitted_total`, `known_metrics_count`. These feed existing monitoring dashboards. |
| D-6 | **Expired-entry pruning during periodic persist** | Go service prunes expired entries from `fm.metrics` during every `AppendAndWrite` call (which happens on every successful poll cycle, up to every 5 seconds). The Rust transform persists on a slower 30s interval, so pruning should happen on the same cadence as persist (or on flush, to bound memory). | LOW | Pruning during the persist pass is the natural model (iterate the map, write non-expired, delete expired in-place). Rust HashMap iteration with conditional delete is straightforward. |
| D-7 | **Configurable HTTP timeout as transform config** | Go service exposes `DD_HTTP_TIMEOUT`. Rust transform should expose it as a YAML config field (default 10s), consistent with the other transform config fields. | LOW | Operational concern: if the SaaS endpoint is slow, flush cycles must not block the event pipeline. The timeout must be surfaced to operators. |

### Anti-Features (Do Not Build in v0.1)

Features that seem reasonable but should be explicitly excluded from the first version.

| # | Anti-Feature | Why Avoid | Alternative |
|---|-------------|-----------|-------------|
| AF-1 | **Retry queue for failed flushes** | Adds stateful complexity (bounded queue, backoff logic, duplicate prevention on retry). The Go service explicitly drops on failure. Re-detection via the in-memory HashMap miss is the designed recovery path. | Drop pending list on any flush error. Metrics will be re-detected on next arrival. Document this in config as expected behavior. |
| AF-2 | **Per-metric-type TTL configuration** | Some callers may want gauges to expire faster than counts. But the Go service uses a single TTL range for all metric types, and the SaaS API does not distinguish TTL requirements by type. Adding per-type TTL adds config surface without validated need. | Single `ttl_min` / `ttl_max` range applied uniformly. Revisit after production validation. |
| AF-3 | **Health check HTTP endpoint** | Go service exposes `/healthz` and `/readyz` over HTTP (port 8080). The Rust transform runs inside Vector, which has its own health endpoint infrastructure. Building a second HTTP server inside the transform creates port conflicts and duplicates Vector's existing health reporting. | Rely on Vector's built-in health/ready endpoints. Expose transform health via Vector internal metrics (flush errors, last flush timestamp). |
| AF-4 | **Distributed/cross-node deduplication** | The Go service is node-local. The Rust transform is also node-local (same deployment model). Cross-node coordination to deduplicate across replicas requires a consensus store and is explicitly out of scope in PROJECT.md. | Node-local deduplication only. Duplicate submissions from multiple nodes are acceptable — the SaaS endpoint is idempotent (upsert semantics, `succeeded_metrics` response). |
| AF-5 | **Backpressure propagation to metric source** | If the SaaS endpoint is slow or the pending list is large, the transform should not stall the Vector event processing pipeline. Adding backpressure propagation requires modifying the Vector transform interface beyond `FunctionTransform`. | Async flush via Tokio task. The `FunctionTransform::transform` call only updates in-memory state; the HTTP flush runs in a background task. If the pending list exceeds a hard cap, drop overflow silently (same as Go service's batch cap behavior). |
| AF-6 | **NDJSON input file format** | The Go service reads NDJSON from a file that Vector writes. The Rust transform receives events directly — no file intermediary, no NDJSON parsing. Building NDJSON compatibility for any reason (migration bridge, dual-mode) adds dead code. | Direct Vector event processing via `FunctionTransform`. The five YAML nodes being retired are simply removed from the pipeline YAML when the Rust transform is deployed. |
| AF-7 | **CSV output compatibility with Go service** | The persistence file format is CSV (`metric_name,expires_at`). Changing the format would break hot-swap deployments (running Go service reads Rust-written file or vice versa). CSV format should be kept exactly. However, adding new columns or changing the header for future extensibility is an anti-feature — it would break the Go service during any rollback. | Keep CSV format identical to Go service: two columns, `metric_name,expires_at`, unix timestamp for expiry. No schema versioning. |
| AF-8 | **Metrics tagging with org_id label** | Go service tags all StatsD metrics with `org_id:$ORG_ID`. This is a StatsD pattern for multi-tenant monitoring. The Rust transform emits Vector-native metrics; adding org_id as a label on internal metrics requires the org_id to be threaded through the metrics registration, which is non-trivial in the `LazyLock` statics pattern Quickwit uses. | Emit org_id in log messages for observability. Defer metric tagging until the monitoring story for pomsky-intake is clearer. |

---

## Feature Dependencies

```
TS-10 (Load from file on startup)
    └──enables──> TS-1 (In-memory HashMap pre-populated)

TS-1 (In-memory HashMap)
    └──required by──> TS-12 (Deduplication: lookup in known map to skip already-known)
    └──required by──> TS-3 (Type mapping: only applied to newly detected metrics)
    └──required by──> TS-4 (Pending list: only new metrics enter pending)

TS-3 (Metric type mapping)
    └──required by──> TS-4 (Pending list entries need normalized type + interval)

TS-4 (Pending list)
    └──required by──> TS-5 (Flush mechanism: flushes the pending list)

TS-5 (Batch flush on interval/threshold)
    └──required by──> TS-6 (HTTP POST: the flush mechanism calls the HTTP client)

TS-6 (HTTP POST)
    └──required by──> TS-7 (Update known map: SaaS response drives what enters known map)
    └──required by──> TS-8 (Drop on failure: HTTP error triggers discard)
    └──required by──> TS-14 (Error classification: applied to HTTP response)

TS-7 (Update known map with succeeded names)
    └──enables──> TS-2 (TTL expiry: TTL is assigned at this point)

TS-7 (Update known map)
    └──required by──> TS-9 (Persist: known map is what gets written to disk)

TS-9 (Periodic persistence)
    └──requires──> Atomic write (temp file + rename, same directory, to avoid EXDEV)

TS-11 (Pass-through all events)
    └──independent of all above (applies to every event regardless of novelty)

TS-13 (Configurable flush interval and batch size)
    └──configures──> TS-5 (Flush mechanism)

D-5 (Structured observability)
    └──depends on──> TS-5, TS-6, TS-7, TS-8 (metrics emitted from flush lifecycle)
```

### Dependency Notes

- **TS-10 must run before the event loop starts:** The file is loaded at `build()` time (TransformConfig trait), before any events arrive. This mirrors the Go service's `New()` constructor loading the CSV.
- **TS-5 (flush) runs asynchronously from TS-11 (pass-through):** The flush timer fires on a background Tokio task; the `FunctionTransform::transform` call is synchronous and must not await HTTP. This is the key architectural constraint.
- **TS-7 and TS-8 are mutually exclusive outcomes of TS-6:** A successful HTTP response triggers TS-7 (update known map). Any error triggers TS-8 (drop pending list). There is no partial success path at the transform level — the SaaS may return a partial `succeeded_metrics` list, but the transform treats the HTTP call itself as atomic.

---

## MVP Definition

### Launch With (v0.1)

Minimum viable product — what's needed to replace the Go service in production.

- [ ] TS-1: In-memory HashMap with expiry — core detection mechanism
- [ ] TS-2: Randomized TTL (12-36h) — prevents re-submission storms
- [ ] TS-3: Metric type mapping (counter/gauge/sketch → count/rate/gauge/ddsketch) — required for SaaS compatibility
- [ ] TS-4: In-memory pending list — accumulates between flushes
- [ ] TS-5: Async batch flush (interval + size threshold) — drives submission cadence
- [ ] TS-6: HTTP POST to byoc-ingest-metadata-svc — the SaaS integration
- [ ] TS-7: Update known map from succeeded_metrics response — close the loop
- [ ] TS-8: Drop on flush failure — designed recovery path
- [ ] TS-9: Periodic CSV persistence (default 30s) — restart recovery
- [ ] TS-10: Load CSV on startup — restart recovery complement
- [ ] TS-11: Pass-through all events unchanged — mandatory for pipeline correctness
- [ ] TS-12: Deduplication within batch — prevents duplicate submissions per flush cycle
- [ ] TS-13: Configurable flush interval, batch size, HTTP timeout, TTL range — operator tuning
- [ ] TS-14: Error classification (timeout, auth, 4xx, 5xx) — structured logging

### Add After Validation (v0.1.x)

Features to add once the transform is running in production.

- [ ] D-5: Structured Vector internal metrics — add after confirming the flush lifecycle works correctly; metrics help diagnose production issues
- [ ] D-7: Configurable HTTP timeout in YAML config — currently hardcoded at 10s; expose after operators request tuning

### Future Consideration (v0.2+)

Features to defer until production behavior is understood.

- [ ] Retry queue for transient errors (currently AF-1) — add only if production shows excessive re-detection churn from 5xx errors
- [ ] Per-metric-type TTL (currently AF-2) — add only if SaaS signals different expiry needs per type
- [ ] Distributed deduplication (currently AF-4) — add only if multi-node duplicate submission rate is measurably problematic

---

## Feature Prioritization Matrix

| Feature | User Value | Implementation Cost | Priority |
|---------|------------|---------------------|----------|
| TS-1 (In-memory HashMap) | HIGH | MEDIUM | P1 |
| TS-2 (Randomized TTL) | HIGH | LOW | P1 |
| TS-3 (Type mapping) | HIGH | LOW | P1 |
| TS-4 (Pending list) | HIGH | LOW | P1 |
| TS-5 (Async flush) | HIGH | MEDIUM | P1 |
| TS-6 (HTTP POST) | HIGH | LOW | P1 |
| TS-7 (Update from response) | HIGH | LOW | P1 |
| TS-8 (Drop on failure) | HIGH | LOW | P1 |
| TS-9 (Periodic persistence) | HIGH | MEDIUM | P1 |
| TS-10 (Load on startup) | HIGH | LOW | P1 |
| TS-11 (Pass-through) | HIGH | LOW | P1 |
| TS-12 (Dedup within batch) | MEDIUM | LOW | P1 |
| TS-13 (Configurable params) | MEDIUM | LOW | P1 |
| TS-14 (Error classification) | MEDIUM | LOW | P1 |
| D-5 (Vector metrics) | MEDIUM | MEDIUM | P2 |
| D-7 (HTTP timeout config) | LOW | LOW | P2 |

**Priority key:**
- P1: Must have for v0.1 launch
- P2: Should have, add in v0.1.x
- P3: Nice to have, future consideration

---

## Go Service Baseline Comparison

This table maps each Go service feature to the Rust transform equivalent, confirming nothing is silently dropped.

| Go Service Feature | Go Service Location | Rust Transform Equivalent | Delta |
|-------------------|--------------------|-----------------------------|-------|
| `map[string]int64` known metrics | `fileManager.metrics` | In-memory HashMap (TS-1) | Same semantics, no file intermediary |
| Randomized TTL `[ttlMin, ttlMax)` | `randomExpiresAt()` | TS-2, same range | Same |
| Load CSV on startup | `loadCSV()` in `New()` | TS-10 | Same |
| Read NDJSON input file | `ReadAndClearInput()` | Not needed — events arrive directly | Eliminated (structural benefit) |
| Deduplicate by metric_name | `seen` map in `ReadAndClearInput` | TS-12 | Same logic, different input |
| Cap at batch_size | `batchSize` in `ReadAndClearInput` | TS-13 (batch_size threshold) | Same |
| Metric type mapping | `metric_metadata_remap` YAML node | TS-3 | Moved from YAML to Rust |
| Poll timer (5s default) | `time.NewTicker(pollInterval)` | TS-5 flush timer (15s default per PROJECT.md) | Interval changed; Go service default was 5s |
| HTTP POST to SaaS | `SubmitMetrics()` in `client.go` | TS-6 | Identical API contract |
| Error classification | `errors.go` error types | TS-14 | Same taxonomy |
| Update known map from succeeded | `AppendAndWrite(succeeded)` | TS-7 | Same semantics |
| Prune expired during write | In `AppendAndWrite` loop | D-6 (during persist) | Same mechanism, different trigger cadence |
| Atomic CSV write (temp+rename) | `os.CreateTemp(dir, ...)` + `os.Rename` | D-3 (same directory temp file) | Same correctness requirement |
| Drop on poll failure | `pollOnce` returns on error without `AppendAndWrite` | TS-8 | Same |
| StatsD metrics | `statsd.Incr/Gauge` in `poller.go` | D-5 (Vector internal metrics) | Mechanism changes, semantics preserved |
| Health/readyz HTTP server | `health.go` | AF-3 (not built) | Removed — Vector handles health |
| Enrichment table lookup | `find_new_metrics` YAML node | TS-1 (direct HashMap lookup) | Eliminated (structural benefit) |
| `strip_new_metric_tag` YAML node | YAML pipeline | TS-11 (pass-through) | Eliminated — no internal tag needed |
| `filter_new_metrics` YAML node | YAML pipeline | TS-4 + TS-5 (in-process) | Eliminated |
| `new_metric_to_log` YAML node | YAML pipeline | TS-3 (in-process type mapping) | Eliminated |
| `metric_metadata_remap` YAML node | YAML pipeline | TS-3 | Eliminated |
| File sink for NDJSON | YAML pipeline | Not needed | Eliminated |

---

## Sources

- Go service source (HIGH confidence — primary reference):
  - `dd-source/domains/quickhouse/apps/byoc-metrics-metadata/internal/filemanager/filemanager.go`
  - `dd-source/domains/quickhouse/apps/byoc-metrics-metadata/internal/client/client.go`
  - `dd-source/domains/quickhouse/apps/byoc-metrics-metadata/internal/config/config.go`
  - `dd-source/domains/quickhouse/apps/byoc-metrics-metadata/internal/poller/poller.go`
  - `dd-source/domains/quickhouse/apps/byoc-metrics-metadata/internal/client/errors.go`
  - `dd-source/domains/quickhouse/apps/byoc-metrics-metadata/internal/health/health.go`
- Vector YAML pipeline (HIGH confidence — the 5 nodes being retired):
  - `dd-source/domains/quickhouse/apps/byoc-pipeline/vector.yaml` (nodes: `find_new_metrics`, `filter_new_metrics`, `new_metric_to_log`, `metric_metadata_remap`, `strip_new_metric_tag`)
- PROJECT.md (HIGH confidence — authoritative project decisions):
  - `.planning/workstreams/alans-workstream/PROJECT.md`
- Existing Rust transforms (HIGH confidence — implementation patterns):
  - `quickwit/pomsky-intake/src/transforms/preprocess_metric.rs`

---
*Feature research for: metric metadata tracking Vector transform (byoc-metrics-metadata replacement)*
*Researched: 2026-04-16*
