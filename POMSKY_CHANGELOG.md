# Pomsky Changelog

All notable changes to Pomsky are documented here.
Pomsky is a private fork of [Quickwit](https://github.com/quickwit-oss/quickwit).
This changelog covers Pomsky-specific changes and noteworthy upstream merges.

---

## [v0.1.27] — 2026-05-21 — `652587c0f`

### Search

- **Search request batching** (CLOUDPREM-416): `BatchingSearchService` batches concurrent
  `root_search` calls that share the same query into a single execution, then fans results back
  out. Includes AST normalization (strips redundant `field_presence` clauses, quantizes RFC3339
  timestamp ranges to 3-second buckets), aggregation merging with `__b{idx}_` prefix
  namespacing, a per-org feature flag gate (`enable_request_batching` proto field set by bridge),
  and metrics (`batch_size` histogram, `batch_fallbacks_total` counter). Default batch window is
  200 ms (`CP_BATCH_WINDOW_MS`).

### Intake — Traces / Spans

- **Canonical spans**: Reworked span schema remapping (`span_to_schema`, formerly
  `remap_dd_span_to_schema`), extended to handle canonical spans and other span types.
- **Spans schema**: Added `message` as a concatenated field to `datadog-spans` for free-text
  search; fixed typo `opeartion_name` → `operation_name` in spans search fields.
- **Agent metadata passthrough**: Added HTTP endpoints on port 8787 (`POST /intake`, agent v5
  host metadata) and 8788 (`POST /api/v1/metadata`, agent inventory host metadata), both
  forwarded to the configured DD site.
- **Connections source**: `intake/connections` now stamps `now()` when an envelope carries no
  timestamp, and returns V8-encoded `ResCollector` responses.

### Metrics (DogStatsD / BYOC)

- **Metric dual shipping**: Initial implementation of metric dual shipping in `pomsky-intake` —
  routes metrics to both the Datadog backend and local storage.
- **Sketch metrics**: Added endpoint and feature flag gate for sketch metrics ingestion.
- **Metrics refactor**: Migrated Datadog-specific metrics to the new `quickwit-metrics`
  infrastructure; renamed and restored DogStatsD invariant metrics; disabled metric namespace
  splitting (`split_metric_namespace = false`).

### Observability & Diagnostics

- **Distributed tracing across reverse connection** (`#599`): Extracts `traceparent` /
  `x-datadog-*` headers from `AnyRequest.context` and attaches them as the active OTel parent
  span, so traces span the full path from bridge caller to Pomsky.
- **Standard DD trace propagator** (`#605`): Replaced custom Datadog trace context propagator
  with the standard `otel-dd` one.
- **Enriched diagnostics**: Added `env_info` to cluster diagnostics, deployment info to node
  diagnostics responses, and `CP_`/`DD_`/`QW_` env vars to debug info.
- **Resource stats logging** (`#632`): Logs resource stats (CPU, memory) on the node.
- **Cluster ID validation** (`#616`): Validates `cluster_id` format on startup (alphanumeric
  kebab-case, max 256 chars).

### Build & Infrastructure

- **PomChi vendored** as `quickwit-processing` workspace crate — no longer fetched as an
  external private git dependency, removing the `octo-sts-pomchi` CI token-minting step.
- **`quickwit-metrics` binary** added to Docker image (`#608`).
- **dd-octo-sts**: Made OIDC secret optional in Dockerfile; tokens are now minted inside the
  Docker build.

### From Quickwit Upstream (OSS merges up to 2026-05-21)

- **Metrics infrastructure**: Migrated all Quickwit metrics to `metrics-rs` (`#6374`);
  configurable system prefix and separator (`#6445`); IO metrics tracking bytes written to WAL
  (`#6429`); tokio-console integration.
- **Distributed tracing on gRPC** (`#6403`): Added OpenTelemetry tracing to the gRPC stack.
- **OTLP `http/proto` support**: Quickwit now accepts OTLP over HTTP+Protobuf in addition to
  gRPC.
- **Parquet improvements**: Column-major streaming reader and writer primitives (`#6386`,
  `#6384`); page-level stats and `rg_partition_prefix_len` marker (`#6377`); zone map pruning
  for metrics (`#6363`).
- **Search optimizations**: Optimized list fields (`#6439`); resource stats tracked across
  split/leaf/root search (`#6416`).
- **Ingester**: Separated index/merge upload semaphores (`#6376`); improved lock instrumentation
  (`#6383`).
- **Substrait**: Exposed execution metadata (`#6364`).
- **Removed telemetry** (`#6431`).
- **Rust + dependency upgrades** (`#6432`); UI yarn security fixes (`#6341`).

### From Tantivy (edfb02b → 46b3fb9)

- **Switch to upstream datasketches**: Dropped the internal `datasketches-rust` fork in favour
  of the upstream crate, and stopped using HLL4. Fixes CLOUDPREM-625 (HLL4 aux-array compact
  flag incompatibility with the Java datasketches library).
- **SSTable index optimisation**: Skip binary search over the block index when consecutive
  ordinal lookups fall in the same block — improves sequential term iteration.
- **Bug fix**: Fixed an error when opening the positions file in the segment reader.
