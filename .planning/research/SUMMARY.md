# Research Summary: Metric Metadata Transform

**Project:** Pomsky Intake — Metric Metadata Transform (v0.1)
**Domain:** Custom stateful Vector transform for metric metadata tracking
**Researched:** 2026-04-16
**Confidence:** HIGH

## Executive Summary

Replace the Go sidecar (`byoc-metrics-metadata`) and five Vector YAML nodes with a single Rust `TaskTransform` inside `pomsky-intake`. The Go service uses file-based IPC (Vector writes NDJSON, Go polls file, POSTs to SaaS, updates CSV). The Rust transform eliminates all file-based IPC by observing metrics directly in-process.

## Stack Additions

Only two new crate entries needed: `async-stream = "0.3"` and `csv = "1"`. Both already in `Cargo.lock` as transitive deps. All other requirements (`reqwest`, `tokio`, `tempfile`, `rand`, `serde`) already in the workspace.

**Critical:** Must use `TaskTransform<Event>` (not `FunctionTransform`). `FunctionTransform` requires `Clone + Sync`, incompatible with mutable HashMap and async HTTP.

## Feature Table Stakes (14)

1. In-memory HashMap with TTL expiry for known metrics
2. Randomized TTL (12-36h) preventing re-submission thundering herd
3. Metric type mapping (counter/gauge/sketch → count/rate/gauge/ddsketch)
4. Pending list for newly detected metrics
5. Async batch flush on interval (15s) and size threshold (200)
6. HTTP POST with DD-API-KEY header and org_id
7. Update known map only from succeeded_metrics response
8. Drop pending list on flush failure
9. Periodic CSV persistence (30s), atomic write
10. Load CSV on startup (missing file OK, malformed rows skipped)
11. Pass-through all events unchanged
12. Deduplication within a batch
13. Configurable intervals, batch size, HTTP timeout, TTL range
14. Error classification (timeout, auth, 4xx, 5xx)

## Architecture

Follows `aggregate.rs` pattern: `stream!` macro + `tokio::select!` over three arms (input events, HTTP flush timer, CSV persist timer). State entirely owned by the stream — no Arc, no Mutex, no background tasks. Compliant with GAP-002.

### Components

1. `MetricMetadataConfig` — config parsing, startup I/O, transform construction, env var validation
2. `MetricMetadata` — `TaskTransform<Event>` impl; `stream!` + `select!` loop; owns all state
3. `state.rs` — `KnownMetrics` HashMap with TTL, `PendingList` with size threshold; pure Rust
4. `http.rs` — POST body, `succeeded_metrics` parsing, error classification
5. `persist.rs` — atomic CSV read/write via `tempfile`
6. `types.rs` — `MetricEntry`, `MetricKind`, Vector-to-SaaS type mapping

## Watch Out For

1. **Wrong transform trait** — `FunctionTransform` requires Clone; each clone gets isolated HashMap; all metrics appear "new" every cycle. Use `TaskTransform` from line one.
2. **tokio::sync::Mutex** — Forbidden (GAP-002). Not needed with single-stream ownership.
3. **Blocking I/O in async** — Use `tokio::fs` or `spawn_blocking` for CSV writes.
4. **Shutdown data loss** — Stream-end (`None` arm) must flush pending + persist before returning.
5. **API key leaking** — Read from env in `build()`, never store on config struct.
6. **EXDEV on K8s** — `NamedTempFile::new_in(parent_dir)` to avoid cross-device rename errors.

## Suggested Build Order

1. Architecture skeleton + types (lock in TaskTransform before any logic)
2. State logic + CSV persistence (pure Rust, unit-testable)
3. HTTP flush client (testable against mock server)
4. Full stream integration + shutdown test
5. Observability + config polish
