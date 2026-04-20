# Phase 4: Stream Integration - Context

**Gathered:** 2026-04-20
**Status:** Ready for planning

<domain>
## Phase Boundary

Wire the full `select!` loop in `TaskTransform::transform()` — metric events pass through to the downstream sink unchanged, flush and persist timers fire correctly, batch_size triggers immediate flush, and graceful shutdown flushes pending metrics and persists state before the stream returns. This is the final phase: after this, the transform is production-ready.

</domain>

<decisions>
## Implementation Decisions

### select! Loop Architecture
- **D-01:** Use `tokio::select!` with `biased` mode inside an `async_stream::stream!` macro. Prioritize input events over timers (process events first, fire timers when idle). The `stream!` macro wraps the entire select! loop and yields events via `yield event`.
- **D-02:** Three `select!` branches: (1) input stream `.next()` for metric events, (2) `tokio::time::interval` for flush timer, (3) `tokio::time::interval` for persist timer.
- **D-03:** Per GAP-002: do NOT recreate futures in the select! loop. Use `&mut fut` to resume. Do NOT use `tokio::sync::Mutex` or `JoinHandle::abort()`. All mutable state (known_metrics, pending, config) is owned by the single stream closure — no locks needed.

### Batch Size Trigger
- **D-04:** After processing each event, check `pending.len() >= batch_size` inline. If true, flush immediately before yielding the event. Simple, deterministic — flush happens at the exact threshold.
- **D-05:** The flush timer resets after any flush (whether triggered by interval or batch_size) to avoid double-flushing.

### Graceful Shutdown
- **D-06:** When input stream returns `None`, execute shutdown sequence: (1) flush any pending metrics via `flush_client.flush_pending()`, (2) update known_metrics with succeeded names, (3) prune expired entries, (4) persist CSV unconditionally, (5) break the loop and let the stream end.
- **D-07:** If the shutdown flush (HTTP POST) fails, log at `warn!` and proceed with CSV persist and exit. Matches D-08 from Phase 3 (drop-on-failure design). Metrics will be re-detected after restart. No retry.
- **D-08:** Always persist CSV on shutdown, even if the known-metrics map hasn't changed since the last persist tick. Ensures any metrics added by the final flush are persisted. One extra write is negligible.

### Integration Test
- **D-09:** Integration test constructs `MetricMetadataTransform` via `TransformConfig::build()`, calls `transform()` with a stream of metric events, uses wiremock for the HTTP endpoint. Tests the full lifecycle through the real `TaskTransform` trait — no mocking of transform internals.
- **D-10:** Assertions cover: (1) all input events pass through unchanged (XFRM-01), (2) wiremock received the flush POST with correct body and headers (HTTP-01), (3) CSV file contains the succeeded metrics with valid TTLs after stream closes, (4) known-set was updated only with succeeded metrics (HTTP-03).

### Claude's Discretion
- Timer reset strategy details (whether `interval.reset()` or re-creation after manual flush)
- Exact ordering of persist-tick operations (prune then write vs write then prune)
- Whether to drain remaining events from the input stream after the first `None` or just stop
- Dead code cleanup: remove `#[allow(dead_code)]` on `config`, `api_key`, `save_to_csv` as they become used
- `async_stream` crate version and dependency wiring
- Integration test helper design (reuse existing test helpers from prior phases)
- Whether the batch_size flush should yield all buffered events before flushing or flush first

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Phase 1-3 Foundation (build on this code)
- `quickwit/pomsky-intake/src/transforms/metric_metadata/mod.rs` -- Transform struct, config, build(), current pass-through stream (replace with select! loop)
- `quickwit/pomsky-intake/src/transforms/metric_metadata/flush_client.rs` -- FlushClient::flush_pending() async method (called from select! loop)
- `quickwit/pomsky-intake/src/transforms/metric_metadata/known_metrics.rs` -- KnownMetrics::insert(), contains(), prune_expired()
- `quickwit/pomsky-intake/src/transforms/metric_metadata/csv_persistence.rs` -- save_to_csv() for persist tick and shutdown
- `quickwit/pomsky-intake/src/transforms/metric_metadata/types.rs` -- MetricTypeInfo, MetadataMetricType, map_metric_type()

### Prior Phase Decisions (MUST honor)
- `.planning/workstreams/alans-workstream/phases/01-foundation/01-CONTEXT.md` -- D-11 (TaskTransform), D-01 (config), D-06/D-07 (pipeline wiring)
- `.planning/workstreams/alans-workstream/phases/02-state-and-persistence/02-CONTEXT.md` -- D-06/D-07 (pending dedup), D-08/D-09/D-10 (eager pruning, persist tick scope)
- `.planning/workstreams/alans-workstream/phases/03-http-submission/03-CONTEXT.md` -- D-03/D-05 (FlushClient arch), D-06 (Phase 4 owns triggers), D-07 (flush_pending signature), D-08 (failure handling)

### Requirements
- `.planning/workstreams/alans-workstream/REQUIREMENTS.md` -- XFRM-01 (pass-through), HTTP-02 (interval or batch_size trigger)

### Async Safety (GAP-002)
- `CLAUDE.md` Known Pitfalls table -- tokio::sync::Mutex FORBIDDEN, JoinHandle::abort() FORBIDDEN, select! loop future recreation FORBIDDEN, no locks across await points

### Pipeline Integration
- `quickwit/pomsky-intake/src/intake_runner.rs` -- YAML pipeline config (no changes expected; transform already wired in Phase 1)
- `quickwit/pomsky-intake/Cargo.toml` -- Dependencies; may need `async-stream` and `tokio` with `time` feature added

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `FlushClient::flush_pending(&self, &HashMap<String, MetricTypeInfo>) -> Result<Vec<String>, FlushError>` -- async method ready for the select! loop to call
- `KnownMetrics::insert(name)` -- adds metric with fresh randomized TTL, called for each succeeded name
- `KnownMetrics::prune_expired()` -- sweeps expired entries, called during persist tick
- `csv_persistence::save_to_csv(path, &KnownMetrics)` -- atomic tempfile-then-rename write
- `csv_persistence::load_from_csv(path)` -- called in build(), already wired
- `map_metric_type(&Metric) -> MetricTypeInfo` -- per-event classification, already used in current stream.map()
- Existing test helpers: `FlushClient::new()` test builder, `wiremock` patterns from Phase 3 tests

### Established Patterns
- `TaskTransform::transform(self: Box<Self>, task: Pin<Box<dyn Stream>>) -> Pin<Box<dyn Stream>>` -- returns owned stream; async_stream::stream! fits naturally
- `#[allow(dead_code)]` annotations on `config`, `api_key`, `save_to_csv` -- Phase 4 activates these; remove the annotations
- ENV_LOCK mutex in tests for DD_API_KEY manipulation -- reuse for integration test

### Integration Points
- `MetricMetadataTransform::transform()` (mod.rs:229-248) -- REPLACE the current `stream.map()` with the new `stream! { select! { ... } }` loop
- `config.flush_interval_secs`, `config.persist_interval_secs`, `config.batch_size` -- consumed by the select! loop timers and threshold check
- `config.persist_file_path` -- passed to save_to_csv during persist tick and shutdown

</code_context>

<specifics>
## Specific Ideas

No specific requirements -- open to standard approaches

</specifics>

<deferred>
## Deferred Ideas

None -- discussion stayed within phase scope

</deferred>

---

*Phase: 04-stream-integration*
*Context gathered: 2026-04-20*
