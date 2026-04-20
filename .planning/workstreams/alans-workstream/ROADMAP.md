# Roadmap: Pomsky Intake — Metric Metadata Transform (v0.1)

## Overview

Build a single custom `TaskTransform` in `pomsky-intake` that replaces the Go sidecar (`byoc-metrics-metadata`) and five Vector YAML nodes. The transform runs in-process after `preprocess_metric`, maintaining an in-memory known-metrics map with TTL expiry, persisting state to CSV, and flushing new metric metadata to the SaaS ingest endpoint via HTTP POST. Four phases deliver the architecture skeleton, state+persistence, HTTP submission, and full stream integration in that order.

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

Decimal phases appear between their surrounding integers in numeric order.

- [x] **Phase 1: Foundation** - Architecture skeleton, type definitions, and config wiring — locks in TaskTransform
- [x] **Phase 2: State and Persistence** - In-memory known-metrics map with TTL expiry and atomic CSV persistence (completed 2026-04-20)
- [ ] **Phase 3: HTTP Submission** - Async flush client, interval/size triggers, response-driven state updates
- [ ] **Phase 4: Stream Integration** - Full select! loop wiring, graceful shutdown flush, end-to-end test

## Phase Details

### Phase 1: Foundation
**Goal**: A compiling `TaskTransform` skeleton exists with all configuration parsed, API key validated at startup, and metric type mapping implemented — no logic is missing or stubbed in ways that block subsequent phases
**Depends on**: Nothing (first phase)
**Requirements**: CFG-01, CFG-02, CFG-03, XFRM-03
**Success Criteria** (what must be TRUE):
  1. `MetricMetadataConfig` deserializes from YAML with all configurable fields (flush_interval_secs, persist_interval_secs, batch_size, ttl_min_hours, ttl_max_hours, http_timeout_secs, metadata_svc_url, org_id)
  2. Transform build fails with a descriptive error if `DD_API_KEY` environment variable is absent at startup
  3. All four Vector metric types (counter without interval_ms, counter with interval_ms, gauge, sketch) map to the correct SaaS representation (count/rate/gauge/ddsketch) with correct interval values
  4. The crate compiles cleanly with `cargo clippy --workspace --all-features --tests` producing no warnings
**Plans:** 1 plan

Plans:
- [x] 01-01-PLAN.md — Config, type mapping, TaskTransform skeleton, pipeline wiring

### Phase 2: State and Persistence
**Goal**: The in-memory known-metrics map correctly tracks entries with randomized TTL, prunes expired entries, deduplicates the pending list, and atomically reads/writes the CSV persistence file — all verifiable without HTTP or a running stream
**Depends on**: Phase 1
**Requirements**: STATE-01, STATE-02, STATE-03, PERSIST-01, PERSIST-02, PERSIST-03, XFRM-02, XFRM-04
**Success Criteria** (what must be TRUE):
  1. A metric name inserted into `KnownMetrics` is recognized as known on subsequent lookup and not added to the pending list; the same name with an expired TTL is recognized as unknown and re-added
  2. Per-entry TTL is drawn uniformly from [12h, 36h]; no entry's TTL falls outside this range across 1000 generated entries
  3. Expired entries are removed during the periodic persist tick (eager pruning only per D-09) — no expired entry survives into the CSV
  4. Writing the known-metrics map produces a valid CSV that can be round-tripped back to identical state; a missing file on load is treated as empty; malformed rows are skipped with a warning log
  5. File writes use a tempfile-then-rename pattern so no partial writes are ever visible to readers
**Plans:** 2 plans

Plans:
- [x] 02-01-PLAN.md — Module split, types extraction, KnownMetrics with TTL and pruning
- [x] 02-02-PLAN.md — CSV persistence (atomic load/save), transform integration with pending list

### Phase 3: HTTP Submission
**Goal**: The HTTP flush client correctly POSTs pending metrics to the SaaS endpoint with the required headers, updates the known set only from `succeeded_metrics`, and drops the pending list on any failure — testable against a mock HTTP server
**Depends on**: Phase 2
**Requirements**: HTTP-01, HTTP-02, HTTP-03, HTTP-04
**Success Criteria** (what must be TRUE):
  1. A flush POST includes the `DD-API-KEY` header (value from env) and `org_id` in the JSON body, targeting the configured `{metadata_svc_url}/api/unstable/byoc/ingest/metadata/metric-metadata` path
  2. A flush is triggered when either the configurable interval elapses or the pending list reaches the configured batch size, whichever occurs first
  3. Only metric names listed in the `succeeded_metrics` response field are added to the known set with a fresh TTL; names absent from the response are not added
  4. When the HTTP POST fails (network error, 4xx, 5xx, timeout), the pending list is silently dropped and the next batch starts fresh
**Plans:** 2 plans

Plans:
- [ ] 03-01-PLAN.md — FlushClient TDD: serde wire types, flush_pending() method, wiremock tests
- [ ] 03-02-PLAN.md — Wire FlushClient into MetricMetadataTransform struct and build()

### Phase 4: Stream Integration
**Goal**: The full `stream!` + `select!` loop is wired — metric events pass through to the downstream sink unchanged, all timers fire correctly, and a graceful shutdown flushes pending metrics and persists state before the stream returns
**Depends on**: Phase 3
**Requirements**: XFRM-01
**Success Criteria** (what must be TRUE):
  1. Every metric event received by the transform is emitted to the downstream output unchanged (no fields added, removed, or modified)
  2. When the input stream closes, any pending metrics are flushed to the SaaS endpoint and the known-metrics map is persisted to CSV before the transform exits
  3. An integration test drives the transform through a real `TaskTransform` call chain, verifying pass-through and state persistence without mocking the transform internals
**Plans**: TBD

## Progress

**Execution Order:**
Phases execute in numeric order: 1 -> 2 -> 3 -> 4

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Foundation | 1/1 | Complete | 2026-04-17 |
| 2. State and Persistence | 2/2 | Complete | 2026-04-20 |
| 3. HTTP Submission | 0/2 | Planning complete | - |
| 4. Stream Integration | 0/? | Not started | - |
