# Requirements: Pomsky Intake Metric Metadata Transform

**Defined:** 2026-04-16
**Core Value:** Replace multi-process file-based architecture with an in-process Rust transform for metric metadata tracking and submission.

## v1 Requirements

Requirements for initial release. Each maps to roadmap phases.

### Transform Core

- [ ] **XFRM-01**: All metric events pass through to downstream sink unchanged
- [x] **XFRM-02**: Each metric is checked against the known set; unknowns are added to the pending list with name, type, and interval
- [x] **XFRM-03**: Metric type mapped from Vector representation: counter without interval_ms → count (interval=10), counter with interval_ms → rate (interval=interval_ms/1000), gauge → gauge (interval=0), sketch → ddsketch (interval=0)
- [x] **XFRM-04**: Pending list deduplicates by metric name within a flush cycle

### State Management

- [x] **STATE-01**: In-memory HashMap tracks known metric names with per-entry expiry timestamp
- [x] **STATE-02**: TTL is randomized uniformly in range [12h, 36h] per metric entry
- [x] **STATE-03**: Expired entries are pruned during periodic persistence (eager pruning only per D-09; no lazy eviction on lookup)

### Persistence

- [x] **PERSIST-01**: Known metrics written to CSV file every configurable interval (default 30s)
- [x] **PERSIST-02**: File writes use atomic tempfile-then-rename pattern
- [x] **PERSIST-03**: On startup, known metrics loaded from CSV; missing file treated as empty; malformed rows skipped with warning

### HTTP Submission

- [ ] **HTTP-01**: Pending metrics POSTed to `{metadata_svc_url}/api/unstable/byoc/ingest/metadata/metric-metadata` with `DD-API-KEY` header and org_id in body
- [ ] **HTTP-02**: Flush triggered by interval (default 15s) or pending list size (default 200), whichever comes first
- [ ] **HTTP-03**: Only metrics in `succeeded_metrics` response are added to the known set with fresh TTL
- [ ] **HTTP-04**: On flush failure, pending list is dropped; metrics will be re-detected on next arrival

### Configuration

- [x] **CFG-01**: org_id configured via YAML config file
- [x] **CFG-02**: DD_API_KEY read from environment variable at startup; error if absent
- [x] **CFG-03**: Configurable: flush_interval_secs (15), persist_interval_secs (30), batch_size (200), ttl_min_hours (12), ttl_max_hours (36), http_timeout_secs (10), metadata_svc_url

## v2 Requirements

Deferred to future release. Tracked but not in current roadmap.

### Observability

- **OBS-01**: Vector internal metrics: flush_attempts_total, metrics_submitted_total, known_metrics_count
- **OBS-02**: Error classification counters (timeout, auth_failure, bad_request, server_error)

### Advanced

- **ADV-01**: Per-metric-type TTL ranges
- **ADV-02**: Configurable HTTP timeout via YAML config (currently env-only pattern)

## Out of Scope

| Feature | Reason |
|---------|--------|
| Retry queue for failed submissions | Drop-on-failure is explicit design decision; SaaS is idempotent |
| Distributed cross-node deduplication | Node-local is correct; SaaS handles cross-node |
| Logs/traces metadata transforms | Metrics first; other signals deferred to later milestones |
| Changes to byoc-ingest-metadata-svc API | We consume it as-is |
| Health HTTP server | Vector provides its own health endpoints |
| File-based IPC between processes | The entire point is eliminating this |

## Traceability

Which phases cover which requirements. Updated during roadmap creation.

| Requirement | Phase | Status |
|-------------|-------|--------|
| XFRM-01 | Phase 4 | Pending |
| XFRM-02 | Phase 2 | Complete |
| XFRM-03 | Phase 1 | Complete |
| XFRM-04 | Phase 2 | Complete |
| STATE-01 | Phase 2 | Complete |
| STATE-02 | Phase 2 | Complete |
| STATE-03 | Phase 2 | Complete |
| PERSIST-01 | Phase 2 | Complete |
| PERSIST-02 | Phase 2 | Complete |
| PERSIST-03 | Phase 2 | Complete |
| HTTP-01 | Phase 3 | Pending |
| HTTP-02 | Phase 4 | Pending |
| HTTP-03 | Phase 3 | Pending |
| HTTP-04 | Phase 3 | Pending |
| CFG-01 | Phase 1 | Complete |
| CFG-02 | Phase 1 | Complete |
| CFG-03 | Phase 1 | Complete |

**Coverage:**
- v1 requirements: 17 total
- Mapped to phases: 17
- Unmapped: 0

---
*Requirements defined: 2026-04-16*
*Last updated: 2026-04-20 after Phase 3 planning revision (HTTP-02 reassigned to Phase 4 per D-06)*
