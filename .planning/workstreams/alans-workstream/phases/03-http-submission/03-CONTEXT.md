# Phase 3: HTTP Submission - Context

**Gathered:** 2026-04-20
**Status:** Ready for planning

<domain>
## Phase Boundary

Async HTTP flush client that POSTs pending metrics to the SaaS endpoint with the required headers, updates the known set only from `succeeded_metrics` in the response, and drops the pending list on any failure. Testable against a mock HTTP server. Timer/stream wiring is Phase 4 — this phase delivers the flush method and its tests.

</domain>

<decisions>
## Implementation Decisions

### Request/Response Format
- **D-01:** Exact wire format must match the Go service at `dd-source/domains/quickhouse/apps/byoc-metrics-metadata/`. Researcher reads the Go source to extract JSON request body structure, response shape, and `succeeded_metrics` field format.
- **D-02:** POST to `{metadata_svc_url}/api/unstable/byoc/ingest/metadata/metric-metadata` with `DD-API-KEY` header value from env and `org_id` in JSON body (per HTTP-01).

### Flush Client Architecture
- **D-03:** Separate `FlushClient` struct in a new `flush_client.rs` module. Holds `reqwest::Client`, `api_key: String`, `metadata_svc_url: String`, `org_id: String`. Follows the module-per-responsibility pattern from Phase 2 (`known_metrics.rs`, `csv_persistence.rs`).
- **D-04:** `reqwest::Client` built once in `TransformConfig::build()` with timeout from `http_timeout_secs`. Stored in `FlushClient`. Connection pooling handled automatically by reqwest. No retry per design.
- **D-05:** `FlushClient` exposes an async `flush_pending()` method that takes the pending list and returns the set of succeeded metric names. Phase 4 calls this from the `select!` loop. Phase 3 tests call it directly — no timer wiring needed.

### Flush Trigger Boundary
- **D-06:** Phase 3 implements the flush logic only (HTTP call, response parsing, known-set update, failure handling). Phase 4 implements interval + batch-size trigger checking and the `select!` loop that calls `flush_pending()`.
- **D-07:** The `flush_pending()` method receives `&HashMap<String, MetricTypeInfo>` (the pending list), performs the POST, and returns `Result<Vec<String>, FlushError>` where `Ok(names)` are the succeeded metrics and `Err` means the entire flush failed (pending should be dropped).

### Failure Handling
- **D-08:** On HTTP failure (network error, 4xx, 5xx, timeout), log at `warn!` level with status code or error type, then return an error. The caller (Phase 4's `select!` loop) drops the pending list. This gives operators visibility without requiring OBS-01/OBS-02 metrics (deferred to v2).

### Testing Strategy
- **D-09:** Use `wiremock` crate for mock HTTP server. Register expected requests with matchers, assert headers/body, return canned responses.
- **D-10:** Key test scenario: partial success — response `succeeded_metrics` is a subset of what was sent. Only succeeded names get added to known set; others remain unknown for re-detection on next arrival.

### Claude's Discretion
- Exact error type design for `FlushError` (simple enum vs anyhow)
- Whether `flush_pending()` takes ownership or borrows the pending map
- Request serialization details (serde struct vs manual JSON construction)
- Additional edge-case tests beyond partial success (empty response, malformed response, etc.)

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Go Service Wire Format (CRITICAL — read first)
- `/Users/alan.gates/go/src/github.com/DataDog/dd-source/domains/quickhouse/apps/byoc-metrics-metadata/` — Go service source. Extract exact JSON request body structure (field names, nesting, types), response shape (`succeeded_metrics` field), and Content-Type headers. The Rust implementation must produce identical HTTP requests.

### Phase 1 & 2 Foundation (build on this code)
- `quickwit/pomsky-intake/src/transforms/metric_metadata/mod.rs` — Transform struct, config, build(), pending list wiring
- `quickwit/pomsky-intake/src/transforms/metric_metadata/types.rs` — MetricTypeInfo, MetadataMetricType with serde serialization (D-10 from Phase 1)
- `quickwit/pomsky-intake/src/transforms/metric_metadata/known_metrics.rs` — KnownMetrics::insert() for adding succeeded metrics with fresh TTL
- `quickwit/pomsky-intake/src/transforms/metric_metadata/csv_persistence.rs` — CSV persistence (no changes expected)

### Prior Phase Decisions
- `.planning/workstreams/alans-workstream/phases/01-foundation/01-CONTEXT.md` — D-01 through D-11 (config, type mapping, TaskTransform)
- `.planning/workstreams/alans-workstream/phases/02-state-and-persistence/02-CONTEXT.md` — D-01 through D-10 (CSV format, pending list, pruning)

### Requirements
- `.planning/workstreams/alans-workstream/REQUIREMENTS.md` — HTTP-01, HTTP-02, HTTP-03, HTTP-04

### Crate Dependencies
- `quickwit/pomsky-intake/Cargo.toml` — `reqwest` already present as workspace dependency. Will need `wiremock` added as dev-dependency.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `reqwest`: Already in `pomsky-intake/Cargo.toml` as workspace dependency — no new dependency needed for production code
- `MetricTypeInfo` + `MetadataMetricType`: Serde-serializable types ready for JSON request body construction
- `KnownMetrics::insert()`: Adds a metric with fresh randomized TTL — called for each succeeded metric after flush

### Established Patterns
- Module-per-responsibility: `known_metrics.rs`, `csv_persistence.rs` → new `flush_client.rs`
- `#[allow(dead_code)]` on `config` and `api_key` fields — Phase 3 removes these annotations
- `save_to_csv` has `#[allow(dead_code)]` — remains until Phase 4 persist tick

### Integration Points
- `MetricMetadataTransform` struct: Add `flush_client: FlushClient` field, constructed in `build()`
- `TransformConfig::build()`: Create `reqwest::Client` with timeout, construct `FlushClient`
- Phase 4 will call `flush_client.flush_pending(&pending)` from the `select!` loop and update `known_metrics` with succeeded names

</code_context>

<specifics>
## Specific Ideas

No specific requirements — open to standard approaches

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 03-http-submission*
*Context gathered: 2026-04-20*
