# Phase 31: Metadata Foundation - Context

**Gathered:** 2026-02-23
**Status:** Ready for planning

<domain>
## Phase Boundary

All metadata types, database schema, and core functions that downstream compaction phases need. This includes: SortSchema proto types and string parsing, MetricsSplitMetadata extensions (window_start, sort_schema, num_merge_ops, RowKeys, zonemap regex), PostgreSQL migration, metastore RPCs (list_for_compaction, atomic publish with replace), and the canonical window_start() function. No actor changes. No ingestion changes. No merge logic.

</domain>

<decisions>
## Implementation Decisions

### Proto sync strategy
- Vendored copy of `event_store_sortschema.proto` from dd-source into `quickwit/quickwit-proto/protos/`
- Keep the proto identical to dd-source -- byte-for-byte copy with a comment noting origin
- Do NOT strip unused fields (e.g., `expired` bool on RowKeys stays)
- Code generation via `build.rs` with `prost-build` -- standard Rust approach, proto compiled at cargo build time
- Generated code lives in OUT_DIR (not checked in)

### Sort schema string parsing
- **V2 only** -- reject V1 (INCORRECT_TRIM) strings. We're greenfield for metrics, no legacy data exists
- **Strict parsing** -- any malformed string returns an error. Fail loud at config time, no guessing
- **Named schema versions (the `version` field) skipped for now** -- always use full schema description. Named versions add complexity we don't need yet
- **LSM comparison cutoff (`&` separator) included** -- parse and respect the `&` cutoff. Needed for correct compaction scope decisions
- Direct Go-to-Rust translation of `schemautils.go` -- all test cases from `schemautils_test.go` must pass in Rust

### RowKeys storage
- **PostgreSQL:** Serialized proto bytes stored as BYTEA column. Compact, fast to read/write. Opaque to SQL queries (acceptable -- we don't query RowKeys via SQL in Phase 1)
- **Parquet key_value_metadata:** Both proto bytes (canonical) AND human-readable JSON key for debugging. Belt and suspenders -- operators can inspect files with parquet-tools
- **Zonemap regex:** Separate column in PostgreSQL (not embedded in RowKeys). Independent of RowKeys structure
- **sort_schema field:** Stored as the Husky-style string representation (e.g., `metric_name|host|env|timestamp/V2`). Human-readable, compact, easy to compare and index in PostgreSQL

### Window start semantics
- **UTC epoch aligned** -- `window_start = timestamp - (timestamp % duration)`. Simple, deterministic, no timezone dependency. Use `div_euclid`/`rem_euclid` for correct negative timestamp handling
- **window_start stored as DateTime\<Utc\>** -- type-safe, prevents mixing seconds/millis/nanos
- **window_duration must divide 3600** -- enforced strictly at parse time. ADR-003 TW-2 invariant. Reject durations that don't divide evenly into one hour
- **window_duration is per-index** -- different indexes can have different window durations. Matches sort_schema being per-index. Stored as part of index configuration

### Claude's Discretion
- Exact PostgreSQL column types and index strategy for new metadata fields
- How to organize the proto build.rs integration with existing quickwit-proto build
- Specific error types and messages for sort schema parsing failures
- How DateTime\<Utc\> interacts with the existing MetricsSplitMetadata i64 timestamp fields

</decisions>

<specifics>
## Specific Ideas

- Direct translation of Husky's Go implementations with all test cases ported:
  - `schemautils.go` / `schemautils_test.go` for sort schema parsing
  - `zonemap/` for regex generation
- Proto source: `/Users/george.talbot/dd/dd-source/domains/event-platform/shared/libs/event-store-proto/protos/event_store_sortschema/event_store_sortschema.proto`
- Go implementations: `/Users/george.talbot/go/src/github.com/DataDog/dd-go/logs/apps/logs-event-store/`
- Window assignment reference (don't copy, just understand): `.../storage/compaction/dynamicconfig/table_window_size_updater.go`

</specifics>

<deferred>
## Deferred Ideas

None -- discussion stayed within phase scope.

</deferred>

---

*Phase: 31-metadata-foundation*
*Context gathered: 2026-02-23*
