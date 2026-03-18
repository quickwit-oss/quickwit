---
phase: 31-metadata-foundation
verified: 2026-02-23T22:00:00Z
status: passed
score: 10/10 must-haves verified
gaps:
  - truth: "Parquet key_value_metadata includes sort_schema, window_start, and RowKeys (both proto bytes and JSON)"
    status: failed
    reason: "build_compaction_key_value_metadata() is defined and unit-tested but NOT called by the actual write path (ParquetWriter::write_to_bytes, write_to_file, ParquetSplitWriter::write_split). The function is orphaned — it exists and is exported but the Parquet files produced at runtime will NOT contain qh.* metadata entries."
    artifacts:
      - path: "quickwit/quickwit-parquet-engine/src/storage/writer.rs"
        issue: "build_compaction_key_value_metadata() defined but not wired into write_to_bytes() or write_to_file()"
      - path: "quickwit/quickwit-parquet-engine/src/storage/split_writer.rs"
        issue: "write_split() does not call build_compaction_key_value_metadata() — compaction metadata not injected into Parquet files"
      - path: "quickwit/quickwit-parquet-engine/src/storage/config.rs"
        issue: "to_writer_properties() has no set_key_value_metadata() call"
    missing:
      - "Wire build_compaction_key_value_metadata() into the write path: either in ParquetWriter::write_to_bytes/write_to_file (requires metadata param) or in ParquetSplitWriter::write_split (which has access to metadata)"
      - "ParquetSplitWriter::write_split should accept window_start, sort_schema, and row_keys_proto parameters (or a MetricsSplitMetadata) and call build_compaction_key_value_metadata(), passing the result to WriterProperties::builder().set_key_value_metadata()"
human_verification: []
---

# Phase 31: Metadata Foundation Verification Report

**Phase Goal:** All metadata types, database schema, and core functions exist so downstream phases can read and write compaction-aware split metadata
**Verified:** 2026-02-23T22:00:00Z
**Status:** gaps_found
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|---------|
| 1 | Husky proto provides SortSchema, SortColumn, SortColumnDirection, ColumnValue, ColumnValues, RowKeys; Husky-style string parses with multi-column support, direction suffixes, and V2-only enforcement | VERIFIED | `quickwit-proto/src/codegen/sortschema/sortschema.rs` generates all 6 types; `parse_sort_schema()` implements full Go StringToSchema port; 65 tests pass including V2-only, LSM cutoff, round-trip |
| 2 | MetricsSplitMetadata contains window_start, window_duration_secs, sort_schema, num_merge_ops, RowKeys, and per-column zonemap regex; round-trips through PostgreSQL and JSON without data loss | VERIFIED | All 6 fields present in `metadata.rs` with correct serde defaults; backward-compat and round-trip tests pass |
| 2b | Round-trips through Parquet key_value_metadata without data loss | FAILED | `build_compaction_key_value_metadata()` is defined and tested but NOT wired into the write path — actual Parquet files will not contain qh.* metadata entries |
| 3 | list_metrics_splits_for_compaction RPC returns only Published splits matching (index_id, window_start, sort_schema) | VERIFIED | Proto message defined; PostgreSQL CTE query filters `WHERE index_id=$1 AND window_start=$2 AND sort_schema=$3 AND split_state='Published'`; 3 integration tests cover scope filtering, Published-only, and empty scopes |
| 4 | publish_metrics_splits atomically stages new splits and marks replaced splits in a single PostgreSQL transaction | VERIFIED | `PUBLISH_METRICS_SPLITS_QUERY` is a single CTE (`WITH publish AS ... mark_for_deletion AS ...`) executed inside `run_with_tx!` macro; dual count verification rolls back if either count mismatches |
| 5 | window_start(timestamp, duration) produces correct results for negative timestamps, zero-crossing, and all edge cases, verified by proptest | VERIFIED | `window_start()` uses `rem_euclid`; 3 proptest properties (alignment, determinism, no-overlap) pass across 256+ cases each; unit tests cover -1, 0, 899, 900, -3601 boundary cases |

**Score:** 9/10 must-haves verified (one truth partially fails — Parquet write-path wiring missing)

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `quickwit/quickwit-proto/protos/event_store_sortschema/event_store_sortschema.proto` | Vendored proto with all 6 types | VERIFIED | Present; vendored comment added; all 6 types defined |
| `quickwit/quickwit-proto/src/codegen/sortschema/sortschema.rs` | Generated Rust types with Serialize/Deserialize | VERIFIED | All 6 types generated with `#[derive(serde::Serialize, serde::Deserialize)]` |
| `quickwit/quickwit-proto/build.rs` | prost_build compilation block for sortschema | VERIFIED | `sortschema_config.compile_protos(&["protos/event_store_sortschema/..."])` present |
| `quickwit/quickwit-proto/src/lib.rs` | `pub mod sortschema` export | VERIFIED | `pub mod sortschema { include!("codegen/sortschema/sortschema.rs"); }` present |
| `quickwit/quickwit-parquet-engine/src/sort_schema/parser.rs` | parse_sort_schema() — Go StringToSchema port | VERIFIED | 241-line full port; handles 1/2/3-part column formats, LSM cutoff, V2-only enforcement |
| `quickwit/quickwit-parquet-engine/src/sort_schema/display.rs` | schema_to_string() and schema_to_string_short() | VERIFIED | Both functions present |
| `quickwit/quickwit-parquet-engine/src/sort_schema/column_type.rs` | ColumnTypeId enum with suffix mapping | VERIFIED | Int64=2, Float64=10, String=14, Sketch=17, CpcSketch=20, ItemSketch=22 |
| `quickwit/quickwit-parquet-engine/src/sort_schema/validation.rs` | validate_schema() | VERIFIED | Full port; enforces timestamp requirements, duplicate detection, direction rules |
| `quickwit/quickwit-parquet-engine/src/sort_schema/equivalence.rs` | equivalent_schemas() and equivalent_schemas_for_compaction() | VERIFIED | Both functions present; base comparison + optional LSM cutoff comparison |
| `quickwit/quickwit-parquet-engine/src/sort_schema/window.rs` | window_start() and validate_window_duration() | VERIFIED | rem_euclid implementation; proptest-verified; all boundary tests pass |
| `quickwit/quickwit-parquet-engine/src/split/metadata.rs` | MetricsSplitMetadata with 6 compaction fields | VERIFIED | All 6 fields present with correct serde(default) annotations; builder methods present |
| `quickwit/quickwit-parquet-engine/src/split/postgres.rs` | PgMetricsSplit and InsertableMetricsSplit with compaction columns | VERIFIED | All 6 columns in both structs; MetricsSplits enum variants present |
| `quickwit/quickwit-metastore/migrations/postgresql/26_add-compaction-metadata.up.sql` | ALTER TABLE + composite index | VERIFIED | Adds window_start, window_duration_secs, sort_schema, num_merge_ops, row_keys, zonemap_regexes; creates idx_metrics_splits_compaction_scope |
| `quickwit/quickwit-metastore/migrations/postgresql/26_add-compaction-metadata.down.sql` | Revert migration | VERIFIED | Present; drops index and columns |
| `quickwit/quickwit-proto/protos/quickwit/metastore.proto` | ListMetricsSplitsForCompactionRequest and RPC | VERIFIED | Message at line 397; RPC at line 146; uses ListMetricsSplitsResponse |
| `quickwit/quickwit-metastore/src/metastore/postgres/metastore.rs` | PostgreSQL implementation of both new RPCs | VERIFIED | list_metrics_splits_for_compaction at line 2346; atomic publish at line 1969; 7 integration tests |
| `quickwit/quickwit-metastore/src/metastore/file_backed/mod.rs` | Stub returning Internal error | VERIFIED | Returns `MetastoreError::Internal` with "not supported for file-backed metastore" |
| `quickwit/quickwit-metastore/src/metastore/control_plane_metastore.rs` | Forwarding proxy | VERIFIED | Delegates to `self.metastore.list_metrics_splits_for_compaction(request).await` |
| `quickwit/quickwit-parquet-engine/src/storage/writer.rs` | build_compaction_key_value_metadata() and PARQUET_META_* constants | PARTIAL-ORPHANED | Function defined and unit-tested; PARQUET_META_* constants defined; but function NOT called in write path |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `build.rs` | `event_store_sortschema.proto` | `prost_build::Config::compile_protos` | WIRED | `compile_protos(&["protos/event_store_sortschema/..."])` present at line 280 |
| `parser.rs` | `quickwit-proto sortschema types` | `use quickwit_proto::sortschema::*` | WIRED | `use quickwit_proto::sortschema::{SortColumn, SortColumnDirection, SortSchema}` at line 21 |
| `metadata.rs` | `chrono::DateTime<Utc>` | `window_start_datetime()` accessor | WIRED | `use chrono::{DateTime, TimeZone, Utc}` at line 20; `window_start_datetime()` method implemented |
| `postgres.rs` | `metadata.rs` | `InsertableMetricsSplit` conversion | WIRED | `from_metadata()` in postgres.rs (inferred from struct layout + integration tests passing) |
| migration 26 SQL | `postgres.rs` | column names match struct fields | WIRED | `window_start`, `sort_schema`, `num_merge_ops` match between SQL and Rust struct |
| `metastore.proto` | `postgres/metastore.rs` | `ListMetricsSplitsForCompactionRequest` used in SQL query | WIRED | Request struct used at line 2348; SQL uses request.index_id, request.window_start, request.sort_schema |
| `publish_metrics_splits` SQL CTE | `metrics_splits` table | `run_with_tx!` transaction | WIRED | Single CTE with publish + mark_for_deletion inside `run_with_tx!` |
| `build_compaction_key_value_metadata()` | Parquet write path | should be called in write_to_file/write_to_bytes or split_writer | NOT WIRED | Function defined in writer.rs but not called in write_to_bytes(), write_to_file(), or ParquetSplitWriter::write_split() |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|-------------|-------------|--------|---------|
| META-01 | 31-01 | Sort schema configurable per metrics index using Husky-style string format; reuse Husky proto | SATISFIED | Proto vendored; prost-build compiles it; parse_sort_schema() handles full Husky format |
| META-02 | 31-01 | timeseries_id hash tiebreaker column | SATISFIED | `timeseries_id__i` parses correctly as TypeIDInt64/ascending via `__i` suffix; test_timeseries_id_as_int64 passes. Note: bare `timeseries_id` (without `__i`) is NOT handled as a special case — plan's CRITICAL test for bare form was NOT implemented, but the ROADMAP criterion is satisfied via standard `__i` suffix |
| META-03 | 31-02 | MetricsSplitMetadata extended with window_start, window_duration_secs, sort_schema, num_merge_ops | SATISFIED | All 4 fields + row_keys_proto + zonemap_regexes present in metadata.rs |
| META-04 | 31-02 | PostgreSQL migration adding compaction columns with composite index | SATISFIED | Migration 26 adds 6 columns; creates idx_metrics_splits_compaction_scope |
| META-05 | 31-03 | list_metrics_splits_for_compaction RPC scoped by (index_id, window_start, sort_schema) | SATISFIED | PostgreSQL implementation queries by all 3 scope fields; 3 integration tests verify behavior |
| META-06 | 31-03 | Atomic replace semantics in publish_metrics_splits | SATISFIED | CTE with dual count verification inside run_with_tx!; integration test for atomic rollback |
| META-07 | 31-02 | Self-describing Parquet files with sort_schema, window_start, min/max in key_value_metadata | PARTIALLY SATISFIED | build_compaction_key_value_metadata() is implemented with correct qh.* keys but NOT wired into the actual Parquet write path |
| META-08 | 31-02 | Per-column statistics in MetricsSplitMetadata using RowKeys proto; stored in PostgreSQL and Parquet key_value_metadata | PARTIALLY SATISFIED | RowKeys stored in PostgreSQL (row_keys column in migration); Parquet write path wiring is missing |
| PIPE-10 | 31-04 | Canonical window_start() function using div_euclid/rem_euclid with proptest coverage | SATISFIED | window_start() uses rem_euclid; 3 proptest properties pass; window_start(-1, 900)==-900 verified |

### Anti-Patterns Found

| File | Pattern | Severity | Impact |
|------|---------|----------|--------|
| `quickwit-parquet-engine/src/storage/writer.rs` | `build_compaction_key_value_metadata()` defined and exported but never called in write path | Warning | Parquet files written at runtime will not contain qh.* metadata entries — downstream consumers reading Parquet key_value_metadata will find no compaction data |
| `quickwit-parquet-engine/src/sort_schema/tests.rs` | Plan required `parse_sort_schema("metric_name|host|timeseries_id|timestamp/V2")` -> Ok (bare timeseries_id), but test uses `timeseries_id__i` | Info | Bare `timeseries_id` without `__i` suffix will fail to parse; if Husky ever sends schemas with bare timeseries_id, parsing will break |

### Human Verification Required

None — all automated checks are sufficient for this phase's observable truths.

### Gaps Summary

One gap blocks full goal achievement:

**Parquet key_value_metadata not wired into write path (META-07, META-08 partial)**

The function `build_compaction_key_value_metadata()` in `quickwit-parquet-engine/src/storage/writer.rs` (lines 49-104) is correctly implemented, unit-tested, and exported from the module. It produces the correct `qh.sort_schema`, `qh.window_start`, `qh.row_keys` (base64), and `qh.row_keys_json` entries. However, neither `ParquetWriter::write_to_bytes()`, `ParquetWriter::write_to_file()`, nor `ParquetSplitWriter::write_split()` calls this function. The `ParquetWriterConfig::to_writer_properties()` has no `set_key_value_metadata()` call.

Result: Parquet files produced at runtime contain no compaction metadata in their key_value_metadata section. The "self-describing Parquet files" requirement (META-07) and the Parquet portion of the "stored in PostgreSQL and Parquet key_value_metadata" requirement (META-08) are not satisfied at runtime.

Fix: In `ParquetSplitWriter::write_split()`, after building the `MetricsSplitMetadata`, call `build_compaction_key_value_metadata(&metadata)` and use `.set_key_value_metadata(Some(kvs))` on the `WriterProperties::builder()` before creating the `ArrowWriter`. Or add a new writer method that accepts both `RecordBatch` and `MetricsSplitMetadata`.

All other goals are fully achieved:
- The proto types, parser, display, validation, equivalence, and window functions are complete and all 65 tests pass.
- MetricsSplitMetadata has all 6 compaction fields with correct serde backward compatibility.
- PostgreSQL migration 26 creates the needed columns and composite scope index.
- `list_metrics_splits_for_compaction` correctly filters by (index_id, window_start, sort_schema) and returns only Published splits.
- `publish_metrics_splits` atomically publishes and marks-for-deletion in a single CTE transaction with rollback on count mismatches.
- `window_start()` is mathematically correct and proptest-verified.

---

## Post-Review Updates (2026-02-26)

The following changes were made after initial verification during code review:

### Gaps Resolved
- **Parquet write-path wiring (META-07, META-08):** Fixed. `write_to_file_with_metadata()` and `write_to_bytes_with_metadata()` now call `build_compaction_key_value_metadata()`. End-to-end test `test_meta07_self_describing_parquet_roundtrip` verifies recovery from cold file. **Status: PASSED.**

### Renames
- **Field:** `sort_schema` → `sort_fields` (metadata struct, PostgreSQL column, Parquet kv key, all references)
- **Module:** `sort_schema/` → `sort_fields/`, `parse_sort_schema()` → `parse_sort_fields()`
- **Error:** `SortSchemaError` → `SortFieldsError`, moved to `quickwit-proto`
- Proto type name `SortSchema` unchanged (comes from dd-source)

### API Simplification
- **Merged RPC:** `list_metrics_splits_for_compaction` removed. Compaction scope queries use `list_metrics_splits` with `ListMetricsSplitsQuery::with_compaction_scope(window_start, sort_fields)`.

### Idiomatic Rust Refactoring
- `ColumnTypeId`: `FromStr`, `TryFrom<u64>`, removed `as_u64()` getter
- `SortSchemaExt` extension trait for equivalence methods
- `window_start()` returns `Result` (no panics)
- `From<SortFieldsError> for MetastoreError`
- Parser uses `strip_prefix`/`strip_suffix`, `HashSet<&str>`
- Generated sortschema code no longer checked in (build-time only)
- Direction prefix/suffix support (`+name`, `name-`)

### TLA+ Invariants Added
- SS-1: `debug_assert!` in `sort_batch()` verifying output is sorted
- SS-5: `debug_assert!` at JSON, SQL, and Parquet serialization boundaries
- TW-1: `debug_assert!` pairing `window_start` with `window_duration_secs`
- TW-2: `debug_assert!` at builder, serializer, and `window_start()`

### Final Score
**10/10 must-haves verified.** All gaps resolved. 277 parquet-engine tests, full workspace clean.

---

_Initial verification: 2026-02-23T22:00:00Z_
_Post-review update: 2026-02-26_
_Verifier: Claude (gsd-verifier)_
