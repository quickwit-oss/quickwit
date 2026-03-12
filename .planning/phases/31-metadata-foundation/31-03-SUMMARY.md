---
phase: 31-metadata-foundation
plan: 03
subsystem: compaction
tags: [protobuf, postgres, rpc, metastore, atomic-publish, compaction-scope, sqlx]

# Dependency graph
requires:
  - phase: 31-02
    provides: "PgMetricsSplit with compaction columns, PostgreSQL migration 26 with composite scope index"
provides:
  - "ListMetricsSplitsForCompactionRequest RPC for querying splits by compaction scope"
  - "PostgreSQL implementation querying (index_id, window_start, sort_schema) with composite index"
  - "Atomic publish with replaced_split_ids count verification"
  - "File-backed stub returning unimplemented error for compaction scope queries"
affects: [33-merge-policy, 34-merge-executor, 35-compaction-planning]

# Tech tracking
tech-stack:
  added: []
  patterns: [Row-based column extraction for sqlx queries exceeding 16-field tuple limit, CTE-based atomic publish with dual count verification]

key-files:
  created: []
  modified:
    - quickwit/quickwit-proto/protos/quickwit/metastore.proto
    - quickwit/quickwit-proto/src/codegen/quickwit/quickwit.metastore.rs
    - quickwit/quickwit-metastore/src/metastore/postgres/metastore.rs
    - quickwit/quickwit-metastore/src/metastore/file_backed/mod.rs
    - quickwit/quickwit-metastore/src/metastore/control_plane_metastore.rs

key-decisions:
  - "Row-based sqlx column extraction (sqlx::Row::get) instead of tuple-based query_as for queries with >16 columns"
  - "Dual count verification in publish_metrics_splits: both published_count and marked_count must match expectations"
  - "File-backed metastore returns MetastoreError::Internal for compaction scope queries (compaction is PostgreSQL-only)"

patterns-established:
  - "Compaction scope query pattern: (index_id, window_start, sort_schema) -> Published splits via composite index"
  - "Row-based extraction pattern: use sqlx::query + Row::get(column_name) when column count exceeds sqlx tuple limit"

requirements-completed: [META-05, META-06]

# Metrics
duration: 19min
completed: 2026-02-23
---

# Phase 31 Plan 03: Compaction Scope RPC and Atomic Publish Summary

**ListMetricsSplitsForCompaction RPC with PostgreSQL composite-index query and atomic publish with replaced_split_ids count verification ensuring transactional rollback on partial failures**

## Performance

- **Duration:** 19 min
- **Started:** 2026-02-23T20:57:14Z
- **Completed:** 2026-02-23T21:16:30Z
- **Tasks:** 2
- **Files modified:** 5

## Accomplishments
- New ListMetricsSplitsForCompactionRequest proto message with index_id, window_start, sort_schema fields; PostgreSQL implementation queries by compaction scope using idx_metrics_splits_compaction_scope composite index
- Atomic publish with dual count verification: both staged_split_ids publish AND replaced_split_ids deletion verified, transaction rolls back entirely if any split not in expected state
- Implemented across all MetastoreService trait implementors: PostgreSQL (real), file-backed (stub), control-plane (forwarding proxy)
- Fixed existing list_metrics_splits to include compaction columns (window_start, window_duration_secs, sort_schema, num_merge_ops, row_keys, zonemap_regexes) and migrated to Row-based extraction
- 7 integration tests added covering: empty compaction scope, scope filtering, Published-only filtering, publish-with-replace, atomic rollback, and already-deleted rejection

## Task Commits

Each task was committed atomically:

1. **Task 1: Add ListMetricsSplitsForCompaction RPC and implement PostgreSQL query** - `c61ca61ea` (feat)
2. **Task 2: Implement atomic publish with replace semantics** - `1b46a6f35` (feat)

## Files Created/Modified
- `quickwit/quickwit-proto/protos/quickwit/metastore.proto` - New ListMetricsSplitsForCompaction RPC and ListMetricsSplitsForCompactionRequest message
- `quickwit/quickwit-proto/src/codegen/quickwit/quickwit.metastore.rs` - Auto-generated from proto (trait, client, server, tower service)
- `quickwit/quickwit-metastore/src/metastore/postgres/metastore.rs` - PostgreSQL implementation of list_metrics_splits_for_compaction, replaced_split_ids count verification, Row-based extraction for list_metrics_splits, 7 integration tests
- `quickwit/quickwit-metastore/src/metastore/file_backed/mod.rs` - Stub returning MetastoreError::Internal for compaction scope queries
- `quickwit/quickwit-metastore/src/metastore/control_plane_metastore.rs` - Forwarding proxy for list_metrics_splits_for_compaction

## Decisions Made
- **Row-based column extraction**: sqlx tuple-based `query_as` is limited to 16 fields. With 22 columns (after migration 26 added 6 compaction columns), switched to `sqlx::query` with `Row::get("column_name")` for named column access. This is more maintainable and avoids the tuple limit.
- **Dual count verification in publish**: The existing implementation verified published_count but not marked_count. Added marked_count verification so that if any replaced split is missing or not in Published state, the entire CTE transaction rolls back via the `run_with_tx!` error path.
- **File-backed stub returns Internal error**: Compaction scope queries are a PostgreSQL-only feature (requires the composite index). The file-backed metastore is used for local dev/testing but compaction planning will always use PostgreSQL.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed existing list_metrics_splits missing compaction columns**
- **Found during:** Task 1 (compilation)
- **Issue:** The existing list_metrics_splits query and PgMetricsSplit construction (from Plan 31-02) did not include the 6 new compaction columns, causing missing field errors
- **Fix:** Updated SQL SELECT to include window_start, window_duration_secs, sort_schema, num_merge_ops, row_keys, zonemap_regexes; updated PgMetricsSplit construction to use Row-based named column extraction
- **Files modified:** `quickwit/quickwit-metastore/src/metastore/postgres/metastore.rs`
- **Verification:** Compilation succeeds, all 261 parquet-engine tests pass
- **Committed in:** c61ca61ea (Task 1 commit)

**2. [Rule 3 - Blocking] Switched from tuple-based to Row-based sqlx extraction**
- **Found during:** Task 1 (compilation)
- **Issue:** sqlx `query_as` with tuple type only supports up to 16 fields; 22 columns exceed this limit
- **Fix:** Switched both list_metrics_splits and list_metrics_splits_for_compaction to use `sqlx::query` + `Row::get("column_name")` instead of `query_as` with tuple types
- **Files modified:** `quickwit/quickwit-metastore/src/metastore/postgres/metastore.rs`
- **Verification:** Compilation succeeds, all tests pass
- **Committed in:** c61ca61ea (Task 1 commit)

---

**Total deviations:** 2 auto-fixed (1 bug, 1 blocking)
**Impact on plan:** Both auto-fixes necessary for compilation. The sqlx tuple limit was not anticipated in the plan. No scope creep.

## Issues Encountered
- Pre-existing `Path::exists()` usage in `split_writer.rs` causes clippy deny error (from clippy.toml disallowed methods). Out of scope for this plan.
- Pre-existing clippy warnings in file_backed/mod.rs (manual_ok_err). Out of scope.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- ListMetricsSplitsForCompaction RPC ready for compaction planner (Phase 35) to query splits by scope
- Atomic publish with replace ready for merge executor (Phase 34) to atomically swap old splits with merged splits
- All 261 parquet-engine tests pass, 3 unit tests pass in metastore
- Integration tests (requiring PostgreSQL) are written and ready to run with `make docker-compose-up`

## Self-Check: PASSED

All 5 modified files verified present. Both task commits (c61ca61ea, 1b46a6f35) verified in git history.

---
*Phase: 31-metadata-foundation*
*Completed: 2026-02-23*
