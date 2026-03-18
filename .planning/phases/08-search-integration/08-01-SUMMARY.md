---
phase: 08-search-integration
plan: 01
subsystem: search
tags: [leaf-search, metrics-routing, datafusion, parquet]

# Dependency graph
requires:
  - phase: 07-pipeline-integration
    provides: MetricsDocProcessor, MetricsIndexer, index name routing
provides:
  - is_metrics_index() function for detecting metrics indexes
  - leaf_search_metrics_split() stub for DataFusion execution path
  - Routing infrastructure in leaf.rs for metrics index detection
affects: [08-02, 08-03, search-layer]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - Index name prefix routing (otel-metrics-*, metrics-*)
    - Early routing before Tantivy index loading

key-files:
  created:
    - quickwit/quickwit-search/src/metrics_leaf.rs
  modified:
    - quickwit/quickwit-search/Cargo.toml
    - quickwit/quickwit-search/src/lib.rs
    - quickwit/quickwit-search/src/leaf.rs

key-decisions:
  - "Routing check at leaf_search_single_split level for efficiency"
  - "Use index_id_patterns first element for index detection"
  - "Duplicate is_metrics_index() in search crate to avoid indexing dependency"

patterns-established:
  - "Metrics routing pattern: check index name prefix, route to DataFusion path"
  - "Stub pattern: return empty response while routing infrastructure is established"

# Metrics
duration: 5min
completed: 2026-01-15
---

# Phase 8 Plan 01: Metrics Index Routing Summary

**Added metrics index routing to leaf search layer with MetricsSplit loading stub**

## Performance

- **Duration:** 5 min
- **Started:** 2026-01-15T12:49:05Z
- **Completed:** 2026-01-15T12:54:04Z
- **Tasks:** 3
- **Files modified:** 4

## Accomplishments

- Added quickwit-metrics-engine dependency to quickwit-search crate
- Created metrics_leaf.rs module with is_metrics_index() and leaf_search_metrics_split()
- Integrated routing check in leaf_search_single_split before Tantivy index loading
- Exported is_metrics_index from quickwit-search for use by other crates

## Task Commits

Each task was committed atomically:

1. **Task 1: Add quickwit-metrics-engine dependency** - `b751b693` (chore)
2. **Task 2: Create metrics_leaf module** - `49fc544a` (feat)
3. **Task 3: Add metrics routing to leaf_search_single_split** - `2b1acda4` (feat)

## Files Created/Modified

- `quickwit/quickwit-search/Cargo.toml` - Added quickwit-metrics-engine dependency
- `quickwit/quickwit-search/src/metrics_leaf.rs` - New module with routing logic and stub
- `quickwit/quickwit-search/src/lib.rs` - Added metrics_leaf module and export
- `quickwit/quickwit-search/src/leaf.rs` - Added import and routing check in leaf_search_single_split

## Decisions Made

1. **Routing location**: Added routing check in `leaf_search_single_split` rather than `single_doc_mapping_leaf_search` because the per-split level allows for future flexibility if different splits within an index need different handling.

2. **Index detection via index_id_patterns**: At leaf search level, `index_id_patterns` contains the actual index IDs being searched (not patterns), so checking the first element is sufficient.

3. **Duplicate is_metrics_index()**: Intentionally duplicated the function from indexing_pipeline.rs to avoid a dependency from quickwit-search on quickwit-indexing. Both crates need this check independently: indexing for write path routing, search for read path routing.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- Routing infrastructure complete, ready for Plan 02 (DataFusion execution implementation)
- leaf_search_metrics_split stub returns empty response - needs DataFusion integration
- is_metrics_index correctly identifies otel-metrics-* and metrics-* indexes

---
*Phase: 08-search-integration*
*Completed: 2026-01-15*
