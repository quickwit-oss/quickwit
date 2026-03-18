---
phase: 04-query-engine
plan: 01
subsystem: query
tags: [datafusion, parquet, tableprovider, sql]

# Dependency graph
requires:
  - phase: 02-storage-layer
    provides: MetricsSplit, MetricsSplitMetadata, parquet file storage
  - phase: 01-foundation
    provides: MetricsSchema, TimeRange
provides:
  - SplitRegistry for split management and query pruning
  - MetricsTableProvider implementing DataFusion TableProvider
  - SQL query execution against metrics splits
affects: [05-promql-engine, 06-api-layer]

# Tech tracking
tech-stack:
  added:
    - async-trait (for TableProvider implementation)
  patterns:
    - "DataFusion TableProvider for custom data sources"
    - "Split pruning by time range and metric name"
    - "ParquetExec for efficient Parquet file scanning"

key-files:
  created:
    - quickwit/quickwit-metrics-engine/src/query/registry.rs
    - quickwit/quickwit-metrics-engine/src/query/provider.rs
  modified:
    - quickwit/quickwit-metrics-engine/src/query/context.rs
    - quickwit/quickwit-metrics-engine/src/query/mod.rs
    - quickwit/quickwit-metrics-engine/Cargo.toml

key-decisions:
  - "SplitRegistry uses Vec<MetricsSplit> for simple split collection"
  - "MetricsTableProvider creates ParquetExec from split parquet files"
  - "Empty splits return EmptyExec instead of error"

patterns-established:
  - "TableProvider implementation pattern for metrics data"
  - "Split pruning for query optimization"

# Metrics
duration: 4min
completed: 2026-01-15
---

# Phase 4 Plan 1: TableProvider Implementation Summary

**DataFusion TableProvider enabling SQL queries against Parquet-based metrics splits with split pruning for query optimization**

## Performance

- **Duration:** 4 min
- **Started:** 2026-01-15T05:55:27Z
- **Completed:** 2026-01-15T05:59:35Z
- **Tasks:** 3
- **Files modified:** 5

## Accomplishments

- SplitRegistry for managing queryable splits with time range and metric name pruning
- MetricsTableProvider implementing DataFusion TableProvider trait
- ParquetExec-based scan execution for efficient Parquet file reading
- SQL query execution methods on MetricsSessionContext
- Comprehensive test coverage for all new components

## Task Commits

Each task was committed atomically:

1. **Task 1: Create SplitRegistry for managing queryable splits** - `d09f3296` (feat)
2. **Task 2: Implement MetricsTableProvider for DataFusion integration** - `4b019ea0` (feat)
3. **Task 3: Add query execution methods to MetricsSessionContext** - `8fbd60f3` (feat)

## Files Created/Modified

- `quickwit/quickwit-metrics-engine/src/query/registry.rs` - SplitRegistry with pruning methods
- `quickwit/quickwit-metrics-engine/src/query/provider.rs` - MetricsTableProvider implementing TableProvider
- `quickwit/quickwit-metrics-engine/src/query/context.rs` - Added register_splits() and query_metrics() methods
- `quickwit/quickwit-metrics-engine/src/query/mod.rs` - Exported new modules
- `quickwit/quickwit-metrics-engine/Cargo.toml` - Added async-trait dependency

## Decisions Made

1. **ParquetExec builder API** - Using ParquetExec::builder(config).build() for DataFusion 45 compatibility
2. **EmptyExec for empty splits** - Returns empty execution plan instead of error when no parquet files
3. **ObjectStoreUrl::local_filesystem()** - Using local filesystem object store for file:// URLs

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- Query engine foundation complete with TableProvider implementation
- SQL queries can now be executed against registered metrics tables

---
*Phase: 04-query-engine*
*Completed: 2026-01-15*
