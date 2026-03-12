---
phase: 10-production-hardening
plan: 04
subsystem: observability
tags: [tracing, metrics, datafusion, parquet, arrow-ipc]

# Dependency graph
requires:
  - phase: 10-01
    provides: Prometheus metrics infrastructure
  - phase: 10-02
    provides: Structured tracing patterns in core modules
provides:
  - Integration layer tracing (doc processor, indexer, leaf search)
  - Error-level logging for failure paths
  - Debug logging for batch processing flow
affects: [production-monitoring, debugging, performance-analysis]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "#[instrument] attribute for span creation"
    - "skip(self, data) to avoid cloning large data into spans"
    - "debug! for batch accumulation progress"
    - "info! for split production with metadata"
    - "warn! for error paths with context"

key-files:
  created: []
  modified:
    - quickwit/quickwit-search/src/metrics_leaf.rs
    - quickwit/quickwit-indexing/src/actors/metrics_doc_processor.rs
    - quickwit/quickwit-indexing/src/actors/metrics_indexer.rs

key-decisions:
  - "Fixed parquet_files field access instead of non-existent method"
  - "Added #[instrument] to Handler::handle for automatic span creation"
  - "Log batch format detection at debug level to avoid noise"
  - "Log split production at info level for operational visibility"

patterns-established:
  - "Integration layer observability: debug for flow, info for milestones, warn for errors"

# Metrics
duration: 8 min
completed: 2026-01-15
---

# Phase 10 Plan 04: Integration Layer Observability Summary

**Added comprehensive tracing and metrics instrumentation to doc processor, indexer, and search leaf modules connecting metrics engine to quickwit pipeline**

## Performance

- **Duration:** 8 min
- **Started:** 2026-01-15T00:00:00Z
- **Completed:** 2026-01-15T00:08:00Z
- **Tasks:** 3
- **Files modified:** 3

## Accomplishments

- Fixed broken parquet_path() method call with correct field access
- Added #[instrument] attribute to MetricsDocProcessor for automatic span creation
- Added comprehensive debug/info/warn logging throughout batch processing flow
- Added split production logging with full metadata (split_id, num_rows, size_bytes, time_range)

## Task Commits

Each task was committed atomically:

1. **Task 1: Enhance metrics_leaf.rs observability** - `af7f1774` (fix)
   - Corrected parquet_files field access in tracing
   - Added size_bytes to split metadata debug logging

2. **Task 2: Add observability to metrics doc processor** - `e7287614` (feat)
   - Added #[instrument] with index_id, source_id, batch_len fields
   - Added debug! for batch format detection
   - Added info! when routing batch to metrics pipeline
   - Added warn! for invalid format with first bytes context

3. **Task 3: Add observability to metrics indexer** - `89a5530f` (feat)
   - Added debug! for batch accumulation progress
   - Added info! for split production with full metadata
   - Added debug! for accumulator threshold splits and force commit

## Files Created/Modified

- `quickwit/quickwit-search/src/metrics_leaf.rs` - Fixed parquet_files field access, added size_bytes to logging
- `quickwit/quickwit-indexing/src/actors/metrics_doc_processor.rs` - Added #[instrument], debug/info/warn logging
- `quickwit/quickwit-indexing/src/actors/metrics_indexer.rs` - Added debug/info logging for batch and split flow

## Decisions Made

| Decision | Rationale |
|----------|-----------|
| Fixed parquet_files field access | Previous code called non-existent parquet_path() method |
| Used #[instrument] for span creation | Automatic span creation with field extraction, consistent with 10-02 patterns |
| Skip self and data in instrument | Avoid cloning large data into spans |
| debug! for batch detection | Avoid noise from high-frequency format checks |
| info! for split production | Operational visibility for monitoring |

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed non-existent parquet_path() method call**
- **Found during:** Task 1 (metrics_leaf.rs observability)
- **Issue:** Previous commit (10-02) added `metrics_split.parquet_path()` which doesn't exist
- **Fix:** Changed to `metrics_split.metadata.parquet_files` field access
- **Files modified:** quickwit/quickwit-search/src/metrics_leaf.rs
- **Verification:** cargo check -p quickwit-search succeeds
- **Committed in:** af7f1774

---

**Total deviations:** 1 auto-fixed (1 bug from prior plan)
**Impact on plan:** Essential fix for compilation. No scope creep.

## Issues Encountered

None - plan executed with one necessary bug fix.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- Integration layer fully instrumented for observability
- Operators can trace requests through metrics pipeline
- Debug logging available for batch processing flow
- Info logging for split production milestones
- Phase 10 Production Hardening complete pending final verification

---
*Phase: 10-production-hardening*
*Completed: 2026-01-15*
