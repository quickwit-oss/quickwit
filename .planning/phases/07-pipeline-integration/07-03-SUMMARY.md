---
phase: 07-pipeline-integration
plan: 03
subsystem: indexing
tags: [arrow, parquet, pipeline, routing, actor]

# Dependency graph
requires:
  - phase: 07-02
    provides: MetricsDocProcessor and MetricsIndexer actors
provides:
  - is_metrics_index() routing helper
  - spawn_metrics_pipeline() method
  - Automatic pipeline routing by index type
  - MetricsSourceActor for Source-to-MetricsDocProcessor bridging
affects: [08-search-integration, 09-retention]

# Tech tracking
tech-stack:
  added: []
  patterns: [index-name-based routing, MetricsSourceActor adapter pattern]

key-files:
  created: []
  modified:
    - quickwit/quickwit-indexing/src/actors/indexing_pipeline.rs
    - quickwit/quickwit-indexing/src/source/mod.rs

key-decisions:
  - "Index name prefix routing: otel-metrics and metrics- trigger Parquet pipeline"
  - "MetricsSourceActor adapter: Bridge Source trait to MetricsDocProcessor without modifying Source trait"

patterns-established:
  - "Pipeline routing: Check index_id early in spawn_pipeline() to branch to appropriate pipeline"
  - "Source adapter: When Source trait is typed to specific downstream, create parallel actor type"

# Metrics
duration: 12min
completed: 2026-01-15
---

# Phase 7 Plan 03: Pipeline Integration Summary

**Automatic routing of metrics indexes to Parquet/DataFusion pipeline based on index naming convention**

## Performance

- **Duration:** 12 min
- **Started:** 2026-01-15T15:00:00Z
- **Completed:** 2026-01-15T15:12:00Z
- **Tasks:** 3
- **Files modified:** 2

## Accomplishments
- Implemented index name-based routing for metrics pipelines
- Created MetricsSourceActor to bridge Source trait to MetricsDocProcessor
- Added spawn_metrics_pipeline() to construct metrics-specific actor chain
- Metrics indexes now automatically use Arrow/Parquet path
- Existing Tantivy pipeline unchanged for logs/traces

## Task Commits

Each task was committed atomically:

1. **Task 1: Add metrics pipeline detection helper** - `7859c5f0` (feat)
2. **Task 2: Create metrics indexing pipeline builder** - `9978b1bd` (feat)
3. **Task 3: Route pipeline based on index type** - `1cab8fbc` (feat)

## Files Created/Modified
- `quickwit/quickwit-indexing/src/actors/indexing_pipeline.rs` - Added is_metrics_index(), spawn_metrics_pipeline(), routing logic, and test
- `quickwit/quickwit-indexing/src/source/mod.rs` - Added MetricsSourceActor to route Source to MetricsDocProcessor

## Decisions Made
- **Index name prefix routing:** Using `otel-metrics` and `metrics-` prefixes to identify metrics indexes, aligning with OpenTelemetry conventions
- **MetricsSourceActor adapter:** Created parallel actor type instead of making Source trait generic, to minimize changes to existing code

## Deviations from Plan

### Auto-fixed Issues

**1. [Source trait type mismatch] Created MetricsSourceActor adapter**
- **Found during:** Task 2 (spawn_metrics_pipeline implementation)
- **Issue:** Source trait methods are typed to `Mailbox<DocProcessor>`, cannot use `Mailbox<MetricsDocProcessor>`
- **Fix:** Created MetricsSourceActor that mirrors SourceActor but routes to MetricsDocProcessor
- **Files modified:** quickwit/quickwit-indexing/src/source/mod.rs
- **Verification:** cargo check compiles, all tests pass
- **Committed in:** 9978b1bd (Task 2 commit)

---

**Total deviations:** 1 auto-fixed (type system workaround)
**Impact on plan:** MetricsSourceActor is a clean adapter pattern that avoids modifying the Source trait. Future work may make Source generic.

## Issues Encountered
- Source trait tightly coupled to DocProcessor type, requiring adapter pattern for MetricsDocProcessor routing

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- Phase 7 (Pipeline Integration) complete
- Metrics indexing pipeline fully operational
- Ready for Phase 8 (Search Integration) to wire query path
- MetricsSplits written to disk but not yet integrated with Publisher/Uploader (Phase 8 work)

---
*Phase: 07-pipeline-integration*
*Plan: 03*
*Completed: 2026-01-15*
