---
phase: 07-pipeline-integration
plan: 02
subsystem: indexing
tags: [arrow, parquet, metrics, actor, accumulator]

# Dependency graph
requires:
  - phase: 07-01
    provides: MetricsDocProcessor actor for Arrow IPC processing
  - phase: 03-ingest-pipeline
    provides: MetricsBatchAccumulator for split production
  - phase: 02-storage-layer
    provides: MetricsSplitWriter for Parquet output
provides:
  - MetricsIndexer actor with Handler<ProcessedMetricsBatch>
  - ProcessedMetricsBatch message type for Arrow RecordBatch pipeline
  - MetricsSplitBatch message type for downstream split delivery
  - Full integration of MetricsDocProcessor -> MetricsIndexer pipeline
affects: [07-pipeline-integration, 08-query-integration]

# Tech tracking
tech-stack:
  added: []
  patterns: [actor-based pipeline with Arrow RecordBatch messages]

key-files:
  created:
    - quickwit/quickwit-indexing/src/actors/metrics_indexer.rs
    - quickwit/quickwit-indexing/src/models/processed_metrics_batch.rs
  modified:
    - quickwit/quickwit-indexing/src/actors/mod.rs
    - quickwit/quickwit-indexing/src/actors/metrics_doc_processor.rs
    - quickwit/quickwit-indexing/src/models/mod.rs
    - quickwit/quickwit-indexing/Cargo.toml

key-decisions:
  - "Use ProcessedMetricsBatch as intermediate message type between MetricsDocProcessor and MetricsIndexer"
  - "MetricsIndexer uses MetricsBatchAccumulator for threshold-based split production"
  - "Support optional downstream integration via with_indexer() constructor"
  - "Forward NewPublishLock and NewPublishToken messages for coordination"

patterns-established:
  - "Arrow RecordBatch messages replace TantivyDocument for metrics pipeline"
  - "force_commit flag propagation through pipeline for immediate flush"
  - "Atomic counter recording with careful flush_accumulator separation"

# Metrics
duration: 8min
completed: 2026-01-15
---

# Phase 07 Plan 02: MetricsIndexer Integration Summary

**MetricsIndexer actor using MetricsBatchAccumulator to produce Parquet-based MetricsSplit, completing the metrics ingest pipeline from RawDocBatch to split files**

## Performance

- **Duration:** 8 min
- **Started:** 2026-01-15T06:45:45Z
- **Completed:** 2026-01-15T06:53:33Z
- **Tasks:** 3
- **Files modified:** 6

## Accomplishments
- Created ProcessedMetricsBatch message type for Arrow RecordBatch pipeline messages
- Built MetricsIndexer actor with threshold-based split production using MetricsBatchAccumulator
- Integrated MetricsDocProcessor with MetricsIndexer for full pipeline flow
- Added comprehensive test coverage including end-to-end integration test

## Task Commits

Each task was committed atomically:

1. **Task 1: Create ProcessedMetricsBatch message type** - `ee505661` (feat)
2. **Task 2: Create MetricsIndexer actor** - `09720163` (feat)
3. **Task 3: Export MetricsIndexer and update MetricsDocProcessor** - `aeeb4650` (feat)

## Files Created/Modified
- `quickwit/quickwit-indexing/src/models/processed_metrics_batch.rs` - ProcessedMetricsBatch message type with memory tracking
- `quickwit/quickwit-indexing/src/actors/metrics_indexer.rs` - MetricsIndexer actor with accumulator integration
- `quickwit/quickwit-indexing/src/models/mod.rs` - Export ProcessedMetricsBatch
- `quickwit/quickwit-indexing/src/actors/mod.rs` - Export MetricsIndexer and MetricsSplitBatch
- `quickwit/quickwit-indexing/src/actors/metrics_doc_processor.rs` - Integration with MetricsIndexer
- `quickwit/quickwit-indexing/Cargo.toml` - Added arrow dependency

## Decisions Made
- Used ProcessedMetricsBatch as intermediate message to decouple MetricsDocProcessor from MetricsIndexer
- MetricsIndexer produces MetricsSplitBatch for downstream handling (future plan integration)
- Added with_indexer() constructor to MetricsDocProcessor for optional downstream configuration
- Separated counter recording from flush_accumulator to avoid double-counting splits

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
- Initial implementation double-counted splits in MetricsIndexerCounters due to recording in both flush_accumulator and the calling code. Fixed by removing counter recording from flush_accumulator and placing it in the callers (finalize, NewPublishLock handler, and process_batch).
- Added arrow to regular dependencies (was only in dev-dependencies) since ProcessedMetricsBatch is used in non-test code.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- Metrics ingest pipeline complete: RawDocBatch -> MetricsDocProcessor -> ProcessedMetricsBatch -> MetricsIndexer -> MetricsSplit
- Ready for Plan 03 to integrate with Packager/Uploader for split persistence
- MetricsSplitBatch message type available for downstream actor integration

---
*Phase: 07-pipeline-integration*
*Plan: 02*
*Completed: 2026-01-15*
