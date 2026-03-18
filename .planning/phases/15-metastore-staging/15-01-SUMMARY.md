---
phase: 15-metastore-staging
plan: 01
subsystem: indexing
tags: [actor, uploader, metastore, parquet, metrics]

# Dependency graph
requires:
  - phase: 14-metastore-extension
    provides: stage_metrics_splits() metastore operation
provides:
  - MetricsSplitsUpdate message type for downstream processing
  - MetricsUploader actor with staging and upload capability
  - MetricsSplitsUpdateMailbox enum for downstream routing
affects: [15-02-pipeline-wiring, 16-publishing]

# Tech tracking
tech-stack:
  added: []
  patterns: [actor-based message passing for metrics, split staging/upload workflow]

key-files:
  created:
    - quickwit/quickwit-indexing/src/actors/metrics_uploader.rs
    - quickwit/quickwit-indexing/src/models/metrics_splits_update.rs
  modified:
    - quickwit/quickwit-indexing/src/actors/mod.rs
    - quickwit/quickwit-indexing/src/models/mod.rs

key-decisions:
  - "Publisher-only MetricsSplitsUpdateMailbox initially (Sequencer integration deferred to Phase 16)"
  - "Uses simple index_id: String instead of IndexUid (metrics use simpler identity model)"
  - "Empty file placeholder for Parquet upload (actual file reading needs output_dir from MetricsSplitBatch)"

patterns-established:
  - "MetricsUploader mirrors Uploader pattern: semaphore-gated concurrent uploads, stage-then-upload workflow"
  - "MetricsSplitsUpdate mirrors SplitsUpdate but with metrics-specific types"

# Metrics
duration: 5min
completed: 2026-01-18
---

# Phase 15-01: Metastore Staging Summary

**MetricsUploader actor and MetricsSplitsUpdate message for staging metrics splits via actor-based message passing**

## Performance

- **Duration:** 5 min
- **Started:** 2026-01-18T18:34:50Z
- **Completed:** 2026-01-18T18:39:39Z
- **Tasks:** 3
- **Files modified:** 4

## Accomplishments
- Created MetricsSplitsUpdate message type mirroring SplitsUpdate for metrics pipeline
- Implemented MetricsUploader actor with Handler<MetricsSplitBatch> for staging and uploading
- Added MetricsSplitsUpdateMailbox enum (Publisher variant) for downstream routing
- Added unit tests verifying staging calls and empty batch handling

## Task Commits

Each task was committed atomically:

1. **Task 1: Create MetricsSplitsUpdate message type** - `42ac328b` (feat)
2. **Task 2: Create MetricsUploader actor** - `7704ac8a` (feat)
3. **Task 3: Add unit tests for MetricsUploader** - `313c8bcf` (test)

## Files Created/Modified
- `quickwit/quickwit-indexing/src/models/metrics_splits_update.rs` - MetricsSplitsUpdate struct for downstream message passing
- `quickwit/quickwit-indexing/src/models/mod.rs` - Export MetricsSplitsUpdate
- `quickwit/quickwit-indexing/src/actors/metrics_uploader.rs` - MetricsUploader actor with staging/upload/counters
- `quickwit/quickwit-indexing/src/actors/mod.rs` - Export MetricsUploader, MetricsUploaderCounters, MetricsSplitsUpdateMailbox

## Decisions Made
- **Publisher-only mailbox:** Initially only supporting Publisher variant in MetricsSplitsUpdateMailbox since Sequencer integration requires additional work in Phase 16
- **Placeholder Parquet upload:** Currently uploads empty files as placeholder; actual file reading from MetricsIndexer output_dir needs additional plumbing via MetricsSplitBatch
- **No merge_task field:** MetricsSplitsUpdate omits merge_task field present in SplitsUpdate since metrics merging is not yet implemented

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Fixed empty checkpoint delta in test**
- **Found during:** Task 3 (Unit tests)
- **Issue:** SourceCheckpointDelta::from_range(0..0) is invalid and causes panic
- **Fix:** Changed to valid range 0..1 for empty batch test
- **Files modified:** quickwit/quickwit-indexing/src/actors/metrics_uploader.rs
- **Verification:** All tests pass
- **Committed in:** 313c8bcf (Task 3 commit)

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** Minor test data fix, no scope creep.

## Issues Encountered
- Initial PutPayload usage error (trait vs type confusion) - fixed by using `Box<dyn PutPayload>` with concrete `Vec<u8>`

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- MetricsUploader actor ready for pipeline wiring
- Plan 15-02 can wire MetricsIndexer -> MetricsUploader in the pipeline
- Note: Parquet file upload is placeholder; real integration needs output_dir path from MetricsIndexer

---
*Phase: 15-metastore-staging*
*Completed: 2026-01-18*
