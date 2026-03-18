---
phase: 26-metrics-sequencer
plan: 01
subsystem: indexing
tags: [sequencer, metrics, ordering, oneshot, actor]

# Dependency graph
requires:
  - phase: 25-metrics-testing
    provides: metrics pipeline infrastructure
provides:
  - MetricsSplitsUpdateMailbox with Sequencer variant for ordered delivery
  - MetricsSplitsUpdateSender for oneshot channel routing
  - get_sender() method for position reservation pattern
affects: [metrics-pipeline, merge-pipeline]

# Tech tracking
tech-stack:
  added: []
  patterns: [oneshot-channel-sequencer-pattern, reserve-before-async-work]

key-files:
  created: []
  modified:
    - quickwit/quickwit-indexing/src/actors/metrics_uploader.rs

key-decisions:
  - "Mirror logs Uploader pattern for MetricsUploader sequencer integration"
  - "Reserve sequencer position BEFORE spawning async upload task"

patterns-established:
  - "MetricsSplitsUpdateSender: oneshot routing for Sequencer vs direct Publisher"
  - "get_sender() pattern: reserve position, proceed/discard on completion"

# Metrics
duration: 8min
completed: 2026-01-21
---

# Phase 26 Plan 01: Sequencer Integration Summary

**Integrated Sequencer actor with MetricsUploader for ordered split publishing using oneshot channels**

## Performance

- **Duration:** 8 min
- **Started:** 2026-01-21T00:00:00Z
- **Completed:** 2026-01-21T00:08:00Z
- **Tasks:** 3
- **Files modified:** 1

## Accomplishments

- Extended MetricsSplitsUpdateMailbox with Sequencer variant for ordered delivery
- Implemented MetricsSplitsUpdateSender enum for Publisher/Sequencer routing
- Added get_sender() method to reserve sequencer position before async work
- Updated MetricsUploader handler to use sender pattern with Proceed/Discard
- Added test proving sequencer maintains FIFO ordering

## Task Commits

Each task was committed atomically:

1. **Task 1: Extend MetricsSplitsUpdateMailbox with Sequencer variant** - `2004ba56` (feat)
2. **Task 2: Update MetricsUploader handler to use sender pattern** - `42e85575` (feat)
3. **Task 3: Add sequencer ordering test** - `d90a409f` (test)

## Files Created/Modified

- `quickwit/quickwit-indexing/src/actors/metrics_uploader.rs` - Added MetricsSplitsUpdateSender enum, Sequencer variant, get_sender() method, updated handler, and ordering test

## Decisions Made

- Mirrored the logs Uploader pattern (SplitsUpdateMailbox/SplitsUpdateSender) for MetricsUploader
- Reserve sequencer position BEFORE spawning async upload task to ensure ordering
- Send SequencerCommand::Discard on all error paths to avoid blocking sequencer

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- Sequencer integration complete for metrics pipeline
- Ready for next phase or additional verification
- All existing tests continue to pass (Publisher variant unchanged)

---
*Phase: 26-metrics-sequencer*
*Completed: 2026-01-21*
