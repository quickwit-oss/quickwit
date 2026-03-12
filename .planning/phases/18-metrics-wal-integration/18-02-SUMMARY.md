---
phase: 18-metrics-wal-integration
plan: 02
subsystem: ingest
tags: [wal, metrics, queue-id, adr-2, namespace]

# Dependency graph
requires:
  - phase: 18-metrics-wal-integration/01
    provides: MetricsWal struct with Arc<RwLock<>> wrapper
provides:
  - MetricsQueueId type with metrics/{index_uid}/{source_id}/{shard_id} format
  - METRICS_QUEUE_PREFIX constant for namespace filtering
  - is_metrics_queue() helper for prefix detection
  - check_enough_capacity() for disk/memory capacity checks
  - force_delete_queue() for cleanup operations
  - queue_position_range() for recovery logic
  - list_metrics_queues() for namespace filtering
  - NotEnoughCapacityError for capacity error reporting
  - 25 unit tests total (7 queue_id + 18 metrics_wal)
affects: [19-checkpointing, 21-retry-recovery, 22-rate-limiting]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - ADR-2 queue namespace format (metrics/{index_uid}/{source_id}/{shard_id})
    - Capacity checking pattern from mrecordlog_utils.rs

key-files:
  created:
    - quickwit/quickwit-ingest/src/metrics/queue_id.rs
  modified:
    - quickwit/quickwit-ingest/src/metrics/metrics_wal.rs
    - quickwit/quickwit-ingest/src/metrics/mod.rs

key-decisions:
  - "MetricsQueueId enforces ADR-2 format at compile time"
  - "Utility functions mirror mrecordlog_utils.rs patterns exactly"

patterns-established:
  - "Queue IDs must use metrics/ prefix for metrics namespace"
  - "check_enough_capacity uses config.disk_capacity and config.memory_capacity"

# Metrics
duration: 3min
completed: 2026-01-19
---

# Phase 18 Plan 02: Queue ID and Utilities Summary

**MetricsQueueId type enforcing ADR-2 namespace format, plus check_enough_capacity(), force_delete_queue(), queue_position_range(), and list_metrics_queues() utility functions with 25 total tests**

## Performance

- **Duration:** 3 min
- **Started:** 2026-01-19T07:10:47Z
- **Completed:** 2026-01-19T07:13:59Z
- **Tasks:** 3
- **Files modified:** 3

## Accomplishments

- Created MetricsQueueId type with `metrics/{index_uid}/{source_id}/{shard_id}` format (ADR-2)
- Added METRICS_QUEUE_PREFIX constant and is_metrics_queue() helper for namespace filtering
- Implemented check_enough_capacity() for disk/memory capacity checks (mirrors logs pattern)
- Implemented force_delete_queue() to ignore MissingQueue errors during cleanup
- Implemented queue_position_range() for recovery logic (returns first..=last positions)
- Implemented list_metrics_queues() to filter by metrics/ prefix
- Added NotEnoughCapacityError enum for capacity error reporting
- Added 25 comprehensive unit tests (7 queue_id + 18 metrics_wal)

## Task Commits

Each task was committed atomically:

1. **Task 1: Create metrics queue ID formatting module** - `4fa05a9b` (feat)
2. **Task 2: Add MetricsWal utility functions** - `f9245771` (feat)
3. **Task 3: Add comprehensive tests for queue ID and utilities** - `78e71790` (test)

## Files Created/Modified

- `quickwit/quickwit-ingest/src/metrics/queue_id.rs` - MetricsQueueId type, METRICS_QUEUE_PREFIX, is_metrics_queue()
- `quickwit/quickwit-ingest/src/metrics/metrics_wal.rs` - NotEnoughCapacityError, utility functions, 9 new tests
- `quickwit/quickwit-ingest/src/metrics/mod.rs` - Re-exports for queue_id types and NotEnoughCapacityError

## Decisions Made

1. **MetricsQueueId enforces format at compile time** - Type system ensures queue IDs follow ADR-2 convention
2. **Utility patterns match logs pipeline exactly** - No new patterns introduced, ensuring consistency

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None - all tasks completed successfully.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- MetricsQueueId provides type-safe queue ID formatting for ADR-2 compliance
- Utility functions ready for Phase 19 (Checkpointing) and Phase 21 (Retry & Recovery)
- check_enough_capacity enables Phase 22 (Rate Limiting) capacity checks
- list_metrics_queues() supports namespace filtering for mixed WAL scenarios
- All 25 tests provide confidence matching logs WAL test coverage

---
*Phase: 18-metrics-wal-integration*
*Completed: 2026-01-19*
