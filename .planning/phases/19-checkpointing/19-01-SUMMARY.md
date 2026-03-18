---
phase: 19-checkpointing
plan: 01
subsystem: ingest
tags: [wal, position-tracking, state-management, two-phase-locking, durability]

# Dependency graph
requires:
  - phase: 18-metrics-wal-integration
    provides: MetricsWal struct with Arc<RwLock<>> wrapper, utility functions
provides:
  - MetricsShardPosition for per-shard replication/truncation position tracking
  - MetricsIngesterState wrapping WAL with two-phase locking
  - MetricsIngesterInner with shard_positions HashMap
  - FullyLockedMetricsState for atomic operations
  - Recovery-on-init logic from WAL queues
  - 13 unit tests (7 shard_position + 6 state)
affects: [19-02-metastore-persistence, 20-cluster-gossip, 21-retry-recovery]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - Per-shard position tracking (ADR-4 pattern)
    - Two-phase locking (WAL first, then inner state)
    - Recovery-on-init from WAL position ranges

key-files:
  created:
    - quickwit/quickwit-ingest/src/metrics/shard_position.rs
    - quickwit/quickwit-ingest/src/metrics/state.rs
  modified:
    - quickwit/quickwit-ingest/src/metrics/mod.rs

key-decisions:
  - "Position tracking uses quickwit_proto::types::Position enum (reuse existing type)"
  - "Two-phase locking order: WAL first, then inner state (prevents deadlocks)"
  - "Recovery infers truncation_position from queue start position"

patterns-established:
  - "MetricsShardPosition tracks replication_position_inclusive and truncation_position_inclusive"
  - "lock_fully() acquires both locks atomically in correct order"
  - "init() recovers shard positions from WAL queue_position_range"

# Metrics
duration: 4min
completed: 2026-01-19
---

# Phase 19 Plan 01: Shard Position and State Summary

**MetricsShardPosition for per-shard position tracking and MetricsIngesterState with two-phase locking for WAL + position management, with 13 unit tests**

## Performance

- **Duration:** 4 min
- **Started:** 2026-01-19T07:31:43Z
- **Completed:** 2026-01-19T07:35:32Z
- **Tasks:** 3
- **Files modified:** 3

## Accomplishments

- Created MetricsShardPosition struct tracking replication and truncation positions per shard (ADR-4)
- Implemented position update methods that only advance forward (monotonic progress)
- Created MetricsIngesterState with Arc<RwLock<MetricsWal>> and Arc<Mutex<MetricsIngesterInner>>
- Implemented two-phase locking pattern (lock_fully) preventing deadlocks
- Implemented recovery-on-init logic that restores shard positions from WAL queue position ranges
- Added 13 comprehensive unit tests (7 shard_position + 6 state)

## Task Commits

Each task was committed atomically:

1. **Task 1: Create MetricsShardPosition for per-shard position tracking** - `d0c85a6c` (feat)
2. **Task 2: Create MetricsIngesterState for WAL + position management** - `b806467d` (feat)
3. **Task 3: Add unit tests** - Tests included in Task 1 and Task 2 commits (inline in same files)

## Files Created/Modified

- `quickwit/quickwit-ingest/src/metrics/shard_position.rs` - MetricsShardPosition struct with position tracking methods and 7 tests
- `quickwit/quickwit-ingest/src/metrics/state.rs` - MetricsIngesterState, MetricsIngesterInner, FullyLockedMetricsState, init() recovery, and 6 tests
- `quickwit/quickwit-ingest/src/metrics/mod.rs` - Added module declarations and re-exports

## Decisions Made

1. **Reuse Position enum from quickwit_proto** - No new position type needed, existing type has all required functionality
2. **Two-phase locking order** - WAL lock acquired first (most expensive), then inner state, matching logs pipeline pattern
3. **Recovery truncation position** - Inferred from queue start position: if start > 0, truncation = start - 1, otherwise Beginning

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None - all tasks completed successfully.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- MetricsShardPosition provides foundation for position tracking in checkpointing flow
- MetricsIngesterState ready for integration with metastore persistence (Plan 02)
- Two-phase locking pattern established for safe concurrent access
- Recovery-on-init tested and ready for Phase 21 (Retry & Recovery)
- All 13 tests provide confidence for position tracking and state management

---
*Phase: 19-checkpointing*
*Completed: 2026-01-19*
