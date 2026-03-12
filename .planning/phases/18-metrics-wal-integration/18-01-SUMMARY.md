---
phase: 18-metrics-wal-integration
plan: 01
subsystem: ingest
tags: [wal, mrecordlog, durability, metrics, arc-rwlock]

# Dependency graph
requires:
  - phase: 17-research-deep-dive
    provides: METRICS-DURABILITY-DESIGN.md with ADR decisions
provides:
  - MetricsWal struct wrapping MultiRecordLogAsync
  - MetricsWalConfig with separate wal_dir_path (ADR-1)
  - Queue CRUD operations (create, delete, exists, list)
  - MRecord append/truncate/range operations
  - 9 unit tests for WAL operations
affects: [19-checkpointing, 21-retry-recovery, 22-rate-limiting]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - Arc<RwLock<MultiRecordLogAsync>> for thread-safe WAL access
    - Separate metrics directory for WAL isolation (ADR-1)

key-files:
  created:
    - quickwit/quickwit-ingest/src/metrics/mod.rs
    - quickwit/quickwit-ingest/src/metrics/metrics_wal.rs
  modified:
    - quickwit/quickwit-ingest/src/lib.rs
    - quickwit/quickwit-ingest/src/metrics/prometheus.rs (renamed from metrics.rs)

key-decisions:
  - "Used Arc<RwLock<>> pattern matching logs pipeline locking discipline"
  - "Changed last_record to last_position to avoid lifetime issues with borrowed data"
  - "Preserved existing prometheus metrics by converting to directory module"

patterns-established:
  - "MetricsWal API mirrors MultiRecordLogAsync for consistency"
  - "Queue IDs should follow metrics/{index_uid}/{source_id}/{shard_id} format (ADR-2)"

# Metrics
duration: 5min
completed: 2026-01-19
---

# Phase 18 Plan 01: MetricsWal Struct Summary

**MetricsWal wrapping MultiRecordLogAsync with Arc<RwLock<>> for metrics durability, plus 9 unit tests validating all CRUD and append/truncate operations**

## Performance

- **Duration:** 5 min
- **Started:** 2026-01-19T07:03:46Z
- **Completed:** 2026-01-19T07:08:33Z
- **Tasks:** 2
- **Files modified:** 4

## Accomplishments

- Created MetricsWal struct with Arc<RwLock<MultiRecordLogAsync>> for thread-safe concurrent access
- Implemented MetricsWalConfig with wal_dir_path, disk_capacity, memory_capacity (ADR-1 compliance)
- Implemented all queue CRUD operations: create_queue, delete_queue, queue_exists, list_queues
- Implemented all MRecord operations: append_records, truncate, range, last_position, resource_usage
- Added 9 comprehensive unit tests matching logs WAL test confidence
- Converted metrics.rs to directory module, preserving prometheus metrics

## Task Commits

Each task was committed atomically:

1. **Task 1: Create metrics module with MetricsWal struct and config** - `d8a4bbe2` (feat)
2. **Task 2: Add unit tests for MetricsWal operations** - included in d8a4bbe2 (tests in same file)

## Files Created/Modified

- `quickwit/quickwit-ingest/src/metrics/mod.rs` - Module declaration, re-exports MetricsWal, MetricsWalConfig, INGEST_METRICS
- `quickwit/quickwit-ingest/src/metrics/metrics_wal.rs` - MetricsWal struct implementation + 9 unit tests
- `quickwit/quickwit-ingest/src/metrics/prometheus.rs` - Renamed from metrics.rs, preserved prometheus metrics
- `quickwit/quickwit-ingest/src/lib.rs` - Added re-export for MetricsWal, MetricsWalConfig

## Decisions Made

1. **Arc<RwLock<>> pattern** - Matched logs pipeline locking discipline for consistency
2. **last_position instead of last_record** - Changed return type to avoid lifetime issues with borrowed data from RwLock guard
3. **Directory module conversion** - Preserved existing prometheus metrics by converting metrics.rs to metrics/prometheus.rs

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed impl Trait in bounds compilation error**
- **Found during:** Task 1 (MetricsWal implementation)
- **Issue:** `T: Iterator<Item = impl Buf>` not allowed in trait bounds
- **Fix:** Changed to `T: Iterator<Item = B> where B: Buf`
- **Files modified:** metrics_wal.rs
- **Verification:** cargo check succeeds
- **Committed in:** d8a4bbe2

**2. [Rule 1 - Bug] Fixed lifetime issue in last_record return**
- **Found during:** Task 1 (MetricsWal implementation)
- **Issue:** Cannot return borrowed Record<'_> from async method holding RwLock guard
- **Fix:** Changed to last_position() returning Option<u64> instead
- **Files modified:** metrics_wal.rs
- **Verification:** cargo check succeeds
- **Committed in:** d8a4bbe2

---

**Total deviations:** 2 auto-fixed (2 bugs - Rust type system requirements)
**Impact on plan:** Both auto-fixes were necessary for correct compilation. API slightly modified (last_position vs last_record) but functionality preserved.

## Issues Encountered

None - plan executed successfully with minor API adjustments for Rust type system requirements.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- MetricsWal foundation complete for Phase 19 (Checkpointing)
- ADR-1 compliance verified (separate wal_dir_path config)
- ADR-2 queue ID namespace documented (metrics/ prefix)
- ADR-3 MRecord format reuse confirmed (tests use MRecord::Doc and MRecord::Commit)
- Ready for Phase 21 (Retry & Recovery) and Phase 22 (Rate Limiting)

---
*Phase: 18-metrics-wal-integration*
*Completed: 2026-01-19*
