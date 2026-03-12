---
phase: 31-metadata-foundation
plan: 04
subsystem: metrics-engine
tags: [chrono, proptest, time-window, rem_euclid, compaction]

# Dependency graph
requires:
  - phase: none
    provides: n/a (standalone module)
provides:
  - "window_start() canonical time-window function with rem_euclid for negative timestamps"
  - "validate_window_duration() enforcing 3600-divisibility invariant"
  - "sort_schema module in quickwit-parquet-engine"
affects: [32-ingestion-pipeline, 33-merge-policy, 35-compaction-planning]

# Tech tracking
tech-stack:
  added: []
  patterns: ["rem_euclid for epoch-aligned integer division", "proptest property verification for arithmetic invariants"]

key-files:
  created:
    - "quickwit/quickwit-parquet-engine/src/sort_schema/mod.rs"
    - "quickwit/quickwit-parquet-engine/src/sort_schema/window.rs"
  modified:
    - "quickwit/quickwit-parquet-engine/src/lib.rs"

key-decisions:
  - "validate_window_duration accepts all positive divisors of 3600 (45 total), not just those >= 60"
  - "SortSchemaError defined in sort_schema/mod.rs as thiserror enum, matching crate-wide error patterns"

patterns-established:
  - "sort_schema module structure: mod.rs for error types + re-exports, submodules for functionality"
  - "proptest for arithmetic invariants: alignment, containment, determinism, no-overlap"

requirements-completed: [PIPE-10]

# Metrics
duration: 7min
completed: 2026-02-23
---

# Phase 31 Plan 04: Window Start Summary

**Canonical window_start() with rem_euclid for negative timestamps and proptest-verified containment invariant, plus validate_window_duration enforcing 3600-divisibility**

## Performance

- **Duration:** 7 min
- **Started:** 2026-02-23T20:30:34Z
- **Completed:** 2026-02-23T20:38:14Z
- **Tasks:** 1
- **Files modified:** 3

## Accomplishments
- Implemented `window_start(timestamp_secs, duration_secs) -> DateTime<Utc>` using `rem_euclid` for correct negative timestamp handling
- Implemented `validate_window_duration(duration_secs)` enforcing ADR-003 TW-2 invariant (must divide 3600)
- 3 proptest properties verify alignment, determinism, and no-overlap across 256+ random cases each
- 8 unit tests cover negative crossing, zero, boundary, large negative, and 60s window edge cases
- 4 validation tests cover all 12 common durations, 22 small divisors, 11 non-divisors, and error messages

## Task Commits

Each task was committed atomically:

1. **Task 1: Implement window_start and validate_window_duration with proptest** - `b3e4493d0` (feat)

**Plan metadata:** [see final commit below] (docs: complete plan)

## Files Created/Modified
- `quickwit/quickwit-parquet-engine/src/sort_schema/mod.rs` - Sort schema module root with SortSchemaError enum and re-exports
- `quickwit/quickwit-parquet-engine/src/sort_schema/window.rs` - window_start() and validate_window_duration() with full test suite
- `quickwit/quickwit-parquet-engine/src/lib.rs` - Added `pub mod sort_schema` to crate root

## Decisions Made
- **validate_window_duration accepts all 45 positive divisors of 3600**, not just the 12 >= 60. The function's contract is "must divide 3600" per the locked decision. Restricting to >= 60 would be an arbitrary additional constraint not specified in ADR-003.
- **SortSchemaError uses thiserror with static reason strings** (`&'static str`), matching the pattern used by other error types in the crate (IngestError, ParquetWriteError, etc.).

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed incorrect test data in invalid window durations list**
- **Found during:** Task 1 (test execution)
- **Issue:** Plan listed 45, 50, 100, 150 as invalid durations, but they all evenly divide 3600 (3600/45=80, 3600/50=72, 3600/100=36, 3600/150=24). Tests correctly failed.
- **Fix:** Replaced those values with truly invalid non-divisors (11, 13, 17). Added separate test `test_small_valid_divisors_also_accepted` covering all 22 small divisors (1-50 range).
- **Files modified:** `quickwit/quickwit-parquet-engine/src/sort_schema/window.rs`
- **Verification:** All 14 tests pass including corrected validation tests
- **Committed in:** b3e4493d0 (Task 1 commit)

---

**Total deviations:** 1 auto-fixed (1 bug in plan test data)
**Impact on plan:** Essential correctness fix. The plan's test data was mathematically incorrect. No scope creep.

## Issues Encountered
None beyond the test data correction documented above.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- `window_start()` and `validate_window_duration()` are ready for use by Phase 32 (ingestion pipeline), Phase 33 (merge policy), and Phase 35 (compaction planning)
- The `sort_schema` module provides the foundation for Plan 01 (sort schema parsing) to add its types alongside the window functions
- Pre-existing clippy warnings in `leaf_service.rs` and `accumulator.rs` are out of scope (not introduced by this plan)

## Self-Check: PASSED

All files found, all commits verified.

---
*Phase: 31-metadata-foundation*
*Completed: 2026-02-23*
