---
phase: 21-retry-error-handling
plan: 01
subsystem: error-handling
tags: [retry, error-classification, transient, permanent, retryable-trait]

# Dependency graph
requires:
  - phase: 20-cluster-gossip
    provides: MetricsShardPositionsService for cluster-wide position updates
provides:
  - MetricsIngestError enum with transient vs permanent classification
  - Retryable trait implementation for error handling
  - is_transient() and into_retry() helper methods
affects: [21-02, retry-logic, ingest-pipeline]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Error classification pattern from SubworkbenchFailure (workbench.rs)"
    - "Retryable trait implementation for retry decision"

key-files:
  created:
    - quickwit/quickwit-ingest/src/metrics/error.rs
  modified:
    - quickwit/quickwit-ingest/src/metrics/mod.rs

key-decisions:
  - "7 error variants: 3 permanent (IndexNotFound, SourceNotFound, InvalidMetricsData), 4 transient (WalFull, IoError, MetastoreUnavailable, InternalError, Timeout)"
  - "Follow SubworkbenchFailure pattern from logs pipeline for consistency"
  - "Implement both is_transient() for simple checks and into_retry() for Retry<E> conversion"

patterns-established:
  - "Error classification: permanent errors fail immediately, transient errors worth retrying"

# Metrics
duration: 2min
completed: 2026-01-19
---

# Phase 21 Plan 01: MetricsIngestError Enum Summary

**MetricsIngestError enum with 7 variants implementing Retryable trait for transient vs permanent error classification, following logs pipeline patterns**

## Performance

- **Duration:** 2 min
- **Started:** 2026-01-19T16:26:41Z
- **Completed:** 2026-01-19T16:28:43Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments

- Created MetricsIngestError enum with error classification (3 permanent, 4 transient variants)
- Implemented Retryable trait from quickwit-common for integration with retry infrastructure
- Added is_transient() and into_retry() helper methods for flexible error handling
- Created 3 unit tests verifying error classification works correctly

## Task Commits

Each task was committed atomically:

1. **Task 1: Create MetricsIngestError enum with error classification** - `1d6d48fa` (feat)
2. **Task 2: Add error module to metrics exports** - `a8c4dfa6` (feat)

## Files Created/Modified

- `quickwit/quickwit-ingest/src/metrics/error.rs` - New error module with MetricsIngestError enum, Retryable impl, and unit tests
- `quickwit/quickwit-ingest/src/metrics/mod.rs` - Added error module declaration and re-export

## Decisions Made

1. **7 error variants split into permanent and transient:**
   - Permanent (not retryable): IndexNotFound, SourceNotFound, InvalidMetricsData
   - Transient (retryable): WalFull, IoError, MetastoreUnavailable, InternalError, Timeout
   - Rationale: Follows the established SubworkbenchFailure pattern from workbench.rs for consistency

2. **Implemented Retryable trait from quickwit-common:**
   - Enables integration with existing retry infrastructure
   - Provides is_retryable() method for uniform error handling

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- MetricsIngestError ready for use in retry policy implementation
- Plan 21-02 can build MetricsRetryPolicy using this error classification
- All tests passing, module properly exported

---
*Phase: 21-retry-error-handling*
*Completed: 2026-01-19*
