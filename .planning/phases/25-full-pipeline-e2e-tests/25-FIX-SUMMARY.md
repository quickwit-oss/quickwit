---
phase: 25-full-pipeline-e2e-tests
plan: FIX
subsystem: testing
tags: [e2e, pipeline, postgres, minio, actors]

# Dependency graph
requires:
  - phase: 25-01
    provides: E2E test infrastructure and full pipeline tests
provides:
  - Idempotent test execution (can run tests multiple times)
  - Working split publication through full pipeline
affects: []

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Test idempotency via cleanup-before-setup pattern"
    - "Actor pipeline explicit processing with accelerated time universe"

key-files:
  created: []
  modified:
    - quickwit/quickwit-indexing/tests/metrics_infra_e2e_test.rs

key-decisions:
  - "Cleanup index before setup rather than handling 'already exists' errors"
  - "Explicitly process uploader handle before waiting for publisher in tests"

patterns-established:
  - "Test idempotency: call cleanup_test_index before setup_test_index"
  - "Actor testing: process all intermediate stages when using accelerated time"

# Metrics
duration: 5min
completed: 2026-01-21
---

# Phase 25 FIX: UAT Issue Fixes Summary

**Fixed 4 UAT issues: 3 blockers and 1 major issue preventing E2E tests from passing consistently**

## Performance

- **Duration:** 5 min
- **Started:** 2026-01-21T11:00:00Z
- **Completed:** 2026-01-21T11:05:00Z
- **Tasks:** 2
- **Files modified:** 1

## Accomplishments

- Fixed UAT-003 (major): Tests now idempotent - cleanup happens before setup
- Fixed UAT-001/002/004 (blockers): Splits now flow through to publisher correctly
- Root cause identified: uploader handle was being ignored, not processed

## Task Commits

Each task was committed atomically:

1. **Task 1: Fix UAT-003 - Add test cleanup before setup** - `086657e6` (fix)
2. **Task 2: Fix UAT-001/002/004 - Process uploader before publisher** - `28e9288b` (fix)

## Files Created/Modified

- `quickwit/quickwit-indexing/tests/metrics_infra_e2e_test.rs` - Added cleanup before setup, captured uploader handle, added explicit uploader processing

## Decisions Made

1. **Cleanup-before-setup pattern** - Rather than handling "already exists" errors after the fact, proactively delete any existing index before creating a new one. This makes tests truly idempotent.

2. **Explicit actor processing** - With `Universe::with_accelerated_time()`, messages don't flow automatically between actors. Each stage must be explicitly processed with `process_pending_and_observe()`.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None - the fixes were straightforward once the root causes were identified in the plan.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- UAT issues resolved, ready for re-verification with `/gsd:verify-work 25`
- All E2E tests should now pass consistently
- Tests are idempotent and can be run multiple times

---
*Phase: 25-full-pipeline-e2e-tests*
*FIX Plan Completed: 2026-01-21*
