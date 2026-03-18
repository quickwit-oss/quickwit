---
phase: 28-rest-grpc-endpoints
plan: 01
subsystem: api
tags: [protobuf, grpc, search, distributed-query]

# Dependency graph
requires:
  - phase: 27-metrics-sql-leaf
    provides: MetricsSqlLeafService for executing SQL on splits
provides:
  - PlanMetricsSplits RPC that returns split assignments with node IDs
  - Split planning endpoint for external orchestration
  - Reuse of existing pruning logic (time range, index patterns)
affects: [28-02-root-service, 28-03-rest-endpoint]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - Split planning returns node assignments for external orchestration
    - Reuse SearchRequest for pruning (no SQL parsing at root)

key-files:
  created:
    - quickwit-proto/protos/quickwit/search.proto (PlanMetricsSplits messages)
  modified:
    - quickwit-proto/protos/quickwit/search.proto
    - quickwit-proto/src/codegen/quickwit/quickwit.search.rs
    - quickwit-proto/src/codegen/cloudprem/quickwit.search.rs
    - quickwit-search/src/service.rs
    - quickwit-proto/build.rs

key-decisions:
  - "Reuse SearchRequest for split planning (time range, index patterns) instead of parsing SQL"
  - "Return node assignments with each split for external orchestrator to choose"
  - "Use existing plan_splits_for_root_search for pruning logic"

patterns-established:
  - "Split planning returns SplitAssignment with split metadata + node IDs"
  - "External orchestrator picks nodes (not Quickwit)"

# Metrics
duration: 15min
completed: 2026-01-23
---

# Phase 28 Plan 01: Split Planning Protobuf & Service Method Summary

**PlanMetricsSplits RPC with split assignments and node IDs for external orchestration, reusing existing pruning logic**

## Performance

- **Duration:** 15 min
- **Started:** 2026-01-23T15:52:10Z
- **Completed:** 2026-01-23T16:07:31Z
- **Tasks:** 7 (6 previously completed by user, 1 completed now)
- **Files modified:** 6

## Accomplishments
- Protobuf messages for PlanMetricsSplits (PlanMetricsSplitsRequest, SplitAssignment, PlanMetricsSplitsResponse)
- SearchService RPC definition for PlanMetricsSplits
- SearchServiceImpl implementation using existing plan_splits_for_root_search
- Comprehensive unit tests covering all scenarios (6 tests, all passing)

## Task Commits

Tasks 1-6 were completed in previous commits:

1. **Task 1: Add protobuf messages** - `f08d1245` (feat)
2. **Task 2: Add RPC to SearchService** - `f08d1245` (feat)
3. **Task 3: Regenerate proto code** - `93cad455`, `6567855c` (chore, fix)
4. **Task 4: Add SearchService trait method** - `90f80bed` (feat)
5. **Task 5: Implement in SearchServiceImpl** - `99427b31` (feat)
6. **Task 6: Implement node assignment helper** - `99427b31` (feat)
7. **Task 7: Add unit tests** - `554365ad` (test)

**Plan metadata:** To be committed

## Files Created/Modified
- `quickwit-proto/protos/quickwit/search.proto` - Added PlanMetricsSplitsRequest, SplitAssignment, PlanMetricsSplitsResponse messages and PlanMetricsSplits RPC
- `quickwit-proto/src/codegen/quickwit/quickwit.search.rs` - Regenerated proto code
- `quickwit-proto/src/codegen/cloudprem/quickwit.search.rs` - Regenerated proto code
- `quickwit-search/src/service.rs` - Added plan_metrics_splits trait method and implementation with unit tests
- `quickwit-proto/build.rs` - Fixed proto generation issue

## Decisions Made
- **Reuse SearchRequest:** Don't parse SQL at root level - use existing SearchRequest with time range, index patterns, and filters for split pruning
- **Return all available nodes:** Each SplitAssignment includes all node IDs that can serve the split; external orchestrator picks one
- **Leverage existing logic:** Use plan_splits_for_root_search to avoid duplicating pruning logic

## Deviations from Plan

None - plan executed exactly as written. Tasks 1-6 were completed by user in prior work, Task 7 (unit tests) completed now.

## Issues Encountered

None - implementation was straightforward following existing patterns in the codebase.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

Ready for next plan (28-02):
- PlanMetricsSplits RPC fully implemented and tested
- Split pruning logic works (time range, index patterns)
- Node assignments populated from cluster
- Next: MetricsSqlRootService to receive SQL and call PlanMetricsSplits

No blockers or concerns.

---
*Phase: 28-rest-grpc-endpoints*
*Completed: 2026-01-23*
