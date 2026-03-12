---
phase: 29-arrow-ipc-extension
plan: 01
subsystem: sql-engine
tags: [arrow-ipc, protobuf, datafusion, sql, metrics-query]

# Dependency graph
requires:
  - phase: 27-sql-query-service
    provides: MetricsSqlLeafService with execute_on_splits function
  - phase: 28-metrics-sql-root-service
    provides: Proto definitions for MetricsSqlLeafRequest/Response
provides:
  - Optional Arrow IPC toggle in execute_on_splits via return_arrow parameter
  - Proto field return_arrow in MetricsSqlLeafRequest for future gRPC integration
  - Backward compatible function signature (callers can skip IPC serialization)
affects:
  - 29-02 (Arrow Flight service integration will use return_arrow proto field)
  - future-grpc-integration (service adapter will wire request.return_arrow to function parameter)

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Conditional serialization pattern: skip expensive Arrow IPC when not needed"
    - "Proto-first design: add proto field before gRPC wiring for future integration"
    - "Backward compatibility: new bool parameter defaults to false in proto constructor"

key-files:
  created: []
  modified:
    - quickwit/quickwit-proto/protos/quickwit/metrics_sql.proto
    - quickwit/quickwit-proto/src/codegen/quickwit/quickwit.metrics_sql.rs
    - quickwit/quickwit-proto/src/quickwit/metrics_sql/mod.rs
    - quickwit/quickwit-metrics-engine/src/sql/leaf_service.rs
    - quickwit/quickwit-remote-api/src/grpc_client.rs

key-decisions:
  - "return_arrow defaults to false for backward compatibility (skip serialization unless requested)"
  - "Proto field added now (field 7) even though gRPC wiring is a future phase"
  - "Conditional IPC serialization via if/else instead of Option type for clearer control flow"

patterns-established:
  - "Proto evolution pattern: add request field before service integration"
  - "Audit-first signature changes: verify all callers before changing function signature"
  - "Test coverage for both behaviors: explicit tests for true and false cases"

# Metrics
duration: 8min
completed: 2026-01-26
---

# Phase 29 Plan 01: Arrow IPC Extension Summary

**Optional Arrow IPC serialization toggle added to execute_on_splits with proto field return_arrow for future gRPC integration**

## Performance

- **Duration:** 8 min 11 sec
- **Started:** 2026-01-26T15:50:31Z
- **Completed:** 2026-01-26T15:58:44Z
- **Tasks:** 3
- **Files modified:** 5

## Accomplishments

- Proto field `return_arrow` (field 7) added to MetricsSqlLeafRequest with backward compatible default
- execute_on_splits conditionally serializes Arrow IPC only when return_arrow=true
- Test coverage for both return_arrow=true (IPC populated) and return_arrow=false (empty bytes)
- Audit confirmed execute_on_splits is internal-only (no production callers to migrate)

## Task Commits

Each task was committed atomically:

1. **Task 1: Add return_arrow field to protobuf request and regenerate** - `6b7be22f` (feat)
2. **Task 2: Make Arrow IPC serialization conditional in execute_on_splits** - `73eaeaa9` (feat)
3. **Task 3: Add tests for both return_arrow behaviors and verify backward compatibility** - `3dae099f` (test)

**Deviation fix:** `15bc34c7` (fix: add plan_metrics_splits stub to CloudPremRootSearchService)

## Files Created/Modified

- `quickwit/quickwit-proto/protos/quickwit/metrics_sql.proto` - Added bool return_arrow = 7 to MetricsSqlLeafRequest
- `quickwit/quickwit-proto/src/codegen/quickwit/quickwit.metrics_sql.rs` - Regenerated with prost tag 7 for return_arrow
- `quickwit/quickwit-proto/src/quickwit/metrics_sql/mod.rs` - Added return_arrow: false default and with_return_arrow() builder
- `quickwit/quickwit-metrics-engine/src/sql/leaf_service.rs` - Added return_arrow parameter, conditional IPC serialization, test coverage
- `quickwit/quickwit-remote-api/src/grpc_client.rs` - Added plan_metrics_splits stub (deviation fix)

## Decisions Made

**Proto field placement:** Added return_arrow as field 7 in MetricsSqlLeafRequest now, before gRPC service integration. This proto-first approach means the proto schema is ready when the gRPC adapter is built in a future phase.

**Default value:** return_arrow defaults to false in the proto constructor for backward compatibility. Callers who don't need Arrow IPC can pass false to skip expensive serialization.

**Conditional pattern:** Used if/else instead of Option<Vec<u8>> for clearer control flow. Empty Vec<u8> when return_arrow=false makes the "skip serialization" case explicit.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Fixed MetricsSqlLeafRequest constructor**
- **Found during:** Task 1 (cargo build after proto regeneration)
- **Issue:** Regenerated proto added return_arrow field, but MetricsSqlLeafRequest::new() constructor was missing it, causing compilation error
- **Fix:** Added `return_arrow: false` to constructor with backward compatible default, added with_return_arrow() builder method
- **Files modified:** quickwit/quickwit-proto/src/quickwit/metrics_sql/mod.rs
- **Verification:** cargo build -p quickwit-proto succeeds
- **Committed in:** 6b7be22f (Task 1 commit)

**2. [Rule 3 - Blocking] Added plan_metrics_splits to CloudPremRootSearchService**
- **Found during:** Overall verification (cargo check --workspace)
- **Issue:** SearchService trait gained plan_metrics_splits method in Phase 28, but CloudPremRootSearchService stub implementation was not updated, blocking workspace compilation
- **Fix:** Added plan_metrics_splits method using unimplemented() pattern (matching other leaf methods in this stub service), added PlanMetricsSplitsRequest/Response imports
- **Files modified:** quickwit/quickwit-remote-api/src/grpc_client.rs
- **Verification:** cargo check --workspace succeeds
- **Committed in:** 15bc34c7 (separate deviation commit)

**3. [Rule 1 - Bug] Removed flaky elapsed_micros assertion**
- **Found during:** Task 3 (cargo test)
- **Issue:** test_execute_with_return_arrow_false asserted elapsed_micros > 0, but execution can be so fast that elapsed_micros = 0, causing flaky test failure
- **Fix:** Replaced assertion with comment that elapsed_micros is populated (may be 0 on very fast execution)
- **Files modified:** quickwit/quickwit-metrics-engine/src/sql/leaf_service.rs
- **Verification:** cargo test -p quickwit-metrics-engine passes consistently
- **Committed in:** 3dae099f (Task 3 commit)

---

**Total deviations:** 3 auto-fixed (2 blocking, 1 bug)
**Impact on plan:** All auto-fixes were necessary to unblock compilation and prevent flaky tests. No scope creep - fixes enabled plan completion.

## Issues Encountered

None - all issues were auto-fixed via deviation rules.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

**Ready for Phase 29 Plan 02 (Arrow Flight service integration):**
- Proto field return_arrow available for gRPC request mapping
- execute_on_splits accepts return_arrow parameter
- Test coverage demonstrates both behaviors work correctly
- Audit confirmed function is internal-only (safe to wire to gRPC in next phase)

**No blockers.** The gRPC service adapter can now wire MetricsSqlLeafRequest.return_arrow to execute_on_splits(return_arrow) parameter in the next plan.

---
*Phase: 29-arrow-ipc-extension*
*Completed: 2026-01-26*
