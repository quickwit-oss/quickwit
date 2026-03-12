---
phase: 09-testing-validation
plan: 01
subsystem: testing
tags: [integration-tests, otlp, grpc, metrics, tonic]

# Dependency graph
requires:
  - phase: 08-search-integration
    provides: DataFusion query execution for metrics splits
provides:
  - MetricsServiceClient in ClusterSandbox for integration tests
  - OTLP metrics client infrastructure ready for e2e testing
affects: [09-testing-validation, end-to-end-tests]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "OTLP client pattern in ClusterSandbox (logs_client, trace_client, metrics_client)"

key-files:
  created: []
  modified:
    - quickwit/quickwit-integration-tests/src/test_utils/cluster_sandbox.rs

key-decisions:
  - "Follow existing logs_client/trace_client pattern for consistency"
  - "Connect MetricsServiceClient to Indexer service like other OTLP clients"

patterns-established:
  - "OTLP service clients in ClusterSandbox follow same pattern: ServiceClient::new(self.channel(QuickwitService::Indexer))"

# Metrics
duration: 2min
completed: 2026-01-15
---

# Phase 9 Plan 01: OTLP Metrics Client Infrastructure Summary

**Added MetricsServiceClient to ClusterSandbox following existing LogsServiceClient and TraceServiceClient patterns for integration testing**

## Performance

- **Duration:** 2 min
- **Started:** 2026-01-15T16:00:00Z
- **Completed:** 2026-01-15T16:02:00Z
- **Tasks:** 3
- **Files modified:** 1

## Accomplishments

- Added MetricsServiceClient import from quickwit-proto OpenTelemetry types
- Implemented metrics_client() method on ClusterSandbox
- Verified integration tests crate builds successfully with new client

## Task Commits

Each task was committed atomically:

1. **Task 1: Add OTLP metrics client import to cluster_sandbox** - `15ae3dd8` (feat)
2. **Task 2: Add metrics_client() method to ClusterSandbox** - `70b648e7` (feat)
3. **Task 3: Verify test infrastructure compiles** - No separate commit (verification only)

## Files Created/Modified

- `quickwit/quickwit-integration-tests/src/test_utils/cluster_sandbox.rs` - Added MetricsServiceClient import and metrics_client() method

## Decisions Made

- **Follow existing pattern**: Used the same pattern as logs_client() and trace_client() for consistency across OTLP clients
- **Connect to Indexer service**: MetricsServiceClient connects to the Indexer service, same as other OTLP endpoints

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- MetricsServiceClient infrastructure is ready for use in metrics integration tests
- Ready for 09-02-PLAN.md (OTLP metrics ingestion test)

---
*Phase: 09-testing-validation*
*Completed: 2026-01-15*
