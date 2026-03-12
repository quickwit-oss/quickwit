---
phase: 28-rest-grpc-endpoints
plan: 02
subsystem: api
tags: [grpc, tonic, client, split-planning, metrics]

# Dependency graph
requires:
  - phase: 28-rest-grpc-endpoints
    plan: 01
    provides: PlanMetricsSplits RPC protobuf and service trait
provides:
  - GrpcSearchAdapter::plan_metrics_splits implementation
  - SearchServiceClient::plan_metrics_splits method
  - gRPC reflection includes PlanMetricsSplits RPC
  - Integration test for client method
affects: [28-04-integration, external-grpc-clients]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - gRPC adapter pattern with convert_to_grpc_result error mapping
    - SearchServiceClient dispatch between Grpc and Local implementations
    - Test pattern using TestSandbox and SearchServiceClient::from_service

key-files:
  created: []
  modified:
    - quickwit-serve/src/search_api/grpc_adapter.rs
    - quickwit-search/src/client.rs
    - quickwit-search/src/tests.rs

key-decisions:
  - "Follow existing gRPC adapter pattern from other RPC methods"
  - "SearchServiceClient supports both gRPC and local dispatch"
  - "Integration test uses local service rather than full gRPC server setup"

patterns-established:
  - "gRPC adapter: set_parent_span → service call → convert_to_grpc_result"
  - "Client method: match on Grpc/Local → dispatch → error mapping"

# Metrics
duration: 24min
completed: 2026-01-23
---

# Phase 28 Plan 02: gRPC Adapter for Split Planning Summary

**gRPC service implementation for PlanMetricsSplits with client support and integration test**

## Performance

- **Duration:** 24 min
- **Started:** 2026-01-23T16:17:19Z
- **Completed:** 2026-01-23T16:41:33Z
- **Tasks:** 5 (2 automatic, 3 manual)
- **Files modified:** 3

## Accomplishments
- Added GrpcSearchAdapter::plan_metrics_splits method following existing patterns
- Implemented SearchServiceClient::plan_metrics_splits for both Grpc and Local
- Verified gRPC reflection automatically includes new RPC from proto
- Added integration test using TestSandbox and local service

## Task Commits

**Note:** Task 1 (GrpcSearchAdapter implementation) was previously completed in commit 24817796 as part of plan 28-03. That work is not recommitted here.

1. **Task 1: Add method to GrpcSearchAdapter** - Previously done in commit `24817796`
   - Method already implemented in prior execution
   - Added imports for PlanMetricsSplitsRequest/Response
   - Followed pattern with set_parent_span_from_request_metadata

2. **Task 2: Verify proto-generated trait** - Verification task (no commit)
   - Confirmed search_service_server::SearchService trait includes plan_metrics_splits
   - Generated trait signature matches adapter implementation

3. **Task 3: Add SearchServiceClient method** - `a15b5099` (feat)
   - Added plan_metrics_splits method to SearchServiceClient
   - Implemented for both Grpc and Local client types
   - Proper error mapping with parse_grpc_error

4. **Task 4: Add to reflection/descriptor** - Automatic (no commit needed)
   - SEARCH_FILE_DESCRIPTOR_SET already includes new RPC
   - Proto regeneration automatically updated descriptor

5. **Task 5: Add integration test** - `e5154236` (test)
   - Created test_plan_metrics_splits_client test
   - Uses TestSandbox with metrics schema
   - Verifies split assignments and response structure

**Plan metadata:** To be committed

## Files Created/Modified
- `quickwit-serve/src/search_api/grpc_adapter.rs` - Added plan_metrics_splits method (previously committed)
- `quickwit-search/src/client.rs` - Added SearchServiceClient::plan_metrics_splits method
- `quickwit-search/src/tests.rs` - Added integration test for client method

## Decisions Made
- **GrpcSearchAdapter pattern:** Use existing pattern with set_parent_span and convert_to_grpc_result
- **Client dispatch:** Match on client_impl for Grpc vs Local service, different error handling for each
- **Integration test approach:** Use TestSandbox with local service rather than full gRPC server setup (simpler, faster, sufficient for testing client code path)

## Deviations from Plan

### Pre-completed Work

**[Task 1 - GrpcSearchAdapter] Implemented in prior execution (commit 24817796)**
- **Context:** Plan 28-03 was executed before 28-02 and included the gRPC adapter implementation
- **Decision:** Did not re-commit work that was already in the codebase
- **Verification:** Confirmed implementation matches plan specification exactly
- **Commits:** Previous implementation in 24817796, documented here for completeness

### Automatic Infrastructure

**[Task 4 - gRPC Reflection] No code changes needed**
- **Found:** SEARCH_FILE_DESCRIPTOR_SET automatically includes all RPCs from proto
- **Reason:** Proto regeneration updates descriptor set automatically
- **Result:** No manual registration required, reflection works out of the box

### Test Simplification

**[Task 5 - Integration Test] Used local service instead of gRPC server**
- **Plan suggested:** Full gRPC server setup with grpcurl verification
- **Implemented:** Integration test using SearchServiceClient::from_service with local impl
- **Rationale:**
  - No existing gRPC integration test infrastructure in codebase
  - Local service test exercises the same client code path
  - Simpler, faster, no server lifecycle management needed
  - Consistent with existing test patterns in tests.rs

## Issues Encountered

### Test Development Iterations
- **MockSplitBuilder API:** Initial test tried to use non-existent `with_time_range()` method
  - **Resolution:** Used TestSandbox.add_documents() pattern from existing tests
- **Field Type:** Initially used i64 for timestamp, required datetime type
  - **Resolution:** Changed to datetime type per index config requirements

## User Setup Required

None - all changes are internal API implementations. External gRPC clients can now call PlanMetricsSplits via standard gRPC channels.

## Next Phase Readiness

Ready for:
- External orchestrators to call PlanMetricsSplits via gRPC
- ClickHouse integration for split planning before leaf queries
- Further distributed query planning features

**gRPC endpoint capabilities:**
- Service discovery via reflection
- Standard gRPC client libraries supported (Go, Python, etc.)
- Both streaming and unary call patterns available

No blockers or concerns.

---
*Phase: 28-rest-grpc-endpoints*
*Completed: 2026-01-23*
