---
phase: 28-rest-grpc-endpoints
plan: 03
subsystem: api
tags: [rest, http, json, warp, split-planning, metrics]

# Dependency graph
requires:
  - phase: 28-rest-grpc-endpoints
    plan: 01
    provides: PlanMetricsSplits RPC and service implementation
provides:
  - POST /api/v1/metrics/plan-splits REST endpoint
  - JSON API for split planning with node assignments
  - OpenAPI documentation for split planning
  - HTTP alternative to gRPC for external orchestration
affects: [28-04-integration, external-orchestrators]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - REST endpoint pattern using warp filters and into_rest_api_response
    - OpenAPI documentation with utoipa macros
    - Request/response type conversion between REST and proto

key-files:
  created:
    - quickwit-serve/src/metrics_sql_api/mod.rs
    - quickwit-serve/src/metrics_sql_api/rest_handler.rs
  modified:
    - quickwit-serve/src/lib.rs
    - quickwit-serve/src/rest.rs
    - quickwit-serve/src/openapi.rs
    - quickwit-serve/src/search_api/grpc_adapter.rs

key-decisions:
  - "Use warp filter pattern consistent with other API endpoints"
  - "Convert REST request to proto SearchRequest for service integration"
  - "Return JSON with split assignments including node IDs and footer offsets"

patterns-established:
  - "REST handler pattern: warp filter → async handler → into_rest_api_response"
  - "OpenAPI documentation using #[utoipa::path] macros"

# Metrics
duration: 7min
completed: 2026-01-23
---

# Phase 28 Plan 03: REST Endpoint for Split Planning Summary

**POST /api/v1/metrics/plan-splits REST endpoint with JSON request/response and OpenAPI documentation**

## Performance

- **Duration:** 7 min
- **Started:** 2026-01-23T16:17:22Z
- **Completed:** 2026-01-23T16:24:07Z
- **Tasks:** 7 (all completed in single commit)
- **Files modified:** 6

## Accomplishments
- Created metrics_sql_api module with REST handlers
- Implemented POST /api/v1/metrics/plan-splits endpoint
- Added OpenAPI documentation with request/response schemas
- Integrated with existing SearchService.plan_metrics_splits method
- Added gRPC adapter method for plan_metrics_splits

## Task Commits

All tasks completed in single commit:

1. **Tasks 1-7: REST endpoint implementation** - `24817796` (feat)
   - Task 1: Create metrics_sql_api module structure
   - Task 2: Define REST request/response types
   - Task 3: Implement POST handler with service integration
   - Task 4: Register route in REST server
   - Task 5: Add OpenAPI documentation
   - Task 6: Export module types
   - Task 7: Add gRPC adapter method

**Plan metadata:** To be committed

## Files Created/Modified
- `quickwit-serve/src/metrics_sql_api/mod.rs` - Module exports
- `quickwit-serve/src/metrics_sql_api/rest_handler.rs` - REST handlers, request/response types, OpenAPI docs
- `quickwit-serve/src/lib.rs` - Added metrics_sql_api module
- `quickwit-serve/src/rest.rs` - Registered plan_splits_handler route
- `quickwit-serve/src/openapi.rs` - Added MetricsSqlApi to OpenAPI docs
- `quickwit-serve/src/search_api/grpc_adapter.rs` - Added gRPC adapter for plan_metrics_splits

## Decisions Made
- **Warp filter pattern:** Used consistent pattern with existing API endpoints (.then() → into_rest_api_response)
- **Type conversion:** Convert REST request to proto SearchRequest, then to PlanMetricsSplitsRequest
- **Error handling:** Use into_rest_api_response for consistent error formatting
- **OpenAPI documentation:** Use utoipa macros for automatic schema generation

## Deviations from Plan

None - plan executed exactly as written.

Plan specified all types and implementation details. Linter automatically reformatted code to match project style.

## Issues Encountered

None - implementation was straightforward following existing warp filter patterns in the codebase.

## User Setup Required

None - no external service configuration required. Endpoint uses existing SearchService.

## Next Phase Readiness

Ready for integration testing and further endpoint development:
- REST endpoint fully functional
- OpenAPI documentation complete
- Integrated with plan_metrics_splits service method
- Returns JSON with split assignments and node IDs
- Next: Integration tests, MetricsSqlRootService implementation

No blockers or concerns.

---
*Phase: 28-rest-grpc-endpoints*
*Completed: 2026-01-23*
