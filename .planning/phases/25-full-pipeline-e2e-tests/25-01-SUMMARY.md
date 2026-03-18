---
phase: 25-full-pipeline-e2e-tests
plan: 01
subsystem: testing
tags: [e2e-tests, minio, postgres, datafusion, parquet, integration-tests]

# Dependency graph
requires:
  - phase: 24-e2e-test-infrastructure
    provides: Test infrastructure module with storage and metastore helpers
  - phase: 23-docker-compose-setup
    provides: Docker Compose infrastructure (Minio, Postgres)
provides:
  - Full pipeline E2E test (test_full_metrics_pipeline_e2e)
  - Query accuracy E2E test (test_query_accuracy_e2e)
  - Updated Makefile target (test-metrics-e2e)
affects: [phase-26-if-any, production-readiness]

# Tech tracking
tech-stack:
  added: []
  patterns: [E2E test patterns with real infrastructure, DataFusion query validation]

key-files:
  created: []
  modified:
    - quickwit/quickwit-indexing/tests/metrics_infra_e2e_test.rs
    - quickwit/Makefile

key-decisions:
  - "Use helper functions duplicated in test module since src/actors helpers are not public"
  - "Query validation uses DataFusion SessionContext with MetricsTableProvider"
  - "Tests gracefully skip when infrastructure is not available"

patterns-established:
  - "Full pipeline E2E test pattern: wire up all actors with real services"
  - "Query accuracy validation: ingest known values, verify via SQL queries"

# Metrics
duration: 8min
completed: 2026-01-21
---

# Phase 25 Plan 01: Full Pipeline E2E Tests Summary

**Full metrics pipeline E2E tests against real Minio S3 storage and Postgres metastore with DataFusion query validation**

## Performance

- **Duration:** 8 min
- **Started:** 2026-01-21T10:00:00Z
- **Completed:** 2026-01-21T10:08:00Z
- **Tasks:** 3
- **Files modified:** 2

## Accomplishments

- Created comprehensive full pipeline E2E test (test_full_metrics_pipeline_e2e) that validates data flows through all pipeline stages
- Created query accuracy E2E test (test_query_accuracy_e2e) that validates DataFusion queries return correct results
- Updated test-metrics-e2e Makefile target to run all infrastructure E2E tests

## Task Commits

Each task was committed atomically:

1. **Task 1: Add full pipeline E2E test with real infrastructure** - `7e21dcbb` (test)
2. **Task 2: Add query accuracy E2E test with real infrastructure** - `8b6c42d6` (test)
3. **Task 3: Update test-metrics-e2e Makefile target** - `35789237` (build)

## Files Created/Modified

- `quickwit/quickwit-indexing/tests/metrics_infra_e2e_test.rs` - Added:
  - `test_full_metrics_pipeline_e2e()` - Full pipeline E2E test wiring DocProcessor -> Indexer -> Uploader -> Publisher with real Minio/Postgres
  - `test_query_accuracy_e2e()` - Query validation test with known values and DataFusion queries
  - Helper functions: create_dict_array, create_nullable_dict_array, create_variant_array, create_varied_test_batch, create_raw_doc_batch, wait_for_published_splits
- `quickwit/Makefile` - Updated test-metrics-e2e target to run infrastructure E2E tests with --nocapture

## Decisions Made

1. **Helper function duplication:** Duplicated test helper functions (create_dict_array, create_varied_test_batch, etc.) from metrics_e2e_test.rs since those are in src/actors/ and not public to integration tests in tests/ directory.

2. **Query validation approach:** Use DataFusion SessionContext with MetricsTableProvider to validate query results against known input values. This provides end-to-end validation that Parquet files are correctly written and queryable.

3. **Graceful skip behavior:** Tests check infrastructure availability via check_infra_available() and gracefully skip with helpful messages when Docker Compose is not running.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None.

## User Setup Required

None - uses existing Docker Compose setup from Phase 23.

To run E2E tests:
```bash
make docker-metrics-up   # Start Minio + Postgres
make test-metrics-e2e    # Run full pipeline + query accuracy tests
make docker-metrics-down # Stop infrastructure
```

## Next Phase Readiness

- E2E test suite complete for v0.4 milestone
- Full pipeline validation provides confidence to ship
- Query accuracy tests ensure data integrity through entire pipeline
- Ready for milestone completion

---
*Phase: 25-full-pipeline-e2e-tests*
*Completed: 2026-01-21*
