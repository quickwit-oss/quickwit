---
phase: 24-e2e-test-infrastructure
plan: 01
subsystem: testing
tags: [minio, postgres, s3, e2e-tests, integration-tests]

# Dependency graph
requires:
  - phase: 23-docker-compose-setup
    provides: Docker Compose infrastructure (Minio, Postgres)
provides:
  - Test infrastructure module with storage and metastore helpers
  - Infrastructure connectivity validation (check_infra_available)
  - Index setup/cleanup utilities for E2E tests
  - Makefile targets for running infrastructure tests
affects: [24-02, 24-03, phase-25]

# Tech tracking
tech-stack:
  added: [quickwit-storage S3CompatibleObjectStorage, quickwit-metastore PostgresqlMetastore]
  patterns: [feature-gated postgres support, conditional compilation for optional dependencies]

key-files:
  created:
    - quickwit/quickwit-indexing/tests/metrics_infra_e2e_test.rs
  modified:
    - quickwit/quickwit-indexing/Cargo.toml
    - quickwit/Makefile

key-decisions:
  - "Gate postgres-dependent code behind cfg(feature = postgres) to keep testsuite feature lightweight"
  - "Use IndexMetadataResponseExt trait to deserialize metastore responses"
  - "Add separate test-metrics-infra target for smoke testing connectivity"

patterns-established:
  - "Integration tests in tests/ directory with required-features in Cargo.toml"
  - "Environment variable configuration with sensible defaults for local Docker setup"
  - "Feature gating: postgres feature enables quickwit-metastore/postgres"

# Metrics
duration: 12min
completed: 2026-01-20
---

# Phase 24 Plan 01: E2E Test Infrastructure Summary

**Metrics E2E test infrastructure module with Minio S3 storage and Postgres metastore helpers, plus Makefile targets for connectivity validation**

## Performance

- **Duration:** 12 min
- **Started:** 2026-01-20T10:00:00Z
- **Completed:** 2026-01-20T10:12:00Z
- **Tasks:** 3
- **Files modified:** 3

## Accomplishments

- Created comprehensive test infrastructure module in tests/metrics_infra_e2e_test.rs
- Added postgres feature to quickwit-indexing enabling PostgreSQL metastore in tests
- Added test-metrics-infra Makefile target for infrastructure connectivity validation
- Provided helper functions: create_test_storage(), create_test_metastore(), check_infra_available()

## Task Commits

Each task was committed atomically:

1. **Task 1: Create metrics E2E test infrastructure module** - `83f654ba` (test)
2. **Task 2: Update Cargo.toml for integration test dependencies** - `6cc848f6` (build)
3. **Task 3: Add E2E test Makefile target validation** - `b5ff5e33` (build)

## Files Created/Modified

- `quickwit/quickwit-indexing/tests/metrics_infra_e2e_test.rs` - Test infrastructure module with:
  - TestInfra struct combining storage + metastore + temp_dir
  - create_test_storage() for Minio S3 client
  - create_test_metastore() for PostgreSQL metastore
  - check_infra_available() for connectivity validation
  - setup_test_index()/cleanup_test_index() helpers
  - test_infra_connectivity() smoke test (ignored by default)
- `quickwit/quickwit-indexing/Cargo.toml` - Added postgres feature and [[test]] declaration
- `quickwit/Makefile` - Added test-metrics-infra target

## Decisions Made

1. **Feature gating for postgres:** Gate all PostgreSQL-related code behind `cfg(feature = "postgres")` to keep the testsuite feature lightweight. This allows running tests without requiring PostgreSQL when not needed.

2. **Use IndexMetadataResponseExt trait:** The IndexMetadataResponse stores metadata as serialized JSON. Used the deserialize_index_metadata() method from the extension trait rather than trying to access fields directly.

3. **Separate Makefile target:** Added test-metrics-infra as a separate target from test-metrics-e2e since the infrastructure test uses --ignored flag and requires different features (testsuite,postgres).

## Deviations from Plan

### Auto-fixed Issues

**1. [API Mismatch] IndexMetadataResponse field access**
- **Found during:** Task 2 (compilation with postgres feature)
- **Issue:** Plan assumed IndexMetadataResponse.index_uid field exists directly, but it's stored as serialized JSON
- **Fix:** Used IndexMetadataResponseExt::deserialize_index_metadata() to properly access index metadata
- **Files modified:** quickwit/quickwit-indexing/tests/metrics_infra_e2e_test.rs
- **Verification:** Compilation succeeds with testsuite,postgres features
- **Committed in:** 6cc848f6 (Task 2 commit)

---

**Total deviations:** 1 auto-fixed (API mismatch)
**Impact on plan:** Minor fix to use correct API. No scope creep.

## Issues Encountered

None beyond the auto-fixed deviation above.

## User Setup Required

None - infrastructure uses existing Docker Compose setup from Phase 23.

To use:
```bash
make docker-metrics-up   # Start Minio + Postgres
make test-metrics-infra  # Run connectivity test
make docker-metrics-down # Stop infrastructure
```

## Next Phase Readiness

- Test infrastructure foundation complete
- Ready for Plan 02: Metrics pipeline test helpers
- Plan 03 will add full pipeline integration tests

---
*Phase: 24-e2e-test-infrastructure*
*Completed: 2026-01-20*
