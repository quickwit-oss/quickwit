---
phase: 13-metadata-schema-design
plan: 02
subsystem: database
tags: [rust, postgresql, migration, sea-query, metrics, pruning]

# Dependency graph
requires:
  - phase: 13-01
    provides: MetricsSplitMetadata struct with two-tier tag storage
provides:
  - PostgreSQL metrics_splits table with GIN indexes for Tier 1 pruning
  - Rust PgMetricsSplit model for reading database rows
  - Rust InsertableMetricsSplit for writing metadata to Postgres
  - MetricsSplits sea-query Iden for query building
affects: [14-metastore-extension, 15-stage-splits, 16-publish-splits]

# Tech tracking
tech-stack:
  added:
    - sea-query (optional, behind postgres feature flag)
  patterns:
    - "PgMetricsSplit -> MetricsSplitMetadata via JSON deserialization"
    - "InsertableMetricsSplit::from_metadata() for Rust-to-DB conversion"
    - "MetricsSplits Iden enum for sea-query SQL building"

key-files:
  created:
    - quickwit/quickwit-metastore/migrations/postgresql/25_create-metrics-splits.up.sql
    - quickwit/quickwit-metastore/migrations/postgresql/25_create-metrics-splits.down.sql
    - quickwit/quickwit-metrics-engine/src/split/postgres.rs
  modified:
    - quickwit/quickwit-metrics-engine/Cargo.toml
    - quickwit/quickwit-metrics-engine/src/split/mod.rs

key-decisions:
  - "TEXT[] for tag columns with GIN indexes for array containment queries"
  - "split_metadata_json as authoritative source, columns for indexing"
  - "Optional postgres feature to avoid mandatory sea-query dependency"

patterns-established:
  - "Dual storage: column values for indexing, JSON for full metadata"
  - "Conditional Iden derive with cfg_attr for optional features"

# Metrics
duration: 5min
completed: 2026-01-18
---

# Phase 13 Plan 02: PostgreSQL Model Summary

**PostgreSQL metrics_splits table with GIN indexes for Tier 1 pruning and Rust model for conversion between MetricsSplitMetadata and database rows.**

## Performance

- **Duration:** 5 min
- **Started:** 2026-01-18T07:05:00Z
- **Completed:** 2026-01-18T07:10:00Z
- **Tasks:** 5
- **Files modified:** 6

## Accomplishments

- Created PostgreSQL migration for metrics_splits table with GIN indexes for metric names and low-cardinality tags
- Created down migration for clean rollback
- Implemented Rust postgres.rs module with PgMetricsSplit, InsertableMetricsSplit, and MetricsSplits types
- Added optional sea-query dependency behind postgres feature flag
- All 132 tests pass including new postgres module tests

## Task Commits

Each task was committed atomically:

1. **Task 1: Create metrics_splits PostgreSQL migration (up)** - `9f1def32` (feat)
2. **Task 2: Create metrics_splits PostgreSQL migration (down)** - `25c365e7` (feat)
3. **Task 3: Create Rust postgres model for metrics_splits** - `eeb247b0` (feat)
4. **Task 4: Export postgres module from split mod.rs** - `26a54909` (feat)
5. **Task 5: Run all tests to verify integration** - `970ef87e` (fix - test updates)

## Files Created/Modified

- `quickwit/quickwit-metastore/migrations/postgresql/25_create-metrics-splits.up.sql` - PostgreSQL table creation with GIN indexes
- `quickwit/quickwit-metastore/migrations/postgresql/25_create-metrics-splits.down.sql` - Rollback migration
- `quickwit/quickwit-metrics-engine/src/split/postgres.rs` - Rust model with PgMetricsSplit, InsertableMetricsSplit, MetricsSplits
- `quickwit/quickwit-metrics-engine/Cargo.toml` - Added optional sea-query dependency
- `quickwit/quickwit-metrics-engine/src/split/mod.rs` - Exported postgres module

## Decisions Made

1. **TEXT[] columns with GIN indexes** - Arrays enable efficient containment queries via GIN, matching metrics query patterns
2. **split_metadata_json as authoritative** - JSON stores complete metadata, column values used only for indexing/pruning
3. **Optional postgres feature** - Avoids mandatory sea-query dependency for downstream crates not using Postgres

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed tests missing required index_id field**
- **Found during:** Task 5 (test verification)
- **Issue:** 19 tests failed because Plan 01 made index_id required but existing test helpers did not set it
- **Fix:** Added `.index_id("test-metrics-index")` to create_test_split helpers in registry.rs, partition.rs, provider.rs
- **Files modified:** quickwit-metrics-engine/src/query/registry.rs, quickwit-metrics-engine/src/query/provider.rs, quickwit-metrics-engine/src/split/partition.rs
- **Verification:** All 132 tests pass
- **Committed in:** 970ef87e

**2. [Rule 1 - Bug] Fixed flaky test_split_id_generation**
- **Found during:** Task 5 (test verification)
- **Issue:** Test generated two IDs at same nanosecond timestamp causing equality assertion failure
- **Fix:** Added 1ms sleep between SplitId::generate() calls
- **Files modified:** quickwit-metrics-engine/src/split/metadata.rs
- **Verification:** Test now passes reliably
- **Committed in:** 970ef87e

---

**Total deviations:** 2 auto-fixed (2 bugs - test failures)
**Impact on plan:** Bug fixes necessary for test suite to pass. No scope creep.

## Issues Encountered

None.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- PostgreSQL schema ready for metastore integration (Phase 14)
- Rust model ready for stage_splits/publish_splits calls (Phase 15-16)
- GIN indexes optimized for metrics query patterns (metric names, tags, time ranges)

---
*Phase: 13-metadata-schema-design*
*Completed: 2026-01-18*
