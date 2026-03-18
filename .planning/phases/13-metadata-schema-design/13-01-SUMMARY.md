---
phase: 13-metadata-schema-design
plan: 01
subsystem: database
tags: [rust, metadata, parquet, pruning, two-tier, cardinality]

# Dependency graph
requires:
  - phase: 12-metadata-analysis
    provides: Two-tier pruning strategy documentation
provides:
  - MetricsSplitMetadata struct with two-tier tag storage
  - MetricsSplitState enum for split lifecycle
  - Cardinality management helpers for tag routing
  - Unit tests for tag cardinality logic
affects: [13-02-postgres-conversion, 14-metastore-extension]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Two-tier tag storage: low_cardinality_tags HashMap + high_cardinality_tag_keys HashSet"
    - "Cardinality threshold: 1000 unique values determines Postgres vs Parquet storage"
    - "Builder pattern with add_low_cardinality_tag and add_high_cardinality_tag_key"

key-files:
  created: []
  modified:
    - quickwit/quickwit-metrics-engine/src/split/metadata.rs
    - quickwit/quickwit-metrics-engine/src/split/mod.rs
    - quickwit/quickwit-metrics-engine/src/split/format.rs
    - quickwit/quickwit-metrics-engine/src/storage/split_writer.rs

key-decisions:
  - "CARDINALITY_THRESHOLD = 1000 as constant on MetricsSplitMetadata"
  - "index_id field required for Postgres foreign key relationship"
  - "service_names() convenience method for common query pattern"
  - "finalize_tag_cardinality() promotes high-cardinality tags at split finalization"

patterns-established:
  - "Two-tier tag storage pattern for Postgres + Parquet pruning"
  - "Tag routing during ingestion (all to low cardinality) + finalization (threshold check)"

# Metrics
duration: 4min
completed: 2026-01-18
---

# Phase 13 Plan 01: Metadata Schema Design Summary

**Finalized MetricsSplitMetadata struct with two-tier tag storage, MetricsSplitState enum, and cardinality management helpers for the Phase 12 pruning strategy.**

## Performance

- **Duration:** 4 min
- **Started:** 2026-01-18T03:58:45Z
- **Completed:** 2026-01-18T04:03:09Z
- **Tasks:** 5
- **Files modified:** 4

## Accomplishments

- Added two-tier tag storage to MetricsSplitMetadata (low_cardinality_tags HashMap + high_cardinality_tag_keys HashSet)
- Added MetricsSplitState enum with Staged/Published/MarkedForDeletion variants
- Added CARDINALITY_THRESHOLD constant (1000) and cardinality management helpers
- Added comprehensive unit tests for tag cardinality logic
- Updated split_writer.rs to use new add_low_cardinality_tag API

## Task Commits

Each task was committed atomically:

1. **Task 1: Add two-tier tag storage to MetricsSplitMetadata** - `72784b7a` (feat)
2. **Task 2: Add MetricsSplitState enum** - `b720b748` (feat)
3. **Task 3: Update MetricsSplitMetadataBuilder for new fields** - `c80845ab` (feat)
4. **Task 4: Add helper methods for tag cardinality management** - `fda4284c` (feat)
5. **Task 5: Add unit tests for tag cardinality logic** - `0fb86746` (test)

## Files Created/Modified

- `quickwit/quickwit-metrics-engine/src/split/metadata.rs` - Added two-tier tag storage, MetricsSplitState enum, cardinality helpers, and unit tests
- `quickwit/quickwit-metrics-engine/src/split/mod.rs` - Exported new types and tag constants
- `quickwit/quickwit-metrics-engine/src/split/format.rs` - Updated test to use new API
- `quickwit/quickwit-metrics-engine/src/storage/split_writer.rs` - Updated to use add_low_cardinality_tag with TAG_SERVICE

## Decisions Made

1. **CARDINALITY_THRESHOLD = 1000** - Tags with fewer unique values go to Postgres, tags with more use Parquet bloom filters
2. **index_id required field** - Builder panics if not set, ensures Postgres foreign key relationship
3. **Two-step tag routing** - During ingestion, all tags route to low_cardinality_tags; during finalization, threshold check promotes high-cardinality tags

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- MetricsSplitMetadata struct complete with all fields from Phase 12 design
- MetricsSplitState enum ready for metastore integration
- Ready for Plan 02: Postgres conversion functions

---
*Phase: 13-metadata-schema-design*
*Completed: 2026-01-18*
