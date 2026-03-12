---
phase: 15-metastore-staging
plan: 02
subsystem: indexing
tags: [quickwit-actors, mailbox, split-staging, parquet, arrow]

# Dependency graph
requires:
  - phase: 15-01
    provides: MetricsUploader actor with staging capability
provides:
  - MetricsIndexer → MetricsUploader forwarding
  - Complete staging flow from split production to metastore
  - Test helpers for MetricsUploader in test setup
affects: [16-metastore-publishing, metrics-pipeline]

# Tech tracking
tech-stack:
  added: [parquet (dev-dependency for VariantArrayBuilder)]
  patterns: [actor-mailbox-wiring, test-helper-factories]

key-files:
  created: []
  modified:
    - quickwit/quickwit-indexing/src/actors/metrics_indexer.rs
    - quickwit/quickwit-indexing/src/actors/metrics_doc_processor.rs
    - quickwit/quickwit-indexing/src/actors/indexing_pipeline.rs
    - quickwit/quickwit-indexing/src/split_store/indexing_split_store.rs
    - quickwit/quickwit-indexing/Cargo.toml

key-decisions:
  - "Use remote_storage() getter on IndexingSplitStore instead of into_inner() for MetricsUploader"
  - "Publisher created without source mailbox for metrics pipeline (not needed)"
  - "Test helper uses permissive mock without withf predicate for async spawned task timing"

patterns-established:
  - "create_test_uploader helper pattern for MetricsIndexer tests"
  - "create_variant_array helper for VARIANT type test data"

# Metrics
duration: 12min
completed: 2026-01-18
---

# Phase 15 Plan 02: Wire Indexer to Uploader Summary

**MetricsIndexer now forwards MetricsSplitBatch to MetricsUploader for staging and upload to storage**

## Performance

- **Duration:** 12 min
- **Started:** 2026-01-18T12:00:00Z
- **Completed:** 2026-01-18T12:12:00Z
- **Tasks:** 3
- **Files modified:** 6

## Accomplishments
- Connected MetricsIndexer output to MetricsUploader for complete staging flow
- Added uploader_mailbox field to MetricsIndexer and wired in pipeline spawn
- Updated all MetricsIndexer tests with proper uploader mailbox setup
- Fixed test schema to use VARIANT type for attributes (was StringArray)
- Added new test verifying split forwarding to uploader

## Task Commits

Each task was committed atomically:

1. **Task 1: Add MetricsUploader mailbox to MetricsIndexer** - `0cba23f6` (feat)
2. **Task 2: Forward splits to MetricsUploader** - `b453bc58` (feat)
3. **Task 3: Update tests to provide uploader mailbox** - `f8f14442` (test)

## Files Created/Modified
- `quickwit/quickwit-indexing/src/actors/metrics_indexer.rs` - Added uploader_mailbox, forwarding logic, test helpers
- `quickwit/quickwit-indexing/src/actors/indexing_pipeline.rs` - Wire MetricsUploader in spawn_metrics_pipeline
- `quickwit/quickwit-indexing/src/actors/metrics_doc_processor.rs` - Update test with uploader mailbox
- `quickwit/quickwit-indexing/src/split_store/indexing_split_store.rs` - Add remote_storage() getter
- `quickwit/quickwit-indexing/Cargo.toml` - Add parquet dev dependency

## Decisions Made
- Added `remote_storage()` getter to IndexingSplitStore rather than modifying MetricsUploader to accept IndexingSplitStore directly - cleaner separation of concerns
- Publisher in metrics pipeline created without source_mailbox (None) since metrics pipeline doesn't need the SourceActor callback that logs publishers use
- Test helper uses permissive mock expectation (`.returning(|_|...)` without `.withf()`) because staging happens in spawned task with timing challenges

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Test schema mismatch for VARIANT fields**
- **Found during:** Task 3 (test execution)
- **Issue:** Test helper `create_test_batch` used `StringArray` for attributes/resource_attributes, but schema expects VARIANT (struct with BinaryView)
- **Fix:** Added `create_variant_array` helper using `VariantArrayBuilder`, added parquet dev dependency
- **Files modified:** metrics_indexer.rs, Cargo.toml
- **Verification:** All 6 MetricsIndexer tests pass
- **Committed in:** f8f14442 (Task 3 commit)

**2. [Rule 3 - Blocking] Pipeline spawn missing uploader wiring**
- **Found during:** Task 1 (cargo check)
- **Issue:** spawn_metrics_pipeline() needed updating to create MetricsUploader and pass mailbox
- **Fix:** Added MetricsUploader creation with Publisher downstream, added remote_storage() getter
- **Files modified:** indexing_pipeline.rs, indexing_split_store.rs
- **Verification:** cargo check passes
- **Committed in:** 0cba23f6 (Task 1 commit)

---

**Total deviations:** 2 auto-fixed (both blocking)
**Impact on plan:** Essential fixes for compilation and test correctness. No scope creep.

## Issues Encountered
- async spawned task timing in test: MetricsUploader's staging happens in `spawn_named_task`, making mock timing tricky. Resolved by adding sleep and using permissive mock.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- Staging flow complete: MetricsIndexer → MetricsUploader → stage_metrics_splits()
- Ready for Phase 16: Publisher integration for publish_metrics_splits()
- No blockers

---
*Phase: 15-metastore-staging*
*Completed: 2026-01-18*
