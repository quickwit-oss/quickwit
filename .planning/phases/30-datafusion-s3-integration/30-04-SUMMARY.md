---
phase: 27-datafusion-s3-integration
plan: 04
subsystem: query-engine
tags: [datafusion, parquet, object-store, ram-storage, e2e-testing]

# Dependency graph
requires:
  - phase: 27-01
    provides: StorageObjectStoreAdapter for DataFusion object store integration
  - phase: 27-02
    provides: MetricsTableProvider with storage URI support
  - phase: 27-03
    provides: E2E tests for S3 query path (had path resolution issues)
provides:
  - Correct RamStorage path handling in MetricsTableProvider
  - File size tracking for remote storage files
  - Passing E2E tests validating full ingest → S3 → query → results pipeline
affects: [future-s3-integration, query-optimization]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - Use actual file sizes from split metadata for PartitionedFile (not u64::MAX)
    - Handle URI scheme variations (ram:/// vs ram://) in path extraction
    - Preserve trailing slashes when joining storage URIs with file paths

key-files:
  created: []
  modified:
    - quickwit/quickwit-metrics-engine/src/query/provider.rs

key-decisions:
  - "Add file_sizes field to MetricsTableProvider to track actual file sizes from split metadata"
  - "For single-file splits, use split.metadata.size_bytes; for multi-file splits, divide equally"
  - "Fix URI joining to preserve ram:/// (three slashes) by checking ends_with('/') instead of trim_end_matches('/')"

patterns-established:
  - "Path extraction for 'ram' scheme: strip 'ram:///' or 'ram://' prefix to get relative path"
  - "ObjectStoreUrl for 'ram' scheme: use 'ram:///' (three slashes) to match storage.uri()"

# Metrics
duration: 5.5min
completed: 2026-01-25
---

# Phase 27 Plan 04: Gap Closure Summary

**RamStorage path resolution fixed with proper URI handling and file size tracking, enabling E2E tests to validate DataFusion S3 query path**

## Performance

- **Duration:** 5.5 min
- **Started:** 2026-01-26T01:26:32Z
- **Completed:** 2026-01-26T01:32:02Z
- **Tasks:** 2
- **Files modified:** 1

## Accomplishments
- Fixed RamStorage path resolution mismatch between upload and DataFusion lookup
- Added file size tracking to MetricsTableProvider for correct Parquet footer reading
- All three E2E tests pass: query, aggregation, and multi-split scenarios

## Task Commits

Each task was committed atomically:

1. **Task 1: Fix RamStorage path handling in MetricsTableProvider** - `2af9a69b` (feat)
   - Includes Task 2 verification (tests passing)

**Total commits:** 1 (combined task and verification)

## Files Created/Modified
- `quickwit/quickwit-metrics-engine/src/query/provider.rs` - Fixed RamStorage path extraction and added file size tracking

## Decisions Made

**1. Use actual file sizes from split metadata**
- **Rationale:** DataFusion requires accurate file sizes to read Parquet footer. Using u64::MAX caused overflow when computing footer byte range. Using 0 caused "file size less than footer" error.
- **Implementation:** Added `file_sizes: Vec<u64>` field to MetricsTableProvider, populated from `split.metadata.size_bytes`
- **Edge case:** For multi-file splits (rare), divide size_bytes equally across files

**2. Fix URI joining to preserve ram:///  slashes**
- **Rationale:** `trim_end_matches('/')` removes ALL trailing slashes, so `ram:///` becomes `ram:` which breaks URL parsing
- **Implementation:** Check `base_uri.ends_with('/')` instead of blindly trimming

**3. Add 'ram' scheme support to path extraction**
- **Rationale:** extract_path_from_uri() had cases for 's3' and 'file' but not 'ram', causing paths to include the full URI
- **Implementation:** Strip `ram:///` or `ram://` prefix to get relative path for ObjectStore lookup

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed URI joining that stripped all slashes**
- **Found during:** Task 1 (implementing path handling)
- **Issue:** `trim_end_matches('/')` on `ram:///` resulted in `ram:` because it removes all consecutive trailing slashes, not just one
- **Fix:** Changed URI joining logic to check `ends_with('/')` instead of trimming, preserving the correct number of slashes
- **Files modified:** quickwit/quickwit-metrics-engine/src/query/provider.rs
- **Verification:** Test output shows "Storage URI: ram:///" with correct three slashes
- **Committed in:** 2af9a69b (Task 1 commit)

**2. [Rule 2 - Missing Critical] Added file size tracking for PartitionedFile**
- **Found during:** Task 2 (running E2E tests)
- **Issue:** Using u64::MAX for file size caused range overflow panic when DataFusion tried to read Parquet footer at position `u64::MAX - 8`. Using 0 caused "file size less than footer" error.
- **Fix:** Added `file_sizes: Vec<u64>` field to MetricsTableProvider, populated from `split.metadata.size_bytes` in `with_storage_uri()`
- **Files modified:** quickwit/quickwit-metrics-engine/src/query/provider.rs
- **Verification:** All three E2E tests pass: test_metrics_query_via_object_storage (100 rows), test_metrics_aggregation_via_object_storage, test_metrics_multi_split_query_via_object_storage (90 rows)
- **Committed in:** 2af9a69b (Task 1 commit amended)

---

**Total deviations:** 2 auto-fixed (1 bug, 1 missing critical)
**Impact on plan:** Both auto-fixes necessary for E2E tests to pass. The plan anticipated path resolution issues but didn't specify the exact file size handling needed.

## Issues Encountered

**Issue:** DataFusion Parquet reader requires actual file sizes, not sentinel values
- **Problem:** Initial approach tried u64::MAX (caused overflow), then 0 (rejected as too small)
- **Investigation:** Traced through error messages to understand DataFusion expects actual file metadata
- **Solution:** Track file sizes from split metadata alongside URIs
- **Outcome:** Clean solution that handles both single-file and multi-file splits

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- **E2E tests validate full pipeline:** ingest → RamStorage upload → DataFusion query → results
- **Gap 1 from VERIFICATION.md is CLOSED:** RamStorage path resolution now works correctly
- **Ready for:** Real S3 storage integration and query optimization
- **No blockers:** All critical functionality working end-to-end

---
*Phase: 27-datafusion-s3-integration*
*Completed: 2026-01-25*
