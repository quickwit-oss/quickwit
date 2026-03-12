---
phase: 14-metastore-extension
plan: 01
subsystem: api
tags: [protobuf, grpc, rust, metastore, metrics]

# Dependency graph
requires:
  - phase: 13-02
    provides: PostgreSQL metrics_splits table with GIN indexes
provides:
  - Protobuf message definitions for metrics split lifecycle
  - gRPC RPC methods in MetastoreService trait
  - Generated Rust types for all metrics split operations
affects: [14-02-postgresql-impl, 15-stage-splits, 16-publish-splits]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "JSON-serialized metadata in protobuf (split_metadata_list_serialized_json)"
    - "index_id string (not IndexUid) for simpler metrics identity"

key-files:
  created: []
  modified:
    - quickwit/quickwit-proto/protos/quickwit/metastore.proto
    - quickwit/quickwit-proto/src/codegen/quickwit/quickwit.metastore.rs

key-decisions:
  - "Use index_id (string) instead of IndexUid for metrics splits"
  - "Follow JSON serialization pattern from existing split messages"
  - "Query predicate as JSON in ListMetricsSplitsRequest for flexibility"

patterns-established:
  - "Parallel API surface for metrics splits mirroring Tantivy splits"

# Metrics
duration: 2min
completed: 2026-01-18
---

# Phase 14 Plan 01: Protobuf Messages Summary

**Protobuf message definitions and gRPC RPC methods for metrics split lifecycle operations (stage, publish, list, mark, delete) with generated Rust types.**

## Performance

- **Duration:** 2 min
- **Started:** 2026-01-18T05:10:25Z
- **Completed:** 2026-01-18T05:11:58Z
- **Tasks:** 3
- **Files modified:** 2

## Accomplishments

- Added 6 protobuf message definitions for metrics split operations
- Added 5 RPC methods to MetastoreService for complete metrics split lifecycle
- Successfully regenerated Rust code with all new types and trait methods

## Task Commits

Each task was committed atomically:

1. **Task 1: Add metrics split message definitions** - `a0b47714` (feat)
2. **Task 2: Add metrics split RPC methods** - `8acd68e3` (feat)
3. **Task 3: Run protobuf code generation** - `83fe722a` (chore)

## Files Created/Modified

- `quickwit/quickwit-proto/protos/quickwit/metastore.proto` - Added 6 messages and 5 RPCs
- `quickwit/quickwit-proto/src/codegen/quickwit/quickwit.metastore.rs` - Generated Rust types

## Protobuf Messages Added

| Message | Purpose |
|---------|---------|
| `StageMetricsSplitsRequest` | Stage new splits after Parquet creation |
| `PublishMetricsSplitsRequest` | Make staged splits queryable |
| `ListMetricsSplitsRequest` | Query planning and Tier 1 pruning |
| `ListMetricsSplitsResponse` | Return splits as JSON |
| `MarkMetricsSplitsForDeletionRequest` | Cleanup flow |
| `DeleteMetricsSplitsRequest` | Final deletion |

## RPC Methods Added

| RPC | Request | Response |
|-----|---------|----------|
| `StageMetricsSplits` | StageMetricsSplitsRequest | EmptyResponse |
| `PublishMetricsSplits` | PublishMetricsSplitsRequest | EmptyResponse |
| `ListMetricsSplits` | ListMetricsSplitsRequest | ListMetricsSplitsResponse |
| `MarkMetricsSplitsForDeletion` | MarkMetricsSplitsForDeletionRequest | EmptyResponse |
| `DeleteMetricsSplits` | DeleteMetricsSplitsRequest | EmptyResponse |

## Decisions Made

1. **index_id (string) vs IndexUid** - Metrics splits use simple string index_id, not composite IndexUid, following Phase 13 design decision for simpler identity model
2. **JSON serialization for metadata** - Follows existing pattern where complex metadata is JSON-serialized in protobuf, not native fields
3. **query_json for ListMetricsSplitsRequest** - Flexible JSON predicate allows evolving query structure without proto changes

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- Protobuf interface complete and code-generated
- Ready for Plan 02: PostgreSQL implementation of stage/publish/list methods
- MetastoreService trait now includes all metrics split methods awaiting implementation

---
*Phase: 14-metastore-extension*
*Completed: 2026-01-18*
