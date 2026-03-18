---
phase: 14-metastore-extension
plan: 02
subsystem: database
tags: [postgres, metastore, metrics-splits, sqlx, rust]

# Dependency graph
requires:
  - phase: 14-01
    provides: PostgreSQL model types (InsertableMetricsSplit, PgMetricsSplit, MetricsSplitRecord)
provides:
  - PostgresqlMetastore metrics split CRUD operations
  - ListMetricsSplitsQuery struct for filtering
  - Extension traits for request/response serialization
  - Stub implementations for FileBackedMetastore and ControlPlaneMetastore
affects: [14-03-integration, search-pruning, indexing-pipeline]

# Tech tracking
tech-stack:
  added: [quickwit-metrics-engine/postgres]
  patterns: [JSON array encoding for sqlx, dynamic SQL query building]

key-files:
  created: []
  modified:
    - quickwit/quickwit-metastore/src/metastore/mod.rs
    - quickwit/quickwit-metastore/src/metastore/postgres/metastore.rs
    - quickwit/quickwit-metastore/src/metastore/file_backed/mod.rs
    - quickwit/quickwit-metastore/src/metastore/control_plane_metastore.rs
    - quickwit/quickwit-metastore/src/lib.rs
    - quickwit/quickwit-metastore/Cargo.toml
    - quickwit/quickwit-metrics-engine/src/split/postgres.rs
    - quickwit/quickwit-metrics-engine/src/split/mod.rs

key-decisions:
  - "Used JSON encoding for array fields due to sqlx 2D array limitations"
  - "Dynamic SQL query building for list_metrics_splits with parameterized filters"
  - "Renamed postgres MetricsSplit to MetricsSplitRecord to avoid conflict with format module type"

patterns-established:
  - "Metrics split state transitions: Staged -> Published -> MarkedForDeletion"
  - "Array fields serialized as JSON strings, converted to arrays in SQL"

# Metrics
duration: 35min
completed: 2026-01-18
---

# Phase 14-02: PostgreSQL Metastore Integration Summary

**Full PostgresqlMetastore CRUD implementation for metrics_splits with stage/publish/list/mark/delete operations**

## Performance

- **Duration:** 35 min
- **Started:** 2026-01-18T05:00:00Z
- **Completed:** 2026-01-18T05:35:00Z
- **Tasks:** 5
- **Files modified:** 8

## Accomplishments

- Implemented stage_metrics_splits with bulk UNNEST insert and ON CONFLICT upsert
- Implemented publish_metrics_splits with atomic state transition and replacement marking
- Implemented list_metrics_splits with dynamic SQL filters for time range, metrics, tags
- Implemented mark_metrics_splits_for_deletion and delete_metrics_splits with state guards
- Added extension traits for building/deserializing metrics split protobuf messages
- Added stub implementations for FileBackedMetastore and ControlPlaneMetastore

## Task Commits

Each task was committed atomically:

1. **Task 1: ListMetricsSplitsQuery struct and stubs** - `48e2c4d0` (feat)
2. **Task 2: Extension traits** - `d50335cd` (feat)
3. **Task 3: stage_metrics_splits** - `0899809c` (feat)
4. **Task 4: publish/list/mark/delete** - `0e98a51c` (feat)
5. **Task 5: Export types and tests** - `57ec2c1d` (feat)

## Files Created/Modified

- `quickwit/quickwit-metastore/src/metastore/mod.rs` - ListMetricsSplitsQuery struct and extension traits
- `quickwit/quickwit-metastore/src/metastore/postgres/metastore.rs` - Full CRUD implementations
- `quickwit/quickwit-metastore/src/metastore/file_backed/mod.rs` - Stub implementations (unsupported)
- `quickwit/quickwit-metastore/src/metastore/control_plane_metastore.rs` - Proxy implementations
- `quickwit/quickwit-metastore/src/lib.rs` - Public exports
- `quickwit/quickwit-metastore/Cargo.toml` - quickwit-metrics-engine dependency
- `quickwit/quickwit-metrics-engine/src/split/postgres.rs` - Renamed MetricsSplit to MetricsSplitRecord
- `quickwit/quickwit-metrics-engine/src/split/mod.rs` - Export MetricsSplitRecord

## Decisions Made

1. **JSON array encoding** - sqlx doesn't support 2D array bindings, so array fields (metric_names, tag_*, high_cardinality_tag_keys) are serialized as JSON strings and converted to arrays in SQL using json_array_elements_text
2. **Dynamic SQL building** - list_metrics_splits builds SQL dynamically based on which filters are provided, rather than having separate queries or always including all conditions
3. **Type rename** - Renamed postgres MetricsSplit to MetricsSplitRecord to avoid collision with format::MetricsSplit which has different semantics

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

1. **sqlx 2D array types** - Discovered that sqlx doesn't implement PgHasArrayType for Vec<Vec<String>>. Solved by serializing array fields to JSON strings and using PostgreSQL's json_array_elements_text function.

2. **Type collision** - postgres module's MetricsSplit (with state and update_timestamp) collided with format module's MetricsSplit (immutable data). Renamed to MetricsSplitRecord.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- PostgreSQL metastore fully implements MetastoreService trait for metrics splits
- Ready for integration with indexing pipeline (14-03)
- All 116 non-postgres tests pass
- All 132 quickwit-metrics-engine tests pass

---
*Phase: 14-metastore-extension*
*Completed: 2026-01-18*
