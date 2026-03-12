---
phase: 01-foundation
plan: 04
subsystem: split
tags: [rust, parquet, split, metadata, serialization, pruning]

# Dependency graph
requires:
  - phase: 01-02
    provides: MetricsSchema types for schema definitions
provides:
  - MetricsSplit type with JSON serialization
  - SplitId unique identifier with generation
  - TimeRange type with overlap/containment checks
  - MetricsSplitMetadata with builder pattern
  - Query pruning helpers for time range and metric names
affects: [02-indexing, 03-query, metastore integration]

# Tech tracking
tech-stack:
  added: [serde_json]
  patterns: [builder pattern for metadata construction]

key-files:
  created:
    - quickwit/quickwit-metrics-engine/src/split/metadata.rs
    - quickwit/quickwit-metrics-engine/src/split/format.rs
  modified:
    - quickwit/quickwit-metrics-engine/src/split/mod.rs
    - quickwit/quickwit-metrics-engine/Cargo.toml

key-decisions:
  - "Added serde_json dependency for JSON serialization"
  - "TimeRange uses seconds (u64) for efficient storage and comparison"
  - "Empty metric_names set means split might contain any metric (no pruning possible)"

patterns-established:
  - "Builder pattern for MetricsSplitMetadata construction"
  - "Immutable split design - once created, MetricsSplit is read-only"
  - "Format versioning for forward compatibility"

# Metrics
duration: 5min
completed: 2026-01-15
---

# Phase 1 Plan 04: Metrics Split Format Summary

**MetricsSplit type with SplitId, TimeRange, metadata builder, JSON serialization, and time/metric pruning helpers for query optimization**

## Performance

- **Duration:** 5 min
- **Started:** 2026-01-15T10:30:00Z
- **Completed:** 2026-01-15T10:35:00Z
- **Tasks:** 2
- **Files modified:** 4

## Accomplishments

- Defined SplitId type with unique ID generation using nanosecond timestamps
- Created TimeRange type with overlap, containment, and duration methods
- Built MetricsSplitMetadata with builder pattern for flexible construction
- Implemented MetricsSplit with JSON serialization via serde_json
- Added query pruning helpers for time range and metric name filtering
- Unit tests verify split creation, pruning, and JSON roundtrip

## Task Commits

Each task was committed atomically:

1. **Task 1: Define MetricsSplitMetadata for split properties** - `0462cc29` (feat)
2. **Task 2: Create MetricsSplit and serialization** - `6c13e6bd` (feat)

## Files Created/Modified

- `quickwit/quickwit-metrics-engine/src/split/metadata.rs` - SplitId, TimeRange, MetricsSplitMetadata with builder
- `quickwit/quickwit-metrics-engine/src/split/format.rs` - MetricsSplit with serialization and pruning helpers
- `quickwit/quickwit-metrics-engine/src/split/mod.rs` - Module exports
- `quickwit/quickwit-metrics-engine/Cargo.toml` - Added serde_json dependency

## Decisions Made

1. **Added serde_json workspace dependency:** Required for JSON serialization of MetricsSplit. Already available in workspace.
2. **TimeRange uses u64 seconds:** Efficient storage, sufficient precision for metrics (not nanoseconds like logs).
3. **Empty metric_names set means no pruning:** If metric_names is empty, might_contain_metric returns true for any metric.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- MetricsSplit type ready for use in indexing and query components
- Split metadata can be serialized for metastore storage
- Time range and metric name pruning available for query optimization
- All 4 plans in Phase 1 Foundation complete

---
*Phase: 01-foundation*
*Completed: 2026-01-15*
