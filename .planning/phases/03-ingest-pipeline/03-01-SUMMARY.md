---
phase: 03-ingest-pipeline
plan: 01
subsystem: ingest
tags: [arrow, ipc, parquet, accumulator, metrics]

# Dependency graph
requires:
  - phase: 02-storage-layer
    provides: MetricsSplitWriter, ParquetWriterConfig, MetricsParquetWriter
  - phase: 01-foundation
    provides: MetricsSchema, MetricsSplit
provides:
  - MetricsIngestConfig for accumulation thresholds
  - MetricsIngestProcessor for Arrow IPC to RecordBatch conversion
  - MetricsBatchAccumulator for split production
  - IngestError for ingest operation errors
affects: [07-pipeline-integration]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Arrow IPC stream reading/writing"
    - "RecordBatch concatenation for accumulation"

key-files:
  created:
    - quickwit/quickwit-metrics-engine/src/ingest/mod.rs
    - quickwit/quickwit-metrics-engine/src/ingest/config.rs
    - quickwit/quickwit-metrics-engine/src/ingest/processor.rs
    - quickwit/quickwit-metrics-engine/src/ingest/accumulator.rs
  modified:
    - quickwit/quickwit-metrics-engine/src/lib.rs

key-decisions:
  - "1M rows / 128MB as default accumulation thresholds"
  - "Direct Arrow IPC parsing (no quickwit-opentelemetry dependency)"
  - "Schema validation on IPC decode"

patterns-established:
  - "Builder pattern for ingest config"
  - "Accumulator pattern for batched split production"

# Metrics
duration: 4min
completed: 2026-01-15
---

# Phase 3 Plan 1: Ingest Pipeline Summary

**MetricsIngestProcessor and MetricsBatchAccumulator providing Arrow IPC to Parquet split pipeline, bypassing Tantivy entirely**

## Performance

- **Duration:** 4 min
- **Started:** 2026-01-15T05:44:40Z
- **Completed:** 2026-01-15T05:48:29Z
- **Tasks:** 3
- **Files modified:** 5

## Accomplishments

- MetricsIngestConfig with configurable max_rows (1M) and max_bytes (128MB) thresholds
- MetricsIngestProcessor converts Arrow IPC bytes to validated RecordBatch
- MetricsBatchAccumulator buffers batches and produces splits when thresholds exceeded
- IngestError enum for comprehensive error handling
- Schema validation ensures incoming data matches metrics schema

## Task Commits

Each task was committed atomically:

1. **Task 1: Create MetricsIngestConfig with builder pattern** - `7ec2d883` (feat)
2. **Task 2: Create MetricsIngestProcessor for Arrow IPC conversion** - `9cbe5eca` (feat)
3. **Task 3: Create MetricsBatchAccumulator for split production** - `79cbb79b` (feat)

## Files Created/Modified

- `quickwit/quickwit-metrics-engine/src/ingest/mod.rs` - Ingest module declarations and exports
- `quickwit/quickwit-metrics-engine/src/ingest/config.rs` - MetricsIngestConfig with builder pattern
- `quickwit/quickwit-metrics-engine/src/ingest/processor.rs` - MetricsIngestProcessor and IPC utilities
- `quickwit/quickwit-metrics-engine/src/ingest/accumulator.rs` - MetricsBatchAccumulator for split production
- `quickwit/quickwit-metrics-engine/src/lib.rs` - Added `pub mod ingest;`

## Decisions Made

1. **1M rows / 128MB default thresholds** - Standard accumulation sizes for metrics splits
2. **Direct Arrow IPC parsing** - Avoided quickwit-opentelemetry dependency to keep module lightweight
3. **Schema validation after IPC decode** - Ensures field count and names match expected schema

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- Ingest pipeline infrastructure complete
- Ready for Phase 4: Query Engine or additional Phase 3 plans
- Arrow IPC can be converted to RecordBatch and accumulated into MetricsSplit

---
*Phase: 03-ingest-pipeline*
*Completed: 2026-01-15*
