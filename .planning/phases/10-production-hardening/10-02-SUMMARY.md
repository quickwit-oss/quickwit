---
phase: 10-production-hardening
plan: 02
subsystem: observability
tags: [tracing, logging, debugging, instrument]

# Dependency graph
requires:
  - phase: 03-ingest-pipeline
    provides: MetricsIngestProcessor, MetricsBatchAccumulator
  - phase: 02-storage-layer
    provides: MetricsParquetWriter, MetricsSplitWriter
provides:
  - Structured tracing instrumentation for ingest modules
  - Structured tracing instrumentation for storage modules
  - Debug and info level logs for operational visibility
affects: [11-performance, 12-monitoring]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Use #[instrument] macro for span creation with field extraction"
    - "Use debug! for detailed operational info"
    - "Use info! for important state changes (split creation)"
    - "Use warn! for threshold conditions and schema mismatches"

key-files:
  created: []
  modified:
    - quickwit/quickwit-metrics-engine/src/ingest/processor.rs
    - quickwit/quickwit-metrics-engine/src/ingest/accumulator.rs
    - quickwit/quickwit-metrics-engine/src/storage/writer.rs
    - quickwit/quickwit-metrics-engine/src/storage/split_writer.rs

key-decisions:
  - "Use tracing crate (already in workspace) for structured logging"
  - "Instrument at function level with #[instrument] for automatic span creation"
  - "Skip self and large data parameters in spans to avoid overhead"
  - "Include relevant metrics in span fields (bytes_len, batch_rows, etc.)"

# Metrics
duration: 5min
completed: 2026-01-15
---

# Phase 10 Plan 02: Structured Tracing/Logging Summary

**Added structured tracing instrumentation to ingest and storage modules using tracing crate for debugging and operational visibility**

## Performance

- **Duration:** 5 min
- **Started:** 2026-01-15T08:49:24Z
- **Completed:** 2026-01-15T08:54:36Z
- **Tasks:** 2
- **Files modified:** 4

## Accomplishments

- Added tracing instrumentation to MetricsIngestProcessor.process_ipc() with bytes_len field
- Added debug logging for successful IPC decode with row count
- Added warn logging for schema mismatch errors with field details
- Added tracing to MetricsBatchAccumulator with batch size and threshold logging
- Added tracing instrumentation to MetricsParquetWriter.write_to_bytes() and write_to_file()
- Added tracing to MetricsSplitWriter.write_split() with time range and split metadata logging

## Task Commits

Each task was committed atomically:

1. **Task 1: Add tracing dependency and instrument ingest** - `73fab55d` (feat)
2. **Task 2: Instrument storage layer with tracing** - `89cc16e9` (feat)

## Files Created/Modified

- `quickwit/quickwit-metrics-engine/src/ingest/processor.rs` - Added tracing imports and instrumentation to process_ipc() and validate_schema()
- `quickwit/quickwit-metrics-engine/src/ingest/accumulator.rs` - Added tracing imports and debug/info/warn logging to add_batch() and flush()
- `quickwit/quickwit-metrics-engine/src/storage/writer.rs` - Added tracing imports and instrumentation to write_to_bytes() and write_to_file()
- `quickwit/quickwit-metrics-engine/src/storage/split_writer.rs` - Added tracing imports and instrumentation to write_split() with time range and metadata logging

## Decisions Made

| Decision | Rationale |
|----------|-----------|
| Use workspace tracing crate | Already a dependency, consistent with rest of quickwit |
| Skip self and data parameters in instrument | Avoid cloning large data structures into spans |
| Use fields() to capture relevant metrics | Provides searchable structured data in traces |
| debug! for operational details | Appropriate verbosity for day-to-day debugging |
| info! for state changes (split creation) | Important events visible at default log level |
| warn! for threshold exceeded | Operational visibility into accumulator behavior |

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- Tracing instrumentation complete for ingest and storage paths
- Ready for 10-03: Error handling improvements
- Ready for 10-04: Configuration validation

---
*Phase: 10-production-hardening*
*Completed: 2026-01-15*
