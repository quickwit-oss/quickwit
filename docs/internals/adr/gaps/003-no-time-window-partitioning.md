# GAP-003: No Time-Window Partitioning at Ingestion

**Status**: Open
**Discovered**: 2026-02-19
**Context**: Codebase analysis during Phase 1 locality compaction design

## Problem

The metrics ingestion pipeline does not partition splits by time window. When the `ParquetIndexer` accumulates rows over a commit interval (default 60 seconds), the resulting split may contain rows whose timestamps span multiple time windows. There is no mechanism to ensure that each split belongs to exactly one time window, and no `window_start` or `window_duration_secs` is recorded in split metadata.

Without time-window partitioning:

1. **Compaction scope is unbounded by time.** The merge planner cannot scope compaction to individual windows, meaning there is no natural partitioning dimension to bound merge working sets.
2. **Retention is per-split, not per-window.** Deleting old data requires inspecting individual split time ranges rather than dropping entire windows.
3. **Queries cannot prune by window.** Without window assignment, the query planner must consult each split's time range individually rather than discarding entire windows.
4. **Late data has no acceptance boundary.** Without a `late_data_acceptance_window`, arbitrarily late data can be ingested, potentially triggering expensive operations on old data.

The codebase has a `PartitionGranularity` enum in `quickwit-parquet-engine/src/split/partition.rs` with `Hour`, `Day`, `Week` variants. This does not match the design requirement: the Phase 1 design calls for finer-grained, epoch-aligned windows (1-60 minutes, default 15 minutes) that evenly divide one hour. The existing partitioning is too coarse for compaction scoping and is not aligned to the compaction scope model.

## Evidence

**No window assignment in MetricsSplitMetadata:**
```rust
// quickwit-parquet-engine/src/split/metadata.rs
pub struct MetricsSplitMetadata {
    pub split_id: SplitId,
    pub index_id: String,
    pub time_range: TimeRange,           // Coarse time range, not window assignment
    pub num_rows: u64,
    pub size_bytes: u64,
    pub metric_names: HashSet<String>,
    pub low_cardinality_tags: HashMap<String, HashSet<String>>,
    pub high_cardinality_tag_keys: HashSet<String>,
    pub created_at: SystemTime,
    pub parquet_files: Vec<String>,
    // No window_start, no window_duration_secs, no sort_schema
}
```

**No window partitioning in ParquetIndexer:** The `ParquetBatchAccumulator` in `quickwit-parquet-engine/src/ingest/accumulator.rs` concatenates all pending batches into a single combined batch and writes one split. There is no grouping of rows by time window before writing.

**No late data rejection at ingestion.** There is no check that compares a data point's timestamp against a configurable maximum age. All data points are accepted regardless of timestamp.

**Existing PartitionGranularity is too coarse:**
```rust
// quickwit-parquet-engine/src/split/partition.rs
pub enum PartitionGranularity {
    Hour,   // 3600 seconds
    Day,    // 86400 seconds
    Week,   // 604800 seconds
}
```

The design requires granularities from 1 minute to 60 minutes, with the default of 15 minutes.

## State of the Art

- **Husky (Datadog)**: Fragments are bucketed by fixed-duration time windows. Each fragment belongs to exactly one window. Compaction is scoped per window.
- **ClickHouse**: Partitioning by `toYYYYMM(timestamp)` or finer granularity. Parts belong to a single partition. Merges only combine parts within the same partition.
- **Apache Iceberg**: Partition specs define time-based partitioning (hours, days, months). Each data file belongs to a single partition. Compaction operates within partitions.
- **Prometheus/Mimir**: Blocks cover fixed time ranges (2 hours by default). Compaction combines blocks with overlapping or adjacent time ranges into larger blocks.

Time-based partitioning is universal in observability storage systems.

## Potential Solutions

- **Option A (Proposed by ADR-003)**: Implement epoch-aligned time-window partitioning at ingestion. Before writing, group rows by window assignment, produce a separate Parquet file per window. Add `window_start`, `window_duration_secs`, and `compaction_start_time` to configuration and metadata. Implement `late_data_acceptance_window` to drop points older than a configurable threshold.

- **Option B**: Extend the existing `PartitionGranularity` enum to support finer granularities (1m, 5m, 15m, etc.) and use it for window assignment. This reuses existing code but may require significant refactoring of the partition logic to match the epoch-aligned design.

**Recommended**: Option A, with the possibility of refactoring `PartitionGranularity` to support the required granularities if the existing code is close enough.

## Signal Impact

**Metrics**: Directly affected. No time-window partitioning exists for the Parquet pipeline.

**Traces and logs**: Not directly affected (Tantivy pipeline has time ranges on splits but no formal window partitioning). Phase 4 would benefit from formal time windowing for the same reasons.

## Impact

- **Severity**: High
- **Frequency**: Constant (every split is affected by the lack of window assignment)
- **Affected Areas**: `quickwit-parquet-engine/src/ingest/accumulator.rs`, `quickwit-parquet-engine/src/split/metadata.rs`, `quickwit-indexing/src/actors/parquet_indexer.rs`, index configuration

## Next Steps

- [ ] Define epoch-aligned time-window computation: `window_start = t - (t % window_duration_seconds)`
- [ ] Implement row partitioning by window in `ParquetBatchAccumulator` (group rows by timestamp window before concatenation)
- [ ] Produce one split per window when commit interval straddles a window boundary
- [ ] Add `window_duration` configuration to index settings (default 15 minutes, valid: 1m-60m, must evenly divide 1 hour)
- [ ] Add `compaction_start_time` configuration (required for Phase 1 enablement)
- [ ] Implement `late_data_acceptance_window` at ingestion (drop points older than threshold)
- [ ] Add `window_start` and `window_duration_secs` to MetricsSplitMetadata
- [ ] Add window columns to `metrics_splits` PostgreSQL table
- [ ] Handle null-timestamp rows: assign to overflow window (window_start = 0)

## References

- [ADR-003: Time-Windowed Sorted Compaction](../003-time-windowed-sorted-compaction.md)
- [Phase 1: Sorted Splits for Parquet](../../locality-compaction/phase-1-sorted-splits.md)
- Current implementation: `quickwit-parquet-engine/src/split/partition.rs`, `quickwit-parquet-engine/src/ingest/accumulator.rs`
