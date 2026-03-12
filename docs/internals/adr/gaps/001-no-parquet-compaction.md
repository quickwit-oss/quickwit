# GAP-001: No Parquet Split Compaction

**Status**: Open
**Discovered**: 2026-02-19
**Context**: Codebase analysis during Phase 1 locality compaction design. Confirmed by [compaction-architecture.md](../../compaction-architecture.md) section "Why No Compaction (Yet)?"

## Problem

The Parquet pipeline has no compaction. Splits accumulate without merging, and the only mechanism for reducing split count is time-based retention that removes expired data. Between ingestion and retention expiry, every split produced remains in its original form.

While metrics is the immediate priority and the first product to use the Parquet pipeline, this is not a metrics-specific problem. Any product that stores data as Parquet splits — including future Parquet-based traces, logs, or other observability signals — will face the same unbounded split accumulation and query fan-out degradation. The compaction pipeline should be designed as generic Parquet infrastructure, not as a metrics-only feature. Metrics drives the initial implementation, but the architecture must not bake in metrics-specific assumptions that would need to be reworked when other products adopt Parquet storage.

At current ingestion rates, each indexer node produces Parquet splits of approximately 600 KiB (the result of 128 MiB in-memory / 1M row batching thresholds combined with high compression ratios). At 10 GiB/s aggregate ingestion, the system produces ~1,024 splits per second, accumulating ~921,600 splits per 15-minute window before any compaction. Every query within that window's time range must fan out to all splits, creating O(split_count) metadata lookups, file opens, and DataFusion task scheduling overhead.

The existing logs/traces compaction system (`StableLogMergePolicy` with Tantivy merge actors) cannot be applied to Parquet splits. The merge executor (`merge_executor.rs`) uses Tantivy's `UnionDirectory` to combine segments, which does not apply to Parquet files. A purpose-built Parquet merge pipeline is needed.

## Evidence

**No metrics-specific merge code exists.** Searching the codebase for `MetricsMerge*`, `ParquetMerge*`, `MetricsCompact*`, or any Parquet-specific merge executor returns no results. The metrics indexing pipeline has four actors (Source -> ParquetDocProcessor -> ParquetIndexer -> ParquetUploader -> ParquetPublisher) with no merge counterparts.

**Metrics pipeline explicitly skipped by merge scheduler.** The `MergePlanner` in `quickwit-indexing/src/actors/merge_planner.rs` operates on `SplitMetadata` (the Tantivy split type). `MetricsSplitMetadata` is a separate type in `quickwit-parquet-engine/src/split/metadata.rs` with no merge planner integration.

**Compaction architecture doc confirms absence:**
> "Metrics splits accumulate without compaction. This is tolerable in the short term because DataFusion can query many small Parquet files, and time-based retention eventually removes old data. But it is not ideal, and metrics compaction is a planned goal."

## State of the Art

- **ClickHouse**: MergeTree engine performs background merges of parts (equivalent to splits) using a sorted merge. Parts are organized by partition key (typically time) and merged within partitions.
- **Apache Iceberg**: Compaction rewrites small data files into fewer larger files. Sort-order-aware compaction produces files with non-overlapping key ranges.
- **Husky (Datadog)**: Size-tiered compaction within time buckets. Sort columns read first to determine merge order, then columns streamed through merge. Achieved 25-33% compression improvement and reduced query latency.
- **Prometheus/Mimir**: Head block compaction produces sorted, time-bounded blocks. Vertical compaction merges blocks with overlapping time ranges.

All of these systems treat compaction as essential infrastructure, not optional optimization.

## Potential Solutions

- **Option A (Proposed by ADR-003)**: Build a dedicated Parquet merge pipeline with sorted k-way merge, time-windowed scope, and StableLogMergePolicy adaptation. This is the approach described in [ADR-003: Time-Windowed Sorted Compaction](../003-time-windowed-sorted-compaction.md).

- **Option B**: Enable the existing Tantivy merge pipeline on Parquet splits by writing a Parquet-aware merge executor that plugs into the existing merge planner/scheduler/downloader/uploader/publisher actors. This reuses more infrastructure but requires the merge executor to handle format differences and does not address sort-order preservation.

- **Option C**: External compaction via a separate service (e.g., a Spark/DataFusion job). Decouples compaction from the Quickwit process but adds operational complexity and latency.

**Recommended**: Option A. It builds on the existing actor framework while being purpose-built for the Parquet format and sorted merge requirements.

## Signal Impact

**Metrics**: The immediate priority. Metrics is the first product on the Parquet pipeline and suffers from no compaction today.

**All Parquet-based products**: This gap affects any product that adopts Parquet storage. The compaction pipeline — time windowing, sorted merge, split metadata, compaction scope — is generic to the Parquet format, not to metrics semantics. The sort schema, window duration, and scope components are configurable per index, so the same infrastructure serves different products with different sort orders and time characteristics. Traces and logs on Parquet would use the same compaction pipeline with different sort schemas (e.g., `service_name|trace_id|timestamp` for traces).

## Impact

- **Severity**: High
- **Frequency**: Constant (every query pays the cost of no compaction)
- **Affected Areas**: `quickwit-parquet-engine`, `quickwit-indexing` (merge pipeline), `quickwit-metastore` (merge publication), query performance

## Next Steps

- [ ] Validate compression improvement: sort existing metrics Parquet files by proposed schema, compare sizes (existential experiment)
- [ ] Design Parquet merge actor pipeline (planner, downloader, executor, uploader, publisher)
- [ ] Implement sorted k-way merge executor using Arrow/Parquet APIs
- [ ] Adapt StableLogMergePolicy for metrics (or design metrics-specific merge policy)
- [ ] Run compaction policy experiments (fanin sweep, target size sweep)
- [ ] Integration test: end-to-end compaction through the full pipeline

## References

- [ADR-003: Time-Windowed Sorted Compaction](../003-time-windowed-sorted-compaction.md)
- [Compaction Architecture](../../compaction-architecture.md)
- [Phase 1: Sorted Splits for Parquet](../../locality-compaction/phase-1-sorted-splits.md)
- [StableLogMergePolicy](../../../quickwit/quickwit-indexing/src/merge_policy/stable_log_merge_policy.rs)
