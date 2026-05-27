# GAP-005: No Per-Point Deduplication

**Status**: Open
**Discovered**: 2026-02-19
**Context**: Data model analysis during Phase 1 locality compaction design ([ADR-001](../001-parquet-data-model.md))

## Problem

Quickwit provides deduplication at coarse granularities — WAL checkpoint exactly-once (prevents re-indexing the same batch on crash recovery) and file-level dedup for queue sources (prevents re-ingesting the same S3 file). However, there is no per-point deduplication: if the same metric data point (identical metric name, tags, timestamp, and value) arrives in two separate ingest requests, both copies are stored.

This can occur due to:

- **Client retries.** A client retries an ingest request after a timeout, but the original request succeeded. Both copies are ingested.
- **Overlapping sources.** Two data sources (e.g., two Kafka consumers with overlapping offsets, or a primary and failover pipeline) submit the same points.
- **Upstream replay.** An upstream system replays a window of data (e.g., for backfill or correction), producing duplicates with the existing data.

The impact depends on the query semantics. For `SUM` aggregations, duplicates inflate the result. For `MAX`/`MIN`/`AVG`, the impact varies. For `COUNT`, duplicates overcount. For queries that reconstruct individual timeseries (e.g., "plot CPU for host X"), duplicates produce repeated values at the same timestamp, which may or may not be visible depending on the visualization.

## Evidence

**No per-point dedup in ingest path.** The `IngestRouter` and `Ingester` in `quickwit-ingest/src/ingest_v2/` store all received documents without checking for duplicates against existing data. The `subrequest_id` field tracks request identity for response correlation, not for deduplication.

**WAL checkpoint dedup is batch-level.** The checkpoint mechanism in the indexing pipeline (`quickwit-indexing/src/actors/indexing_pipeline.rs`) provides exactly-once at the WAL position level — it prevents the same WAL segment from being indexed twice on crash recovery. It does not prevent the same data from being submitted in two different WAL writes.

**File-level dedup is source-specific.** The queue source coordinator (`quickwit-indexing/src/source/queue_sources/coordinator.rs`) tracks ingested files via `PartitionId` derived from file URI. This prevents re-ingesting the same file but does not detect duplicate points within or across files.

## State of the Art

- **Prometheus**: Accepts the latest sample per timeseries. Effectively LWW within the head block. TSDB compaction deduplicates samples with identical timestamps during vertical compaction.
- **Mimir/Thanos**: Deduplicate at query time using replica labels. Each replica stores its own copy; the query frontend selects one replica's data per series.
- **InfluxDB**: Supports upsert semantics — writing a point with the same measurement, tag set, and timestamp overwrites the previous value (LWW).
- **Husky**: Does not deduplicate individual points. Dedup is handled upstream in the intake pipeline before data reaches storage.

There is no consensus in the industry. Some systems dedup at ingest (Prometheus, InfluxDB), some at query time (Mimir/Thanos), some at compaction, and some not at all (Husky).

## Potential Solutions

- **Option A: Upstream dedup (Husky model).** Deduplication is the responsibility of the intake pipeline before data reaches Quickwit. This keeps the storage layer simple and moves complexity to a layer that already understands the data semantics. This is the current implicit approach.

- **Option B: Query-time dedup.** Store all duplicates, deduplicate during query execution (e.g., `DISTINCT ON (metric_name, tags, timestamp)` or selecting one value per series per timestamp). Adds query cost proportional to the duplication rate. Similar to Mimir/Thanos.

- **Option C: Compaction-time dedup.** During sorted merge, detect adjacent rows with identical (metric_name, tags, timestamp) and keep only one. This is cheap once data is sorted (duplicates are adjacent) but provides only eventual consistency — duplicates exist until the next compaction cycle.

- **Option D: Ingest-time dedup with a bloom filter or dedup index.** Maintain a probabilistic (bloom filter) or exact index of recently-seen points, and drop duplicates at ingest. This adds memory and CPU overhead at ingest and introduces a new stateful component that must be consistent across nodes (or accept per-node dedup only).

## Signal Impact

**Metrics**: Most affected. Aggregation queries (SUM, COUNT) are sensitive to duplicates. The product may require per-point dedup guarantees for correctness.

**Traces**: Less affected. Spans are typically idempotent (same trace_id + span_id is the same span). Trace storage systems commonly deduplicate by span ID.

**Logs**: Less affected. Log entries are generally append-only without dedup expectations. Duplicate log lines are tolerable in most use cases.

## Impact

- **Severity**: Medium (depends on whether the product requires per-point dedup guarantees)
- **Frequency**: Depends on upstream behavior (client retries, source overlap, replay frequency)
- **Affected Areas**: `quickwit-ingest`, `quickwit-parquet-engine` (if compaction-time dedup), query engine (if query-time dedup)

## Next Steps

- [ ] Determine whether the product requires per-point deduplication guarantees for metrics
- [ ] If yes, evaluate Options A-D based on the expected duplication rate and acceptable latency for dedup consistency
- [ ] If compaction-time dedup (Option C) is chosen, design the adjacent-row dedup logic for the sorted merge executor

## References

- [ADR-001: Parquet Data Model](../001-parquet-data-model.md) — no-LWW decision and dedup discussion
- [Compaction Architecture](../../compaction-architecture.md) — WAL checkpoint exactly-once mechanism
- WAL checkpoint code: `quickwit-indexing/src/actors/indexing_pipeline.rs`
- File-level dedup: `quickwit-indexing/src/source/queue_sources/coordinator.rs`
