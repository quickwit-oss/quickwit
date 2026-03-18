# Feature Landscape: Parquet Compaction Pipeline

**Domain:** Time-windowed sorted Parquet compaction for metrics workloads
**Researched:** 2026-02-23
**Overall Confidence:** HIGH (extensive internal design docs + well-documented external systems)

## Executive Summary

Parquet compaction for time-series observability data is a well-understood domain with strong consensus across systems (ClickHouse, Iceberg, Hudi, Husky, Prometheus/Mimir, RocksDB/LSM-tree literature). The table-stakes features are narrow and well-defined: time-partitioned merge scope, size-tiered merge policy, sorted merge execution, late data handling, and basic monitoring. The differentiators -- leading-edge prioritization, compaction-time deduplication, RLE merge order optimization, and self-describing files -- are where Pomsky can gain advantage given its specific position as a Husky-informed system built on commodity Parquet.

The internal design (ADR-003, Phase 1 design doc, TLA+ spec, GAP-001 through GAP-010) is exceptionally thorough. The feature landscape below is organized to highlight what the roadmap should prioritize vs. defer, cross-referenced against how ClickHouse, Iceberg, Hudi, Delta Lake, Husky, and Prometheus/Mimir implement each feature.

---

## Table Stakes

Features that every production compaction system implements. Absence of any of these means the system is not production-ready for compaction.

| # | Feature | Why Expected | Complexity | System Precedent | Internal Ref |
|---|---------|-------------|------------|-----------------|-------------|
| TS-1 | **Time-window partitioning at ingestion** | Every system partitions by time. Queries always filter by time range. Without windowing, compaction scope is unbounded. | Medium | ClickHouse (partition by month/day), Iceberg (partition specs), Husky (time buckets), Prometheus (2h blocks) | ADR-003 Sec 1-2, GAP-003 |
| TS-2 | **Late data acceptance window** | All systems bound how far back data can arrive. Without it, arbitrarily old data triggers re-merges indefinitely. | Low | Husky (configurable per product), Prometheus (out-of-order ingestion window), ClickHouse (TTL-based partition management) | ADR-003 Sec 6, Phase 1 "Late-Arriving Data" |
| TS-3 | **Merge planner with scope-aware candidate selection** | Every system has a component that decides *what* to merge. Must respect compatibility constraints (schema, partition, window). | Medium | ClickHouse (SimpleMergeSelector), Husky (per-table per-window planner), Mimir (split-and-merge per-tenant compactor), Iceberg (rewrite_data_files procedure) | ADR-003 Sec 3, 8 |
| TS-4 | **Size-tiered merge policy** | The most basic merge strategy: combine small files into larger ones. Every system starts here. Without it, file count grows unboundedly. | Medium | RocksDB (Universal compaction = tiered), ClickHouse (SimpleMergeSelector prefers similar-sized parts), Husky (size-tiered as "table stakes"), Delta Lake (OPTIMIZE bin-pack) | ADR-003 Sec 8, compaction-architecture.md "StableLogMergePolicy" |
| TS-5 | **Sorted merge execution (k-way or equivalent)** | Preserving sort order through compaction is what turns compaction from "file cleanup" into "query optimization." All modern systems do sorted merge. | High | ClickHouse (parts sorted by ORDER BY, merge preserves order), Husky (k-way sorted merge with RLE index), Iceberg (sort-order-aware rewrite), Hudi (sorted merge in log compaction) | ADR-003 Sec 4, Phase 1 "Sorted Merge at Compaction" |
| TS-6 | **Merge split downloader** | Fetching input splits from object storage for merge. Straightforward but critical for correctness (must handle partial failures, retries, checksums). | Low | All systems (any that use object storage) | ADR-003 Sec "Implementation Status" |
| TS-7 | **Merge uploader** | Uploading merged output to object storage. Must handle multipart upload, retries, and ensure atomicity with metadata update. | Low | All systems | ADR-003 Sec "Implementation Status" |
| TS-8 | **Atomic merge publication** | The metastore update (replace N old splits with 1 new split) must be atomic. Without atomicity, crashes during publication produce duplicate or lost data. | Medium | ClickHouse (rename part directories atomically), Husky (FoundationDB transaction), Iceberg (manifest commit with optimistic concurrency), Prometheus (block swap), Mimir (object storage + bucket index) | compaction-architecture.md "Physical Merge Process" |
| TS-9 | **Metadata extensions for compaction scope** | Split metadata must record window_start, window_duration, sort_schema to enable merge compatibility checks and query planning. | Medium | Iceberg (manifest per-file metadata), Husky (fragment metadata with sort schema, min/max/regex), Delta Lake (transaction log per-file stats), ClickHouse (part metadata with primary key ranges) | ADR-003 Sec 9, GAP-004 |
| TS-10 | **Pre-existing data transition strategy** | Clean cutoff: old unsorted data is NOT compacted; it ages out via retention. Avoids the complexity of mixed sorted/unsorted merges. | Low | ClickHouse (schema changes produce incompatible parts that don't merge), Iceberg (partition evolution -- old files keep old spec), Hudi (schema evolution preserves existing data) | ADR-003 Sec 7 |
| TS-11 | **Basic compaction monitoring** | Operators must know: Is compaction running? Is it keeping up? What is the current split count per window? | Medium | ClickHouse (system.merges, system.parts), Mimir (cortex_compactor_* Prometheus metrics, dedicated Grafana dashboards), Prometheus (compact.* metrics), Husky (internal monitoring) | Phase 1 "Monitoring" |
| TS-12 | **Schema evolution during merge** | Input splits may have different column sets. Output must contain the union of all columns, filling nulls for missing columns. Type conflicts must fail explicitly. | Medium | Iceberg (schema evolution with column IDs), Hudi (schema evolution with Avro compatibility), ClickHouse (ALTER TABLE ADD COLUMN, parts with different schemas merged during compaction) | ADR-003 Sec 5, MC-4 invariant |

### Dependency Map for Table Stakes

```
TS-9 (Metadata extensions) ─── required by ──> TS-1 (Time windowing at ingestion)
                           └── required by ──> TS-3 (Merge planner)

TS-1 (Time windowing)     ─── required by ──> TS-3 (Merge planner)
TS-2 (Late data window)   ─── required by ──> TS-1 (drops old data at ingestion)

TS-3 (Merge planner)      ─── triggers ────> TS-6 (Downloader)
TS-6 (Downloader)          ─── feeds ──────> TS-5 (Sorted merge)
TS-5 (Sorted merge)        ─── feeds ──────> TS-7 (Uploader)
TS-7 (Uploader)            ─── feeds ──────> TS-8 (Atomic publication)

TS-4 (Size-tiered policy)  ─── configured in ─> TS-3 (Merge planner)
TS-10 (Transition strategy)─── configured in ─> TS-3 (compaction_start_time filter)
TS-12 (Schema evolution)   ─── handled in ───> TS-5 (column union during merge)
TS-11 (Monitoring)         ─── wraps ────────> all of the above
```

---

## Differentiators

Features that set Pomsky's compaction apart from a generic Parquet compaction system. Not expected by all users, but high-value for the specific metrics workload.

| # | Feature | Value Proposition | Complexity | System Precedent | Internal Ref |
|---|---------|------------------|------------|-----------------|-------------|
| D-1 | **RLE merge order representation** | Sorted inputs produce long contiguous runs from the same input split. Representing the merge order as `(split_index, start_row, row_count)` triples enables bulk take/copy operations instead of row-by-row processing. Creates a self-reinforcing feedback loop: each compaction cycle produces longer runs, making the next merge cheaper. | Medium | Husky (explicitly uses this approach; blog confirms "index array" merge order). No equivalent in ClickHouse (inline merge), Iceberg (full rewrite), or Prometheus (chunk-level merge). | ADR-003 Sec 4, Phase 1 "Sorted Merge" |
| D-2 | **Leading-edge compaction prioritization** | Recent time windows accumulate small splits fastest and are queried most. Prioritizing compaction of the leading edge (most recent N windows) ensures the most query-hot data gets compacted first. | Medium | Husky (compactor prioritizes leading edge, auto-scales based on backlog), ClickHouse (SimpleMergeSelector heuristics prefer partitions with many small parts), Mimir (compactor processes smallest time ranges first) | GAP-009 |
| D-3 | **Self-describing Parquet files** | Embedding sort_schema, window metadata, and min/max/regex in Parquet key_value_metadata makes files interpretable without the metastore. Enables debugging, offline analysis, disaster recovery, and future metadata store migration. | Low | Iceberg (partition stats in manifest, but data files lack embedded metadata), Delta Lake (stats in transaction log, not in files), Husky (fragment headers contain metadata). Pomsky's approach of putting metadata in BOTH the file and the metastore is more robust. | ADR-003 Sec 9, Phase 1 "Split Metadata" |
| D-4 | **Parquet column index / offset index enablement** | Enabling page-level min/max statistics at write time is what makes sort order useful for intra-file pruning. Without this, DataFusion cannot skip pages within a sorted file. Most systems don't emit these because they don't use Parquet natively. | Low | Iceberg (Parquet column indexes enabled by default since spec v2), Delta Lake (relies on file-level stats, not page-level). ClickHouse and Husky use custom formats with built-in granule-level indexes. | GAP-004 |
| D-5 | **Compaction-time deduplication (optional)** | During sorted merge, adjacent rows with identical (metric_name, tags, timestamp) can be deduplicated cheaply. Provides eventual consistency for duplicates from client retries or overlapping sources. | Medium | ClickHouse ReplacingMergeTree (dedups by sort key during merge, eventual consistency), Prometheus/Mimir (vertical compaction deduplicates samples with identical timestamps) | GAP-005 |
| D-6 | **Configurable sort schema per index** | Different metrics workloads have different query patterns. Allowing the sort schema to be configured (and changed over time) means the system adapts to workload characteristics. | Low | Husky (sort schema per table/track), Iceberg (sort order per table, can be changed), ClickHouse (ORDER BY per table, immutable after creation) | ADR-002, Phase 1 "Sort Schema" |
| D-7 | **timeseries_id tiebreaker column** | Hash of all tag names/values placed after explicit sort columns and before timestamp. Clusters points from the same source without requiring all tags in the sort schema. Improves compression of value columns through temporal coherence within a source. | Medium | Husky (uses similar concept), no direct equivalent in ClickHouse/Iceberg/Prometheus. | Phase 1 "Timeseries ID" |
| D-8 | **Page-level streaming for non-sort columns** | During merge, process non-sort columns one at a time at page granularity rather than loading full columns. Bounds memory for large columns (high-cardinality string tags, attribute maps). | High | Husky (streams columns through merge order one at a time). ClickHouse and Prometheus merge at granule/chunk level respectively. Iceberg/Delta do full file rewrites. | ADR-003 Sec 4 "Phase 2: Stream columns" |
| D-9 | **Compression validation experiment infrastructure** | The entire compaction project's value proposition rests on sorted data compressing 20-35% better. Building experiment infrastructure to validate this on real metrics data before committing to the full pipeline is a differentiator in engineering rigor. | Low | Husky (validated 25-33% for logs/APM before building). This is unique to Pomsky's context -- most systems built compaction without this explicit validation step. | Phase 1 "Appendix: Critical Analysis" |

---

## Anti-Features

Features to explicitly NOT build. These are tempting but wrong for Pomsky's context.

| # | Anti-Feature | Why Avoid | What to Do Instead |
|---|-------------|-----------|-------------------|
| AF-1 | **Cross-node compaction (in Phase 1)** | ADR-003 explicitly scopes Phase 1 to node-local compaction. Cross-node compaction requires coordination infrastructure that doesn't exist. Building it now adds complexity without proportional benefit (size-tiered compaction provides the critical split-count reduction). | Exclude `node_id` from the compaction scope definition (forward-looking) but enforce node-local merge in the planner. Phase 2 lifts this constraint. |
| AF-2 | **m:n merge (split output across sort-key ranges)** | Producing multiple output splits with non-overlapping key ranges is a Phase 2 feature. It requires sort-key-range tracking, output splitting logic, and changes to query planning. The Phase 1 merge is always m:1 (N inputs -> 1 output). | Simple m:1 merge within each window. Phase 2 introduces range partitioning. |
| AF-3 | **Split-level query pruning** | Using per-split min/max/regex metadata to skip entire splits at query time is Phase 3. The metadata SHOULD be recorded now (to avoid re-compaction later), but the query planner should NOT use it yet. | Record min/max/regex metadata in Phase 1 (TS-9, D-3). Defer query planner changes to Phase 3. |
| AF-4 | **Ingest-time deduplication** | Per-point dedup at ingest requires a bloom filter or dedup index -- a new stateful component that must be consistent across nodes. Too complex for Phase 1, and Husky explicitly does not do ingest-time dedup. | Accept duplicates. Optionally add compaction-time dedup (D-5) as a lower-risk path. |
| AF-5 | **Timeseries-per-row data model** | Storing an array of timestamps/values per timeseries per row requires custom DataFusion operators, complicates merge semantics (intra-row series merge), and couples storage format to query semantics. | Point-per-row with sorted layout and RLE encoding achieves comparable scan performance without the complexity. See Phase 1 "Data Model: Point Per Row". |
| AF-6 | **Custom merge policy from scratch** | Building a metrics-specific merge policy from scratch is premature. The StableLogMergePolicy is proven, and the parameters (target size, fanin) should be tuned via experiments first. | Adapt StableLogMergePolicy with window-aware grouping. Tune parameters experimentally. Replace only if experiments show the policy is fundamentally wrong for metrics. |
| AF-7 | **Affinity-based shard routing** | Using consistent hashing to bias shard selection so same-metric data lands on the same node. This is Phase 1.5 and orthogonal to compaction. Building it alongside compaction doubles the change surface. | Keep current round-robin routing. Phase 1.5 adds affinity after compaction is stable. |
| AF-8 | **Rewriting pre-existing unsorted data** | Backfilling/rewriting data produced before Phase 1 enablement. Adds massive complexity with time-limited value (old data ages out via retention anyway). | Use `compaction_start_time` cutoff. Old data ages out via retention. |
| AF-9 | **PostgreSQL metadata store replacement** | The current PostgreSQL metastore won't scale to extreme split counts, but replacing it is a separate project. Phase 1 should use PostgreSQL and design metadata to be portable. | Use PostgreSQL. Make metadata schema simple and portable. Self-describing Parquet files (D-3) provide a safety net. |
| AF-10 | **Wide-table optimization (multiple metrics per row)** | Storing multiple metric values as separate columns sharing one tag set. Requires fundamental changes to the compactor and data model. Research-phase idea only. | Defer to post-Phase 4 research. |

---

## Feature Dependencies (Phased)

```
Phase 1a: Foundation (must be built first)
  TS-9  (Metadata extensions)
  TS-1  (Time-window partitioning at ingestion)
  TS-2  (Late data acceptance window)
  D-6   (Configurable sort schema) -- already partially exists as hardcoded sort
  D-4   (Column index enablement in Parquet writer)

Phase 1b: Merge Pipeline (depends on 1a)
  TS-3  (Merge planner with scope-aware selection)
  TS-4  (Size-tiered merge policy -- StableLogMergePolicy adaptation)
  TS-6  (Merge split downloader)
  TS-5  (Sorted merge executor)
  TS-7  (Merge uploader)
  TS-8  (Atomic merge publication)
  TS-12 (Schema evolution during merge)
  TS-10 (Pre-existing data transition -- compaction_start_time filter)
  D-1   (RLE merge order -- part of TS-5 implementation)

Phase 1c: Operational Readiness (depends on 1b)
  TS-11 (Basic compaction monitoring)
  D-3   (Self-describing Parquet files -- metadata in both file and DB)

Phase 1d: Optimization (depends on 1b, can be done in parallel with 1c)
  D-2   (Leading-edge compaction prioritization)
  D-7   (timeseries_id tiebreaker column)
  D-8   (Page-level streaming for non-sort columns)
  D-5   (Compaction-time deduplication -- optional)

Phase 1-pre: Validation (should happen BEFORE 1b)
  D-9   (Compression validation experiment)
```

---

## Comparison with Real Systems

### Merge Policy Comparison

| System | Policy | Description | Write Amp | Read Amp | Fit for Pomsky |
|--------|--------|------------|-----------|----------|---------------|
| **RocksDB Leveled** | Leveled compaction | Each level ~10x larger. Merge one file at a time with overlapping files in next level. Minimizes space amplification. | High (10-30x) | Low | Poor -- too much write amp for large Parquet files |
| **RocksDB Universal** | Size-tiered (called "Universal") | Merge all files of similar size together. Minimizes write amplification. | Low (2-5x) | Higher | Good -- closest match to current StableLogMergePolicy |
| **ClickHouse MergeTree** | Partition-scoped merge | SimpleMergeSelector picks similarly-sized parts within a partition. Merge produces a single sorted part. Heuristics consider part count, size ratio, age. | Medium | Low | Good reference -- partition = time window |
| **Husky** | Size-tiered + locality LSM | Size-tiered to a threshold, then locality compaction (LSM-style non-overlapping ranges within levels). | Low base + medium for locality | Low after locality | Aspirational -- size-tiered = Phase 1, locality = Phase 2 |
| **Iceberg** | Bin-pack / Sort / Z-order | Operator-triggered rewrite. Bin-pack combines small files. Sort rewrites with global sort. Z-order for multi-dimensional clustering. | Depends on strategy | Low with good pruning | Different model -- explicit operator action vs. background. Sort strategy is relevant reference. |
| **Delta Lake** | OPTIMIZE (bin-pack + Z-order) | Similar to Iceberg: explicit command to compact. Target 800MB-1GB files. Z-order for multi-column skipping. | Depends on frequency | Low with Z-order | Same as Iceberg -- explicit vs. background. File size targets are useful reference. |
| **Prometheus/Mimir** | Time-block compaction | 2h blocks compacted into 2h, 6h, 24h ranges. Mimir adds split-and-merge for horizontal scalability. | Low (each generation doubles range) | Low (fewer blocks) | Relevant for time-range expansion pattern. Mimir's split-and-merge is Phase 2 reference. |

**Recommendation for Pomsky Phase 1:** Adapt StableLogMergePolicy (closest to RocksDB Universal / ClickHouse SimpleMergeSelector) with window-aware grouping. This is the approach ADR-003 proposes, and the comparison confirms it is the correct starting point.

### Late Data Handling Comparison

| System | Approach | Configurable | Notes |
|--------|----------|-------------|-------|
| **ClickHouse** | Late data goes to existing partition. Merge handles naturally. TTL for old data deletion. | No explicit late window | Works because partitions are coarse (month) |
| **Husky** | Configurable acceptance window per product (1h for metrics, 3h for HSI). Drop beyond window. | Yes | Directly analogous to Pomsky's design |
| **Prometheus** | Out-of-order ingestion window (configurable, default 30m in recent versions). Data beyond window dropped or errored. | Yes | Tighter window than Pomsky's 1h default |
| **Iceberg** | No concept of late data at the table level. Late data just creates new files. Compaction is explicit. | N/A | Different model -- no background compaction |
| **Mimir** | Accepts all data. Vertical compaction merges overlapping blocks. | No explicit window | Handles overlaps at compaction time, not ingestion |

**Recommendation:** Pomsky's approach (drop at ingestion beyond configurable window, handle naturally within window) matches Husky and is the right choice. 1 hour default is appropriate for metrics.

### Monitoring Comparison

| System | Key Compaction Metrics | Dashboard | Alerting |
|--------|----------------------|-----------|----------|
| **ClickHouse** | `system.merges` (active merges), `system.parts` (part count per partition), `MergedRows`, `MergedUncompressedBytes`, merge duration, disk space reserved | Built-in system tables, Grafana dashboards available | Alert on part count > threshold (parts_to_delay_insert) |
| **Mimir** | `cortex_compactor_runs_started/completed/failed_total`, `cortex_compactor_blocks_cleaned_total`, `cortex_compactor_block_cleanup_failures_total`, per-tenant compaction status, compaction duration | Dedicated Grafana dashboards (Compactor + Compactor Resources) | Alert on failed compaction runs, compaction not keeping up |
| **Prometheus** | `prometheus_tsdb_compactions_total`, `prometheus_tsdb_compaction_duration_seconds`, `prometheus_tsdb_blocks_loaded` | Standard Prometheus self-monitoring | Alert on compaction duration, head block count |
| **Husky** | Fragment count per window, compaction backlog, compression ratio, pruning effectiveness (only 3.4% of queries scan real data) | Internal Datadog monitoring | Backlog-based auto-scaling |

**Recommended monitoring for Pomsky Phase 1:**

| Category | Metric | Threshold/Alert | Priority |
|----------|--------|----------------|----------|
| **Health** | Compaction runs started/completed/failed per window | Alert on failed > 0 | P1 |
| **Progress** | Splits per window (pre-compaction vs. post-compaction) | Alert on pre-compaction count growing unboundedly | P1 |
| **Performance** | Merge duration (p50, p99) | Alert on p99 > SLO | P1 |
| **Efficiency** | Write amplification ratio (bytes written by compaction / bytes ingested) | Track, no alert initially | P2 |
| **Compression** | Sorted vs unsorted compression ratio | Track for validation | P1 (existential) |
| **Resources** | Compactor CPU, memory, disk I/O | Alert on saturation | P1 |
| **Query impact** | Query latency before/after compaction rollout | Track for validation | P1 |
| **Late data** | Late data drop rate, age distribution of late-arriving data | Alert on sustained high drop rate | P2 |
| **Backlog** | Number of windows with pending compaction | Alert on backlog growth | P2 |

---

## MVP Recommendation

Prioritize in this order:

1. **D-9: Compression validation experiment** -- Run FIRST, before building anything. Sort existing metrics Parquet files by the proposed schema, compare sizes. If improvement is <10%, the cost-benefit changes. This is the existential experiment. (LOW effort, HIGH value)

2. **Phase 1a: Foundation** (TS-9, TS-1, TS-2, D-6, D-4) -- Metadata extensions, time-window partitioning at ingestion, late data window, configurable sort schema, column index enablement. These are prerequisites for everything else and independently improve query performance (window-level pruning, page-level pruning from column indexes on sorted data).

3. **Phase 1b: Merge Pipeline** (TS-3, TS-4, TS-5, TS-6, TS-7, TS-8, TS-10, TS-12, D-1) -- The core compaction pipeline. This is the largest work item but well-specified by ADR-003. The RLE merge order (D-1) should be built into the sorted merge executor from the start, not retrofitted.

4. **Phase 1c: Operational Readiness** (TS-11, D-3) -- Monitoring and self-describing files. Required before production rollout.

**Defer:** D-2 (leading-edge prioritization), D-5 (compaction-time dedup), D-7 (timeseries_id), D-8 (page-level streaming). These are valuable but not required for Phase 1 to deliver split-count reduction and compression improvement. They can be added incrementally after the core pipeline is stable.

---

## Sources

### Internal Design Documents
- ADR-003: Time-Windowed Sorted Compaction (the primary design specification)
- Phase 1: Sorted Splits for Parquet (detailed implementation design)
- TimeWindowedCompaction.tla (formal TLA+ specification with 10 invariants)
- GAP-001 through GAP-010 (architecture gap analyses)
- compaction-architecture.md (current system description)

### External Systems (HIGH confidence -- well-documented, authoritative sources)
- [ClickHouse MergeTree Architecture](https://deepwiki.com/ClickHouse/ClickHouse/3.1-mergetree-engine-architecture) -- merge selection, part management, monitoring
- [ClickHouse SimpleMergeSelector Discussion](https://github.com/ClickHouse/ClickHouse/issues/16595) -- merge algorithm details
- [Husky: Efficient compaction at Datadog scale](https://www.datadoghq.com/blog/engineering/husky-storage-compaction/) -- size-tiered + locality compaction, sorted merge, 30% query cost reduction
- [Apache Iceberg Compaction](https://www.dremio.com/blog/compaction-in-apache-iceberg-fine-tuning-your-iceberg-tables-data-files/) -- bin-pack, sort, z-order strategies
- [Apache Iceberg Spark Procedures](https://iceberg.apache.org/docs/latest/spark-procedures/) -- rewrite_data_files
- [AWS S3 Sort and Z-Order Compaction for Iceberg](https://aws.amazon.com/blogs/aws/new-improve-apache-iceberg-query-performance-in-amazon-s3-with-sort-and-z-order-compaction/) -- sort-order-aware compaction (June 2025)
- [Apache Hudi Compaction](https://hudi.apache.org/docs/compaction/) -- MOR compaction, log compaction
- [Hudi RFC-48: Log Compaction](https://github.com/apache/hudi/blob/master/rfc/rfc-48/rfc-48.md) -- sorted merge for MOR tables
- [Delta Lake Optimizations](https://docs.delta.io/latest/optimizations-oss.html) -- OPTIMIZE bin-pack, Z-ORDER
- [RocksDB Compaction Wiki](https://github.com/facebook/rocksdb/wiki/Compaction) -- leveled vs universal vs FIFO
- [RocksDB Universal Compaction](https://github.com/facebook/rocksdb/wiki/universal-compaction) -- size-tiered strategy details
- [Prometheus TSDB Compaction](https://ganeshvernekar.com/blog/prometheus-tsdb-compaction-and-retention/) -- block compaction and retention
- [Mimir Split-and-Merge Compactor](https://grafana.com/blog/how-grafana-mimirs-split-and-merge-compactor-enables-scaling-metrics-to-1-billion-active-series/) -- horizontal scalability for compaction
- [Mimir Compactor Dashboard](https://grafana.com/docs/mimir/latest/manage/monitor-grafana-mimir/dashboards/compactor/) -- monitoring best practices
- [LSM Compaction Design Space (VLDB 2021)](https://vldb.org/pvldb/vol14/p2216-sarkar.pdf) -- academic analysis of compaction tradeoffs
