# Phase 1: Sorted Splits for Parquet

**Authors:** gtt@ **Date:** 2026-02-19 **Status:** draft **Scope:** Parquet splits only

## Overview

This document describes Phase 1 of locality-aware compaction for Quickwit, focused on the Parquet-format data in the metrics pipeline. Phase 1 introduces a configurable **sort schema** for Parquet-format indexes and ensures that all Parquet splits \-- whether produced at ingestion or by compaction \-- contain rows in sorted order according to that schema. It also introduces **time windowing**: all data is partitioned into fixed-duration, epoch-aligned time windows (default 15 minutes), and compaction is scoped to individual windows so that data is never merged across window boundaries.

This is directly analogous to Phase 1 of the [Husky locality project](https://www.datadoghq.com/blog/engineering/husky-storage-compaction/), where sorting individual fragment files by a subset of columns achieved 25-33% compression improvement and measurable reductions in query latency and network bandwidth.

Phase 1 does not change the compaction planning algorithm or introduce cross-node coordination. It modifies only how individual splits are written, so that the data within each split is physically organized in an order aligned with the most common query predicates. Compaction continues to use size-tiered merging (m:1), but the merge process is modified to produce sorted output from sorted inputs via k-way merge.

### Goals

Phase 1 succeeds if: (a) the Parquet pipeline has size-tiered compaction within time windows, reducing split count per window over time; (b) all newly-written splits contain rows sorted according to the configured sort schema; (c) sort order is preserved through compaction merges; and (d) sorted splits achieve measurably better compression than unsorted splits (target: 20%+ reduction in Parquet file size).

## Background and Motivation

### The Data Scattering Problem

Metrics data arrives at Quickwit through load-balanced routing: an external load balancer distributes requests across nodes, each node's `IngestRouter` picks a shard via round-robin, and the indexing pipeline produces splits stamped with the producing QW indexing node's identity. This means that points for any given time series \-- identified by metric name, tags, and timestamp \-- are scattered across whichever nodes happened to receive them.

Within each split, rows are stored in **ingestion order**: the order in which they arrived at the indexing pipeline. There is no relationship between the physical layout of rows and the logical structure of the data. A query for a specific metric name must scan all rows in every split in the time range.

### Why Sorting Helps

Sorting rows within each split by a schema aligned with common query predicates produces two immediate benefits:

1. **Compression improvement.** Columnar formats like Parquet compress data by encoding runs of similar values. When rows are sorted by metric name and tags, the columns for those fields contain long runs of identical or similar values. This benefits multiple encoding layers: Parquet's native RLE and dictionary encoding produce compact representations for columns with repeated values, and general-purpose compression (ZSTD) compresses the encoded output further. In Husky Phase 1, this yielded \~33% size reduction for APM data and \~25% for Logs data. Analysis of single-file locality confirms that datacenter, service, and host columns provide strong locality benefits.
     
2. **Query efficiency.** Parquet files typically contain a single row group, so row-group-level statistics provide no intra-file pruning. However, Parquet's **column index** (format v2) stores min/max statistics per page within each column chunk. When data is sorted, pages within each column naturally have non-overlapping value ranges for the sort columns. DataFusion supports page index pruning, allowing it to skip pages that cannot match a query predicate \-- for example, jumping directly to the pages containing `metric_name = "cpu.usage"` and skipping the rest of the file.

These benefits are achieved without any change to the compaction planning algorithm, query routing, or cluster coordination. They require only that the split writing path sorts rows before writing and that the merge path preserves sort order.

### Prior Art: Husky Phase 1

In Husky, Phase 1 was implemented as follows:

- A **sort schema** was defined per table/track, specifying a short list of columns to sort by, with an optional LSM cutoff and version suffix. Example for logs: `service__s|status__s|tag.env__s|timestamp|tiebreaker__i/V2`  
- **Writers** sort rows within each fragment file by the sort schema before writing to object storage.  
- **Compactors** perform sorted m:1 merges, reading the sort columns from each input fragment first, determining the global sort order, then streaming all columns through the merge in that order.  
- **Sort columns are written first** in the output fragment, followed by remaining columns in lexicographic order by name. This is necessary in Husky's custom columnar format because columns are laid out sequentially with headers, so the sort columns must be physically first to avoid seeking past other columns during merge reads.  
- **Null handling:** nulls sort after non-null values.

The result was compression improvements of 25-33%, reduced compactor CPU (ZSTD compresses sorted data more easily), reduced network bandwidth, and reduced query latency at leaf reader nodes.

## Data Model: Point Per Row

Each row in a Parquet split represents a single data point: one metric value at one timestamp for one timeseries. This is in contrast to a "timeseries per row" model where each row would contain an array of timestamps and values for an entire series.

Point-per-row is the right starting point for several reasons:

- **Simpler compaction semantics.** Sorted k-way merge operates directly on rows. With timeseries-per-row, merging requires both row-level merge (interleaving rows from different splits) *and* intra-row series merge (combining the timestamp/value arrays of the same timeseries across splits). Point-per-row avoids this second level of merge entirely.
- **No last-write-wins (LWW).** We explicitly do not support LWW semantics, where a later write for the same timeseries and timestamp overwrites an earlier one. Without LWW, there is no need for sticky routing or series-level deduplication during compaction. This is a deliberate simplification that avoids the reliability challenges of sticky routing (single-partition overload, constant shuffles on rebalancing) that other systems have encountered.
- **No timeseries-level interpolation.** Interpolation across points in a timeseries is not performed at the storage layer. If needed in the future, it will operate at query time. This may be slower than storage-level interpolation but avoids coupling the storage format to query semantics.
- **Performance equivalence with good encoding.** With sorted data, columnar encodings like RLE (run-length encoding) and dictionary encoding produce long runs of repeated values in the sort columns -- the same runs that timeseries-per-row would capture by grouping values into arrays. When these encodings are preserved through query execution, point-per-row achieves comparable scan performance to timeseries-per-row without the implementation complexity.
- **Generic DataFusion improvements over custom code.** Timeseries-per-row requires significant custom DataFusion operator support (nested array types, custom aggregation kernels). Point-per-row uses standard columnar operations, allowing us to contribute generic improvements to DataFusion rather than maintaining timeseries-specific extensions.

**RLE and dictionary encoding in DataFusion.** Currently, RLE and dictionary encoding are lost relatively quickly through generic DataFusion operators -- decoded to plain arrays early in the query pipeline. As DataFusion grows operator-level support for dictionary/RLE encodings, the performance benefits of sorted point-per-row data will increase, since longer runs in sorted columns translate directly to better RLE compression ratios that are maintained through query execution.

## Sort Schema Definition

### Configuration

A sort schema is defined as part of the index configuration. It specifies an ordered list of columns that determine the sort order of rows within each split, and optionally additional columns for which metadata (min/max/regex) is emitted but which do not participate in sorting.

Using the same shorthand as Husky, a sort schema uses the following format:

```
[schema_name=]column[+/-]|...[&column[+/-]|...]/V2
```

The components are:

- **Schema name** (optional): A name for the schema, followed by `=`. When present, it labels the schema for identification. Example: `metrics_default=metric_name|...`  
- **Sort columns**: Pipe-delimited column names. These columns define the sort order. Each column may have a `+` (ascending) or `-` (descending) suffix. If omitted, the default direction is ascending, except for `timestamp` which defaults to descending.  
- **LSM cutoff** (`&`): Separates sort columns from metadata-only columns. Columns listed after `&` are **not** used for sort ordering, but min/max/regex metadata is still emitted for them in the split metadata. This allows future query pruning on dimensions that don't participate in the physical sort order.  
- **Version suffix** (`/V2`): Indicates version 2 of the sort schema format (the current version).

Each column in the schema has:

- **Name:** The column name as it appears in the Parquet schema.  
- **Sort direction:** Ascending (`+`, default for most columns) or descending (`-`, default for `timestamp`). Indicated by a suffix on the column name: `timestamp-` means sort timestamp descending (redundant with the default, but explicit). `timestamp+` would override the default to sort ascending.  
- **Type:** Inferred from the Parquet schema. Supported types:  
  - **String/binary:** Sorted lexicographically by byte value.  
  - **Integer types** (i8, i16, i32, i64, u8, u16, u32, u64): Sorted numerically.  
  - **Float types** (f32, f64): Sorted numerically, with NaN handling matching IEEE 754 total order (NaN sorts after all other values).  
- **Null handling:** Null values always sort **after** all non-null values, regardless of sort direction (`nulls_first: false`). This enables nulls to be implicit in the sorted_series key encoding — absent columns are simply omitted from the key, and their keys naturally sort after keys that include those columns.

### Timeseries ID (Optional Locality Tiebreaker)

The sort schema typically includes only a few high-value tag columns (e.g., `host`, `env`). When the explicit sort columns are not granular enough to distinguish individual point sources, points from different sources that share the same sort column values will be interleaved in the sorted output. For example, if the sort schema is `metric_name|env` and a single environment has hundreds of hosts, points from all those hosts are interleaved within each `(metric_name, env)` group.

To improve locality in this case, the sort schema may optionally include a **`timeseries_id`** column: a hash of all tag names and values, placed after the explicit sort columns and before `timestamp`. This acts as a **tiebreaker** -- within each group defined by the explicit sort columns, it further clusters points that come from the same combination of tags before ordering by time. This is purely a physical layout optimization for compression and scan efficiency. It is not a semantic concept, not part of the data model, and nothing in the query path or correctness of the system depends on it.

**`timeseries_id` is optional.** If the explicit sort columns are already granular enough to distinguish individual point sources (e.g., the schema already includes `host` or `container`), `timeseries_id` adds little value and can be omitted from the sort schema. It is most useful when the sort schema is coarse (few columns, high-cardinality tags not included) and the operator wants better intra-group locality without adding more explicit sort columns. It can be added or removed from the sort schema at any time -- this is a schema change handled by the normal transition mechanism (new splits use the new schema, old splits age out via retention).

When used, the hash function should be deterministic, fast, and produce good distribution. A suitable choice is xxHash64 or SipHash over the canonicalized (sorted by key name) tag key/value pairs. The exact hash function is an implementation detail; what matters is that the same set of tags always produces the same `timeseries_id`.

**Limitations.** `timeseries_id` provides best-effort grouping, not a guarantee. Hash collisions (extremely unlikely with 64-bit hashes) would interleave two distinct point sources, but this affects only physical layout, not correctness. More practically, if a tag value flaps (e.g., a tag alternates between `NA` and an actual value due to intermittent enrichment), the hash changes and points from what a user would consider "the same source" end up with different `timeseries_id` values. This is inherent to any hash-of-all-tags approach and is acceptable because it only degrades locality, never correctness. If tag flapping is prevalent in a workload, omitting `timeseries_id` and relying on the explicit sort columns alone may be preferable.

### Schema Requirements

- The sort columns (before `&`) should be a small subset of columns \-- typically 3-5 columns that correspond to the most common query predicates, optionally followed by `timeseries_id`, followed by `timestamp`.
- Columns referenced in the sort schema do not need to exist in the Parquet schema of every split. If a sort column is missing from a particular split (e.g., because the data predates a schema addition, or because a column is being introduced incrementally), all rows in that split are treated as having null values for that column. This is not an error condition.
- If used, `timeseries_id` is a synthetic column computed by the indexer, not present in the incoming data. It should appear immediately before `timestamp` in the sort columns. It is optional and may be omitted if the explicit sort columns provide sufficient granularity.
- The schema string must end with `/V2`.
- Metadata-only columns (after `&`) are optional. They do not affect sort order but have min/max/regex metadata emitted for future query pruning.

### Example

For a metrics index with data points identified by metric name, host tag, and environment tag, with `service` tracked as a metadata-only column:

```
metric_name|host|env|timeseries_id|timestamp&service/V2
```

This sorts rows first by `metric_name` (ascending), then `host` (ascending), then `env` (ascending), then `timeseries_id` (ascending, clustering points from the same tag combination), then `timestamp` (descending, by default). The `service` column after `&` does not participate in sorting, but min/max/regex metadata is emitted for it to enable future query pruning.

Within a split, all points for the source `cpu.usage{host="host-01", env="prod", region="us-east-1", instance="i-abc123"}` will be physically contiguous and ordered by timestamp, even though `region` and `instance` are not explicit sort columns -- the `timeseries_id` hash groups them together.

A schema without `timeseries_id`, relying on the explicit sort columns for all grouping:

```
metric_name|host|env|timestamp&service/V2
```

Here, points are grouped by `(metric_name, host, env)` and then ordered by timestamp. Points from different sources that share the same `(metric_name, host, env)` but differ in other tags (e.g., `instance`) will be interleaved within each group. This is acceptable when `host` provides sufficient granularity.

A minimal schema without metadata-only columns, a schema name, or `timeseries_id`:

```
metric_name|timestamp/V2
```

### Storage

The sort schema is stored in two places:

**PostgreSQL (`MetricsSplitMetadata`)**. The schema string is stored alongside other split metadata so that:

1. The compaction merge process knows the sort order of input splits.  
2. Future phases can use the schema for query pruning and locality-aware compaction planning.  
3. Schema changes can be detected \-- splits with different sort schemas are not merged together without re-sorting.

**Parquet file metadata**. The schema is also embedded in the Parquet file itself, making each file self-describing:

1. **`key_value_metadata`**: The full sort schema string is stored as a key-value pair in the Parquet file-level metadata (key: `sort_schema`, value: the schema string, e.g., `metric_name|host|env|timeseries_id|timestamp&service/V2`). This preserves the complete schema including LSM cutoff, metadata-only columns, and version suffix.  
     
2. **`sorting_columns`**: The sort columns (those before `&`) are also declared using Parquet's native `sorting_columns` field in the file metadata. Each entry specifies the column index, ascending/descending, and nulls-first/nulls-last. This allows Parquet-native tooling and DataFusion to leverage the sort order without understanding our custom schema format.

## Time Windowing

### Concept

All compaction in the Parquet pipeline is scoped to **time windows**: fixed-duration, non-overlapping intervals of wall-clock time aligned to the Unix epoch. Splits are assigned to a time window based on the timestamps of the data they contain, and compaction only merges splits within the same window. Data is never merged across window boundaries.

This is directly analogous to Husky's time bucketing, where each fragment belongs to a single time window and queries use time as a primary filter to restrict the set of fragments that must be examined. Time windowing provides a natural partitioning dimension that:

1. **Bounds compaction scope.** Each window is an independent compaction unit. The compactor processes windows independently, and the total amount of data eligible for a single merge operation is bounded by the window duration and ingestion rate.  
2. **Aligns with query patterns.** Observability queries always include a time range predicate. When splits are organized by time window, the query engine can immediately discard all windows outside the query range without examining individual split metadata.  
3. **Enables retention.** Dropping old data becomes a window-level operation: all splits in windows older than the retention period can be deleted as a batch, without needing to inspect individual rows. The retention period is user-configured per index (e.g., 15 days, 90 days) and is not specified by this design -- it is an existing Quickwit capability that time windowing makes more efficient.  
4. **Limits compaction write amplification.** Because windows are independent, data is only rewritten within its window. Old, fully-compacted windows are never disturbed by new data arriving in newer windows.

### Window Configuration

| Parameter | Default | Description |
| :---- | :---- | :---- |
| `window_duration` | 15 minutes | The duration of each time window. Must evenly divide one hour (valid values: 1m, 2m, 3m, 4m, 5m, 6m, 10m, 12m, 15m, 20m, 30m, 60m). |
| `compaction_start_time` | (required) | Unix timestamp (seconds). Only data in time windows at or after this time is eligible for sorted compaction. Data before this time is left as-is and expires via retention. Should be set to the time Phase 1 is enabled (or the start of the next window boundary after enablement). |
| `late_data_acceptance_window` | 1 hour | Maximum age of a data point (wall-clock time minus point timestamp) accepted at ingestion. Points older than this are dropped. Bounds the window of time during which late data can disturb already-compacted windows. Should be set based on product lateness guarantees (e.g., 1h for metrics, 3h for HSI-style use cases). |

Windows are aligned to the Unix epoch. A 15-minute window duration produces windows at `[00:00, 00:15)`, `[00:15, 00:30)`, `[00:30, 00:45)`, `[00:45, 01:00)`, and so on. The window containing a given Unix timestamp `t` is computed as:

```
window_start = t - (t % window_duration_seconds)
window_end   = window_start + window_duration_seconds
```

Each window is identified by its `window_start` timestamp (seconds since Unix epoch).

### Split-to-Window Assignment

At ingestion time, a split is assigned to a time window based on the timestamps of the rows it contains. Because the indexing pipeline accumulates rows over a commit interval before flushing a split, a single split may contain rows spanning more than one time window.

When a split contains rows from multiple windows, the split is **partitioned by window** before writing: rows are grouped by their window assignment, and a separate Parquet file is written for each window. Each output split belongs to exactly one time window. This ensures the invariant that every split in object storage is associated with exactly one window.

The window assignment uses the same timestamp column referenced in the sort schema (typically `timestamp`). If a row has a null timestamp, it is assigned to a designated overflow window (window\_start \= 0), which is compacted separately.

### Compaction Scope

The full compaction scope for splits becomes:

```
(index_uid, source_id, partition_id, doc_mapping_uid, sort_schema, window_start)
```

The components are:

- **`index_uid`**: Unique identifier for the Quickwit index (e.g., a metrics index). Each index has its own configuration, schema, and retention policy.
- **`source_id`**: Identifies the data source feeding the index (e.g., a Kafka topic or push API endpoint). Different sources may produce data with different characteristics or schemas.
- **`partition_id`**: A partition within the source (e.g., a Kafka partition). Splits from different partitions are kept separate to preserve ordering guarantees within a partition.
- **`doc_mapping_uid`**: A unique identifier for the document mapping (schema) version. When the index schema changes (columns added/removed/retyped), a new `doc_mapping_uid` is assigned. This prevents merging splits with incompatible schemas.
- **`sort_schema`**: The sort schema string (as defined above). Prevents merging splits sorted with different schemas.
- **`window_start`**: The time window start timestamp. Prevents merging data from different time windows.

Only splits sharing all components of this scope key may be merged together.

Note: `node_id` is intentionally excluded from the compaction scope. In Phase 1, this is a forward-looking design choice \-- initially, each node compacts its own splits (the current behavior), but the scope definition does not require it. Phase 2 lifts the node constraint to enable cross-node compaction.

### Late-Arriving Data

Data may arrive late \-- a data point with a timestamp in a past window may be ingested after that window has already been compacted.

**Late data acceptance window.** Points with timestamps older than a configurable maximum age are **dropped at ingestion time** rather than accepted into the storage layer. This bounds the window of time during which late data can disturb already-compacted windows. The acceptance window should be set based on the product's lateness guarantees (e.g., 1-3 hours after the point's timestamp is typical for metrics systems). Without this cutoff, arbitrarily late data -- driven by customer behavior such as delayed batch uploads or misconfigured clocks -- can trigger expensive re-merges of fully-compacted windows indefinitely.

Within the acceptance window, late-arriving data is handled naturally:

- The late data is written to a new split assigned to the historical window (based on timestamp, not ingestion time).
- The next compaction cycle for that window picks up the new split and merges it with existing compacted splits.
- There is no special handling required; the time window simply gains additional splits that are merged in the normal course.

For windows that have already been fully compacted into a single large split, a late-arriving small split triggers a merge of the large existing split with the small new one. The late data acceptance window bounds how far back this can happen, keeping the number of affected windows small and predictable.

## Sorting at Ingestion

### Current Metrics Ingestion Pipeline

```
Source -> MetricsDocProcessor -> MetricsIndexer -> MetricsUploader -> MetricsPublisher
```

The `MetricsIndexer` accumulates `RecordBatch` batches and writes them as Parquet splits. The current batching thresholds are 128 MiB of in-memory data or 1M rows, whichever is reached first. Due to the high compression ratio of metrics data, this produces very small Parquet files -- currently ~600 KiB on disk. These thresholds will likely need to be revisited to produce larger ingestion-time splits and reduce the split count that compaction must handle. The indexer already sorts rows within each batch using `lexsort_to_indices` on a fixed set of columns (`MetricName`, `TagService`, `TagEnv`, `TagDatacenter`, `TagRegion`, `TagHost`, `TimestampSecs`). Phase 1 makes this sort schema configurable, adds time window partitioning, and optionally adds the `timeseries_id` column (if present in the sort schema).

**Location:** `quickwit/quickwit-indexing/src/actors/indexing_pipeline.rs:600-728`

### Modified Pipeline

The `MetricsIndexer` (or, if more appropriate, the split writer at `quickwit/quickwit-metrics-engine/src/storage/split_writer.rs`) is modified to sort the accumulated `RecordBatch` data by the sort schema before writing the Parquet file.

The windowing, optional timeseries ID computation, and sorting happen in-memory before the Parquet writer begins. The steps are:

1. **Accumulate rows** into one or more Arrow `RecordBatch` arrays as today.
2. **Compute timeseries ID (if configured).** If the sort schema includes `timeseries_id`, compute it for each row by hashing the canonicalized (sorted by key name) set of all tag names and values \-- both explicit tag columns (`tag_service`, `tag_env`, `tag_host`, etc.) and dynamic attributes. Add `timeseries_id` as a new column in the `RecordBatch`. This column persists through compaction and does not need to be recomputed during merges. If the sort schema does not include `timeseries_id`, this step is skipped.  
3. **Partition by time window.** Group rows by their time window assignment based on the timestamp column. Each group contains only rows whose timestamps fall within a single window `[window_start, window_start + window_duration_seconds)`. If the accumulated batch spans multiple windows (common when the commit interval straddles a window boundary), separate groups are produced for each window.  
4. **For each window group:**  
   1. **Extract sort columns** from the group's rows. These are the columns named in the sort schema (including `timeseries_id` if configured).  
   2. **Compute sort indices.** Using Arrow's `lexsort_to_indices` (or equivalent), compute a permutation array that represents the sorted order of all rows across the sort columns, respecting the direction and null ordering specified in the schema.  
   3. **Apply permutation.** Reorder all columns (not just sort columns) according to the computed permutation using Arrow's `take` kernel.  
   4. **Write Parquet file.** Column ordering within the Parquet file does not matter \-- unlike Husky's sequential columnar format, Parquet stores column chunk offsets in the footer and the reader can seek directly to any column. Columns may be written in any order. The Parquet writer must be configured to produce the **column index** (page-level min/max statistics) and the **offset index** (page byte offsets and row counts). These are opt-in features of Parquet format v2 that must be explicitly enabled at write time; without them, DataFusion cannot perform page-level predicate pushdown on the sorted data.
   5. **Record metadata.** The split's metadata records the sort schema, `window_start`, and `window_duration_secs`, so that merges and queries can rely on the sort order and window assignment.

In the common case where all accumulated rows fall within a single 15-minute window, this produces one split \-- the same as today. Only when a commit straddles a window boundary are multiple splits produced for a single commit.

### Cost

Sorting is O(n log n) in the number of rows per split. For typical metrics splits (100K-500K rows), this is inexpensive relative to the cost of Parquet encoding, compression, and upload. In Husky Phase 1, the sorting overhead was approximately 2% additional CPU at the compactor, but this was more than offset by reduced ZSTD compression cost on the better-organized data, resulting in a net CPU reduction.

## Sorted Merge at Compaction

### Current State

The metrics pipeline currently has **no compaction**. Splits accumulate without merging, relying on DataFusion to query many small Parquet files and on time-based retention to eventually remove old data.

### Introducing Basic Compaction with Sorted Merge

Phase 1 introduces size-tiered compaction for the metrics pipeline, producing sorted output. This is the same basic approach as the existing logs/traces `StableLogMergePolicy` adapted for Parquet splits, with the addition that merges maintain sort order.

The merge process for combining N sorted input splits into one or more sorted output splits:

1. **Read sort columns** from each input split. Only the columns named in the sort schema need to be read initially. Parquet's footer-based format allows seeking directly to any column regardless of its physical position in the file.  
     
2. **Compute global sort order.** Perform a k-way merge across the sort columns of all input splits. This produces a merge order indicating how rows from each input should appear in the output. This is the same approach Husky uses: the sort order is determined first and stored in an array, then columns are streamed through the merge.

   The k-way merge uses a min-heap (priority queue) with one entry per input split, comparing rows using the sort schema's comparison rules (lexicographic for strings, numeric for numbers, nulls-last for ascending, nulls-first for descending). For N input splits, each comparison is O(k) where k is the number of sort columns, and advancing through all rows is O(R log N) where R is the total row count.

   The merge order is represented as a run-length encoded sequence of `(split_index, start_row, row_count)` triples rather than individual `(split_index, row_index)` pairs. Because input splits are sorted, the k-way merge naturally produces long contiguous runs from the same input \-- particularly as locality compaction matures and inputs contain increasingly well-sorted data. This representation compresses the merge order significantly and enables bulk operations (bulk `take`, bulk copy) when streaming columns through the merge, rather than processing rows individually.
     
3. **Stream columns through the merge.** Once the global sort order is determined, each column is read from the input splits and written to the output in the sorted order. Columns are processed one at a time (or in small groups for memory efficiency), reading from all input splits and writing to the output according to the sort order array. This keeps memory usage proportional to the number of input splits times the size of one column's data, rather than requiring all data in memory simultaneously.  
     
4. **Emit split metadata.** The output split's metadata records the sort schema, `window_start`, `window_duration_secs`, and min/max/regex values for all columns in the schema (both sort and metadata-only).

### Column Set Differences Across Inputs

The Parquet reader always reads the file footer first \-- the footer contains the full schema and row group metadata, and is the entry point for reading any Parquet file. Discovering each input split's column set is therefore inherent to opening the file, not an extra step.

Input splits may not have identical column sets. Schema evolution (adding or removing columns over time) means that splits from different time periods may have different columns. The merge handles this as follows:

- **Sort columns.** If a sort column is missing from an input split, all rows from that split are treated as having null values for that column. Nulls always sort after non-null values (`nulls_first: false`), regardless of direction. The k-way merge handles this naturally.  
    
- **Non-sort columns.** The merge computes the **union** of all column names across all input splits. The output split contains every column that appears in at least one input. When streaming a column through the merge, rows originating from inputs that lack that column are filled with nulls. The output Parquet schema uses the type from whichever input(s) contain the column; if multiple inputs have the same column name with different types, the merge fails with an error (this indicates a schema evolution conflict that must be resolved at the index configuration level).

### Compaction Scope Mismatches

Splits with different sort schemas must not be merged together. The merge planner groups splits by the full compaction scope key \-- `(index_uid, source_id, partition_id, doc_mapping_uid, sort_schema, window_start)` \-- and only merges splits within the same group. The `window_start` constraint ensures that data from different time windows is never combined, even if all other scope components match.

### Pre-existing Unsorted Data

Splits produced before Phase 1 is enabled have no sort schema and no window assignment. These splits are **not compacted** \-- they remain as-is until they expire via retention. There is no attempt to sort or merge pre-existing data.

A configurable cutoff time (`compaction_start_time`) defines the boundary: only splits whose `window_start` is at or after this time are eligible for compaction. Splits with no window assignment (pre-Phase-1) or with `window_start` before the cutoff are excluded from compaction planning entirely.

This avoids the complexity of merging sorted and unsorted inputs. The transition is clean: once Phase 1 is enabled with a cutoff time, all new data is windowed and sorted from that point forward. Old data ages out via retention without ever being rewritten.

Data that arrives after Phase 1 is enabled but has timestamps before `compaction_start_time` is still written as a sorted, windowed split (the indexer always applies windowing and sorting once Phase 1 is active). However, these splits are not eligible for compaction and will age out via retention alongside pre-existing unsorted splits.

### Comparison with Husky

This is the same approach used by Husky's compactor:

- Sort columns are read first to determine merge order.  
- An index array captures which rows of each input go to which positions in the output.  
- Columns are then streamed through the merge one at a time.

The differences from Husky are: the storage format (Parquet vs. Husky's custom columnar format), the merge planning algorithm (Quickwit's StableLogMergePolicy adapted for metrics vs. Husky's size-tiered \+ LSM composite planner), and the absence of column ordering constraints (Parquet's footer-based layout makes physical column order irrelevant, unlike Husky's sequential format where sort columns must be written first). Phase 1 does not change the merge planning algorithm \-- it only changes how the merge *executes* (sorted output instead of arbitrary order).

### Compaction Policy

Phase 1 uses Quickwit's existing compaction scheduling and the `StableLogMergePolicy` adapted for Parquet splits. The compactor runs on the same schedule and with the same triggering logic as for the logs/traces Tantivy pipeline. Within each time window, the merge policy uses the same maturity/age constraints as `StableLogMergePolicy` to determine when splits are eligible for merging.

The key parameters that need to be determined experimentally are:

- **Target split size after compaction.** How large should the output of a merge be? This determines when the compactor stops merging within a window. Too small and we still have many splits per window; too large and individual merges are expensive.
- **Merge fanin.** How many input splits per merge operation? Higher fanin reduces total write amplification but increases per-merge memory and CPU cost.
- **Interaction with window size.** For a 15-minute window at a given ingestion rate, how many splits accumulate before compaction, and what is the steady-state split count after compaction converges?

These parameters should be determined via experiments on representative metrics workloads before finalizing the merge policy configuration. Suggested experiments:

1. **Baseline measurement.** For a representative metrics index, measure the number of splits produced per 15-minute window, the size of each split, and the total data volume per window.
2. **Merge fanin sweep.** For a fixed target split size, vary the merge fanin (e.g., 4, 8, 16 input splits) and measure merge duration, peak memory usage, and write amplification.
3. **Target size sweep.** For a fixed fanin, vary the target output split size (e.g., 64MB, 128MB, 256MB, 512MB) and measure steady-state split count per window, query latency, and compression ratio.
4. **Compression improvement.** Compare sorted vs. unsorted Parquet file sizes for the same data to validate the expected 20-35% compression improvement.

In the future, the compaction planner may benefit from a hinting mechanism similar to Husky's, where the system can signal that a particular window needs compaction (e.g., due to late-arriving data or a schema change). This would replace the current polling-based approach with event-driven compaction for specific windows.

---

## Split Metadata Changes

The following fields are added to `MetricsSplitMetadata`:

| Field | Type | Description |
| :---- | :---- | :---- |
| `window_start` | `i64` | The Unix timestamp (seconds) of the start of the time window this split belongs to. Computed as `timestamp - (timestamp % window_duration_seconds)`. All rows in the split have timestamps within `[window_start, window_start + window_duration_seconds)`. |
| `window_duration_secs` | `u32` | The time window duration in seconds that was in effect when this split was produced. Stored per-split to detect configuration changes. |
| `sort_schema` | `String` | The full sort schema string including version suffix (e.g., `metric_name|host|env|timeseries_id|timestamp&service/V2`). Empty string if the split was produced before Phase 1\. Parsed internally in Rust code, but stored and compared as a string. |
| `schema_column_min_values` | `Vec<SortColumnValue>` | The minimum value of each column in the schema (both sort and metadata-only columns, in order of appearance in the schema string). |
| `schema_column_max_values` | `Vec<SortColumnValue>` | The maximum value of each column in the schema, in the same positional order. |
| `schema_column_regexes` | `Vec<String>` | A regex pattern for each column in the schema that matches any value present in this split. The computation method follows the existing Husky implementation. |

The `SortColumnValue` type is a tagged union supporting string, i64, u64, f64, and null. All three vectors are positional: element 0 corresponds to the first column in the sort schema string, element 1 to the second, and so on, covering both sort columns and metadata-only columns (those after `&`).

These metadata fields are recorded in **both** PostgreSQL and the Parquet file itself:

**PostgreSQL (`MetricsSplitMetadata`)**: All fields above are stored in PostgreSQL alongside the existing split metadata. They are populated at split publication time \-- both for initial ingestion splits and for merge output splits. PostgreSQL storage enables split-level query pruning without reading any Parquet data.

**Parquet `key_value_metadata`**: The min/max/regex values are also embedded in the Parquet file's `key_value_metadata`, making each file self-describing. The following keys are written:

| Key | Value |
| :---- | :---- |
| `sort_schema` | The full sort schema string (as described in [Storage](#storage) above). |
| `schema_column_min_values` | JSON-serialized array of min values, positional by schema column order. |
| `schema_column_max_values` | JSON-serialized array of max values, positional by schema column order. |
| `schema_column_regexes` | JSON-serialized array of regex strings, positional by schema column order. |
| `window_start` | The window start timestamp as a decimal string. |
| `window_duration_secs` | The window duration in seconds as a decimal string. |

Storing metadata in the Parquet file ensures that files remain interpretable without access to the metastore \-- useful for debugging, offline analysis, and disaster recovery. The PostgreSQL copy is the authoritative source for query planning and compaction.

Note: Parquet also stores min/max statistics natively \-- per row group in the column chunk metadata, and per page in the column index (format v2). Since metrics splits are typically single-row-group files, the per-page column index is the relevant mechanism for intra-file pruning. The split-level min/max/regex metadata described here enables coarser-grained pruning across splits \-- skipping entire splits at query planning time \-- which will be leveraged in future phases.

**Note on window\_duration\_secs:** If the configured window duration changes, splits produced under the old duration have a different `window_duration_secs` value. The merge planner treats splits with different window durations as incompatible for merging (they have different `window_start` alignment). Old-duration splits age out via retention. No rewrite is needed.

## Expected Benefits

Based on Husky Phase 1 results:

### Compression

- **Estimated 20-35% reduction in Parquet file size** for metrics data when sorted by metric name \+ tags \+ timestamp. Metrics data has high tag-value repetition (many points for the same metric/host/env combination), which sorted columnar layout compresses very efficiently.  
- The actual improvement depends on the cardinality of the sort columns and the distribution of values. Metrics data, with its regular time-series structure, is likely to see compression gains at the high end of this range.

### CPU

- Sorting adds approximately 2% CPU overhead at the indexer/compactor.  
- This is expected to be offset by reduced compression cost (ZSTD/Snappy work less on better-organized data) and smaller output files requiring less S3 upload bandwidth.  
- Net CPU effect: approximately neutral or slightly positive (net reduction), as seen in Husky.

### Query Latency

- Since metrics splits are typically single-row-group files, intra-file pruning relies on Parquet's page-level column index rather than row-group statistics. When data is sorted, pages within each column have non-overlapping value ranges, and DataFusion's page index pruning can skip pages that don't match query predicates on sort columns.  
- For queries with predicates on `metric_name` or other leading sort columns, this reduces the volume of data scanned per split.  
- This benefit increases in subsequent phases when split-level min/max/regex metadata enables pruning entire splits at query planning time.

### Storage Cost

- Direct cost reduction proportional to compression improvement.  
- For metrics workloads with long retention, storage cost dominates, so a 25-30% compression improvement translates to a comparable cost reduction.

## Operational Considerations

### Rollout

- **Decoupled from queries.** Sorted splits are fully compatible with the existing query engine. DataFusion can read Parquet files regardless of row order. The sort order is a performance optimization, not a correctness requirement.  
- **Clean cutoff.** The `compaction_start_time` parameter defines a hard boundary. Data before this time is never compacted or rewritten \-- it remains queryable as-is and expires via retention. Data from this time forward is windowed, sorted, and compacted. There is no mixed sorted/unsorted merge path.  
- **Sort schema changes.** If the sort schema is changed, new splits use the new schema. The merge planner prevents merging splits with different schemas. Old-schema splits age out via retention. No rewrite or backfill is needed.  
- **Window duration changes.** If the window duration is changed, new splits use the new duration and alignment. Splits with different `window_duration_secs` are not merged together. Old-duration splits age out via retention.

### Monitoring

Key metrics to track:

| Metric | What It Measures | Threshold |
| :---- | :---- | :---- |
| `split_size_bytes` (before/after) | Compression improvement | Expect 20-35% reduction |
| `indexer_cpu_usage` | CPU cost of sorting | Expect \< 5% increase |
| `compaction_cpu_usage` | CPU cost of sorted merge | Expect neutral or decrease |
| `compaction_duration` | Time to complete a merge | Should not increase significantly |
| `parquet_pages_scanned` | Query-time page index pruning | Expect reduction for predicate queries on sort columns |

### Scale Considerations

The split counts at high ingestion rates are significant. As a concrete example:

- At 10 GiB/s ingestion with 10 MiB splits (the current batch threshold), the system produces ~1,024 splits per second.
- Over a 15-minute window, this accumulates up to ~921,600 splits before compaction.
- After compaction to ~1 GiB files with a ~10x compression ratio (typical for Husky), a 15-minute window still contains ~4,500 Parquet files.

This has two implications:

**Variable window duration.** At the highest scale, 15-minute windows may be too coarse. Shorter windows (down to 1 minute, the minimum supported) reduce the number of splits per window and bound the compaction working set. The `window_duration` parameter is configurable precisely for this reason. Operators should tune it based on ingestion rate: higher throughput warrants shorter windows.

**Metadata scalability.** The current design stores split metadata in PostgreSQL. At ~921K pre-compaction splits per window, or ~4,500 post-compaction splits per window across many windows and indexes, the metadata volume can exceed what a single OLTP database handles efficiently for query planning lookups. Phase 1 uses PostgreSQL as the initial implementation, but the metadata architecture must be prepared for a shift away from "one OLTP DBMS for everything."

The design mitigates this in two ways:

1. **Self-describing Parquet files.** All metadata (sort schema, min/max/regex, window assignment) is embedded in the Parquet file's `key_value_metadata`. The external metadata store is an index for query planning, not the sole source of truth. This means the metadata layer can be replaced or supplemented without re-writing data files.
2. **Structured metadata.** The metadata fields are simple, typed, and positional. They can be stored in any system that supports efficient range queries and filtering -- a dedicated metadata service, a columnar store, or a distributed key-value store. The PostgreSQL schema should be designed with this future migration in mind, keeping the metadata representation portable rather than relying on PostgreSQL-specific features.

### Failure Modes

- **Clock skew within the acceptance window.** Data arriving within the late data acceptance window but for past windows can trigger re-merges of already-compacted windows. The acceptance window bounds the blast radius, but sustained late data (e.g., a source with systematic clock skew) can cause repeated compaction churn for recent windows. Monitoring should track the age distribution of late-arriving data and alert on sources that consistently submit near the acceptance window boundary.
- **Many small windows.** If the window duration is set too small relative to the commit interval, each commit may produce splits for many windows, each containing very few rows. This increases split count and metadata overhead. The window duration should be significantly larger than the commit interval (the default 15 minutes is appropriate for typical commit intervals of 30-60 seconds).

## Future Phases

Phase 1 establishes the foundation for locality-aware compaction. Subsequent phases will build on the sorted-split infrastructure:

- **Phase 1.5: Affinity-based shard routing.** Currently, the `IngestRouter` assigns shards via round-robin, scattering data for the same metric across all nodes. An intermediate optimization is to use consistent hashing on a prefix of the sort key (e.g., metric name) to bias shard selection, so that data for the same metric tends to land on the same shard. This is not strict partitioning (each shard still receives a mix of metrics, avoiding tiny files for low-throughput metrics), but probabilistic affinity that improves locality within each node's splits before full cross-node compaction exists. This is orthogonal to the sort/compaction design in Phase 1 and can be implemented independently. Its benefit compounds with sorting: data that is already co-located by metric within a shard produces longer contiguous runs when sorted, improving both compression and merge efficiency.

- **Phase 2: Cross-node compaction and m:n sorted merge.** Lift the `node_id` constraint from the merge scope, enabling compaction across all splits in a time range regardless of producing node. Introduce m:n merges that spread rows across the sort-key space into non-overlapping output splits, enabling split-level query pruning. This is analogous to Husky Phase 2. Affinity routing from Phase 1.5 remains valuable even after Phase 2: if data is already partially co-located by metric on the same shard, cross-node compaction moves less data, has lower write amplification, and produces longer contiguous runs in the merge order. The two are complementary -- routing reduces the cost of compaction, and compaction achieves the degree of locality that routing alone cannot guarantee (tunable via overlap/cost knobs as in Husky).
    
- **Phase 3: Query pruning.** Use the per-split min/max/regex metadata (recorded in Phase 1\) to skip entire splits at query planning time when the query predicates fall outside a split's value range, following the approach used in Husky.  
    
- **Phase 4: Logs/traces extension.** Apply the same sort schema and sorted merge approach to the Tantivy pipeline (logs/traces), where the benefits are primarily in split pruning (reduce full fan-out across all splits in a time range) and improved compression from sorted Tantivy fast fields.

- **Research: Wide tables.** Metrics from the same source (host, container) share nearly identical tags, so multiple metric names could be stored as separate value columns in a single wide row rather than as individual point-per-row entries (e.g., `k8s.cpu.usage`, `k8s.cpu.limit`, `k8s.mem.usage`, `k8s.mem.limit` as four columns sharing one tag set). This is the approach taken by TimescaleDB's hypertables. It would amortize tag storage across metrics and improve compression further, but requires significant compactor changes: merging splits with disjoint value column sets and reassembling wide rows from narrow inputs across files. Worth investigating as a future research project.

## References

- [Husky Storage Compaction Blog Post](https://www.datadoghq.com/blog/engineering/husky-storage-compaction/) -- prior art for sorted-merge compaction at petabyte scale

---

## Appendix: Critical Analysis

*The following analysis was produced by asking Claude to re-read the document and respond to this prompt:*

> 1. Identify the 3-5 non-obvious insights -- things that aren't stated explicitly but can be inferred from the content. Skip anything the author already highlights as a key point.
> 2. Find the tensions or contradictions. Where does the argument conflict with itself, or with conventional wisdom? What's left unresolved?
> 3. Extract the "so what." If a smart, busy person could only take away one actionable implication from this, what would it be and why?
> 4. Name what's missing. What question does this document raise but never answer? What would you want to know next?

### Non-obvious insights

**The optional timeseries_id tiebreaker has a hidden compression benefit beyond tag columns.** When `timeseries_id` clusters points from the same source, the value columns (the actual metric values) also become more compressible -- you get runs of values from the same source, which often have temporal coherence (slowly-changing values, predictable patterns). The doc frames the compression benefit as coming from sorted tag columns having long runs, but the bigger win may be what happens to the value columns when sources are grouped. This is worth measuring: compare compression with and without `timeseries_id` in the sort schema to see if the tiebreaker justifies its cost.

**The run-length encoded merge order creates a feedback loop.** The doc notes that sorted inputs produce longer contiguous runs. But it doesn't make explicit that this is self-reinforcing across compaction generations: each compaction cycle produces better-sorted outputs, which means the next merge has longer runs, which means the merge order is smaller and cheaper, which makes it practical to do larger merges. The system gets cheaper to compact over time, not more expensive. This is the opposite of naive size-tiered compaction where write amplification grows.

**Time windowing implicitly caps query fan-out, not just compaction scope.** The doc frames windowing as a compaction concern (bounding merge scope, limiting write amplification). But for queries, it also means the query planner knows that any split in a window outside the query's time range is irrelevant *without consulting min/max metadata*. This is free pruning that works today, before Phase 3, and it comes from the window assignment alone. The doc mentions this in passing ("discard all windows outside the query range") but doesn't call out that this is a material query performance improvement independent of sort order.

**The "clean cutoff" transition strategy is actually a bet on retention being shorter than the migration period.** If retention is, say, 90 days, and Phase 1 is enabled on day 0, then for 90 days you have a mixed estate of unsorted (pre-cutoff) and sorted (post-cutoff) splits. Queries spanning the boundary hit both. The doc implicitly assumes this is acceptable, but for long-retention use cases (years), the unsorted tail could persist for a long time and the compression/query benefits don't fully materialize until it's gone.

**The metadata-only columns after `&` are really a bet on Phase 3.** They add complexity and storage overhead in Phase 1 (computing and storing min/max/regex for columns that don't affect sort order) with zero benefit until split-level query pruning exists. If Phase 3 is deprioritized or the pruning approach changes, this is wasted work. The doc doesn't frame this as a calculated bet.

### Tensions and contradictions

**PostgreSQL is simultaneously "the authoritative source for query planning" and acknowledged to not scale.** The Scale Considerations section says PostgreSQL can't handle ~921K splits per window, and the design must prepare for a shift away from it. But the rest of the document treats PostgreSQL as the authoritative metadata store that query planning depends on. There's no intermediate plan -- at what scale does PostgreSQL break, and what happens between "Phase 1 ships" and "we build a replacement metadata service"?

**The compaction policy is borrowed but the workload is fundamentally different.** The doc says "use StableLogMergePolicy adapted for Parquet splits" but StableLogMergePolicy was designed for Tantivy splits in a logs/traces pipeline. Metrics have very different characteristics: higher write rates, smaller individual events, more predictable schemas, time-series structure. The doc acknowledges the need for experiments but doesn't discuss *why* the existing policy might be wrong for metrics, which makes it hard to know what the experiments should be testing for.

**Late-arriving data for old windows is "acceptable because rare" but the scale section suggests otherwise.** At 10 GiB/s, even a small percentage of late data is a lot of data. If 0.1% of data arrives late, that's 10 MiB/s of late data triggering merges of already-compacted windows. The doc assumes this is negligible, but at the scale numbers cited in the Scale Considerations section, it may not be.

### The "so what"

**The single most actionable implication:** Run experiment #4 (sorted vs. unsorted compression comparison) first, on real metrics data, before building anything else. Everything in this document -- the compaction policy, the time windowing, the merge implementation, the metadata infrastructure -- is justified by the assumption that sorting produces 20-35% compression improvement for metrics data. If the actual improvement is, say, 8% (because metrics data has different characteristics than logs/APM data), the cost-benefit calculus changes significantly. The Husky numbers are from logs and APM, not metrics. One afternoon with a script that takes existing metrics Parquet files, sorts them by the proposed schema, and re-encodes them would either validate the entire project or redirect it. Everything else can be designed in parallel, but this is the existential risk.

### What's missing

**What is the min/max/regex for `timeseries_id`?** The timeseries_id is a hash. Its min/max is a range of hash values, which is meaningless for query pruning (nobody queries by hash value). Its regex is similarly useless. But the metadata vectors are positional across all schema columns, so timeseries_id will have a slot. Is it populated with dummy values? Skipped? This is a small thing, but it exposes a design question: should the metadata vectors skip columns where the metadata is meaningless?

**How does the query engine actually discover and use time windows?** The doc describes how splits are assigned to windows and how compaction is scoped to windows, but doesn't describe how the query planner maps a query's time range to windows. Does it scan PostgreSQL for all splits in a time range and filter by `window_start`? Does it compute the relevant window set from the query's time bounds and look up splits per window? The query path is implied but never specified.

**What happens to the `timeseries_id` column when tags change?** If a tag value flaps or a tag is added/removed, the `timeseries_id` hash changes and points from the "same" logical source get different hash values. *Update: this is now addressed in the Timeseries ID section -- tag flapping degrades locality but never affects correctness, and `timeseries_id` can be omitted entirely if the explicit sort columns provide sufficient granularity.*

**What's the interaction between `doc_mapping_uid` in the compaction scope and sort schema changes?** If the sort schema changes, does `doc_mapping_uid` also change? If not, you have two scope dimensions (sort_schema and doc_mapping_uid) that both prevent merging on schema changes, which is redundant. If yes, sort_schema in the scope key is redundant with doc_mapping_uid. The relationship between these two isn't explained.

