# Technology Stack: Parquet Compaction Pipeline

**Project:** Pomsky Metrics Engine -- Time-Windowed Sorted Parquet Compaction
**Researched:** 2026-02-23
**Overall confidence:** HIGH

## Existing Stack (Validated -- DO NOT change)

| Technology | Version | Purpose |
|------------|---------|---------|
| DataFusion | 52.1.0 | Query engine, session context |
| Arrow | 57.2.0 | In-memory columnar format |
| Parquet | 57.2.0 | On-disk columnar format |
| arrow-row | 57.2.0 | Comparable row encoding (transitive dep via `arrow`) |
| arrow-select | 57.2.0 | `interleave_record_batch`, `take` kernels (transitive dep via `arrow`) |
| arrow-ord | 57.2.0 | `lexsort_to_indices`, sort kernels (transitive dep via `arrow`) |
| datafusion-physical-plan | 52.1.0 | Physical execution operators (transitive dep via `datafusion`) |
| zstd | 0.13 | Compression |
| PostgreSQL (sqlx 0.8) | -- | Metastore |
| object_store | 0.12.4 | S3/MinIO storage |
| quickwit-actors | workspace | Actor framework |
| quickwit-storage | workspace | Storage abstraction |

**Key version correction:** The milestone context stated DataFusion 45 / Arrow 54 / Parquet 54. The actual workspace versions are DataFusion 52.1.0 / Arrow 57.2.0 / Parquet 57.2.0. These are materially newer and provide significantly better APIs for the compaction pipeline, particularly `StreamingMergeBuilder` and the loser tree merge implementation.

## Recommended Stack Additions for Compaction

### K-Way Sorted Merge: Use DataFusion's `StreamingMergeBuilder`

| Technology | Version | Crate | Purpose | Confidence |
|------------|---------|-------|---------|------------|
| `StreamingMergeBuilder` | 52.1.0 | `datafusion-physical-plan` | K-way merge of sorted `RecordBatch` streams | HIGH |

**Why this and not a custom implementation:**

DataFusion 52.1.0 provides `datafusion_physical_plan::sorts::streaming_merge::StreamingMergeBuilder` as a **public API** (`pub struct`, `pub fn build`). This is the exact same loser-tree-based k-way merge that powers DataFusion's `SortPreservingMergeExec`, but exposed as a builder pattern that accepts `Vec<SendableRecordBatchStream>` plus `LexOrdering` sort expressions.

Key capabilities already implemented:
- **Loser tree** (tournament tree) with O(log K) comparisons per element, ~50% faster than binary heap
- **Arrow row format** integration via `RowCursorStream` for multi-column comparison
- **Specialized single-column fast paths** for primitive types, Utf8, Binary
- **Streaming output** -- produces `SendableRecordBatchStream` with configurable batch size
- **Memory reservation** integration with DataFusion's memory pool
- **Round-robin tie breaking** for equal keys (distributes across input streams evenly)

```rust
use datafusion_physical_plan::sorts::streaming_merge::StreamingMergeBuilder;
use datafusion_physical_expr_common::sort_expr::LexOrdering;

let merged_stream = StreamingMergeBuilder::new()
    .with_streams(input_streams)          // Vec<SendableRecordBatchStream>
    .with_schema(schema)                   // SchemaRef
    .with_expressions(&sort_expressions)   // &LexOrdering
    .with_metrics(metrics)                 // BaselineMetrics
    .with_batch_size(batch_size)           // usize
    .with_reservation(reservation)         // MemoryReservation
    .with_fetch(None)                      // Optional row limit
    .with_round_robin_tie_breaker(false)   // Disable for deterministic output
    .build()?;                             // -> Result<SendableRecordBatchStream>
```

**What this resolves:** The ADR-003 open question about "k-way merge vs stable sort" and "composite key vs column-at-a-time comparison" is answered by using DataFusion's implementation, which uses:
- Loser tree for the k-way merge (not binary heap, not Timsort)
- Arrow row format for multi-column comparison (composite memcmp-comparable bytes)
- Specialized fast paths for single-column sorts

**Why NOT write our own:**
1. DataFusion's implementation is battle-tested in production query engines
2. It already handles dictionary arrays (important for our Dictionary-encoded tag columns)
3. The loser tree + arrow row format combination is state-of-the-art
4. It integrates with DataFusion's memory management (which we already use via `MetricsSessionContext`)
5. Maintaining a parallel merge implementation doubles the surface area for bugs

**Why NOT use `SortPreservingMergeExec` directly:**
`SortPreservingMergeExec` is an `ExecutionPlan` that requires a child `ExecutionPlan` as input. Our compaction pipeline reads Parquet files from object storage outside the DataFusion execution framework. `StreamingMergeBuilder` is the lower-level building block that `SortPreservingMergeExec` wraps -- it accepts raw streams, which is what we need.

### Sort Key Encoding: Use Arrow Row Format (`arrow_row`)

| Technology | Version | Crate | Purpose | Confidence |
|------------|---------|-------|---------|------------|
| `RowConverter` | 57.2.0 | `arrow-row` (via `arrow`) | memcmp-comparable row encoding for sort keys | HIGH |

**Why `arrow_row` RowConverter over `lexsort_to_indices`:**

The ADR-003 open question about "composite key (Ordered Code, Arrow row format) vs column-at-a-time comparison" is resolved: **use Arrow row format.**

Evidence:
- For multi-column sorts (our case: 3-7 sort columns), the row format is "significantly faster" than `lexsort_to_indices` which uses `DynComparator` with dynamic dispatch per comparison
- DataFusion itself migrated `SortExec` and `SortPreservingMergeExec` to use arrow row format for exactly this reason
- The row format handles our exact data types: Dictionary(Int32, Utf8) for tags, UInt64 for timestamps, with correct null ordering
- Encoding is amortized: encode once per batch, compare many times via `memcmp`

**Note:** When using `StreamingMergeBuilder`, the row format is used automatically internally via `RowCursorStream`. For the initial ingestion sort (currently using `lexsort_to_indices`), migration to row format is a separate optimization -- not required for compaction but recommended for consistency.

```rust
use arrow_row::{RowConverter, SortField};

// Create converter matching the sort schema
let converter = RowConverter::new(vec![
    SortField::new(DataType::Dictionary(
        Box::new(DataType::Int32), Box::new(DataType::Utf8)
    )),
    SortField::new(DataType::Dictionary(
        Box::new(DataType::Int32), Box::new(DataType::Utf8)
    )),
    SortField::new(DataType::UInt64),
])?;

// Convert sort columns to comparable byte rows
let rows = converter.convert_columns(&sort_column_arrays)?;

// rows.row(i) < rows.row(j) uses memcmp -- single comparison for all columns
```

**Dictionary handling caveat:** `arrow_row` flattens dictionaries ("hydrates") during conversion. For high-cardinality dictionary columns across many input splits, this means the row encoding materializes all string values. This is acceptable because:
1. Sort columns are typically low-to-medium cardinality (metric_name, service, env, host)
2. The flattening happens per-batch, not globally
3. DataFusion's `RowCursorStream` handles this case already

**Known issue:** DataFusion issue [#7200](https://github.com/apache/datafusion/issues/7200) documents that `RowConverter` memory grows with high-cardinality dictionary fields because the `OrderPreservingInterner` accumulates dictionary mappings. For our use case, sort columns are low cardinality (metric names, tags), so this is unlikely to be a problem. Monitor memory during benchmarks with real data.

### Column Streaming During Merge: Use `arrow::compute::interleave_record_batch`

| Technology | Version | Crate | Purpose | Confidence |
|------------|---------|-------|---------|------------|
| `interleave_record_batch` | 57.2.0 | `arrow-select` (via `arrow`) | Apply merge permutation to columns | HIGH |

ADR-003's Phase 2 (stream columns through merge order) requires applying a permutation to all columns based on the sorted merge order. Arrow 57.2.0 provides `interleave_record_batch` which takes `&[&RecordBatch]` and `&[(usize, usize)]` indices (batch_index, row_index) and produces a new merged `RecordBatch`.

```rust
use arrow::compute::interleave_record_batch;

// indices: (input_split_index, row_index) pairs in sorted order
let merged_batch = interleave_record_batch(&input_batches, &sorted_indices)?;
```

**However**: When using `StreamingMergeBuilder`, the column interleaving is handled internally. `interleave_record_batch` is useful if we implement a custom merge path (e.g., for the stable-sort alternative mentioned in ADR-003), but the primary path through `StreamingMergeBuilder` does not require calling it directly.

### Bloom Filter Handling During Merge: Use Existing `parquet` APIs

| Technology | Version | Crate | Purpose | Confidence |
|------------|---------|-------|---------|------------|
| `WriterPropertiesBuilder::set_column_bloom_filter_enabled` | 57.2.0 | `parquet` | Enable bloom filters on output splits | HIGH |

**Strategy for bloom filters during merge:**

Bloom filters are **not merged** -- they are **regenerated** on the output. Bloom filters from input splits are discarded during merge. The output `ArrowWriter` regenerates bloom filters from the actual data written.

Rationale:
1. Bloom filter merging (union of bit arrays) increases false positive rate proportionally to the number of inputs. After merging 10 splits, the FPP degrades from 5% to ~40%.
2. Regeneration from data is cheap relative to I/O costs and produces optimal FPP.
3. The existing `ParquetWriterConfig::configure_dictionary_columns()` already sets up bloom filter configuration -- the merge output writer reuses this same config.

**For query-time bloom filter usage during merge planning:** The merge planner does NOT need bloom filters. Bloom filters are for query-time predicate pushdown, not merge candidate selection. Merge candidates are selected by time window and compatibility scope.

### Row Group Statistics During Merge: Use Existing `parquet` APIs

| Technology | Version | Crate | Purpose | Confidence |
|------------|---------|-------|---------|------------|
| `WriterProperties::set_statistics_enabled` | 57.2.0 | `parquet` | Row group and page min/max/null_count | HIGH |
| `WriterProperties::set_column_index_truncate_length` | 57.2.0 | `parquet` | Page-level column index (min/max) | HIGH |
| `WriterProperties::set_sorting_columns` | 57.2.0 | `parquet` | Declare sort order in Parquet metadata | HIGH |

**Strategy for statistics during merge:**

Statistics are **regenerated automatically** by `ArrowWriter`. When we write sorted `RecordBatch` data through `ArrowWriter`, it computes:
- Row group level min/max/null_count (via `EnabledStatistics::Chunk`)
- Page-level column index (via `set_column_index_truncate_length(Some(64))`)
- Sorting columns metadata (via `set_sorting_columns`)

No additional work needed beyond passing the same `WriterProperties` used for ingestion splits. The existing `ParquetWriterConfig::to_writer_properties()` produces correct config.

**Upgrade for compaction output:** Change statistics from `Chunk` to `Page` for compaction output. Compacted splits are larger and benefit more from page-level statistics:

```rust
// For compaction output, enable page-level stats on sort columns
builder = builder.set_statistics_enabled(EnabledStatistics::Page);
```

Note: As of arrow-rs 57, statistics are no longer written to data page headers by default. They are written to the column index (page index), which is more efficient because it gathers all statistics in a single location rather than embedding them in each page header.

### Input Split Reading: Use `ParquetRecordBatchStreamBuilder`

| Technology | Version | Crate | Purpose | Confidence |
|------------|---------|-------|---------|------------|
| `ParquetRecordBatchStreamBuilder` | 57.2.0 | `parquet` | Async streaming Parquet reader | HIGH |

**Why this over `ParquetRecordBatchReaderBuilder`:** The compaction pipeline needs to read from object storage (S3/MinIO) via `object_store`. The async `ParquetRecordBatchStreamBuilder` integrates with `object_store` and produces output that can be adapted to `SendableRecordBatchStream` for feeding into `StreamingMergeBuilder`.

```rust
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;

let reader = ParquetRecordBatchStreamBuilder::new(async_reader)
    .await?
    .with_batch_size(batch_size)
    .with_projection(projection_mask)  // Read only needed columns
    .build()?;
```

**Column projection for sort phase:** During the sort-order computation phase (ADR-003 Phase 1), reading only the sort columns using projection avoids loading all 14 columns when only 3-7 are needed for determining merge order. However, with `StreamingMergeBuilder`, we need all columns in the stream (it outputs complete records). The projection optimization applies only if we implement a two-phase merge (sort columns first, then remaining columns).

## What NOT to Add (Explicit Anti-Recommendations)

### DO NOT add `tournament_kway` or `tournament` crates

The `tournament_kway` crate (by vkrasnov) and similar external k-way merge crates operate on `Iterator<Item = T>` with `Ord` comparisons. They do NOT understand Arrow arrays, batch-oriented processing, or memory pools. DataFusion's `StreamingMergeBuilder` provides the same loser-tree algorithm but integrated with Arrow's type system and batch semantics.

### DO NOT add `rayon` for parallel merge

The merge is I/O-bound (reading from object storage) and memory-bound (materializing sort columns), not CPU-bound. Rayon's work-stealing parallelism adds complexity without benefit. The existing `tokio` runtime handles I/O concurrency. CPU-bound sort within each batch is already fast with Arrow's vectorized kernels.

### DO NOT add custom row encoding (Ordered Code, COBS, etc.)

The ADR-003 mentions Google's Ordered Code as an option. Arrow's row format (`arrow_row`) already implements an equivalent encoding optimized for Arrow types, including proper handling of:
- Variable-length strings (with null/non-null prefix bytes)
- Dictionary arrays (automatic hydration)
- Descending sort order (byte inversion)
- Null ordering (configurable nulls-first/nulls-last)

A custom encoding would need to handle all these cases and still be compatible with Arrow arrays on the way back. The row format is the standard solution.

### DO NOT add a separate Parquet merge/concat tool

The `parquet-concat` binary in arrow-rs concatenates row groups without re-sorting. This is useless for sorted merge compaction -- we need to interleave rows across inputs in sort order, which requires reading, sorting, and rewriting.

### DO NOT add `arrow-row` as a direct dependency

`arrow-row` is already a transitive dependency via `arrow = { version = "57" }` in the workspace. It is re-exported through `arrow::row`. Adding it as a direct dependency would create version coordination headaches. Use `arrow::row::RowConverter` instead (or `arrow_row::RowConverter` if the re-export is not available -- verify at implementation time).

## Dependency Changes Required

### In `quickwit-parquet-engine/Cargo.toml`

**Zero new external crate dependencies are needed.** All required functionality comes from existing workspace dependencies:

```toml
[dependencies]
# EXISTING -- no changes needed to these
arrow = { workspace = true }          # Provides arrow_row, arrow_select, arrow_ord
datafusion = { workspace = true }     # Provides datafusion-physical-plan, streaming merge
parquet = { workspace = true }        # Provides ArrowWriter, bloom filters, statistics
```

The only new internal dependencies would be if the merge actors live in `quickwit-indexing` (following the Tantivy merge pattern) and need to call compaction code in `quickwit-parquet-engine`:

```toml
# In quickwit-indexing/Cargo.toml (likely already present)
quickwit-parquet-engine = { workspace = true }
```

### Feature Flags

Ensure the `arrow` dependency includes `ipc` feature (already present in workspace `Cargo.toml`). No additional features needed.

## Integration Points with Existing Code

### Sort Schema to LexOrdering Conversion

The sort schema parser (ADR-002, not yet implemented) must produce a `LexOrdering` compatible with DataFusion's sort expressions. This is the bridge between the Husky-format schema string and DataFusion's merge API:

```rust
use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_expr::expressions::Column;

fn sort_schema_to_lex_ordering(
    schema: &SortSchema,
    arrow_schema: &SchemaRef,
) -> LexOrdering {
    let exprs: Vec<PhysicalSortExpr> = schema.sort_columns().iter().map(|col| {
        PhysicalSortExpr {
            expr: Arc::new(Column::new(col.name(), col.index())),
            options: SortOptions {
                descending: col.is_descending(),
                nulls_first: col.nulls_first(),
            },
        }
    }).collect();
    LexOrdering::new(exprs)
}
```

### Merge Output Writer: Reuse Existing ParquetWriterConfig

The merge output uses the same `ParquetWriterConfig` as ingestion, with potential modifications:

```rust
let mut config = ParquetWriterConfig::default();
// For compacted splits, consider larger row groups (more data per split)
config = config.with_row_group_size(256 * 1024);  // 256K rows vs 128K for ingestion
```

The `to_writer_properties()` method already configures bloom filters, sorting columns, statistics, and compression correctly. The only addition is updating `set_sorting_columns()` to use the dynamic sort schema instead of the hardcoded `ParquetField::sort_order()`.

### Memory Management

`StreamingMergeBuilder` requires a `MemoryReservation` from DataFusion's memory pool. The merge executor must register with the existing `MetricsSessionContext`'s memory pool:

```rust
let pool = session_ctx.runtime_env().memory_pool.clone();
let reservation = MemoryConsumer::new("parquet_merge_executor").register(&pool);
```

### BaselineMetrics for StreamingMergeBuilder

`StreamingMergeBuilder` requires a `BaselineMetrics` instance. This is DataFusion's internal metrics tracking. For standalone use outside a query plan, create one:

```rust
use datafusion_physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricBuilder};

let metrics_set = ExecutionPlanMetricsSet::new();
let baseline_metrics = BaselineMetrics::new(&metrics_set, 0);
```

### Actor Integration

The 5 new merge actors (MergePlanner, MergeDownloader, MergeExecutor, MergeUploader, MergePublisher) follow the existing `quickwit-actors` framework pattern. The merge executor actor:
1. Receives downloaded split files from MergeDownloader
2. Opens each as an async Parquet reader via `ParquetRecordBatchStreamBuilder`
3. Adapts readers to `SendableRecordBatchStream` (may need a thin wrapper)
4. Feeds them to `StreamingMergeBuilder`
5. Consumes the output stream and writes to a new Parquet file via `ArrowWriter`
6. Sends the output file to MergeUploader

## Alternatives Considered

| Category | Recommended | Alternative | Why Not |
|----------|-------------|-------------|---------|
| K-way merge | `StreamingMergeBuilder` (DataFusion) | Custom loser tree | Reimplements battle-tested code; no Arrow/batch integration |
| K-way merge | `StreamingMergeBuilder` (DataFusion) | Stable sort (concatenate + sort) | ADR-003 notes Husky found stable sort faster in Go, but DataFusion's loser tree uses arrow row format which eliminates the per-comparison overhead that made Go's stable sort faster. Benchmark both, but default to streaming merge. |
| K-way merge | `StreamingMergeBuilder` (DataFusion) | `itertools::kmerge` | Operates on `Ord` items, not Arrow batches; no batch-level memory management |
| K-way merge | `StreamingMergeBuilder` (DataFusion) | `std::collections::BinaryHeap` | 2x more comparisons per element than loser tree; no Arrow integration |
| Row encoding | `arrow_row::RowConverter` (via DataFusion) | Google Ordered Code | Arrow row format handles the same types; custom encoding adds maintenance burden |
| Row encoding | `arrow_row::RowConverter` (via DataFusion) | Column-at-a-time comparison | Slower for multi-column sorts due to dynamic dispatch; arrow row format amortizes encoding |
| Bloom filters | Regenerate on output | Merge input bloom filters | FPP degrades proportionally to input count; regeneration is cheap |
| Statistics | `ArrowWriter` auto-compute | Manual min/max tracking | `ArrowWriter` already computes correct statistics; manual tracking risks bugs |
| Parquet reading | `ParquetRecordBatchStreamBuilder` (async) | `ParquetRecordBatchReaderBuilder` (sync) | Compaction reads from object storage which is async; sync reader blocks the tokio runtime |

## Open Questions Requiring Benchmarks

These are questions from ADR-003 that the stack selection narrows but does not fully resolve:

### 1. StreamingMergeBuilder vs Stable Sort

ADR-003 notes that Husky's Go implementation found stable sort faster than k-way merge due to cache locality on presorted runs. With DataFusion's loser tree + arrow row format, the comparison is different:

- **StreamingMergeBuilder**: O(R log K) comparisons, streaming (bounded memory), no full materialization
- **Stable sort**: O(R log R) comparisons, requires concatenating all sort columns in memory, but Timsort exploits presorted runs

**Recommendation:** Default to `StreamingMergeBuilder`. The streaming property (bounded memory) is critical for large merges. Benchmark the stable sort alternative only if merge performance is a bottleneck.

**How to benchmark:** Create a benchmark that merges 8 and 16 pre-sorted splits of 500K rows each, comparing:
- `StreamingMergeBuilder` (loser tree, streaming)
- `concat_batches` + `lexsort_to_indices` + `take_record_batch` (stable sort, materialized)
- `concat_batches` + `RowConverter` sort (stable sort, row format)

### 2. Page-Level Column Streaming

ADR-003's Phase 2 mentions operating at page granularity for memory efficiency during column streaming. `StreamingMergeBuilder` operates at `RecordBatch` granularity, which is controlled by:
- Input reader batch size (configurable via `ParquetRecordBatchStreamBuilder::with_batch_size`)
- Output batch size (configurable via `StreamingMergeBuilder::with_batch_size`)

Page-level granularity is achievable by setting the input reader batch size to match the Parquet page size. This does not require custom page-level I/O -- the async reader already reads at page granularity internally.

### 3. EnabledStatistics::Page vs EnabledStatistics::Chunk

For compacted output splits, enabling page-level statistics (`EnabledStatistics::Page`) provides better pruning but increases metadata size. The tradeoff depends on:
- Number of pages per column chunk in compacted splits
- Query selectivity (how much pruning page-level stats enable)

**Recommendation:** Start with `EnabledStatistics::Page` for compacted output (sorted data benefits the most from page-level stats). Measure metadata overhead.

### 4. SendableRecordBatchStream Adapter

`ParquetRecordBatchStreamBuilder::build()` returns a `ParquetRecordBatchStream`. `StreamingMergeBuilder::with_streams()` expects `Vec<SendableRecordBatchStream>`. Verify at implementation time whether `ParquetRecordBatchStream` implements `SendableRecordBatchStream` (it should, since it implements `RecordBatchStream + Send`), or if a thin adapter is needed.

## Version Pinning Strategy

All compaction stack components are pinned via the workspace `Cargo.toml`:

```toml
arrow = { version = "57", ... }
datafusion = { version = "52", ... }
parquet = { version = "57", ... }
```

Arrow/Parquet 57 and DataFusion 52 are the current workspace versions. Upgrading these is a workspace-wide decision that affects all crates, not a compaction-specific concern. The APIs used (`StreamingMergeBuilder`, `RowConverter`, `WriterProperties`, `interleave_record_batch`) have been stable across recent versions.

**Risk of API breakage:** `StreamingMergeBuilder` is `pub` but in the `sorts::streaming_merge` module which could change between DataFusion major versions. Pin to `52.x` and test on upgrade. The core `SortPreservingMergeExec` is a stable public API; `StreamingMergeBuilder` is its implementation detail made public. If it becomes private in a future version, we can wrap `SortPreservingMergeExec` or fork the merge logic (it is ~260 lines).

## Sources

- [Apache Arrow Rust 57.0.0 Release Notes](https://arrow.apache.org/blog/2025/10/30/arrow-rs-57.0.0/)
- [Arrow Row Format Blog Post](https://arrow.apache.org/blog/2022/11/07/multi-column-sorts-in-arrow-rust-part-1/)
- [arrow_row crate documentation](https://docs.rs/arrow-row)
- [DataFusion SortPreservingMergeExec docs](https://docs.rs/datafusion/latest/datafusion/physical_plan/sorts/sort_preserving_merge/struct.SortPreservingMergeExec.html)
- [DataFusion loser tree PR #4301](https://github.com/apache/datafusion/pull/4301) -- 50% merge speedup
- [DataFusion tournament tree issue #4300](https://github.com/apache/datafusion/issues/4300)
- [Use Arrow Row Format in SortExec issue #5230](https://github.com/apache/datafusion/issues/5230)
- [RowConverter memory growth issue #7200](https://github.com/apache/datafusion/issues/7200) -- dictionary memory caveat
- [Parquet WriterProperties docs](https://arrow.apache.org/rust/parquet/file/properties/struct.WriterProperties.html)
- [Parquet WriterPropertiesBuilder docs](https://arrow.apache.org/rust/parquet/file/properties/struct.WriterPropertiesBuilder.html)
- [Parquet bloom filter support issue #3023](https://github.com/apache/arrow-rs/issues/3023)
- [Parquet page index support issue #1705](https://github.com/apache/arrow-rs/issues/1705)
- [DuckDB Parquet Bloom Filters](https://duckdb.org/2025/03/07/parquet-bloom-filters-in-duckdb) -- bloom filter design considerations
- [parquet-concat supports page index and bloom filter PR #8811](http://www.mail-archive.com/commits@arrow.apache.org/msg56584.html)
- [Stop writing statistics to page headers by default](https://www.mail-archive.com/commits@arrow.apache.org/msg51580.html) -- arrow-rs 57 behavior change
- [tournament_kway crate](https://docs.rs/tournament-kway/latest/tournament_kway/) -- considered and rejected
- [vkrasnov/tournament crate](https://github.com/vkrasnov/tournament) -- considered and rejected
- Verified against local source: `datafusion-physical-plan-52.1.0/src/sorts/streaming_merge.rs`
- Verified against local source: `datafusion-physical-plan-52.1.0/src/sorts/merge.rs` (loser tree implementation)
- Verified against local source: `arrow-row-57.2.0/src/lib.rs`
- Verified against local source: `arrow-select-57.2.0/src/interleave.rs`
- Verified against local source: `parquet-57.2.0/src/file/properties.rs`
