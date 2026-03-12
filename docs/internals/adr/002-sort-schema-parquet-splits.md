# ADR-002: Configurable Sort Schema for Parquet Splits

## Metadata

- **Status**: Proposed
- **Date**: 2026-02-19
- **Tags**: storage, metrics, compaction, parquet, sorting
- **Components**: quickwit-parquet-engine, quickwit-indexing
- **Authors**: gtt@
- **Related**: [ADR-001](./001-parquet-data-model.md), [ADR-003](./003-time-windowed-sorted-compaction.md), [Phase 1 Design](../locality-compaction/phase-1-sorted-splits.md)

## Context

Metrics data arrives at Quickwit through load-balanced routing: an external load balancer distributes requests across nodes, each node's `IngestRouter` picks a shard via round-robin, and the indexing pipeline produces splits stamped with the producing node's identity. Points for any given timeseries are scattered across whichever nodes happened to receive them.

Within each split, rows are stored in ingestion order (see [ADR-001](./001-parquet-data-model.md) for the point-per-row data model). There is no relationship between the physical layout of rows and the logical structure of the data. A query for a specific metric name must scan all rows in every split in the time range.

Sorting rows within each split by a schema aligned with common query predicates produces two immediate benefits:

1. **Compression improvement.** Columnar formats like Parquet compress data by encoding runs of similar values. When rows are sorted by metric name and tags, the columns for those fields contain long runs of identical or similar values, benefiting RLE, dictionary encoding, and general-purpose compression (ZSTD). In Husky Phase 1, this yielded ~33% size reduction for APM data and ~25% for Logs data.
2. **Query efficiency.** Parquet's column index (format v2) stores min/max statistics per page within each column chunk. When data is sorted, pages within each column naturally have non-overlapping value ranges for the sort columns. DataFusion supports page index pruning, allowing it to skip pages that cannot match a query predicate.

Matthew Kim's implementation added a fixed sort on `(MetricName, TagService, TagEnv, TagDatacenter, TagRegion, TagHost, TimestampSecs)` in the Parquet writer (`quickwit-parquet-engine/src/storage/writer.rs`), demonstrating that sorting is feasible and inexpensive. However, this sort order is hardcoded in `ParquetField::sort_order()` and cannot be customized per index or deployment. Different workloads have different high-value columns; a metrics index tracking Kubernetes containers benefits from sorting by `pod` and `namespace`, while an infrastructure metrics index benefits from `host` and `datacenter`.

This ADR formalizes the sort schema as a configurable, per-index property stored in the metastore.

## Decision

### 1. Sort Schema Format

A sort schema is a per-index (per-table) property stored in the metastore. It specifies an ordered list of columns that determine row sort order within each split, and optionally additional columns for which metadata (min/max/regex) is emitted but which do not participate in sorting.

The sort schema is **mutable at runtime**. When an operator changes the sort schema for an index in the metastore, the change is propagated to the indexing pipelines on the appropriate nodes so that newly-produced splits use the new schema. Already-written splits retain their original sort schema and are not rewritten — they age out via retention. The compaction scope includes `sort_schema` (see [ADR-003](./003-time-windowed-sorted-compaction.md)), so splits with different sort schemas are never merged together.

Format (following Husky convention):

```
[schema_name=]column[+/-]|...[&column[+/-]|...]/V2
```

Components:

- **Schema name** (optional): Labels the schema for identification. Example: `metrics_default=metric_name|...`
- **Sort columns** (pipe-delimited): Define the sort order. Each column may have `+` (ascending) or `-` (descending) suffix. Default direction is ascending, except `timestamp` which defaults to descending.
- **LSM cutoff** (`&`): Separates sort columns from metadata-only columns. Columns after `&` do not affect sort order, but min/max/regex metadata is emitted for them to enable future query pruning.
- **Version suffix** (`/V2`): Format version identifier.

Each column has:

| Property | Description |
|----------|-------------|
| **Name** | Column name as it appears in the Parquet schema |
| **Direction** | Ascending (`+`, default) or descending (`-`). `timestamp` defaults to descending |
| **Type** | Inferred from Parquet schema: string/binary (lexicographic), integer types (numeric), float types (numeric, NaN sorts after all values per IEEE 754 total order) |
| **Null handling** | Nulls sort **after** non-null values for ascending columns, **before** non-null values for descending columns |

**Note on null handling:** The current implementation uses `nulls_first: true` for all columns. This must be changed to match the design: ascending columns should use `nulls_first: false` (nulls last), descending columns should use `nulls_first: true` (nulls first). This ensures nulls cluster at the end of each column's value range in both directions.

### 2. Schema Requirements

- Sort columns should be a small subset (typically 3-5) corresponding to the most common query predicates, optionally followed by `timeseries_id` (see [ADR-001](./001-parquet-data-model.md)), followed by `timestamp`.
- Missing sort columns in a split (e.g., from schema evolution) are treated as null for all rows in that split. This is not an error condition.
- The schema string must end with `/V2`.
- Metadata-only columns (after `&`) are optional.

### 3. Sorting at Ingestion

The Parquet writer is modified to sort accumulated RecordBatch data by the configured sort schema before writing. The steps for each split:

1. **Accumulate rows** into RecordBatch arrays (as today).
2. **Compute timeseries_id** (if configured in the sort schema). See [ADR-001](./001-parquet-data-model.md) for computation details.
3. **Extract sort columns** from the accumulated rows.
4. **Compute sort indices** using Arrow's `lexsort_to_indices`, respecting direction and null ordering per the schema.
5. **Apply permutation** to all columns using Arrow's `take` kernel.
6. **Write Parquet file** with column index (page-level min/max) and offset index enabled. These are opt-in Parquet format v2 features required for DataFusion page-level predicate pushdown.
7. **Record metadata**: sort schema string, per-column min/max/regex.

### 4. Sort Metadata Storage

The sort schema and per-column statistics are stored in two places:

**PostgreSQL (`MetricsSplitMetadata`)**: The schema string and min/max/regex vectors are stored alongside existing split metadata. This enables split-level query pruning without reading Parquet data.

**Parquet `key_value_metadata`**: The schema is embedded in the file, making it self-describing:

| Key | Value |
|-----|-------|
| `sort_schema` | Full schema string (e.g., `metric_name\|host\|env\|timeseries_id\|timestamp&service/V2`) |
| `schema_column_min_values` | JSON array of min values, positional by schema column order |
| `schema_column_max_values` | JSON array of max values, positional by schema column order |
| `schema_column_regexes` | JSON array of regex strings, positional by schema column order |

**Parquet `sorting_columns`**: Sort columns (before `&`) are declared using Parquet's native `sorting_columns` field, specifying column index, direction, and null ordering. This allows Parquet-native tooling and DataFusion to leverage sort order without understanding our custom format.

### 5. Examples

Metrics index with explicit sort on metric name, host, and env, with timeseries_id tiebreaker and service as metadata-only:

```
metric_name|host|env|timeseries_id|timestamp&service/V2
```

Without timeseries_id (when host provides sufficient granularity):

```
metric_name|host|env|timestamp&service/V2
```

Minimal schema:

```
metric_name|timestamp/V2
```

## Invariants

These invariants must hold across all code paths (ingestion, compaction, query).

| ID | Invariant | Rationale |
|----|-----------|-----------|
| **SS-1** | All rows within a split are sorted according to the sort schema recorded in that split's metadata | Foundation for page-level pruning and sorted merge. Violated data produces incorrect merge results |
| **SS-2** | Nulls sort after non-null values for ascending columns and before non-null values for descending columns | Consistent null ordering across ingestion and merge. Matches Husky convention |
| **SS-3** | If a sort column is missing from a split, all rows in that split are treated as null for that column. This is not an error | Enables schema evolution — columns can be added to the sort schema without rewriting existing splits |
| **SS-4** | The sort schema stored in a split's metadata is the schema that was in effect when that split was written. Already-written splits are never re-sorted | Changes propagate forward only. Old splits age out via retention |
| **SS-5** | The sort schema string is the same in the metastore (per-split metadata), the Parquet `key_value_metadata`, and the Parquet `sorting_columns` field for a given split | Three representations of the same truth. Inconsistency between them would cause incorrect merge or pruning behavior |

## Consequences

### Positive

- **20-35% compression improvement** for metrics data (based on Husky Phase 1 results for similar workloads). Sorted columnar layout compresses tag columns with high value repetition very efficiently.
- **Page-level query pruning** via Parquet column index. When data is sorted, pages within each column have non-overlapping value ranges for sort columns. DataFusion can skip irrelevant pages.
- **Customizable per workload.** Different indexes can use different sort schemas optimized for their query patterns.
- **Runtime mutability.** Sort schema changes propagate to indexing pipelines without restart or redeployment. Old-schema splits coexist safely via the compaction scope.
- **Self-describing files.** Sort metadata in the Parquet file enables debugging, offline analysis, and disaster recovery without metastore access.
- **Foundation for compaction.** Sorted splits are a prerequisite for sorted merge compaction ([ADR-003](./003-time-windowed-sorted-compaction.md)).

### Negative

- **~2% CPU overhead at ingestion** for sorting. Expected to be offset by reduced compression cost (ZSTD works less on better-organized data), resulting in net CPU neutral or positive.
- **Schema format complexity.** The pipe-delimited format with direction suffixes, LSM cutoff, and version suffix is non-trivial. Parsing and validation code must be thorough.
- **Metadata-only columns (after `&`) have zero Phase 1 benefit.** They add storage overhead with no payoff until split-level query pruning exists (Phase 3). This is a deliberate bet on future value.

### Risks

- **Compression improvement may differ from Husky.** Husky's 25-33% was measured on logs/APM data. Metrics data has different characteristics (lower cardinality metric names, higher cardinality tag values). The design doc recommends running a validation experiment (sort existing Parquet files by the proposed schema and compare sizes) before committing to the full implementation.

## Signal Generalization

This ADR applies to **metrics** (Parquet pipeline) in Phase 1. The sort schema concept generalizes to all three signals:

- **Traces**: Sort by `service_name|operation_name|trace_id|timestamp` would co-locate spans from the same service and enable page-level pruning on service.
- **Logs**: Sort by `service_name|level|host|timestamp` would co-locate logs from the same service at the same severity level.

Phase 4 of the locality compaction roadmap extends sorting to the Tantivy pipeline for logs/traces. The sort schema format, null handling, and metadata storage are designed to be signal-agnostic. The main adaptation required for Tantivy is integrating sort order with fast fields rather than Parquet columns.

## Decision Log

| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-02-19 | Initial ADR created | Formalize existing sort implementation and design configurable sort schema for Phase 1 locality compaction |
| 2026-02-19 | Husky-compatible sort schema format adopted | Enables knowledge transfer and tooling reuse from Husky locality project |
| 2026-02-19 | Null sort direction: nulls-last for ascending, nulls-first for descending | Matches Husky behavior, ensures nulls cluster at end of value range. Current implementation (nulls_first=true for all) must be corrected |
| 2026-02-19 | Sort schema stored in metastore per-index, mutable at runtime, propagated to pipelines | Schema is a table-level property, not static config. Changes distributed to indexing nodes without restart. Already-written splits keep old schema, age out via retention |

## Implementation Status

### Implemented

| Component | Location | Status |
|-----------|----------|--------|
| Fixed sort at ingestion | `quickwit-parquet-engine/src/storage/writer.rs:84-109` | Done (Matthew Kim). Hardcoded sort on MetricName, TagService, TagEnv, TagDatacenter, TagRegion, TagHost, TimestampSecs |
| Sort column definition | `quickwit-parquet-engine/src/schema/fields.rs:146-158` | Done. `ParquetField::sort_order()` returns fixed column list |
| lexsort_to_indices usage | `quickwit-parquet-engine/src/storage/writer.rs` | Done. Arrow sort + take kernel applied in `sort_batch()` |

### Not Yet Implemented

| Component | Notes | Gap |
|-----------|-------|-----|
| Sort schema parser | Parse `column\|...\|timestamp&metadata/V2` format | [GAP-002](./gaps/002-fixed-sort-schema.md) |
| Sort schema in metastore | Schema stored per-index in metastore, mutable at runtime, propagated to pipelines on change | [GAP-002](./gaps/002-fixed-sort-schema.md) |
| Configurable sort directions | Currently all ascending. Need per-column `+`/`-` | [GAP-002](./gaps/002-fixed-sort-schema.md) |
| Correct null ordering | Currently `nulls_first: true` for all. Need nulls-last for ascending | [GAP-002](./gaps/002-fixed-sort-schema.md) |
| Parquet column index + offset index emission | Enable page-level min/max stats at write time | [GAP-004](./gaps/004-incomplete-split-metadata.md) |
| Sort metadata in PostgreSQL | sort_schema, per-column min/max/regex in MetricsSplitMetadata | [GAP-004](./gaps/004-incomplete-split-metadata.md) |
| Sort metadata in Parquet key_value_metadata | sort_schema, min/max/regex embedded in file | [GAP-004](./gaps/004-incomplete-split-metadata.md) |
| Parquet native sorting_columns field | Declare sort order in Parquet file metadata | [GAP-004](./gaps/004-incomplete-split-metadata.md) |

## References

- [Phase 1: Sorted Splits for Parquet](../locality-compaction/phase-1-sorted-splits.md) — full design document
- [Compaction Architecture](../compaction-architecture.md) — current compaction system description
- [ADR-001: Parquet Data Model](./001-parquet-data-model.md) — point-per-row data model and timeseries_id
- [ADR-003: Time-Windowed Sorted Compaction](./003-time-windowed-sorted-compaction.md) — compaction that depends on sort schema
- [Husky Phase 1: Locality of Reference](https://docs.google.com/document/d/1x9BO1muCTo1TmfhPYBdIxZ-59aU0ECSiEaGPUcDZkPs/edit) — prior art
- [Husky Storage Compaction Blog Post](https://www.datadoghq.com/blog/engineering/husky-storage-compaction/)
