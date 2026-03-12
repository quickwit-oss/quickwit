# GAP-004: Incomplete Split Metadata for Compaction and Query Pruning

**Status**: Open
**Discovered**: 2026-02-19
**Context**: Codebase analysis during Phase 1 locality compaction design

## Problem

`MetricsSplitMetadata` and the `metrics_splits` PostgreSQL table lack the fields needed for sorted compaction (ADR-003) and future split-level query pruning (Phase 3). Additionally, the Parquet writer does not emit page-level column indexes, preventing intra-file query pruning on sorted data.

Specifically, the following are missing:

### Missing metadata fields

| Field | Purpose | Blocked capability |
|-------|---------|-------------------|
| `sort_schema` | Identifies the sort order of rows in the split | Compaction scope grouping (cannot determine if two splits are merge-compatible) |
| `window_start` | Identifies the time window the split belongs to | Time-windowed compaction scoping |
| `window_duration_secs` | Records window duration in effect at split creation | Detecting window duration changes (incompatible splits) |
| `schema_column_min_values` | Per-column minimum values for sort and metadata columns | Split-level query pruning (Phase 3) |
| `schema_column_max_values` | Per-column maximum values | Split-level query pruning (Phase 3) |
| `schema_column_regexes` | Per-column regex matching any value in the split | Split-level query pruning (Phase 3) |

### Missing Parquet file features

| Feature | Purpose | Current state |
|---------|---------|---------------|
| Page-level column index | Min/max statistics per page within each column chunk | Not enabled at write time |
| Offset index | Page byte offsets and row counts | Not enabled at write time |
| `sorting_columns` file metadata | Declares sort order for Parquet-native tooling | Not set |
| `key_value_metadata` sort entries | Sort schema, min/max/regex embedded in file | Not set |

Without page-level column indexes, DataFusion cannot perform page-level predicate pushdown even when data is sorted -- the sort order provides no query benefit within the file.

## Evidence

**MetricsSplitMetadata lacks compaction fields:**
```rust
// quickwit-parquet-engine/src/split/metadata.rs
pub struct MetricsSplitMetadata {
    pub split_id: SplitId,
    pub index_id: String,
    pub time_range: TimeRange,
    pub num_rows: u64,
    pub size_bytes: u64,
    pub metric_names: HashSet<String>,
    pub low_cardinality_tags: HashMap<String, HashSet<String>>,
    pub high_cardinality_tag_keys: HashSet<String>,
    pub created_at: SystemTime,
    pub parquet_files: Vec<String>,
    // Missing: sort_schema, window_start, window_duration_secs
    // Missing: schema_column_min_values, schema_column_max_values, schema_column_regexes
}
```

**PostgreSQL `metrics_splits` table lacks compaction columns:**
```sql
-- quickwit-metastore/migrations/postgresql/25_create-metrics-splits.up.sql
CREATE TABLE metrics_splits (
    split_id VARCHAR(50) PRIMARY KEY,
    split_state VARCHAR(30) NOT NULL,
    index_id VARCHAR(50) NOT NULL,
    time_range_start BIGINT NOT NULL,
    time_range_end BIGINT NOT NULL,
    metric_names TEXT[] NOT NULL,
    tag_service TEXT[],
    tag_env TEXT[],
    -- ... other tag columns ...
    num_rows BIGINT NOT NULL,
    size_bytes BIGINT NOT NULL,
    split_metadata_json TEXT NOT NULL,
    -- Missing: window_start, window_duration_secs, sort_schema
    -- Missing: schema_column_min_values, schema_column_max_values, schema_column_regexes
);
```

**Parquet writer does not enable column index or offset index.** The writer in `quickwit-parquet-engine/src/storage/writer.rs` writes Parquet files using default writer properties. Parquet format v2 column indexes and offset indexes are opt-in features that must be explicitly enabled via `WriterProperties::builder().set_column_index_truncate_length()` and related settings. Without these, page-level statistics are not emitted.

## State of the Art

- **Husky (Datadog)**: Fragment metadata includes sort schema, per-column min/max/regex, time bucket assignment. Used for fragment-level query pruning.
- **Apache Iceberg**: Manifest files store per-column lower/upper bounds for each data file. Used for file-level pruning during query planning.
- **Delta Lake**: Transaction log entries include per-column statistics (min, max, null count). Used by the query optimizer to skip files.
- **ClickHouse**: Part metadata includes primary key min/max per granule. Used for index-level pruning within parts.

Rich per-split (per-file) metadata for query pruning is standard in modern columnar storage systems.

## Potential Solutions

- **Option A (Proposed by ADR-002 and ADR-003)**:
  1. Extend `MetricsSplitMetadata` with `sort_schema`, `window_start`, `window_duration_secs`, and per-column min/max/regex fields.
  2. Add corresponding columns to the `metrics_splits` PostgreSQL table via a new migration.
  3. Enable Parquet column index and offset index at write time.
  4. Set `sorting_columns` in Parquet file metadata.
  5. Write sort schema, min/max/regex to Parquet `key_value_metadata`.

- **Option B**: Store extended metadata only in `split_metadata_json` (the existing JSON blob column). This avoids a schema migration but prevents efficient SQL queries on the new fields (no window-based filtering, no sort-schema grouping in queries).

**Recommended**: Option A. SQL-queryable metadata fields are essential for the merge planner (which queries PostgreSQL to find merge candidates) and for future query planning.

## Signal Impact

**Metrics**: Directly affected. The Parquet pipeline needs these metadata fields for compaction and pruning.

**Traces and logs**: Not directly affected. Tantivy splits have their own metadata in the `splits` table. If Phase 4 extends sorted compaction to Tantivy, similar metadata extensions would be needed for `SplitMetadata`.

## Impact

- **Severity**: High
- **Frequency**: Constant (every split is missing the metadata; every potential merge and pruning operation is blocked)
- **Affected Areas**: `quickwit-parquet-engine/src/split/metadata.rs`, `quickwit-parquet-engine/src/split/postgres.rs`, `quickwit-parquet-engine/src/storage/writer.rs`, `quickwit-metastore/migrations/`, query planner (future)

## Next Steps

- [ ] Add `sort_schema`, `window_start`, `window_duration_secs` to `MetricsSplitMetadata`
- [ ] Add `SortColumnValue` tagged union type (string, i64, u64, f64, null)
- [ ] Add `schema_column_min_values`, `schema_column_max_values`, `schema_column_regexes` to `MetricsSplitMetadata`
- [ ] Create PostgreSQL migration adding new columns to `metrics_splits`
- [ ] Update `PgMetricsSplit` and `InsertableMetricsSplit` in `postgres.rs`
- [ ] Enable Parquet column index and offset index in writer properties
- [ ] Set `sorting_columns` in Parquet file metadata based on sort schema
- [ ] Write `sort_schema`, `schema_column_min_values`, `schema_column_max_values`, `schema_column_regexes`, `window_start`, `window_duration_secs` to Parquet `key_value_metadata`
- [ ] Compute per-column min/max during split writing (scan sort + metadata-only columns)
- [ ] Compute per-column regex during split writing (follow Husky implementation)

## References

- [ADR-001: Parquet Data Model](../001-parquet-data-model.md)
- [ADR-002: Sort Schema for Parquet Splits](../002-sort-schema-parquet-splits.md)
- [ADR-003: Time-Windowed Sorted Compaction](../003-time-windowed-sorted-compaction.md)
- [Phase 1: Sorted Splits for Parquet](../../locality-compaction/phase-1-sorted-splits.md)
- Current metadata: `quickwit-parquet-engine/src/split/metadata.rs`
- Current PostgreSQL schema: `quickwit-metastore/migrations/postgresql/25_create-metrics-splits.up.sql`
