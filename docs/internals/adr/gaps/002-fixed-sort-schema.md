# GAP-002: Fixed Hardcoded Sort Schema

**Status**: Partially resolved
**Discovered**: 2026-02-19
**Context**: Codebase analysis during Phase 1 locality compaction design. The initial sort implementation provides the foundation but is not configurable.
**Resolution**: PRs #6287–#6292 replaced the hardcoded sort with a configurable `TableConfig` + sort schema parser. Remaining: per-index metastore storage, pipeline propagation, null ordering fix.

## Problem

The sort order for Parquet splits is hardcoded in `ParquetField::sort_order()` (`quickwit-parquet-engine/src/schema/fields.rs:146-158`). It returns a fixed list of seven columns:

```rust
pub fn sort_order() -> &'static [ParquetField] {
    &[
        Self::MetricName,
        Self::TagService,
        Self::TagEnv,
        Self::TagDatacenter,
        Self::TagRegion,
        Self::TagHost,
        Self::TimestampSecs,
    ]
}
```

This sort order cannot be changed per index, per deployment, or at runtime. All metrics indexes use the same sort columns regardless of their query patterns. Different workloads have different high-value columns -- a Kubernetes metrics index benefits from `pod` and `namespace`, while an infrastructure metrics index benefits from `host` and `datacenter`. The fixed schema prevents workload-specific optimization.

Additionally, the current sort implementation has three behavioral mismatches with the target design:

1. **Null ordering**: The writer uses `nulls_first: true` for all columns (`writer.rs:95`). The design specifies nulls-last for ascending columns and nulls-first for descending columns, so that nulls cluster at the end of each column's value range in both directions.

2. **No sort direction control**: All columns are sorted ascending (`descending: false`). The design specifies that `timestamp` should default to descending, and other columns should support configurable direction.

3. **No stable sort**: The current sort using `lexsort_to_indices` is not guaranteed to be stable — rows with equal sort keys may be reordered arbitrarily. A stable sort preserves the relative order of rows with equal keys, which matters for compaction: when merging sorted inputs, a stable sort ensures that the merge is deterministic and that rows which compare equal are not needlessly shuffled across compaction generations.

## Evidence

**Hardcoded sort columns in `fields.rs`:**
```rust
// quickwit-parquet-engine/src/schema/fields.rs:146-158
pub fn sort_order() -> &'static [ParquetField] {
    &[
        Self::MetricName,    // Always first
        Self::TagService,    // Always second
        Self::TagEnv,        // etc.
        Self::TagDatacenter,
        Self::TagRegion,
        Self::TagHost,
        Self::TimestampSecs, // Always last
    ]
}
```

**Fixed sort options in `writer.rs`:**
```rust
// quickwit-parquet-engine/src/storage/writer.rs:93-96
SortColumn {
    values: Arc::clone(batch.column(col_idx)),
    options: Some(SortOptions {
        descending: false,   // All ascending
        nulls_first: true,   // All nulls-first (design says nulls-last for ascending)
    }),
}
```

**No sort schema in metastore.** The sort order is not part of the index metadata in the metastore, MetricsSplitMetadata, or the Parquet file metadata. There is no mechanism to specify, store, evolve, or propagate the sort schema to indexing pipelines at runtime.

**No timeseries_id computation.** The optional `timeseries_id` tiebreaker column (hash of canonicalized tag key/value pairs) is not computed anywhere in the pipeline.

## State of the Art

- **Husky**: Sort schema is defined per table/track as a configuration string (e.g., `service__s|status__s|tag.env__s|timestamp|tiebreaker__i/V2`). Different tables use different sort schemas optimized for their query patterns.
- **Apache Iceberg**: Sort order is a table property that can be changed over time. Different sort orders coexist in the same table; the metadata tracks which files use which sort order.

All of these systems treat sort order as a configurable, per-table property.

## Potential Solutions

- **Option A (Proposed by ADR-002)**: Implement the configurable sort schema format (`column|...|timestamp&metadata/V2`) as described in [ADR-002](../002-sort-schema-parquet-splits.md). Store the schema as a per-index property in the metastore, mutable at runtime. When the schema is changed, propagate the update to the indexing pipelines on the appropriate nodes so that new splits use the new schema. Already-written splits retain their original schema and are not rewritten; they age out via retention. Record the sort schema string in each split's MetricsSplitMetadata and Parquet file metadata so that the compaction scope can group splits by schema.

- **Option B**: Extend the current `ParquetField::sort_order()` to accept a configuration parameter. Simpler than full schema parsing but doesn't support direction control, metadata-only columns, or the timeseries_id tiebreaker, and has no runtime change propagation.

**Recommended**: Option A. The full schema format is needed for compaction (merges must know the sort order of inputs) and for future query pruning (metadata-only columns). Runtime mutability is essential for operational flexibility -- changing the sort schema should not require a pipeline restart or redeployment.

## Signal Impact

**Metrics**: Directly affected. All metrics indexes use the same fixed sort order.

**Traces and logs**: Not affected today (Tantivy pipeline has no sort). Phase 4 would need a similar configurable sort schema for Tantivy fast fields.

## Impact

- **Severity**: Medium
- **Frequency**: Constant (every index is constrained to the same sort order)
- **Affected Areas**: `quickwit-parquet-engine/src/schema/fields.rs`, `quickwit-parquet-engine/src/storage/writer.rs`, index configuration, `MetricsSplitMetadata`

## Next Steps

- [x] Define sort schema parser for `column|...|timestamp&metadata/V2` format — PR #6290 (`sort_fields/parser.rs`), supports column names, `+`/`-` direction, `&` LSM cutoff, `/V2` version
- [ ] Store sort schema as per-index property in the metastore (mutable at runtime) — future Phase 32
- [ ] Propagate sort schema changes from metastore to indexing pipelines on appropriate nodes — future Phase 32
- [x] Replace `ParquetField::sort_order()` with schema-driven column selection — PR #6287 (`writer.rs` uses `resolved_sort_fields` from `TableConfig.effective_sort_fields()`)
- [x] Fix null ordering: nulls always sort last (`nulls_first: false`) regardless of column direction — PR #6295 (`sort_batch()`, `sorting_columns()`, SS-1 verification). Tested with ascending and descending columns in `test_nulls_sort_last_ascending_and_descending`
- [x] Support per-column sort direction (`+`/`-` suffix) — PR #6290 (parser) + PR #6287 (writer respects `descending` flag)
- [x] Implement optional timeseries_id computation — PR #6286 (SipHash-2-4 over canonicalized tags, computed at OTLP ingest in `arrow_metrics.rs`)
- [x] Store sort schema in MetricsSplitMetadata and Parquet file key_value_metadata — PR #6292 (`qh.sort_fields` KV entry + `metadata.sort_fields`)

## References

- [ADR-002: Sort Schema for Parquet Splits](../002-sort-schema-parquet-splits.md)
- [Phase 1: Sorted Splits for Parquet](../../locality-compaction/phase-1-sorted-splits.md)
- Current implementation: `quickwit-parquet-engine/src/schema/fields.rs:146-158`, `quickwit-parquet-engine/src/storage/writer.rs:84-109`
