# ADR-003: Time-Windowed Sorted Compaction for Parquet

## Metadata

- **Status**: Proposed
- **Date**: 2026-02-19
- **Tags**: storage, metrics, compaction, parquet, time-windowing
- **Components**: quickwit-parquet-engine, quickwit-indexing, quickwit-metastore
- **Authors**: gtt@
- **Related**: [ADR-001](./001-parquet-data-model.md), [ADR-002](./002-sort-schema-parquet-splits.md), [Phase 1 Design](../locality-compaction/phase-1-sorted-splits.md)

## Context

The metrics pipeline currently has **no compaction**. Splits accumulate without merging, relying on DataFusion to query many small Parquet files and on time-based retention to remove old data. This is documented in [compaction-architecture.md](../compaction-architecture.md) which notes: "Metrics splits accumulate without compaction. This is tolerable in the short term because DataFusion can query many small Parquet files, and time-based retention eventually removes old data. But it is not ideal."

The consequences of no compaction are severe at scale:

- **Unbounded split count within retention window.** At 10 GiB/s ingestion with 10 MiB splits, the system produces ~1,024 splits per second — ~921,600 splits per 15-minute window before compaction.
- **Query fan-out proportional to split count.** Every query must open and scan every split in the relevant time range. More splits means more I/O, more metadata lookups, and more DataFusion task scheduling overhead.
- **No intra-file pruning without sort order.** Without page-level column indexes on sorted data, DataFusion must scan the entire file even when only a small fraction of rows match the query predicate.

The existing logs/traces compaction system (`StableLogMergePolicy` with Tantivy merge) is designed for a different storage format and does not apply to Parquet splits. Metrics compaction requires a purpose-built pipeline that understands Parquet, sort order, and time-based data organization.

This ADR introduces time-windowed sorted compaction: all data is partitioned into fixed-duration time windows, and compaction merges splits within each window using a k-way sorted merge that preserves the sort order established by [ADR-002](./002-sort-schema-parquet-splits.md), operating on the point-per-row data model defined in [ADR-001](./001-parquet-data-model.md).

## Decision

### 1. Time Windowing

All data in the Parquet pipeline is organized into **time windows**: fixed-duration, non-overlapping intervals of wall-clock time aligned to the Unix epoch. Time windowing is enforced by both the **indexing pipeline** (which honors window boundaries — if a batch straddles a boundary, it is split so that each resulting split belongs to exactly one window) and the **compaction pipeline** (which only merges splits within the same window and never combines data across window boundaries).

**Configuration:**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `window_duration` | 15 minutes | Duration of each window. Must evenly divide one hour (valid: 1m, 2m, 3m, 4m, 5m, 6m, 10m, 12m, 15m, 20m, 30m, 60m) |
| `compaction_start_time` | (required) | Unix timestamp (seconds). Only windows at or after this time are eligible for compaction. Data before this time ages out via retention |
| `late_data_acceptance_window` | 1 hour | Maximum age of a data point accepted at ingestion. Points older than this are dropped. Bounds disturbance of compacted windows |

**Window computation:**

```
window_start = t - (t % window_duration_seconds)
window_end   = window_start + window_duration_seconds
```

Each window is identified by its `window_start` timestamp (seconds since Unix epoch).

**Why time windowing:**

1. **Bounds compaction scope.** Each window is an independent compaction unit. The total data eligible for a single merge is bounded by window duration and ingestion rate.
2. **Aligns with query patterns.** Observability queries always include a time range predicate. The query engine can discard all windows outside the query range without examining individual split metadata.
3. **Enables efficient retention.** Dropping old data becomes a window-level operation: all splits in expired windows can be deleted as a batch.
4. **Limits write amplification.** Old, fully-compacted windows are never disturbed by new data in newer windows.

### 2. Split-to-Window Assignment at Ingestion

Splits are assigned to a time window based on the timestamps of the rows they contain. When a split contains rows from multiple windows (common when a commit interval straddles a window boundary), the split is **partitioned by window** before writing: rows are grouped by window assignment, and a separate Parquet file is written for each window.

**Invariant:** Every split in object storage belongs to exactly one time window. This invariant is established at ingestion (when a batch of data straddles a window boundary, the indexing pipeline splits it at the boundary, producing separate splits for each window) and preserved through compaction (the compaction pipeline includes `window_start` in the merge scope, preventing cross-window merges). A window will typically contain many small splits from ingestion; compaction reduces them over time.

The window assignment uses the timestamp column referenced in the sort schema. Rows with null timestamps are assigned to a designated overflow window (`window_start = 0`), compacted separately.

**Note on existing time partitioning:** The codebase has a `PartitionGranularity` enum in `quickwit-parquet-engine/src/split/partition.rs` with `Hour`, `Day`, `Week` variants. This does not match the design requirement for finer-grained, epoch-aligned windows (1-60 minutes). The time windowing implementation should either extend or replace the existing partitioning infrastructure.

### 3. Compaction Scope

The compaction scope for Parquet splits has two layers: a **compatibility scope** that determines which splits may be merged, and a **grouping dimension** (`window_start`) that the merge planner uses to select per-window merge candidates within the compatibility scope.

**Compatibility scope** (6-part key):

```
(index_uid, source_id, partition_id, doc_mapping_uid, sort_schema, window_duration)
```

Only splits sharing all six components are merge-compatible.

| Component | Purpose | Change from current |
|-----------|---------|---------------------|
| `index_uid` | Prevents cross-index merging | No change |
| `source_id` | Prevents cross-source merging | No change |
| `partition_id` | Tenant isolation | No change |
| `doc_mapping_uid` | Prevents incompatible schema merging | No change |
| `sort_schema` | Prevents merging splits with different sort orders | **New** |
| `window_duration` | Prevents merging splits from different window duration configurations | **New** |

**Merge grouping** (within the compatibility scope):

Within a compatibility scope, the merge planner groups splits by `window_start` and only merges splits within the same window. This is analogous to how the existing Tantivy merge planner groups by `(partition_id, doc_mapping_uid)` within a pipeline's filtered set.

`window_duration` rather than `window_start` is the right compatibility dimension because different window durations can produce windows with the same start time. For example, a 5-minute window `[00:00, 00:05)` and a 15-minute window `[00:00, 00:15)` both have `window_start = 0`, but they contain data for different time ranges and must not be merged. Partitioning on `window_duration` prevents this; the merge planner then naturally groups by `window_start` within each duration.

**`node_id` is intentionally excluded.** In Phase 1, each node compacts its own splits (the current behavior), but the scope definition does not require it. This is a forward-looking design choice — Phase 2 lifts the node constraint for cross-node compaction.

**Window duration changes.** If the configured window duration changes, new splits use the new duration. The compatibility scope prevents merging across durations. Old-duration splits age out via retention.

### 4. Sorted Merge

The merge process combines N sorted input splits into one or more sorted output splits. Logically, the rows of all input splits are sorted together by their sort key — the lexicographic ordering of all sort column values for each row. The merge proceeds in two phases: determine the global sort order, then stream all columns through that order.

**Phase 1: Determine global sort order.**

Read the sort columns from each input split. Parquet's footer-based format allows seeking directly to any column. Compute a permutation that represents the sorted interleaving of all rows across all inputs, respecting the sort schema's comparison rules (lexicographic for strings, numeric for numbers, nulls-last for ascending, nulls-first for descending).

The sort order is represented as a **run-length encoded** sequence of `(split_index, start_row, row_count)` triples. Because inputs are already sorted, the merge naturally produces long contiguous runs from the same input. This representation enables bulk operations (bulk `take`, bulk copy) during column streaming.

**Sort order implementation — open question.** There are two candidate approaches for computing the global sort order, and both should be benchmarked on representative workloads:

- **K-way merge.** Use a min-heap (priority queue) with one entry per input split, advancing through rows in sorted order. Complexity O(R log N) where R is total row count and N is number of input splits.
- **Stable sort.** Concatenate the sort columns from all inputs and perform a stable sort over the combined rows. Complexity O(R log R), but stable sort implementations benefit from presorted runs (e.g., Timsort detects and exploits existing order). In Husky's Go implementation, stable sort was faster than k-way merge, likely because of better cache locality and lower per-comparison overhead for the common case of long sorted runs.

**Row comparison — open question.** Two strategies for comparing rows during sorting:

- **Composite key.** Encode all sort column values into a single byte-comparable key per row (e.g., using Google's [Ordered Code](https://github.com/google/orderedcode) encoding or Arrow's row format). Comparisons become a single `memcmp`. Amortizes multi-column comparison cost but requires encoding all sort column values upfront.
- **Column-at-a-time comparison.** Consult the individual sort column values at each row position during comparison, comparing column by column. Avoids the encoding step and may be faster when early columns (e.g., `metric_name`) distinguish most rows without needing to examine later columns.

**Phase 2: Stream columns through the merge.**

Once the global sort order is determined, each column is read from the input splits and written to the output in sorted order. Columns are processed one at a time (or in small groups) for memory efficiency.

For large columns, it may be advantageous to operate at **page granularity** rather than loading an entire column from each input: read individual Parquet pages from inputs as needed and write individual pages to the output. This bounds memory usage for columns with large values (e.g., high-cardinality string tags, large attribute maps) and avoids materializing an entire column across all inputs simultaneously.

**Phase 3: Emit split metadata.** The output split records sort_schema, window_start, window_duration_secs, and per-column min/max/regex.

**Self-reinforcing feedback loop:** Sorted inputs produce longer contiguous runs in the merge order. Each compaction cycle produces better-sorted outputs, meaning the next merge has longer runs, smaller merge orders, and cheaper execution. The system gets cheaper to compact over time.

### 5. Column Set Differences Across Inputs

Schema evolution means input splits may have different column sets:

- **Sort columns missing from an input:** All rows from that split are treated as null for the missing column. Nulls sort according to the schema rules. The k-way merge handles this naturally.
- **Non-sort columns:** The merge computes the **union** of all column names across inputs. The output contains every column that appears in at least one input. Rows from inputs lacking a column are filled with nulls. If the same column name has different types across inputs, the merge fails with an error (schema evolution conflict requiring resolution at the index configuration level).

### 6. Late-Arriving Data

Points with timestamps older than `late_data_acceptance_window` are **dropped at ingestion** rather than accepted. This bounds the window of time during which late data can disturb already-compacted windows.

Within the acceptance window, late data is handled naturally:

1. Late data is written to a new split assigned to the historical window (based on timestamp, not ingestion time).
2. The next compaction cycle for that window picks up the new split and merges it with existing compacted splits.
3. No special handling required; the window gains additional splits merged in the normal course.

For windows already compacted to a single large split, a late-arriving small split triggers a merge of the large split with the small one. The acceptance window bounds how far back this happens.

### 7. Pre-existing Unsorted Data

Splits produced before Phase 1 have no sort schema and no window assignment. These splits are **not compacted** — they remain as-is until they expire via retention.

`compaction_start_time` defines the boundary: only splits whose `window_start` >= this time are eligible for compaction. Splits with no window assignment or `window_start` before the cutoff are excluded from compaction planning entirely.

Data arriving after Phase 1 enablement but with timestamps before `compaction_start_time` is still written as a sorted, windowed split (the indexer always applies windowing and sorting once Phase 1 is active). However, these splits are not eligible for compaction and age out alongside pre-existing unsorted splits.

### 8. Compaction Policy

Phase 1 adapts Quickwit's existing compaction scheduling and `StableLogMergePolicy` for Parquet splits. Within each time window, the merge policy uses the same maturity/age constraints to determine merge eligibility.

Key parameters requiring experimental validation:

| Parameter | Question | Approach |
|-----------|----------|----------|
| Target split size | How large should merge output be? | Sweep 64MB, 128MB, 256MB, 512MB on representative workload |
| Merge fanin | How many inputs per merge? | Sweep 4, 8, 16; measure duration, memory, write amplification |
| Window size interaction | How many splits accumulate per window? What is steady-state after compaction? | Measure at representative ingestion rates |

**Recommended experiments before finalizing policy:**

1. **Baseline:** Measure splits per 15-minute window, size of each split, total data volume per window.
2. **Merge fanin sweep:** Fixed target size, vary fanin (4, 8, 16). Measure merge duration, peak memory, write amplification.
3. **Target size sweep:** Fixed fanin, vary target size (64MB-512MB). Measure steady-state split count, query latency, compression ratio.
4. **Compression validation:** Compare sorted vs. unsorted Parquet files for same data. **This is the existential experiment** — if compression improvement is <10% for metrics data (vs Husky's 25-33% for logs/APM), the cost-benefit calculus changes significantly.

### 9. Split Metadata Extensions

The following fields are added to `MetricsSplitMetadata` and the `metrics_splits` PostgreSQL table:

| Field | Type | Description |
|-------|------|-------------|
| `window_start` | `i64` | Unix timestamp (seconds) of the time window start |
| `window_duration_secs` | `u32` | Window duration in effect when split was produced |
| `sort_schema` | `String` | Full sort schema string including version suffix. Empty if pre-Phase-1 |
| `schema_column_min_values` | `Vec<SortColumnValue>` | Min value per schema column (sort + metadata-only), positional |
| `schema_column_max_values` | `Vec<SortColumnValue>` | Max value per schema column, positional |
| `schema_column_regexes` | `Vec<String>` | Regex matching any value per schema column, positional |

`SortColumnValue` is a tagged union of string, i64, u64, f64, and null.

These metadata fields are stored in **both** PostgreSQL (authoritative source for query planning) and Parquet `key_value_metadata` (making files self-describing).

**PostgreSQL scalability note.** At high ingestion rates (~921K pre-compaction splits per 15-minute window), PostgreSQL metadata volume can exceed what a single OLTP database handles efficiently. The design mitigates this in two ways: (1) self-describing Parquet files mean the external store is an index, not the sole source of truth; (2) metadata fields are simple, typed, and portable — they can be stored in any system supporting efficient range queries.

## Invariants

These invariants must hold across all code paths (ingestion, compaction, query).

### Window invariants

| ID | Invariant | Rationale |
|----|-----------|-----------|
| **TW-1** | Every split in object storage belongs to exactly one time window. Established at ingestion (batches that straddle a window boundary are split), preserved through compaction (merge scope includes `window_start` as a grouping dimension) | Enables window-level retention, query pruning by window, and bounded compaction scope |
| **TW-2** | `window_duration` must evenly divide one hour (valid: 1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30, 60 minutes) | Ensures window boundaries align across hours and days, preventing fragmented or offset windows |
| **TW-3** | Data is never merged across window boundaries | Each window is an independent compaction unit. Cross-window merges would violate TW-1 and make window-level retention impossible |

### Compaction scope invariants

| ID | Invariant | Rationale |
|----|-----------|-----------|
| **CS-1** | Only splits sharing all six compatibility scope components (`index_uid`, `source_id`, `partition_id`, `doc_mapping_uid`, `sort_schema`, `window_duration`) may be merged | Prevents merging incompatible data: different indexes, schemas, sort orders, or window durations |
| **CS-2** | Within a compatibility scope, only splits with the same `window_start` are merged | Enforces TW-3 (no cross-window merges). The merge planner groups by `window_start` |
| **CS-3** | Splits produced before `compaction_start_time` are never compacted | Clean transition: no mixed sorted/unsorted merge path. Pre-Phase-1 data ages out via retention |

### Merge correctness invariants

| ID | Invariant | Rationale |
|----|-----------|-----------|
| **MC-1** | The set of rows does not change during compaction, only their order. The output of a merge contains exactly the same rows as the union of its inputs — no rows are added, removed, or duplicated | Compaction is a physical reorganization, not a logical transformation. Queries over a window must return the same results before and after compaction |
| **MC-2** | Row contents do not change during compaction. The value of every column for every row is identical in the output as in the input, except for explicitly designated bookkeeping columns (e.g., `write_amplification_count`) that track compaction metadata | Data integrity through compaction. The storage layer does not transform, aggregate, or filter user data |
| **MC-3** | The output of a merge is sorted according to the sort schema of the inputs. Sort order is preserved, never degraded, through compaction | Enables sorted merge to be applied iteratively. Each compaction generation is at least as well-sorted as its inputs |
| **MC-4** | If inputs have different column sets (schema evolution), the output contains the union of all columns. Rows from inputs missing a column are filled with nulls. Type conflicts on the same column name are an error | Ensures no data loss during merge. Type conflicts require explicit resolution at the index configuration level |

## Consequences

### Positive

- **Reduces split count per time window** from unbounded accumulation to a bounded steady-state after compaction converges.
- **Larger splits improve query throughput.** Fewer splits means less fan-out, less metadata overhead, less DataFusion scheduling cost.
- **Sort order preserved through merges.** Compaction never degrades the sort quality established at ingestion.
- **Time windows provide free query pruning.** A query for a specific time range can discard entire windows without consulting min/max metadata — this works immediately, before Phase 3 split-level pruning.
- **Clean transition.** `compaction_start_time` cutoff means no mixed sorted/unsorted merge path. Old data ages out via retention.
- **Foundation for Phase 2.** Cross-node compaction, m:n merges, and split-level query pruning all build on the sorted-split + time-window infrastructure.

### Negative

- **New actor pipeline required.** The Parquet merge pipeline (planner, downloader, merge executor, uploader, publisher) must be built from scratch. The Tantivy merge actors cannot be reused directly — they use `UnionDirectory` for Tantivy segments, not Parquet k-way merge.
- **Memory cost of sorted merge.** The sort-order computation phase must hold sort columns from all input splits in memory. For large merges (16 inputs x 500K rows x 5 sort columns), this could be significant. Page-level streaming for non-sort columns mitigates total memory, but the sort-order phase is unavoidable.
- **Compaction policy borrowed from a different workload.** `StableLogMergePolicy` was designed for Tantivy log/trace splits. Metrics have different characteristics (higher write rates, smaller events, time-series structure). The policy may need metrics-specific tuning or replacement.

### Risks

- **PostgreSQL metadata scalability.** At extreme ingestion rates, the per-split metadata volume may exceed PostgreSQL's capacity for efficient query planning lookups. The design explicitly acknowledges this and recommends the metadata architecture be prepared for a future migration to a dedicated metadata service or columnar store.
- **Late data volume at scale.** The design assumes late-arriving data is rare. At 10 GiB/s, even 0.1% late data is 10 MiB/s, triggering re-merges of compacted windows. The `late_data_acceptance_window` bounds this, but sustained late data from a source with systematic clock skew can cause compaction churn.
- **Window duration sensitivity.** Too-short windows relative to commit interval produce many tiny splits per window. Too-long windows at high ingestion rates make the per-window split count unmanageable. Operators must tune `window_duration` based on ingestion rate.
- **`doc_mapping_uid` vs `sort_schema` in compaction scope.** If a sort schema change also triggers a new `doc_mapping_uid`, both scope dimensions prevent merging on schema changes (redundant). If not, they serve complementary purposes (schema structure vs sort order). The relationship between these two should be clarified during implementation.

## Signal Generalization

This ADR applies to **metrics** (Parquet pipeline) in Phase 1. The compaction architecture generalizes to all three signals:

- **Time windowing** is universal. Logs and traces are time-stamped data with time-range queries. Window-scoped compaction applies directly.
- **Sorted merge** applies to any signal with a sort schema. For Tantivy (logs/traces), sorted merge would operate on fast fields. The k-way merge algorithm is format-independent — the Parquet-specific part is reading/writing via arrow-rs rather than Tantivy segment APIs.
- **Compaction scope** generalizes with the addition of `sort_schema` and `window_duration`. The existing Tantivy compaction could adopt this scope if extended.

Phase 4 of the locality compaction roadmap extends time-windowed sorted compaction to the Tantivy pipeline. The main adaptation is replacing the Parquet merge executor with a Tantivy-aware one that produces sorted fast fields.

## Decision Log

| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-02-19 | Initial ADR created | Formalize compaction design for metrics Parquet pipeline, addressing the fundamental gap of no metrics compaction |
| 2026-02-19 | Time windows chosen over unbounded compaction | Bounds merge scope, aligns with query patterns, enables efficient retention, limits write amplification |
| 2026-02-19 | node_id excluded from compaction scope | Forward-looking for Phase 2 cross-node compaction. Merge operations do not interact with checkpoints, so this is safe |
| 2026-02-19 | StableLogMergePolicy adapted for initial compaction policy | Reuse existing, proven merge planning logic. May need metrics-specific tuning after experiments |
| 2026-02-19 | compaction_start_time cutoff for clean transition | Avoids complexity of merging sorted/unsorted inputs. Old data ages out via retention |
| 2026-02-19 | RLE merge order representation | Sorted inputs produce long contiguous runs, enabling bulk operations and creating a positive feedback loop across compaction generations |
| 2026-02-19 | Compaction scope uses window_duration, not window_start | window_start is a merge planner grouping dimension, not a compatibility dimension. Different durations can produce windows with the same start time (e.g., 5m and 15m windows both start at :00), so duration must be in the scope to prevent cross-duration merges |
| 2026-02-19 | Sorted merge strategy is an open question: k-way merge vs stable sort | In Husky's Go impl, stable sort was faster than k-way merge due to cache locality on presorted runs. Both should be benchmarked |
| 2026-02-19 | Row comparison strategy is an open question: composite key vs column-at-a-time | Composite key (Ordered Code, Arrow row format) enables single memcmp; column-at-a-time avoids encoding cost and may short-circuit on leading columns |
| 2026-02-19 | Page-level streaming for column merge phase | Loading/writing individual Parquet pages instead of whole columns bounds memory for large columns |
| 2026-02-20 | Merge correctness invariants MC-1 through MC-4 formalized | Compaction must not change the set of rows or their contents (except bookkeeping columns). Sort order must be preserved. Column set is the union of inputs |

## Implementation Status

### Implemented

| Component | Location | Status |
|-----------|----------|--------|
| (none) | - | No Parquet compaction infrastructure exists yet |

### Not Yet Implemented

| Component | Notes | Gap |
|-----------|-------|-----|
| Time-window partitioning at ingestion | Splits must be partitioned by window before writing | [GAP-003](./gaps/003-no-time-window-partitioning.md) |
| Late data acceptance window (drop at ingestion) | Points older than threshold dropped | [GAP-003](./gaps/003-no-time-window-partitioning.md) |
| Parquet merge planner | Selects merge candidates per window, respecting 6-part scope | [GAP-001](./gaps/001-no-parquet-compaction.md) |
| Parquet merge split downloader | Downloads source splits from object storage | [GAP-001](./gaps/001-no-parquet-compaction.md) |
| Parquet sorted merge executor | K-way merge with RLE merge order, column streaming | [GAP-001](./gaps/001-no-parquet-compaction.md) |
| Parquet merge uploader | Uploads merged split to object storage | [GAP-001](./gaps/001-no-parquet-compaction.md) |
| Parquet merge publisher | Atomically updates PostgreSQL metadata | [GAP-001](./gaps/001-no-parquet-compaction.md) |
| StableLogMergePolicy adaptation for metrics | Size-tiered merge policy within time windows | [GAP-001](./gaps/001-no-parquet-compaction.md) |
| Split metadata extensions | window_start, window_duration_secs, sort_schema, min/max/regex fields | [GAP-004](./gaps/004-incomplete-split-metadata.md) |
| PostgreSQL schema migration | Add new columns to metrics_splits table | [GAP-004](./gaps/004-incomplete-split-metadata.md) |
| compaction_start_time configuration | Index-level config for transition boundary | [GAP-003](./gaps/003-no-time-window-partitioning.md) |
| Compaction policy experiments | Fanin sweep, target size sweep, compression validation | Pre-implementation |

## References

- [Phase 1: Sorted Splits for Parquet](../locality-compaction/phase-1-sorted-splits.md) — full design document
- [Compaction Architecture](../compaction-architecture.md) — current compaction system description
- [ADR-001: Parquet Data Model](./001-parquet-data-model.md) — point-per-row data model
- [ADR-002: Sort Schema for Parquet Splits](./002-sort-schema-parquet-splits.md) — sort schema that compaction preserves
- [StableLogMergePolicy](../../quickwit/quickwit-indexing/src/merge_policy/stable_log_merge_policy.rs) — existing merge policy
- [Merge Planner](../../quickwit/quickwit-indexing/src/actors/merge_planner.rs) — existing merge planner (Tantivy)
- [Husky Storage Compaction Blog Post](https://www.datadoghq.com/blog/engineering/husky-storage-compaction/)
