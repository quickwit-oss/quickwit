# Domain Pitfalls: Parquet Compaction Pipeline

**Domain:** Time-windowed sorted Parquet compaction for metrics time-series storage
**System:** Pomsky Metrics Engine (Quickwit fork, DataFusion 52.1.0, Arrow 57.2.0, Parquet 57.2.0, PostgreSQL metastore, S3 storage)
**Researched:** 2026-02-23

---

## Critical Pitfalls

Mistakes that cause data loss, corruption, rewrites, or production outages.

---

### Pitfall 1: Memory Exhaustion During K-Way Merge Sort Phase

**What goes wrong:** The sorted merge algorithm requires reading sort columns from all input splits to compute the global sort order. With default StableLogMergePolicy parameters (merge_factor=10, max_merge_factor=12) and target split sizes of 256+ MiB, the sort phase can require significant memory. For 12 inputs x 500K rows x 5 sort columns (metric_name as dictionary string, tag_service, tag_env, tag_host as strings, timestamp as i64), this can reach several GiB of Arrow arrays. If the merge executor shares memory with ingestion and query actors, an OOM kills the entire process.

**Why it happens:** The merge must hold sort column data from multiple input splits simultaneously. The total memory is proportional to O(N_inputs x rows_per_input x sort_column_bytes), and sort columns include string columns (metric_name, tag_host) that do not compress well in memory even if dictionary-encoded in Parquet.

**Mitigation from stack choice:** DataFusion's `StreamingMergeBuilder` provides partial mitigation. It operates in a streaming fashion: each input is a `SendableRecordBatchStream` that yields `RecordBatch`es on demand. The merge does NOT require loading all sort columns from all inputs simultaneously -- it only needs the current `RecordBatch` from each input stream (controlled by `with_batch_size`). However, it does need one active batch from each input stream, so memory scales as O(N_inputs x batch_size x sort_column_bytes). With 12 inputs and 8192-row batches, this is manageable (~100 MB), much less than the full-materialization approach.

**Remaining risk:** The `RowConverter` used internally by `StreamingMergeBuilder` accumulates dictionary mappings in its `OrderPreservingInterner`. For high-cardinality dictionary columns, this can grow unboundedly over the course of a merge (DataFusion issue #7200). For our sort columns (low-to-medium cardinality), this is unlikely to be a problem, but should be monitored.

**Consequences:** Process OOM kill. All in-flight actors die. WAL data not yet published is lost (unless on persistent volume). Partial S3 uploads become orphans.

**Prevention:**
1. **Use `StreamingMergeBuilder` (not full materialization).** The streaming merge bounds memory to O(N_inputs x batch_size) rather than O(N_inputs x total_rows).
2. **Separate memory pool for compaction.** Use DataFusion's `MemoryPool` to create an isolated budget. When exhausted, merge fails gracefully rather than killing the process.
3. **Memory budget check before merge.** Estimate memory from Parquet footer metadata. Refuse large merges; reduce fanin by cascading sub-merges.
4. **Monitor `RowConverter` memory.** Track the memory reservation reported by `StreamingMergeBuilder`'s `BaselineMetrics`.

**Detection:**
- Monitor RSS and DataFusion memory pool usage during merges
- Alert when memory reservation exceeds 70% of pool
- Track OOM kills correlated with compaction activity

**Phase:** Phase 1. A compaction pipeline that can OOM the process is worse than no compaction.

**Confidence:** HIGH. Partially mitigated by `StreamingMergeBuilder` streaming property, but `RowConverter` memory growth for dictionaries is a documented issue.

---

### Pitfall 2: Non-Atomic Metadata Update Leaves Queries Seeing Duplicated or Missing Data

**What goes wrong:** Compaction must atomically replace N input splits with 1 output split in PostgreSQL. If this operation is not truly atomic, queries can see either (a) both old and new splits (duplicated data, inflating SUM/COUNT aggregations) or (b) neither (data temporarily invisible). The existing `publish_splits` code in the PostgreSQL metastore uses `staged_split_ids` and `replaced_split_ids` within a single transaction (via `run_with_tx!`), which provides atomicity at the SQL level. But if the compaction actor crashes between uploading the merged split to S3 and committing the PostgreSQL transaction, the merged split becomes an orphan file in S3.

**Why it happens:** Steps 3 (S3 upload) and 4 (PostgreSQL publish) cannot be made atomic across S3 and PostgreSQL -- no distributed transaction exists between them.

**Consequences:**
- **Orphan S3 files:** Merged splits uploaded but never published accumulate, consuming storage.
- **No data loss or duplication from a crash:** PostgreSQL transaction is all-or-nothing. Safe but wasteful.
- **Subtle duplication if atomicity is broken:** If publish is implemented incorrectly (two separate transactions), queries see duplicated data.

**Prevention:**
1. **Single-transaction publish.** Use existing `publish_metrics_splits` with both `staged_split_ids` and `replaced_split_ids` in one transaction.
2. **Orphan file garbage collection.** Extend `quickwit-janitor` for Parquet splits.
3. **Upload-then-publish ordering.** Always upload to S3 first. Orphan files are harmless (cleaned by janitor). Never publish before upload.
4. **Idempotent compaction.** Deterministic split ID from input split IDs so retries produce the same output path.

**Detection:**
- Monitor S3 object count vs PostgreSQL split count
- Alert on `publish_metrics_splits` transaction failures
- Track orphan file count in janitor

**Phase:** Phase 1. Most fundamental correctness property.

**Confidence:** HIGH. Pattern already implemented for Tantivy splits.

---

### Pitfall 3: Compaction Actor Cancellation Violates Invariants (GAP-002 Constraints)

**What goes wrong:** CLAUDE.md and GAP-002 FORBID `tokio::sync::Mutex` (data corruption on cancel) and `JoinHandle::abort()` (arbitrary cancellation violates invariants). Compaction actors must use `CancellationToken` for cooperative shutdown. If a merge is cancelled mid-operation, in-progress state must be safely abandoned.

**Why it happens:** Compaction operations are long-running (seconds to minutes). The system may need to shut down, rebalance, or cancel during this time.

**Consequences:**
- Corrupted partial output files on disk
- Leaked S3 multipart uploads
- Permanently held locks (if tokio::sync::Mutex used across await points)

**Prevention:**
1. **Use the actor mailbox model.** Mutable state lives inside the actor. No shared mutable state.
2. **Check CancellationToken at yield points.** After each input split processed, check for cancellation.
3. **Explicit S3 multipart abort on cancellation.** Set lifecycle policy for incomplete uploads as safety net.
4. **No tokio::sync::Mutex.** Use message passing.
5. **Scratch directory cleanup on startup.** Remove all temp files before starting compaction.

**Detection:**
- Grep for `tokio::sync::Mutex` in compaction code (should find zero)
- Monitor S3 incomplete multipart uploads
- Track scratch directory disk usage

**Phase:** Phase 1. System-wide invariants from GAP-002.

**Confidence:** HIGH. Project policy, not speculative.

---

### Pitfall 4: Read-Write Conflict -- Queries See Inconsistent Split State During Compaction

**What goes wrong:** During the window between when input splits are marked for deletion and when they are actually deleted from S3, a query that resolved split IDs before the publish transaction but fetches data after S3 deletion will fail with `ObjectNotFound`.

**Why it happens:** MarkedForDeletion splits are not immediately deleted -- the janitor deletes them after a grace period. If the grace period is shorter than maximum query execution time, queries fail.

**Prevention:**
1. **Generous grace period.** Wait at least 2x maximum query timeout before S3 deletion.
2. **Rely on existing Quickwit pattern.** Same MarkedForDeletion -> GC grace period -> deletion flow used for Tantivy splits.
3. **Retry on ObjectNotFound.** Search executor re-resolves split list from PostgreSQL.

**Detection:**
- Monitor ObjectNotFound errors correlated with compaction
- Track duration between MarkedForDeletion and actual S3 deletion

**Phase:** Phase 1. Existing patterns handle this; must be extended to metrics splits.

**Confidence:** HIGH.

---

### Pitfall 5: PostgreSQL Metadata Scalability Collapse

**What goes wrong:** At 10 GiB/s ingestion with ~600 KiB splits, the system produces ~1,024 splits per second, accumulating ~921,600 splits per 15-minute window before compaction. PostgreSQL becomes the bottleneck: merge planner queries slow, query planning degrades, compaction cannot keep up.

**Why it happens:** The merge planner scans eligible splits. As split count grows, queries become expensive. PostgreSQL is OLTP, not built for scanning millions of metadata rows.

**Consequences:**
- Compaction falls behind ingestion
- Query planning degrades
- Cascading failure: more splits -> slower compaction -> more splits

**Prevention:**
1. **Index `metrics_splits` aggressively.** Composite indexes on `(index_id, split_state, window_start)`.
2. **Partition planner queries by window.** Process one window at a time, starting with most recent.
3. **Batch metadata reads.** Use pagination.
4. **Monitor planner query latency.** Alert if > 1 second.
5. **Design metadata for portability.** Simple typed fields for future migration.

**Detection:**
- Monitor `metrics_splits` row count and growth rate
- Monitor merge planner query latency
- Alert on uncompacted split count per window exceeding threshold

**Phase:** Phase 1 (indexing and monitoring). Full solution may need Phase 2 metadata service.

**Confidence:** MEDIUM. Absolute numbers assume current split sizes. Larger splits or faster compaction reduce pressure.

---

### Pitfall 6: Compaction Falls Behind Due to StreamingMergeBuilder Overhead

**What goes wrong:** `StreamingMergeBuilder` is designed for query-time merge of sorted streams, not bulk compaction. Its per-row overhead (loser tree adjustment + row format encoding + batch building) may be slower than a bulk stable sort approach for the compaction use case where all data is available upfront and inputs are already sorted with long runs.

**Why it happens:** ADR-003 notes that Husky's Go implementation found stable sort faster than k-way merge due to cache locality on presorted runs. The Go comparison was between a binary heap k-way merge and Go's Timsort. DataFusion's loser tree is faster than a binary heap, but the fundamental tradeoff (streaming with per-row overhead vs batch sort exploiting cache locality) still applies.

**Consequences:**
- Compaction throughput insufficient to keep up with ingestion rate
- Split count grows unboundedly despite compaction being active
- Resource waste: compaction consumes CPU/IO without reducing split count fast enough

**Prevention:**
1. **Benchmark early.** In Phase 1b, benchmark `StreamingMergeBuilder` against `concat_batches + lexsort_to_indices` on representative data (8-16 pre-sorted splits of 500K rows). If stable sort is 2x+ faster, implement a hybrid approach.
2. **The streaming approach has a memory advantage.** `StreamingMergeBuilder` uses O(N x batch_size) memory; stable sort uses O(total_rows x sort_columns). If memory is the constraint, streaming wins regardless of CPU.
3. **Tune batch sizes.** Larger input batch sizes improve cache locality in `StreamingMergeBuilder` at the cost of more memory per input stream. Experiment with 8K, 64K, 128K row batches.
4. **Profile the bottleneck.** If compaction is slow, determine whether it is I/O bound (S3 download/upload) or CPU bound (merge). If I/O bound, the merge algorithm does not matter.

**Detection:**
- Track compaction throughput (rows merged per second, bytes per second)
- Compare to ingestion rate; alert if compaction throughput < ingestion rate for sustained period

**Phase:** Phase 1b. Benchmark during merge executor development.

**Confidence:** MEDIUM. DataFusion's loser tree + arrow row format may have eliminated the advantage Husky saw with Go's Timsort. Needs empirical validation.

---

### Pitfall 7: Resource Contention -- Compaction Starves Ingestion or Queries

**What goes wrong:** Compaction, ingestion, and queries compete for CPU, memory, disk I/O, network, and S3 API calls. Without resource isolation, a large merge can saturate S3 bandwidth and CPU, causing ingestion to fall behind and queries to slow down.

**Why it happens:** Quickwit runs all three workloads on the same nodes. The Tantivy merge pipeline uses `IoControls` for throttling; Parquet compaction must implement similar controls.

**Consequences:**
- Ingestion backpressure (WAL backlog, data loss risk)
- Query latency spikes
- Compaction starvation if queries/ingestion are prioritized

**Prevention:**
1. **IO rate limiting.** Use existing `IoControls` for compaction downloads/uploads.
2. **CPU scheduling.** Use `tokio::task::spawn_blocking` for compute-intensive merge phases.
3. **Concurrent merge limit.** At most 2-3 concurrent merges per node.
4. **Leading-edge priority.** When leading edge split count is high, divert all compaction resources there.
5. **Independent auto-scaling (long-term).** Dedicated compaction nodes (GAP-006).

**Detection:**
- Monitor ingestion latency during compaction
- Monitor query latency during compaction
- Track compaction throughput vs accumulation rate

**Phase:** Phase 1 (IO controls and concurrent limits). Independent scaling is separate milestone.

**Confidence:** HIGH. Universal in storage systems.

---

## Moderate Pitfalls

---

### Pitfall 8: Sort Instability Causes Non-Deterministic Merge Output

**What goes wrong:** Arrow's `lexsort_to_indices` is not guaranteed stable. GAP-002 notes this. Rows with equal sort keys may be reordered, causing non-deterministic output, harder debugging, and potentially degraded page-level statistics.

**Why it happens:** `lexsort_to_indices` uses `sort_unstable_by` (pdqsort) internally. The documentation is ambiguous about stability.

**Mitigation from stack choice:** `StreamingMergeBuilder` uses a loser tree for the k-way merge, which IS naturally stable: when two keys are equal, the loser tree preserves the relative order of inputs (by input stream index). Within each input stream, rows are already sorted. So the merge output is deterministic given deterministic input ordering. The instability concern applies primarily to the ingestion-time sort, not the merge.

**Prevention:**
1. **Add `timeseries_id` tiebreaker** in sort schema (ADR-002) to reduce equal-key frequency.
2. **For ingestion-time sort:** Consider using `RowConverter` + `sort_unstable_by` on row bytes (effectively stable for all practical purposes since row encoding disambiguates more aggressively than column-at-a-time).
3. **Verify empirically** on Arrow 57.2.0.

**Phase:** Phase 1 (sort schema implementation).

**Confidence:** MEDIUM. `StreamingMergeBuilder` mitigates for merge; ingestion sort remains a concern.

---

### Pitfall 9: Window Boundary Edge Cases -- Exactly-Once Window Assignment

**What goes wrong:** When a timestamp falls exactly on a window boundary, different code paths may compute different window assignments due to: timestamp unit mismatch (seconds vs nanoseconds), integer overflow, floating-point contamination, or off-by-one in range checks.

**Why it happens:**
- Rust `%` operator preserves dividend sign for negative values: `-1 % 900 == -1`, not `899`
- Timestamp unit inconsistency across pipeline stages
- Half-open interval `[start, end)` requires `<` not `<=` for upper bound

**Consequences:** Data in wrong window. Silent correctness violation. TW-1 invariant broken.

**Prevention:**
1. **Single canonical function:** `window_start(t: i64, duration: u32) -> i64` using `div_euclid`/`rem_euclid`.
2. **Timestamp normalization:** Convert to seconds before window assignment.
3. **Property-based tests** with `proptest`.
4. **`debug_assert` in split writer** checking all rows are within declared window.

**Phase:** Phase 1 (day-one infrastructure).

**Confidence:** HIGH. Concrete mathematical issue.

---

### Pitfall 10: Late Data Re-Compaction Churn

**What goes wrong:** Late-arriving data in already-compacted windows creates small splits that trigger re-merges with large existing splits. At 10 GiB/s with 0.1% late data, each of the 4 most recent windows gets steady small splits, causing repeated rewrites of large compacted files.

**Consequences:** Write amplification approaching infinity for small late splits. CPU waste. Resource contention.

**Prevention:**
1. **Minimum merge size threshold.** Don't re-compact unless accumulated late data exceeds 10% of existing split.
2. **Late data batching.** Accumulate late splits per window; merge on schedule, not immediately.
3. **Monitor late data sources.** Alert on sustained high late data rate.

**Phase:** Phase 1 for acceptance window cutoff. Batching optimization can be deferred.

**Confidence:** HIGH. ADR-003 identifies this risk explicitly.

---

### Pitfall 11: Compaction Scope Mismatch Between Planner and Publisher

**What goes wrong:** The 6-component scope key must be computed identically in planner and publisher. If sort_schema string normalization, window_duration encoding, or doc_mapping_uid differs between the two, published splits end up in wrong scopes.

**Prevention:**
1. **Pass full scope from planner through pipeline.** Publisher records scope from planner, not re-derived from data.
2. **Canonical sort schema normalization.** One parser, one canonical form.
3. **`debug_assert` comparing planner scope vs published metadata.**

**Phase:** Phase 1.

**Confidence:** MEDIUM. Subtle integration bug.

---

## Minor Pitfalls

---

### Pitfall 12: Column Set Union Causes Schema Explosion

**What goes wrong:** MC-4 (union of columns across inputs) can produce merged splits with far more columns than any input, inflating Parquet footer and wasting space on null-heavy columns.

**Prevention:** Track column cardinality. Warn if union exceeds 2x expected count. Consider pruning in future phases.

**Phase:** Phase 2.

---

### Pitfall 13: Parquet Writer Config Mismatch Between Ingestion and Compaction

**What goes wrong:** Different `WriterProperties` between ingestion and compaction output causes inconsistent file characteristics.

**Prevention:** Share a single `ParquetWriterConfig` instance. The existing `to_writer_properties()` method produces correct config. For compaction, potentially upgrade `EnabledStatistics` from `Chunk` to `Page`.

**Phase:** Phase 1.

---

### Pitfall 14: `StreamingMergeBuilder` API Stability Risk

**What goes wrong:** `StreamingMergeBuilder` is `pub` in DataFusion 52.1.0 but lives in `sorts::streaming_merge` which is an implementation detail of `SortPreservingMergeExec`. It could become `pub(crate)` in a future DataFusion version.

**Prevention:**
1. Pin DataFusion to `52.x`.
2. The builder is ~260 lines. If it becomes private, fork the implementation or wrap `SortPreservingMergeExec`.
3. Track DataFusion releases for breaking changes in the `sorts` module.

**Phase:** Ongoing maintenance concern.

**Confidence:** MEDIUM. The API is public today. DataFusion generally maintains backward compatibility within major versions.

---

## Phase-Specific Warnings

| Phase Topic | Likely Pitfall | Mitigation | Severity |
|-------------|---------------|------------|----------|
| Sort schema parser | Window boundary computation with negative timestamps (Pitfall 9) | Use `div_euclid`/`rem_euclid`, single canonical function, property-based tests | Critical |
| Sort schema parser | Sort instability at ingestion (Pitfall 8) | Add tiebreaker, verify Arrow 57.2.0 stability empirically | Moderate |
| Metadata extensions | PostgreSQL migration breaks existing queries (Pitfall 5) | Add columns with defaults, add indexes, test migration on production-scale data | Critical |
| Merge executor | `StreamingMergeBuilder` throughput insufficient (Pitfall 6) | Benchmark early, tune batch sizes, compare to stable sort | Moderate |
| Merge executor | Memory from `RowConverter` dictionary growth (Pitfall 1) | Monitor memory pool, low-cardinality sort columns mitigate | Moderate |
| Merge executor | Cancellation violates GAP-002 (Pitfall 3) | Actor model, CancellationToken, temp file cleanup | Critical |
| Merge publisher | Non-atomic publish (Pitfall 2) | Single-transaction publish, orphan GC, upload-then-publish | Critical |
| Merge publisher | Scope mismatch (Pitfall 11) | Pass scope from planner, canonical normalization, debug_assert | Moderate |
| Read-write conflicts | Query failures during compaction (Pitfall 4) | Generous GC grace period, retry on ObjectNotFound | Critical |
| Compaction policy | Late data churn (Pitfall 10) | Minimum merge threshold, batching, tighter acceptance window | Moderate |
| Resource management | Compaction starves ingestion/queries (Pitfall 7) | IO controls, spawn_blocking, concurrent merge limits | Critical |
| Stack maintenance | `StreamingMergeBuilder` API changes (Pitfall 14) | Pin version, ~260 lines to fork if needed | Minor |
| Parquet writer | Inconsistent WriterProperties (Pitfall 13) | Shared config | Minor |
| Column evolution | Schema explosion from column union (Pitfall 12) | Monitor, warn, prune in Phase 2 | Minor |

---

## Sources

### Internal (Codebase and ADRs)
- GAP-001: No Parquet Split Compaction (`docs/internals/adr/gaps/001-no-parquet-compaction.md`)
- GAP-002: Fixed Hardcoded Sort Schema (`docs/internals/adr/gaps/002-fixed-sort-schema.md`) -- notes sort instability
- GAP-003: No Time-Window Partitioning (`docs/internals/adr/gaps/003-no-time-window-partitioning.md`)
- GAP-004: Incomplete Split Metadata (`docs/internals/adr/gaps/004-incomplete-split-metadata.md`)
- GAP-005: No Per-Point Deduplication (`docs/internals/adr/gaps/005-no-per-point-deduplication.md`)
- GAP-009: No Leading Edge Prioritization (`docs/internals/adr/gaps/009-no-leading-edge-prioritization.md`)
- ADR-002: Sort Schema for Parquet Splits (`docs/internals/adr/002-sort-schema-parquet-splits.md`)
- ADR-003: Time-Windowed Sorted Compaction (`docs/internals/adr/003-time-windowed-sorted-compaction.md`)
- Verified: `datafusion-physical-plan-52.1.0/src/sorts/streaming_merge.rs` -- `StreamingMergeBuilder` API
- Verified: `datafusion-physical-plan-52.1.0/src/sorts/merge.rs` -- loser tree implementation
- Verified: `arrow-row-57.2.0/src/lib.rs` -- `RowConverter` API and dictionary handling
- [DataFusion RowConverter memory growth issue #7200](https://github.com/apache/datafusion/issues/7200)

### External
- [Handling Commit Conflicts in Apache Iceberg](https://www.ryft.io/blog/handling-commit-conflicts-in-apache-iceberg-patterns-and-fixes)
- [ClickHouse S3 Orphan Files Issue #54912](https://github.com/ClickHouse/ClickHouse/issues/54912)
- [WarpStream GC at Scale](https://www.warpstream.com/blog/taking-out-the-trash-garbage-collection-of-object-storage-at-massive-scale)
- [DataFusion loser tree PR #4301](https://github.com/apache/datafusion/pull/4301) -- 50% merge speedup
- [DataFusion tournament tree issue #4300](https://github.com/apache/datafusion/issues/4300)
- [Husky Storage Compaction Blog Post](https://www.datadoghq.com/blog/engineering/husky-storage-compaction/)
