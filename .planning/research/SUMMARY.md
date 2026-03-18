# Project Research Summary

**Project:** Pomsky Metrics Engine -- Time-Windowed Sorted Parquet Compaction
**Domain:** Observability metrics storage, LSM-style sorted compaction over Parquet on S3
**Researched:** 2026-02-23
**Confidence:** HIGH

## Executive Summary

Time-windowed sorted Parquet compaction for metrics is a well-understood domain with strong precedent across ClickHouse, Husky (Datadog's internal store), Prometheus/Mimir, Iceberg, and RocksDB. The Pomsky approach -- size-tiered merge policy scoped to `(index_id, window_start, sort_schema)` triples, k-way merge preserving sort order, atomic metastore publish -- is the canonical pattern used by every production compaction system. The compression improvement (20-35% expected; Husky validated 25-33% for logs/APM) is real, but it has not been validated for metrics data specifically. That validation experiment must run first. The implementation path is fully specified by ADR-003, GAP-001 through GAP-010, and the Phase 1 design doc. The critical stack finding is that **zero new external dependencies are needed**: DataFusion 52.1.0 (already in the workspace, not 45 as the milestone context stated) provides `StreamingMergeBuilder`, a public loser-tree-based k-way merge API that directly resolves the ADR-003 open questions about merge algorithm and sort key encoding strategy.

The recommended build order flows strictly from data dependencies: metadata extensions first (every downstream component reads `window_start`, `sort_schema`, `num_merge_ops`), then ingestion-time window enforcement (compaction needs pre-sorted window-scoped inputs), then the merge policy (pure algorithm, no actor dependencies), then the merge executor in isolation (most complex, must be benchmarked before wiring), then the full actor pipeline, then integration tests and monitoring. The 5 new compaction actors mirror the existing Tantivy `MergePipeline` structure exactly -- this is the strongest architectural recommendation: copy the proven supervision model (mailbox recycling, kill switch hierarchy, 1-second health-check loop, `protect_zone` for S3 operations), replace only the format-specific internals.

The top operational risk is memory exhaustion during k-way merge. `StreamingMergeBuilder` provides the primary mitigation by streaming (O(N_inputs x batch_size) memory rather than O(total_rows)), but the `RowConverter`'s `OrderPreservingInterner` can grow for high-cardinality dictionary columns (DataFusion issue #7200). Sort columns in this system are low-to-medium cardinality (metric names, service, env, host), so the risk is manageable -- but a dedicated DataFusion memory pool must be provisioned so exhaustion fails the merge gracefully rather than killing the process. The second major risk is correctness of the atomic publish: the single-transaction PostgreSQL replace (`staged_split_ids` + `replaced_split_ids` together) and the janitor GC grace period for `MarkedForDeletion` splits must be implemented from day one, not retrofitted.

## Key Findings

### Recommended Stack

The entire compaction pipeline is buildable from the existing workspace dependency graph with zero new external crates. The workspace pins DataFusion 52.1.0, Arrow 57.2.0, and Parquet 57.2.0 -- materially newer than the milestone context suggested (DataFusion 45 / Arrow 54 / Parquet 54). All APIs were verified against local source (`datafusion-physical-plan-52.1.0/src/sorts/streaming_merge.rs`, `arrow-row-57.2.0/src/lib.rs`, `parquet-57.2.0/src/file/properties.rs`).

See `.planning/research/STACK.md` for full API signatures, alternatives considered, and version pinning strategy.

**Core technologies:**

- `StreamingMergeBuilder` (datafusion-physical-plan 52.1.0) -- k-way sorted merge for the executor; loser tree with O(log K) comparisons per element (~50% faster than binary heap); Arrow row format for multi-column comparison; streaming bounded memory; resolves ADR-003 open questions on merge algorithm and sort key comparison strategy; use `with_round_robin_tie_breaker(false)` for deterministic output
- `arrow::row::RowConverter` (arrow-row 57.2.0, transitive via `arrow`) -- memcmp-comparable row encoding for multi-column sort keys; used internally by `StreamingMergeBuilder` via `RowCursorStream`; also use directly for ingestion-time sort; caveat: `OrderPreservingInterner` grows with high-cardinality dictionary columns (DataFusion issue #7200)
- `ParquetRecordBatchStreamBuilder` (parquet 57.2.0) -- async streaming Parquet reader; integrates with `object_store`; required because S3 reads are async; compatibility as `SendableRecordBatchStream` needs verification at implementation time
- `ArrowWriter` + `WriterProperties` (parquet 57.2.0) -- output writer; regenerates bloom filters and statistics from written data automatically; set `EnabledStatistics::Page` (not `Chunk`) and `set_sorting_columns` for compaction output
- `quickwit-actors` (workspace) -- actor framework for the 5-actor merge pipeline; supervision, kill switch hierarchy, mailbox recycling, `protect_zone` for S3 operations
- PostgreSQL via sqlx 0.8 -- metastore for atomic split replacement; single-transaction `publish_metrics_splits` with both `staged_split_ids` and `replaced_split_ids` in `run_with_tx!` provides atomicity

**Critical version note:** `StreamingMergeBuilder` is `pub` in DataFusion 52.1.0 but lives in `sorts::streaming_merge`, an implementation detail of `SortPreservingMergeExec`. Pin DataFusion to `52.x`. The builder is ~260 lines; if it goes private in a future version, fork the implementation.

### Expected Features

The feature landscape contains 12 table stakes, 9 differentiators, and 10 explicit anti-features. See `.planning/research/FEATURES.md` for full cross-system comparison against ClickHouse, Husky, Prometheus/Mimir, Iceberg, Delta Lake, Hudi, and RocksDB.

**Must have (table stakes -- required for production correctness):**

- Time-window partitioning at ingestion (TS-1) -- prerequisite for merge scoping; without this, compaction scope is unbounded
- Late data acceptance window (TS-2) -- configurable cutoff (1 hour default); bounds active window count, prevents perpetual re-compaction of sealed windows
- Merge planner with scope-aware candidate selection (TS-3) -- groups splits by `(index_id, window_start, sort_schema)`; drives the entire pipeline
- Size-tiered merge policy (TS-4) -- adapts `StableLogMergePolicy` for Parquet file sizes (bytes-based levels, not doc count); prevents unbounded split count growth
- Sorted merge execution (TS-5) -- k-way merge preserving sort order; transforms compaction from "file cleanup" into "query optimization"
- Downloader, uploader, atomic publisher (TS-6, TS-7, TS-8) -- mechanical but correctness-critical; atomic publish is non-negotiable
- Metadata extensions (TS-9) -- `window_start`, `window_duration_secs`, `sort_schema`, `num_merge_ops` on `MetricsSplitMetadata`; required by planner, executor, and future query pruning
- Pre-existing data transition strategy (TS-10) -- `compaction_start_time` cutoff; old unsorted data ages out via retention, is not rewritten
- Basic compaction monitoring (TS-11) -- split count per window, runs started/completed/failed, merge duration p50/p99
- Schema evolution during merge (TS-12) -- column set union across inputs; fills nulls for missing columns; explicit fail on type conflicts

**Should have (differentiators -- build in Phase 1 where low complexity):**

- Configurable sort schema per index (D-6) -- low complexity, partially exists as hardcoded sort; complete during ingestion phase
- `timeseries_id` tiebreaker column (D-7) -- hash of all tag key/value pairs appended to sort schema; clusters same-source data, reduces equal-key frequency, addresses sort instability risk
- Self-describing Parquet files (D-3) -- embed `sort_schema`, `window_start`, min/max in Parquet `key_value_metadata`; low complexity, high operational value for debugging and disaster recovery
- Parquet column index / offset index enablement (D-4) -- enable `EnabledStatistics::Page` at write time; required for sort order to enable intra-file pruning; low complexity
- RLE merge order representation (D-1) -- sorted inputs produce long contiguous runs; build into merge executor from the start, not retrofitted
- Compression validation experiment (D-9) -- sort existing metrics Parquet files, measure size reduction; existential validation before pipeline build

**Defer (v2+):**

- Page-level streaming for non-sort columns (D-8) -- high complexity; `StreamingMergeBuilder` at `RecordBatch` granularity is sufficient for Phase 1
- Leading-edge compaction prioritization (D-2) -- medium complexity; correct but not required for initial correctness
- Compaction-time deduplication (D-5) -- optional; add after pipeline is stable and validated
- Cross-node compaction (AF-1 lifted) -- Phase 2; requires coordination infrastructure not yet in scope
- m:n merge with output splitting (AF-2) -- Phase 2; requires sort-key-range tracking

### Architecture Approach

The compaction pipeline introduces 5 new actors that mirror the existing Tantivy `MergePipeline` in `quickwit-indexing/src/actors/merge_pipeline.rs` exactly, replacing only the format-specific internals. The supervision model, scheduling model (shared `MergeSchedulerService`), planner-publisher feedback loop, mailbox recycling pattern, kill switch hierarchy, inventory tracking, and `protect_zone` usage for S3 operations are all copied directly. This is not a shortcut -- it is the correct design: the actor framework provides cancellation safety, supervision, and backpressure that a compaction pipeline requires.

The `MetricsIndexer` (`ParquetIndexer`) requires modification to partition rows by time window before writing, maintaining one `HashMap<window_start, accumulator>` (at most 2-3 active windows given a 1-hour late data window and 60-second commit timeout). Each window's accumulator sorts by the configured `sort_schema` at write time, so every split enters the compaction pipeline pre-sorted.

See `.planning/research/ARCHITECTURE.md` for full component boundaries, step-by-step data flow, integration points, and the complete list of new files to create vs existing files to modify.

**Major components:**

1. `MetricsIndexer` (modified) -- window-partitioned accumulator; sort-at-write; late data rejection; produces pre-sorted window-scoped splits
2. `WindowedSortMergePolicy` -- adapts `StableLogMergePolicy` for Parquet file sizes; maturity by size (256 MiB target) or age (48 hours); pure logic, no actor dependencies
3. `ParquetMergePlanner` -- queries metastore for immature published splits; groups by compaction scope; calls policy; uses inventory tracking to prevent duplicate scheduling; `QueueCapacity::Bounded(1)`
4. `ParquetMergeSplitDownloader` -- downloads Parquet files from S3 to scratch directory via raw `Storage::copy_to_file`; uses `protect_zone` for long S3 operations
5. `ParquetMergeExecutor` -- opens each file as `ParquetRecordBatchStreamBuilder`; feeds streams to `StreamingMergeBuilder`; writes sorted output with `ArrowWriter`; runs on `RuntimeType::Blocking`
6. `ParquetMergeUploader` -- reuses existing `ParquetUploader` with `UploaderType::MergeUploader`; `replaced_split_ids` populated, `checkpoint_delta_opt` is `None`
7. `ParquetMergePublisher` -- single-transaction `publish_metrics_splits` with both `staged_split_ids` and `replaced_split_ids`; sends `NewMetricsSplits` feedback to planner
8. `ParquetMergePipeline` -- supervisor; 1-second health-check loop; kill switch hierarchy; mailbox recycling; shares `MergeSchedulerService` with Tantivy pipeline

### Critical Pitfalls

See `.planning/research/PITFALLS.md` for all 14 pitfalls with phase warnings, prevention strategies, and detection signals. Top 5:

1. **Memory exhaustion during k-way merge (Critical)** -- Use `StreamingMergeBuilder` (streaming, O(N x batch_size) memory not O(total_rows)); provision a separate DataFusion memory pool for compaction so exhaustion fails the merge gracefully; monitor `RowConverter` dictionary memory for sort columns at runtime
2. **Non-atomic metadata update causes data duplication or loss (Critical)** -- Use single-transaction `publish_metrics_splits` with both `staged_split_ids` and `replaced_split_ids`; always upload to S3 before publishing to PostgreSQL; extend `quickwit-janitor` GC to handle orphaned metrics splits
3. **Cancellation violates GAP-002 invariants (Critical)** -- FORBID `tokio::sync::Mutex` in all compaction code; use actor mailbox model for all mutable state; `CancellationToken` for cooperative shutdown; clean scratch directory on startup
4. **Queries see inconsistent split state during compaction (Critical)** -- `MarkedForDeletion` grace period must be at least 2x maximum query timeout before S3 deletion; this is the existing Quickwit pattern, extend it to metrics splits
5. **Window boundary edge cases produce wrong window assignment (Critical)** -- single canonical `window_start(t: i64, duration: u32) -> i64` function using `div_euclid`/`rem_euclid`; Rust `%` preserves sign for negative dividends and will silently assign to the wrong window; property-based tests with `proptest`; `debug_assert` in split writer

## Implications for Roadmap

The build order is driven by data dependencies. Each phase produces artifacts the next phase consumes. There is no ambiguity about sequencing.

### Phase 1: Compression Validation Experiment

**Rationale:** D-9 from FEATURES.md -- the entire project's value proposition rests on sorted data compressing 20-35% better. Run this before investing in the full pipeline. Cost is low (sort existing files, measure). Husky validated 25-33% but for logs/APM data. If improvement on real metrics data is below 10%, the cost-benefit changes and scope needs reassessment.

**Delivers:** Empirical measurement of sorted vs unsorted compression ratio on real metrics Parquet data; go/no-go signal for the full pipeline; baseline for production compression ratio monitoring.

**Addresses:** D-9 (compression validation)

**Research flag:** Standard -- script-level work. Sort existing files using `lexsort_to_indices`, rewrite, measure sizes.

### Phase 2: Metadata Foundation

**Rationale:** TS-9 from FEATURES.md -- every downstream component reads `window_start`, `sort_schema`, and `num_merge_ops`. Without these fields the planner cannot scope merges and the executor cannot determine sort order. Zero actor changes in this phase.

**Delivers:** Extended `MetricsSplitMetadata` with `window_start`, `window_duration_secs`, `sort_schema`, `num_merge_ops`; PostgreSQL migration adding columns with defaults and composite index on `(index_id, split_state, window_start)`; `list_metrics_splits_for_compaction` metastore RPC; replace semantics in `publish_metrics_splits` (single-transaction, Pitfall 2 foundation).

**Addresses:** TS-9, TS-8 (atomic publish foundation)

**Avoids:** Pitfall 2 (non-atomic publish -- transaction semantics established here), Pitfall 5 (PostgreSQL scalability -- composite index added here), Pitfall 11 (scope mismatch -- canonical scope key defined here)

**Research flag:** Standard -- PostgreSQL migration and metastore RPC extension follow existing codebase patterns.

### Phase 3: Ingestion-Time Window Enforcement

**Rationale:** TS-1, TS-2 from FEATURES.md. The compaction pipeline needs pre-sorted, window-scoped input splits. Without this, the merge executor would need a full sort (O(R log R)) instead of a k-way merge (O(R log K)), and cross-window data cannot be handled. Depends on Phase 2 metadata fields being present.

**Delivers:** Window-aware accumulator in `ParquetIndexer` (`HashMap<window_start, ParquetBatchAccumulator>`); sort-at-write using configured `sort_schema`; late data rejection at `late_data_acceptance_window` boundary; `timeseries_id` tiebreaker column in sort schema; Parquet column index and offset index enabled on all new splits; `compaction_start_time` cutoff for transition strategy.

**Addresses:** TS-1, TS-2, TS-10, D-4 (column index), D-6 (configurable sort schema), D-7 (timeseries_id tiebreaker)

**Avoids:** Pitfall 8 (sort instability -- tiebreaker reduces equal-key frequency), Pitfall 9 (window boundary edge cases -- canonical `window_start()` function required here), Pitfall 10 (late data churn -- acceptance window limits re-compaction)

**Research flag:** Standard for `ParquetIndexer` modification. Pitfall 9 (window boundary arithmetic with negative timestamps) needs property-based test coverage; this is a concrete day-one risk.

### Phase 4: Merge Policy

**Rationale:** The planner needs a policy before it can generate merge operations. `WindowedSortMergePolicy` is pure logic with no actor or I/O dependencies -- develop, test with proptest, and validate in isolation before wiring into actors.

**Delivers:** `WindowedSortMergePolicy` adapting `StableLogMergePolicy` for Parquet file sizes (bytes-based levels, not document count); maturity model by size (256 MiB target) or age (48 hours); minimum merge threshold to prevent late-data churn; unit tests with proptest.

**Addresses:** TS-4 (size-tiered merge policy)

**Avoids:** Pitfall 10 (late data churn -- minimum merge threshold configured here)

**Research flag:** Standard -- `StableLogMergePolicy` is the template; direct adaptation.

### Phase 5: Merge Executor

**Rationale:** The most complex new component. Develop and benchmark in isolation before wiring into the actor pipeline. Must validate that `StreamingMergeBuilder` provides sufficient throughput (Pitfall 6) and acceptable memory behavior (Pitfall 1) on representative data before committing to the full pipeline.

**Delivers:** `ParquetMergeExecutor` actor (standalone, testable); k-way sorted merge using `StreamingMergeBuilder`; separate DataFusion memory pool (`MemoryConsumer::new("parquet_merge_executor")`); merged `MetricsSplitMetadata` computation (union of metric names, tags, time ranges); output files with `EnabledStatistics::Page`, `set_sorting_columns`, `key_value_metadata` (D-3 self-describing files); benchmark comparing `StreamingMergeBuilder` vs `concat_batches + lexsort_to_indices` on 8-16 pre-sorted splits of 500K rows.

**Addresses:** TS-5 (sorted merge execution), TS-12 (schema evolution -- column set union), D-1 (RLE merge order), D-3 (self-describing Parquet files)

**Uses:** `StreamingMergeBuilder` (DataFusion 52.1.0), `ParquetRecordBatchStreamBuilder`, `ArrowWriter` + `WriterProperties` with page-level statistics

**Avoids:** Pitfall 1 (memory exhaustion -- separate pool, streaming O(N x batch_size)), Pitfall 3 (GAP-002 cancellation -- `RuntimeType::Blocking`, actor model), Pitfall 6 (throughput -- benchmarked here), Pitfall 13 (WriterProperties mismatch -- shared `ParquetWriterConfig`)

**Research flag:** Needs implementation-time verification of `ParquetRecordBatchStream` as `SendableRecordBatchStream` -- a thin adapter may be needed. `StreamingMergeBuilder` standalone setup (`BaselineMetrics` + `ExecutionPlanMetricsSet` outside a query plan) requires verification. If benchmark shows `StreamingMergeBuilder` is 2x+ slower than stable sort for the compaction use case, a `concat_batches + lexsort_to_indices` fallback path needs evaluation.

### Phase 6: Actor Pipeline

**Rationale:** With the executor and policy tested, wire all remaining actors into the full pipeline following the Tantivy `MergePipeline` supervision pattern.

**Delivers:** `ParquetMergePlanner` (incarnation handling, inventory tracking, `QueueCapacity::Bounded(1)`); `ParquetMergeSplitDownloader` (S3 download with `protect_zone`); `ParquetMergePublisher` (single-transaction publish with replace semantics and planner feedback loop); `ParquetMergePipeline` supervisor (1-second health-check, kill switch hierarchy, mailbox recycling); `IndexingService` modification to spawn `ParquetMergePipeline` for metrics indexes; janitor GC extension for metrics splits.

**Addresses:** TS-3 (merge planner), TS-6 (downloader), TS-7 (uploader via reused `ParquetUploader`), TS-8 (atomic publisher)

**Avoids:** Pitfall 3 (GAP-002 -- actor model, no `tokio::sync::Mutex`), Pitfall 2 (non-atomic publish -- single transaction), Pitfall 4 (read-write conflicts -- GC grace period configured here), Pitfall 11 (scope mismatch -- scope passed from planner, not re-derived downstream)

**Research flag:** Standard -- direct structural copy of `MergePipeline` with Parquet internals substituted. Well-specified in ARCHITECTURE.md.

### Phase 7: Integration Testing and Monitoring

**Rationale:** All components exist. Verify end-to-end correctness and operational observability before production rollout. Tests must go through HTTP/gRPC stack per CLAUDE.md policy.

**Delivers:** E2E test: ingest -> compact -> query correctness through HTTP/gRPC; DST crash-recovery test (upload succeeded, publish not committed); compaction monitoring metrics (TS-11: runs started/completed/failed, split count per window, merge duration p50/p99, write amplification ratio, compression ratio validated against Phase 1 experiment, backlog count); Grafana dashboard; `quickwit-janitor` GC validated for metrics splits.

**Addresses:** TS-11 (monitoring), GC correctness

**Avoids:** Pitfall 4 (read-write conflicts -- janitor GC grace period validated here), Pitfall 5 (PostgreSQL scalability -- planner query latency monitored)

**Research flag:** Standard for monitoring patterns. Reference Mimir compactor dashboard for metric naming conventions. DST crash-recovery test requires careful setup but follows existing DST workflow.

### Phase Ordering Rationale

- Phase 1 (experiment) before Phase 2 (metadata) because a negative compression result changes project scope, and the experiment costs days not weeks
- Phase 2 (metadata) before Phase 3 (ingestion) because `window_start` and `sort_schema` fields must exist in the schema before the indexer can populate them
- Phase 2 (metadata) before Phase 4 (policy) because the policy operates on `MetricsSplitMetadata` types
- Phase 3 (ingestion) before Phase 5 (executor) because the executor needs real pre-sorted window-scoped splits to test against; synthesizing them artificially is error-prone
- Phase 4 (policy) is order-independent relative to Phase 3; Phases 3 and 4 can proceed in parallel
- Phase 5 (executor) before Phase 6 (actors) because the executor is the highest-complexity component and must be validated in isolation; wiring a buggy executor into the full pipeline makes debugging harder
- Phase 6 (actors) before Phase 7 (integration) by definition

### Research Flags

Phases needing deeper research during planning:

- **Phase 5 (Merge Executor):** `ParquetRecordBatchStream` to `SendableRecordBatchStream` compatibility needs implementation-time verification. Standalone `StreamingMergeBuilder` setup (outside a DataFusion query plan) needs validation. If throughput benchmark shows streaming merge is 2x+ slower than stable sort, evaluate fallback path.

Phases with standard patterns (no additional research needed):

- **Phase 1 (Experiment):** Script-level. Sort files, measure sizes.
- **Phase 2 (Metadata):** PostgreSQL migration and metastore RPC. Direct copy of existing patterns.
- **Phase 3 (Ingestion):** `ParquetIndexer` modification. Pattern is clear; requires test coverage, not research. Pitfall 9 (window boundary arithmetic) is a known concrete risk, not an unknown.
- **Phase 4 (Policy):** Direct adaptation of `StableLogMergePolicy`. Algorithm is fully specified in ADR-003.
- **Phase 6 (Actors):** Direct structural copy of `MergePipeline`. Specified in ARCHITECTURE.md.
- **Phase 7 (Integration):** Standard testing and monitoring. Reference Mimir compactor dashboard.

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Stack | HIGH | All APIs verified against local source (`datafusion-physical-plan-52.1.0`, `arrow-row-57.2.0`, `parquet-57.2.0`). Zero new dependencies confirmed. `StreamingMergeBuilder` is `pub` in current version. |
| Features | HIGH | Internal design docs (ADR-003, GAP-001 through GAP-010, Phase 1 doc, TLA+ spec with 10 invariants) are authoritative. External systems (ClickHouse, Husky, Mimir, Iceberg, Delta Lake, RocksDB) provide strong cross-validation for all 12 table stakes and 9 differentiators. |
| Architecture | HIGH | Based on direct codebase analysis of all existing merge actor files (`merge_pipeline.rs`, `merge_planner.rs`, `merge_executor.rs`, `publisher.rs`, `merge_scheduler_service.rs`, `parquet_indexer.rs`, `parquet_uploader.rs`, `parquet_publisher.rs`). Component boundaries and data flow are grounded in actual code, not speculation. |
| Pitfalls | HIGH (critical 7), MEDIUM (operational 4), LOW (maintenance 3) | Critical pitfalls are concrete and well-understood. Operational pitfalls (PostgreSQL scalability thresholds, `StreamingMergeBuilder` throughput vs stable sort) have MEDIUM confidence because they depend on real data characteristics that must be measured empirically. |

**Overall confidence:** HIGH

### Gaps to Address

- **Compression validation for metrics data (gate for entire project):** Husky numbers (25-33%) are for logs/APM data. Real metrics Parquet files may compress differently. Phase 1 experiment resolves this. If below 10% improvement, project scope needs reassessment.

- **`StreamingMergeBuilder` vs stable sort throughput for compaction workload:** ADR-003 notes Husky found stable sort faster in Go due to Timsort's cache locality on presorted runs. DataFusion's loser tree + Arrow row format changes the comparison, but must be benchmarked empirically in Phase 5 on representative data (8-16 pre-sorted splits of 500K rows). If streaming merge is 2x+ slower, implement `concat_batches + lexsort_to_indices` fallback for small fan-in merges.

- **`RowConverter` dictionary memory growth:** DataFusion issue #7200 documents that `OrderPreservingInterner` accumulates dictionary mappings over a merge. For low-cardinality sort columns (metric names, service, env, host) this is likely safe but must be monitored during Phase 5 benchmarks with production-scale data volumes.

- **`ParquetRecordBatchStream` / `SendableRecordBatchStream` adapter:** The type system compatibility between the Parquet async reader output and `StreamingMergeBuilder`'s `Vec<SendableRecordBatchStream>` input needs verification at implementation time. May require a thin wrapper implementing `RecordBatchStream + Send`.

- **PostgreSQL metadata scalability ceiling:** Phase 2 adds composite index `(index_id, split_state, window_start)`. Monitor planner query latency in Phase 7. If planner query exceeds 1 second consistently, Phase 2's metastore RPC may need pagination or a dedicated metadata service is needed as a future project.

- **`doc_mapping_uid` vs `sort_schema` in compaction scope:** The Tantivy merge scope includes `doc_mapping_uid` as a disambiguation key. The Parquet compaction scope uses `sort_schema` for the same purpose. Whether these are redundant or complementary needs clarification during Phase 2 scope key definition.

## Sources

### Primary (HIGH confidence -- verified against local source)

- `datafusion-physical-plan-52.1.0/src/sorts/streaming_merge.rs` -- `StreamingMergeBuilder` public API confirmed
- `datafusion-physical-plan-52.1.0/src/sorts/merge.rs` -- loser tree implementation, ~50% faster than binary heap
- `arrow-row-57.2.0/src/lib.rs` -- `RowConverter` API, dictionary hydration, `OrderPreservingInterner` memory growth
- `arrow-select-57.2.0/src/interleave.rs` -- `interleave_record_batch` API
- `parquet-57.2.0/src/file/properties.rs` -- `WriterProperties`, `EnabledStatistics::Page`, `set_sorting_columns`, bloom filter config
- `quickwit-indexing/src/actors/merge_pipeline.rs` -- supervision pattern, spawn order, health check, mailbox recycling
- `quickwit-indexing/src/actors/merge_planner.rs` -- incarnation handling, inventory tracking
- `quickwit-indexing/src/actors/merge_executor.rs` -- error handling pattern, `RuntimeType::Blocking`
- `quickwit-indexing/src/actors/publisher.rs` -- merge planner feedback loop
- `quickwit-indexing/src/actors/merge_scheduler_service.rs` -- shared scheduler, priority queue, semaphore
- `quickwit-parquet-engine/src/split/metadata.rs` -- `MetricsSplitMetadata` current fields
- `docs/internals/adr/003-time-windowed-sorted-compaction.md` -- primary design specification
- `docs/internals/adr/gaps/001-010` -- confirmed gaps being addressed by this project

### Secondary (HIGH confidence -- well-documented, authoritative external sources)

- [Husky: Efficient compaction at Datadog scale](https://www.datadoghq.com/blog/engineering/husky-storage-compaction/) -- 25-33% compression improvement validated, size-tiered + locality, RLE merge order
- [DataFusion loser tree PR #4301](https://github.com/apache/datafusion/pull/4301) -- 50% merge speedup over binary heap
- [Arrow Row Format Blog Post](https://arrow.apache.org/blog/2022/11/07/multi-column-sorts-in-arrow-rust-part-1/) -- multi-column sort performance rationale
- [ClickHouse MergeTree Architecture](https://deepwiki.com/ClickHouse/ClickHouse/3.1-mergetree-engine-architecture) -- merge selection, monitoring patterns
- [Mimir Split-and-Merge Compactor](https://grafana.com/blog/how-grafana-mimirs-split-and-merge-compactor-enables-scaling-metrics-to-1-billion-active-series/) -- compactor monitoring, dashboard reference
- [DataFusion RowConverter memory growth issue #7200](https://github.com/apache/datafusion/issues/7200) -- documented dictionary memory risk
- [RocksDB Universal Compaction](https://github.com/facebook/rocksdb/wiki/universal-compaction) -- size-tiered strategy reference
- [LSM Compaction Design Space (VLDB 2021)](https://vldb.org/pvldb/vol14/p2216-sarkar.pdf) -- academic tradeoff analysis

### Tertiary (MEDIUM confidence -- needs empirical validation)

- Compression improvement for metrics data specifically: Husky validated for logs/APM; metrics data characteristics may differ
- `StreamingMergeBuilder` throughput vs stable sort for compaction: no direct benchmark for Rust + Arrow row format in the compaction use case
- PostgreSQL `metrics_splits` scalability thresholds: estimates based on assumed split sizes; actual thresholds require benchmarking

---
*Research completed: 2026-02-23*
*Ready for roadmap: yes*
