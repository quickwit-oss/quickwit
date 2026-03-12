# Requirements: Pomsky Metrics Engine -- v0.8 Parquet Compaction

**Defined:** 2026-02-23
**Core Value:** High-volume metrics ingestion without backpressure -- the system must handle massive ingest throughput while maintaining query capability.

## v0.8 Requirements

Requirements for Parquet Compaction (Phase 1). Each maps to roadmap phases.

### Metadata & Schema

- [x] **META-01**: Sort schema configurable per metrics index using Husky-style string format (`metric_name|host|env|timeseries_id|timestamp/V2`) — reuse Husky's SortSchema/SortColumn/SortColumnDirection protos from dd-source (`event_store_sortschema.proto`), with Rust codegen via prost
- [x] **META-02**: `timeseries_id` hash tiebreaker column computed from all tag key/value pairs
- [x] **META-03**: MetricsSplitMetadata extended with `window_start`, `window_duration_secs`, `sort_schema`, `num_merge_ops`
- [x] **META-04**: PostgreSQL migration adding compaction columns to `metrics_splits` with composite index
- [x] **META-05**: `list_metrics_splits_for_compaction` metastore RPC scoped by `(index_id, window_start, sort_schema)`
- [x] **META-06**: Atomic replace semantics in `publish_metrics_splits` (staged + replaced in single transaction)
- [x] **META-07**: Self-describing Parquet files with sort_schema, window_start, min/max in key_value_metadata
- [x] **META-08**: Per-column statistics in MetricsSplitMetadata using Husky's RowKeys proto from dd-source (`event_store_sortschema.proto`) for sort-key min/max boundaries, plus zonemap regex for sort-schema columns -- computed at write time (ingestion and compaction), stored in PostgreSQL and Parquet key_value_metadata

### Ingestion

- [ ] **INGEST-01**: ParquetIndexer partitions rows by time window using configurable window duration (15-min default)
- [ ] **INGEST-02**: Sort-at-write by configured sort_schema within each window accumulator
- [ ] **INGEST-03**: Late data handling with configurable per-index acceptance window (4-hour default) beyond which data is rejected, and warning threshold (1-hour default) that emits metrics for late-but-accepted data
- [ ] **INGEST-04**: `compaction_start_time` cutoff for transition strategy (old data ages out, not rewritten)
- [ ] **INGEST-05**: Parquet column index and offset index enabled on all new splits

### Merge Policy

- [ ] **POLICY-01**: WindowedSortMergePolicy adapting StableLogMergePolicy for Parquet file sizes (bytes-based levels)
- [ ] **POLICY-02**: Configurable parameters: target size (default 256 MiB), max fanin, maturity age (default 48h)
- [ ] **POLICY-03**: Minimum merge threshold to prevent late-data re-compaction churn

### Merge Pipeline

- [ ] **PIPE-01**: ParquetMergePlanner with scope-aware candidate selection and inventory tracking
- [ ] **PIPE-02**: ParquetMergeSplitDownloader with S3 download and protect_zone for long operations
- [ ] **PIPE-03**: ParquetMergeExecutor using StreamingMergeBuilder for k-way sorted merge
- [ ] **PIPE-04**: Separate DataFusion memory pool for compaction to prevent OOM from killing the process
- [ ] **PIPE-05**: Merged MetricsSplitMetadata computation (union of metric names, tags, time ranges, per-column stats)
- [ ] **PIPE-06**: Schema evolution during merge -- column set union with null fill, explicit fail on type conflicts
- [ ] **PIPE-07**: ParquetMergePublisher with atomic publish and planner feedback loop
- [ ] **PIPE-08**: ParquetMergePipeline supervisor (health checks, kill switch, mailbox recycling)
- [ ] **PIPE-09**: IndexingService spawns ParquetMergePipeline for metrics indexes
- [x] **PIPE-10**: Canonical `window_start()` function using div_euclid/rem_euclid with proptest coverage

### Monitoring & Testing

- [ ] **OPS-01**: E2E test: ingest -> compact -> query correctness through HTTP/gRPC
- [ ] **OPS-02**: Compaction monitoring metrics (runs started/completed/failed, merge duration, split count per window)
- [ ] **OPS-03**: Janitor GC extension for metrics splits with MarkedForDeletion grace period
- [ ] **OPS-04**: Write amplification ratio and compression ratio monitoring
- [ ] **OPS-05**: Unit tests for each component -- merge policy, window_start canonical function, merge executor, sort schema parsing, metadata computation
- [ ] **OPS-06**: Property-based tests (proptest) for window boundary assignment, merge policy level selection, sort stability with tiebreaker
- [ ] **OPS-07**: Stateright exhaustive model checking for compaction state machine (ingest -> compact -> publish -> GC lifecycle)
- [ ] **OPS-08**: Implementation invariants matching TLA+ TimeWindowedCompaction.tla -- all 10 invariants (TW-1/2/3, CS-1/2/3, MC-1/2/3/4) enforced via debug_assert! and verified in tests
- [ ] **OPS-09**: DST crash-recovery test -- upload succeeded but publish not committed; verify no data loss or duplication

## Future Requirements

Deferred to subsequent milestones. Tracked but not in current roadmap.

### Phase 2: Cross-Node Compaction

- **XNODE-01**: Lift node_id constraint from compaction scope
- **XNODE-02**: m:n merge with output splitting across sort-key ranges
- **XNODE-03**: Affinity-based shard routing via consistent hashing

### Phase 3: Query Pruning

- **PRUNE-01**: Split-level query pruning using per-split min/max/regex metadata
- **PRUNE-02**: DataFusion query planner integration with split statistics

### Other Deferred

- **PERF-01**: Leading-edge compaction prioritization (recent windows first)
- **PERF-02**: Page-level streaming for non-sort columns during merge
- **PERF-03**: Compaction-time deduplication of identical (metric_name, tags, timestamp)
- **PERF-04**: Wire rate limiter into production MetricsIngester actor
- **PERF-05**: Performance benchmarking with production workloads

## Out of Scope

Explicitly excluded. Documented to prevent scope creep.

| Feature | Reason |
|---------|--------|
| Cross-node compaction | Requires coordination infrastructure not yet built (Phase 2) |
| m:n merge (split output across key ranges) | Requires sort-key-range tracking (Phase 2) |
| Split-level query pruning | Metadata recorded now, query planner changes deferred (Phase 3) |
| Ingest-time deduplication | Requires stateful bloom filter/dedup index; too complex for Phase 1 |
| Timeseries-per-row data model | Requires custom DataFusion operators; point-per-row sufficient |
| Custom merge policy from scratch | StableLogMergePolicy adaptation is proven; custom only if experiments show fundamental issues |
| Affinity-based shard routing | Orthogonal to compaction; Phase 1.5 after compaction is stable |
| Rewriting pre-existing unsorted data | Old data ages out via retention; compaction_start_time cutoff |
| PostgreSQL metadata store replacement | Design for portability but use PostgreSQL; self-describing files as safety net |
| Wide-table optimization | Research-phase idea only; post-Phase 4 |
| Compression validation experiment | Trust Husky data (25-33% improvement); skip experiment |
| Logs/traces compaction extension | Phase 4; sorted fast fields instead of Parquet columns |

## Traceability

Which phases cover which requirements. Updated during roadmap creation.

| Requirement | Phase | Status |
|-------------|-------|--------|
| META-01 | Phase 31 | Complete |
| META-02 | Phase 31 | Complete |
| META-03 | Phase 31 | Complete |
| META-04 | Phase 31 | Complete |
| META-05 | Phase 31 | Complete |
| META-06 | Phase 31 | Complete |
| META-07 | Phase 31 | Complete |
| META-08 | Phase 31 | Complete |
| INGEST-01 | Phase 32 | Pending |
| INGEST-02 | Phase 32 | Pending |
| INGEST-03 | Phase 32 | Pending |
| INGEST-04 | Phase 32 | Pending |
| INGEST-05 | Phase 32 | Pending |
| POLICY-01 | Phase 33 | Pending |
| POLICY-02 | Phase 33 | Pending |
| POLICY-03 | Phase 33 | Pending |
| PIPE-01 | Phase 35 | Pending |
| PIPE-02 | Phase 35 | Pending |
| PIPE-03 | Phase 34 | Pending |
| PIPE-04 | Phase 34 | Pending |
| PIPE-05 | Phase 34 | Pending |
| PIPE-06 | Phase 34 | Pending |
| PIPE-07 | Phase 35 | Pending |
| PIPE-08 | Phase 35 | Pending |
| PIPE-09 | Phase 35 | Pending |
| PIPE-10 | Phase 31 | Complete |
| OPS-01 | Phase 36 | Pending |
| OPS-02 | Phase 35 | Pending |
| OPS-03 | Phase 35 | Pending |
| OPS-04 | Phase 35 | Pending |
| OPS-05 | Phase 34 | Pending |
| OPS-06 | Phase 32 | Pending |
| OPS-07 | Phase 36 | Pending |
| OPS-08 | Phase 36 | Pending |
| OPS-09 | Phase 36 | Pending |

**Coverage:**
- v0.8 requirements: 35 total
- Mapped to phases: 35
- Unmapped: 0

---
*Requirements defined: 2026-02-23*
*Last updated: 2026-02-23 after roadmap creation*
