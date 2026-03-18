# Pomsky Metrics Engine

## What This Is

A high-throughput metrics engine for Quickwit that replaces Tantivy with DataFusion/Arrow for metrics workloads. Uses Parquet for storage efficiency and DataFusion's columnar query execution for fast aggregations. Features automatic pipeline routing, time-series aggregations, and full integration with Quickwit's actor system. Logs and traces remain on Tantivy.

## Core Value

High-volume metrics ingestion without backpressure — the system must handle massive ingest throughput while maintaining query capability.

## Requirements

### Validated

- ✓ OTLP ingestion (logs, traces, metrics) via REST/gRPC — existing
- ✓ Arrow IPC support for metrics batching — existing (`quickwit-opentelemetry/src/otlp/arrow_metrics.rs`)
- ✓ Multi-cloud storage backends (S3, Azure, GCS, local) — existing
- ✓ Tantivy search engine for logs/traces — existing (unchanged)
- ✓ Distributed cluster with gossip protocol — existing
- ✓ PostgreSQL metastore for cluster metadata — existing
- ✓ DataFusion query execution engine for metrics — v0.1
- ✓ Parquet file storage for metrics (replace Tantivy segments) — v0.1
- ✓ Metrics-specific split format (Arrow/Parquet instead of Tantivy) — v0.1
- ✓ Direct Arrow-to-Parquet write path (bypass Tantivy conversion) — v0.1
- ✓ Aggregation queries via DataFusion (sum, avg, min, max, count) — v0.1
- ✓ Time-range partitioning optimized for metrics access patterns — v0.1
- ✓ Pipeline integration with automatic routing by index name — v0.1
- ✓ Search integration with DataFusion SQL execution — v0.1
- ✓ Prometheus metrics and tracing instrumentation — v0.1
- ✓ Actor-based indexing pipeline (MetricsDocProcessor → MetricsIndexer → MetricsUploader → MetricsPublisher) — v0.2
- ✓ Parquet optimization with bloom filters and row group statistics — v0.2
- ✓ MetricsSplitMetadata with two-tier tag storage for efficient pruning — v0.2
- ✓ PostgreSQL metrics_splits table with GIN indexes for Tier 1 pruning — v0.2
- ✓ Metastore CRUD operations (stage/publish/list/mark/delete) for metrics splits — v0.2
- ✓ MetricsWal with queue-per-shard durability architecture — v0.3
- ✓ MetricsShardPosition and MetricsIngesterState with two-phase locking — v0.3
- ✓ MetricsShardPositionsService for cluster-wide position gossip via chitchat — v0.3
- ✓ MetricsIngestError with transient vs permanent error classification — v0.3
- ✓ MetricsShardRateLimiter with per-shard token bucket rate limiting — v0.3
- ✓ DataFusion object_store integration for S3/MinIO Parquet reads — v0.6
- ✓ StorageObjectStoreAdapter bridging quickwit_storage to DataFusion — v0.6
- ✓ MetricsSessionContext::with_storage() for remote storage backends — v0.6
- ✓ URI-based Parquet file resolution in MetricsTableProvider — v0.6
- ✓ Dedicated `/api/v1/{index}/metrics/query` endpoint for SQL queries — v0.6
- ✓ E2E tests validating ingest → S3 → query → results pipeline — v0.6

### Active

## Current Milestone: v0.8 Parquet Compaction (Phase 1)

**Goal:** Node-local time-windowed sorted compaction for Parquet metrics splits — eliminate unbounded split accumulation and improve compression through sorted storage.

**Target features:**
- Sort schema implementation with configurable per-index sort order and `timeseries_id` hash tiebreaker
- Time-window partitioning at ingestion (15-min default, configurable 1-60 min)
- Split metadata extensions (`window_start`, `window_duration_secs`, `sort_schema`, per-column statistics)
- Parquet merge pipeline — 5 new actors (Planner, Downloader, Executor, Uploader, Publisher)
- K-way sorted merge algorithm for merging splits within a window
- Late data acceptance window (1-hour default, configurable)
- Configurable merge policy parameters (target size, fanin, maturity)
- PostgreSQL migration for metadata extensions

**Closes gaps:** GAP-001 (no Parquet compaction), GAP-003 (no time-window partitioning), GAP-004 (incomplete split metadata)

### Deferred

- [ ] Wire rate limiter into production MetricsIngester actor
- [ ] Performance benchmarking with production workloads
- [ ] Distributed query execution with split-to-node assignment (ClickHouse integration)
- [ ] Multi-node query execution validation (ClickHouse integration)
- [ ] Cross-node compaction (Phase 2)
- [ ] Query pruning via min/max/regex metadata (Phase 3)
- [ ] Metadata caching for Parquet footers (GAP-007)
- [ ] Multi-level caching — columnar data, predicate, result (GAP-010)
- [ ] Logs/traces compaction extension (Phase 4)

### Out of Scope

- Logs/traces migration — Keep Tantivy for logs and traces, only replace metrics engine
- Backwards compatibility with existing Tantivy metrics indices — New format only
- Cross-node compaction — Requires coordination protocol, deferred to Phase 2
- Compression validation experiment — Trust Husky data (25-33% improvement expected)

## Context

The current architecture already has Arrow IPC support for metrics batching (`arrow_metrics.rs`, `arrow_to_tantivy.rs`). The existing flow converts Arrow batches to Tantivy documents, which is inefficient for metrics workloads. This project removes the Tantivy conversion step and writes directly to Parquet files, using DataFusion for query execution.

Key existing files to modify/replace:
- `quickwit-opentelemetry/src/otlp/arrow_to_tantivy.rs` — Currently converts Arrow to Tantivy
- `quickwit-indexing/src/actors/doc_processor.rs` — Routes to different indexing paths
- `quickwit-indexing/src/actors/indexer.rs` — Tantivy-specific indexing
- `quickwit-search/src/leaf.rs` — Tantivy-specific search execution

The codebase already uses Apache Arrow 54 and has Arrow IPC infrastructure in place.

## Constraints

- **Tech Stack**: Must use DataFusion (Apache Arrow ecosystem) — aligns with existing Arrow usage
- **Architecture**: Must integrate with existing actor-based pipeline — no architectural rewrites
- **Compatibility**: Must coexist with Tantivy for logs/traces — same cluster, different engines per signal type

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Full Tantivy replacement for metrics | Query performance + storage efficiency justify clean break | ✓ Good |
| DataFusion over custom query engine | Mature, well-maintained, Arrow-native | ✓ Good |
| Parquet storage format | Industry standard, excellent compression, DataFusion-native | ✓ Good |
| Logs/traces stay on Tantivy | Full-text search requirements differ from metrics | ✓ Good |
| DataFusion 52.1 / Arrow 57.2 / Parquet 57.2 | Workspace-pinned versions; StreamingMergeBuilder available | ✓ Good |
| Zstd level 3 default compression | Balanced speed/ratio for metrics workloads | ✓ Good |
| 128K rows per row group | Efficient columnar scan patterns | ✓ Good |
| Index name prefix routing (otel-metrics, metrics-) | Automatic pipeline selection without config changes | ✓ Good |
| SQL generation for MVP | Full QueryAst translation deferred for simplicity | ⚠️ Revisit |
| Two-tier pruning (Postgres + Parquet bloom) | Postgres for 90%+ coarse filtering, bloom for fine-grained | ✓ Good |
| Cardinality threshold 1000 for tag routing | Tags <1000 values in Postgres, >=1000 use Parquet bloom | ✓ Good |
| Separate metrics_splits table | Independent schema evolution, clean separation from log splits | ✓ Good |
| JSON array encoding for sqlx | sqlx 2D array limitation workaround, converted in SQL | ✓ Good |
| MetricsPublisher separate from Publisher | Metrics use different metastore API, cleaner separation | ✓ Good |
| Separate metrics_wal_dir_path (ADR-1) | WAL isolation prevents collision with logs | ✓ Good |
| Queue ID prefix metrics/ (ADR-2) | Namespace isolation in mrecordlog | ✓ Good |
| Two-phase locking order (WAL then inner) | Prevents deadlocks, matches logs pipeline | ✓ Good |
| Error classification (transient vs permanent) | Enables intelligent retry decisions | ✓ Good |
| Token bucket rate limiting (10MB burst, 5MB/s) | Prevents OOM, ensures fair shard distribution | ✓ Good |
| Sequencer pattern with oneshot channels | Reserve position before async work, matches logs Uploader | ✓ Good |
| Direct SQL API bypassing QueryAst | Full DataFusion SQL capabilities, Arrow IPC responses | ⚠️ v0.6 |
| Arrow IPC response format | Self-describing, schema included, ecosystem compatible | ⚠️ v0.6 |
| Arrow Flight for streaming | Standard protocol, ClickHouse compatible, backpressure | ⚠️ v0.6 |
| Single-node execution only (v0.6) | Distributed execution deferred to ClickHouse integration | ⚠️ v0.6 |
| DataFusion object_store integration for S3/MinIO | Parquet reads from remote storage | ✓ v0.6 |                                                               
| StorageObjectStoreAdapter bridging quickwit_storage | DataFusion compatibility layer | ✓ v0.6 |                                                               
| MetricsSessionContext::with_storage() | Remote storage backend support | ✓ v0.7 |                                                                             
| URI-based Parquet file resolution | MetricsTableProvider S3 path handling | ✓ v0.7 |                                                                          
| Dedicated `/api/v1/{index}/metrics/query` endpoint | SQL queries for metrics | ✓ v0.7 |                                                                       
| E2E tests validating full pipeline | Ingest → S3 → query → results verification | ✓ v0.7 |   

## Context

Shipped through v0.7 Metrics Query. Full pipeline operational: OTLP ingest → MetricsDocProcessor → MetricsIndexer → MetricsUploader → Sequencer → MetricsPublisher, with S3 storage and DataFusion SQL queries via `/api/v1/{index}/metrics/query`.
Tech stack: DataFusion 45, Parquet 54, Arrow 54, zstd compression, PostgreSQL with GIN indexes, mrecordlog WAL, chitchat gossip, generic Sequencer actor.
Architecture: Actor-based pipeline with durability, ordering, and S3 query integration.
Durability: MetricsWal (queue-per-shard), MetricsShardPosition, MetricsIngesterState (two-phase locking), MetricsShardPositionsService (cluster gossip), MetricsIngestError (error classification), MetricsShardRateLimiter (backpressure).
Parquet optimizations: Dictionary encoding on 7 string columns, bloom filters (5% FPP) on 6 filtering columns, row group statistics for DataFusion pruning.
Two-tier pruning: Postgres metastore for coarse filtering (time range, metric names, low-cardinality tags), Parquet bloom filters for high-cardinality tags.
Query: MetricsSessionContext wraps DataFusion, MetricsTableProvider for Parquet access, StorageObjectStoreAdapter for S3/MinIO.
Compaction design: ADR-001 (data model), ADR-002 (sort schema), ADR-003 (time-windowed sorted compaction) all proposed. TLA+ spec verified all 10 invariants. No implementation yet — splits accumulate indefinitely (GAP-001).
160+ passing tests covering ingest, storage, query, aggregation, metastore, durability, and S3 query paths.

---
*Last updated: 2026-02-23 — v0.8 Parquet Compaction milestone started*
