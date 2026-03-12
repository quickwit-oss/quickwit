# Project Milestones: Pomsky Metrics Engine

## v0.7 Metrics Query (Shipped: 2026-01-25)

**Delivered:** DataFusion S3 integration enabling SQL queries against Parquet files stored in S3/MinIO, with dedicated metrics query API endpoint.

**Phases completed:** 30 (4 plans total)

**Key accomplishments:**

- Created StorageObjectStoreAdapter bridging quickwit_storage to DataFusion's object_store API
- Implemented MetricsSessionContext::with_storage() for S3/remote storage backends
- Built URI-based Parquet file resolution in MetricsTableProvider for all storage schemes
- Added dedicated `/api/v1/{index}/metrics/query` endpoint for SQL queries (pulled from v0.7)
- Fixed RamStorage path handling with proper URI joining and file size tracking
- E2E tests validate full pipeline: ingest → S3 upload → DataFusion query → results

**What's next:** Dedicated metrics query API with JSON format, advanced aggregations (rate/delta, percentiles), query performance optimization.

---

## v0.6 DataFusion SQL API (Planning: 2026-01-22)

**Status:** 🚧 Planning — architecture finalized

**Phases planned:** 27-29 (8 plans estimated)

**Goal:** Add a direct DataFusion SQL endpoint that accepts SQL queries and returns Arrow IPC responses, enabling native SQL access to metrics data.

**Architecture:** Follows existing Quickwit root/leaf pattern:
- **Root service** (coordinator): Receives SQL, fetches splits from metastore, distributes to leaves
- **Leaf service** (worker): Receives splits + SQL, executes DataFusion query, returns Arrow IPC
- Leaf does NOT access metastore (matches `leaf_search` pattern exactly)

**Key features:**
- MetricsSqlLeafService for splits + SQL → DataFusion → Arrow IPC (worker node)
- MetricsSqlRootService for SQL parsing, metastore split fetch, leaf coordination (coordinator)
- REST endpoint (`POST /api/v1/sql/metrics`) returning Arrow IPC
- gRPC endpoints: client-facing (`MetricsSql`) + internal (`MetricsSqlLeaf`)
- Arrow Flight protocol for streaming (ClickHouse compatible)

**Phases:**
- Phase 27: MetricsSqlLeafService Core (protobuf, leaf service, Arrow IPC)
- Phase 28: MetricsSqlRootService & Endpoints (root service, REST/gRPC)
- Phase 29: Arrow Flight Service (GetFlightInfo, DoGet wrapping root service)

**Deferred:** Distributed query execution — to be addressed in future ClickHouse integration

**See:** [v0.6 ROADMAP](milestones/v0.6-ROADMAP.md) for detailed plan

---

## v0.5 Metrics Sequencer (Shipped: 2026-01-21)

**Delivered:** Integrated Sequencer actor with MetricsUploader for ordered split publishing using oneshot channels, ensuring FIFO delivery semantics.

**Phases completed:** 26 (1 plan total)

**Key accomplishments:**

- Extended MetricsSplitsUpdateMailbox with Sequencer variant for ordered delivery
- Implemented MetricsSplitsUpdateSender enum for Publisher/Sequencer routing
- Added get_sender() method to reserve sequencer position before async work
- Updated MetricsUploader handler to use sender pattern with Proceed/Discard
- Added test proving sequencer maintains FIFO ordering

**Stats:**

- 1 file modified (metrics_uploader.rs)
- 157 insertions, 10 deletions
- 1 phase, 1 plan, 3 tasks completed
- <1 day from v0.4 to v0.5 (2026-01-21)

**Git range:** `2004ba56` (feat 26-01) → `174b5cbf` (test 26)

**What's next:** Query integration with list_metrics_splits for pruning, performance benchmarking with production workloads, full QueryAst to SQL translation.

---

## v0.4 Local Testing (Shipped: 2026-01-21)

**Delivered:** Local testing environment with Docker Compose (Minio + Postgres) to validate the full metrics pipeline end-to-end.

**Phases completed:** 23-25 (3 plans total)

**Key accomplishments:**

- Created Docker Compose environment with Minio S3-compatible storage and Postgres
- Built comprehensive E2E test infrastructure module (TestInfra struct with storage/metastore helpers)
- Implemented full pipeline E2E test validating all pipeline stages
- Added query accuracy E2E test with DataFusion validation
- Established graceful skip pattern when infrastructure unavailable

**Stats:**

- 4 files modified
- Phases 23-25 complete
- 3 plans completed
- 1 day from v0.3 to v0.4 (2026-01-20 → 2026-01-21)

**Git range:** Phase 23 → Phase 25

**What's next:** Metrics sequencer integration for ordered delivery semantics.

---

## v0.3 Durability (Shipped: 2026-01-20)

**Delivered:** Production durability infrastructure with WAL, checkpointing, cluster gossip, intelligent retry, and per-shard rate limiting — matching logs pipeline patterns exactly.

**Phases completed:** 17-22 (9 plans total)

**Key accomplishments:**

- Created MetricsWal with queue-per-shard architecture using mrecordlog and metrics/ queue ID prefix for isolation
- Implemented MetricsShardPosition and MetricsIngesterState with two-phase locking for atomic operations
- Built MetricsShardPositionsService actor for cluster-wide position gossip via chitchat
- Designed MetricsIngestError with transient vs permanent error classification for intelligent retry decisions
- Created MetricsShardRateLimiter with per-shard token bucket rate limiting (10MB burst, 5MB/s rate)
- Established 5 ADRs for architectural decisions: WAL isolation, queue prefix, MRecord reuse, per-shard positions, replication factor

**Stats:**

- 49 files modified
- 8,937 insertions, 39 deletions
- 6 phases, 9 plans completed
- 2 days from v0.2 to v0.3 (2026-01-18 → 2026-01-20)

**Git range:** `a8b596c0` (feat 17-01) → `03228ebf` (feat 22-01)

**What's next:** Wire rate limiter into production ingestion, query integration with list_metrics_splits for pruning, performance benchmarking with production workloads.

---

## v0.2 Metadata (Shipped: 2026-01-18)

**Delivered:** Parquet optimization with bloom filters/statistics, two-tier metadata pruning strategy, and full metastore integration for metrics split staging and publishing.

**Phases completed:** 11-16 (9 plans total)

**Key accomplishments:**

- Optimized Parquet format with dictionary encoding, bloom filters (5% FPP), and row group statistics for DataFusion query pruning
- Designed two-tier pruning strategy: Postgres metastore for coarse filtering (90%+ elimination), Parquet bloom filters for fine-grained pruning
- Created MetricsSplitMetadata struct with two-tier tag storage (low_cardinality_tags + high_cardinality_tag_keys) and MetricsSplitState lifecycle
- Built PostgreSQL metrics_splits table with GIN indexes and full CRUD metastore operations (stage/publish/list/mark/delete)
- Created MetricsUploader actor for split staging flow and MetricsPublisher actor for publish lifecycle
- Wired complete metrics pipeline: MetricsIndexer → MetricsUploader → MetricsPublisher with metastore integration

**Stats:**

- 73 files modified
- 13,133 insertions, 1,149 deletions
- 6 phases, 9 plans completed
- 2 days from v0.1 to v0.2 (2026-01-16 → 2026-01-18)

**Git range:** `2836853` (feat 11-01) → `58e4880` (feat 16-01)

**What's next:** Query integration with list_metrics_splits for pruning, production testing with real metrics workloads, performance benchmarking, and multi-node query execution validation.

---

## v0.1 Alpha (Shipped: 2026-01-15)

**Delivered:** Complete DataFusion/Parquet metrics engine with full ingest, query, and search integration into Quickwit.

**Phases completed:** 1-10 (23 plans total)

**Key accomplishments:**

- Created quickwit-metrics-engine crate with DataFusion 45 and Parquet 54 integration
- Implemented Parquet writer with zstd compression and configurable row groups
- Built Arrow IPC to Parquet ingest pipeline with batch accumulation
- Created DataFusion TableProvider for SQL query execution against Parquet splits
- Implemented time-series aggregations (SUM, AVG, MIN, MAX, COUNT) and time bucketing
- Integrated metrics pipeline into Quickwit actor system with automatic routing
- Added Prometheus metrics and structured tracing for production observability

**Stats:**

- 97 files created/modified
- 8,738 lines of Rust (metrics-engine crate)
- 18,842 insertions, 827 deletions total
- 10 phases, 23 plans completed
- ~21 hours from start to ship

**Git range:** `e16d8849` (chore 01-01) → `fc0f9d9d` (HEAD)

**What's next:** Deciding what metadata to use for pruning, storing metadata in the metastore, implementing publishing of metrics splits (which are just Parquet files), production testing with real metrics workloads, performance benchmarking, and ensuring multi-node metrics ingestion and querying is correct end-to-end.

---
