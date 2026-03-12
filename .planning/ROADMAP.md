# Roadmap: Pomsky Metrics Engine

## Overview

Replace Tantivy with DataFusion/Parquet for metrics workloads in Quickwit. Starting with core infrastructure, then building storage, ingest, and query layers before integrating with the existing actor pipeline and search system. The project maintains full compatibility with Tantivy for logs/traces while providing a high-throughput columnar engine for metrics.

## Milestones

- ✅ [v0.1 Alpha](milestones/v0.1-ROADMAP.md) (Phases 1-10) -- SHIPPED 2026-01-15
- ✅ [v0.2 Metadata](milestones/v0.2-ROADMAP.md) (Phases 11-16) -- SHIPPED 2026-01-18
- ✅ [v0.3 Durability](milestones/v0.3-ROADMAP.md) (Phases 17-22) -- SHIPPED 2026-01-20
- ✅ [v0.4 Local Testing](milestones/v0.4-ROADMAP.md) (Phases 23-25) -- SHIPPED 2026-01-21
- ✅ [v0.5 Metrics Sequencer](milestones/v0.5-ROADMAP.md) (Phase 26) -- SHIPPED 2026-01-21
- ✅ [v0.6 DataFusion SQL API](milestones/v0.6-ROADMAP.md) (Phases 27-29) -- SHIPPED 2026-01-26
- ✅ [v0.7 Metrics Query](milestones/v0.7-ROADMAP.md) (Phase 30) -- SHIPPED 2026-01-25
- 🚧 [v0.8 Parquet Compaction](milestones/v0.8-ROADMAP.md) (Phases 31-36) -- IN PROGRESS

## Domain Expertise

None


## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

<details>
<summary>v0.1 Alpha (Phases 1-10) -- SHIPPED 2026-01-15</summary>

- [x] **Phase 1: Foundation** - Core DataFusion/Parquet infrastructure and types (4/4 plans)
- [x] **Phase 2: Storage Layer** - Parquet file writing and split format for metrics (2/2 plans)
- [x] **Phase 3: Ingest Pipeline** - Direct Arrow-to-Parquet write path (1/1 plan)
- [x] **Phase 4: Query Engine** - DataFusion query execution for metrics (1/1 plan)
- [x] **Phase 5: Aggregations** - Time-series aggregation functions (1/1 plan)
- [x] **Phase 6: Time Partitioning** - Time-range partitioning for metrics access (1/1 plan)
- [x] **Phase 7: Pipeline Integration** - Connect to existing actor pipeline (3/3 plans)
- [x] **Phase 8: Search Integration** - Integrate DataFusion into search layer (2/2 plans)
- [x] **Phase 9: Testing & Validation** - End-to-end testing and benchmarks (4/4 plans)
- [x] **Phase 10: Production Hardening** - Error handling and operational readiness (4/4 plans)

</details>

<details>
<summary>v0.2 Metadata (Phases 11-16) -- SHIPPED 2026-01-18</summary>

- [x] **Phase 11: Parquet Optimization** - Dictionary encoding, bloom filters, statistics (1/1 plan)
- [x] **Phase 12: Metadata Analysis** - Pruning requirements and two-tier strategy (1/1 plan)
- [x] **Phase 13: Metadata Schema Design** - MetricsSplitMetadata struct (2/2 plans)
- [x] **Phase 14: Metastore Extension** - Protobuf messages, PostgreSQL implementation (2/2 plans)
- [x] **Phase 15: Metastore Staging** - MetricsUploader actor (2/2 plans)
- [x] **Phase 16: Metastore Publishing** - MetricsPublisher actor (1/1 plan)

</details>

<details>
<summary>v0.3 Durability (Phases 17-22) -- SHIPPED 2026-01-20</summary>

- [x] **Phase 17: Research Deep Dive** - Metrics Durability Design Document (1/1 plan)
- [x] **Phase 18: Metrics WAL Integration** - MetricsWal with queue-per-shard (2/2 plans)
- [x] **Phase 19: Checkpointing** - MetricsShardPosition and MetricsIngesterState (2/2 plans)
- [x] **Phase 20: Cluster Gossip** - MetricsShardPositionsService (1/1 plan)
- [x] **Phase 21: Retry & Error Handling** - MetricsIngestError (2/2 plans)
- [x] **Phase 22: Rate Limiting & Backpressure** - MetricsShardRateLimiter (1/1 plan)

</details>

<details>
<summary>v0.4 Local Testing (Phases 23-25) -- SHIPPED 2026-01-21</summary>

- [x] **Phase 23: Docker Compose Setup** - Minio and Postgres containers (1/1 plan)
- [x] **Phase 24: E2E Test Infrastructure** - TestInfra struct (1/1 plan)
- [x] **Phase 25: Full Pipeline E2E Tests** - Pipeline and query accuracy tests (1/1 plan)

</details>

<details>
<summary>v0.5 Metrics Sequencer (Phase 26) -- SHIPPED 2026-01-21</summary>

- [x] **Phase 26: Metrics Sequencer** - Sequencer integration for ordered delivery (1/1 plan)

</details>

<details>
<summary>v0.6 DataFusion SQL API (Phases 27-29) -- SHIPPED 2026-01-26</summary>

- [x] **Phase 27: MetricsSqlLeafService Core** - Leaf worker with DataFusion execution (3/3 plans)
- [x] **Phase 28: MetricsSqlRootService & Endpoints** - Root coordinator with REST/gRPC (3/3 plans)
- [x] **Phase 29: gRPC Arrow IPC Extension** - Arrow IPC format in gRPC responses (1/1 plan)

</details>

<details>
<summary>v0.7 Metrics Query (Phase 30) -- SHIPPED 2026-01-25</summary>

- [x] **Phase 30: DataFusion S3 Integration** - S3/MinIO Parquet reads via object_store (4/4 plans)

</details>

<details open>
<summary>v0.8 Parquet Compaction (Phases 31-36) -- IN PROGRESS</summary>

**Milestone Goal:** Node-local time-windowed sorted compaction for Parquet metrics splits -- eliminate unbounded split accumulation and improve compression through sorted storage. Closes GAP-001, GAP-003, GAP-004.

- [x] **Phase 31: Metadata Foundation** - Sort schema, split metadata extensions, PostgreSQL migration, canonical window_start function (completed 2026-02-23)
  Plans:
  - [ ] 31-01-PLAN.md -- Vendor event_store_sortschema.proto and implement sort schema parser
  - [ ] 31-02-PLAN.md -- MetricsSplitMetadata extensions, PostgreSQL migration, Parquet key_value_metadata
  - [ ] 31-03-PLAN.md -- ListMetricsSplitsForCompaction RPC and atomic publish with replace
  - [ ] 31-04-PLAN.md -- Canonical window_start function with proptest
- [ ] **Phase 32: Ingestion Window Enforcement** - Window-partitioned accumulator, sort-at-write, late data rejection, column indexes
- [ ] **Phase 33: Merge Policy** - WindowedSortMergePolicy adapting StableLogMergePolicy for Parquet file sizes
- [ ] **Phase 34: Merge Executor** - K-way sorted merge via StreamingMergeBuilder with schema evolution and memory isolation
- [ ] **Phase 35: Actor Pipeline & Monitoring** - Planner, downloader, publisher, supervisor, janitor GC, monitoring metrics
- [ ] **Phase 36: Integration Testing & Verification** - E2E correctness, Stateright model checking, TLA+ invariant enforcement, DST crash-recovery

See [v0.8 ROADMAP](milestones/v0.8-ROADMAP.md) for detailed phase descriptions, success criteria, and dependency graph.

</details>

## Progress

| Phase | Milestone | Plans Complete | Status | Completed |
|-------|-----------|----------------|--------|-----------|
| 1. Foundation | v0.1 | 4/4 | Complete | 2026-01-15 |
| 2. Storage Layer | v0.1 | 2/2 | Complete | 2026-01-15 |
| 3. Ingest Pipeline | v0.1 | 1/1 | Complete | 2026-01-15 |
| 4. Query Engine | v0.1 | 1/1 | Complete | 2026-01-15 |
| 5. Aggregations | v0.1 | 1/1 | Complete | 2026-01-15 |
| 6. Time Partitioning | v0.1 | 1/1 | Complete | 2026-01-15 |
| 7. Pipeline Integration | v0.1 | 3/3 | Complete | 2026-01-15 |
| 8. Search Integration | v0.1 | 2/2 | Complete | 2026-01-15 |
| 9. Testing & Validation | v0.1 | 4/4 | Complete | 2026-01-15 |
| 10. Production Hardening | v0.1 | 4/4 | Complete | 2026-01-15 |
| 11. Parquet Optimization | v0.2 | 1/1 | Complete | 2026-01-17 |
| 12. Metadata Analysis | v0.2 | 1/1 | Complete | 2026-01-17 |
| 13. Metadata Schema Design | v0.2 | 2/2 | Complete | 2026-01-18 |
| 14. Metastore Extension | v0.2 | 2/2 | Complete | 2026-01-18 |
| 15. Metastore Staging | v0.2 | 2/2 | Complete | 2026-01-18 |
| 16. Metastore Publishing | v0.2 | 1/1 | Complete | 2026-01-18 |
| 17. Research Deep Dive | v0.3 | 1/1 | Complete | 2026-01-19 |
| 18. Metrics WAL Integration | v0.3 | 2/2 | Complete | 2026-01-19 |
| 19. Checkpointing | v0.3 | 2/2 | Complete | 2026-01-19 |
| 20. Cluster Gossip | v0.3 | 1/1 | Complete | 2026-01-19 |
| 21. Retry & Error Handling | v0.3 | 2/2 | Complete | 2026-01-19 |
| 22. Rate Limiting & Backpressure | v0.3 | 1/1 | Complete | 2026-01-20 |
| 23. Docker Compose Setup | v0.4 | 1/1 | Complete | 2026-01-21 |
| 24. E2E Test Infrastructure | v0.4 | 1/1 | Complete | 2026-01-20 |
| 25. Full Pipeline E2E Tests | v0.4 | 1/1 | Complete | 2026-01-21 |
| 26. Metrics Sequencer | v0.5 | 1/1 | Complete | 2026-01-21 |
| 27. MetricsSqlLeafService Core | v0.6 | 3/3 | Complete | 2026-01-22 |
| 28. MetricsSqlRootService & Endpoints | v0.6 | 3/3 | Complete | 2026-01-23 |
| 29. gRPC Arrow IPC Extension | v0.6 | 1/1 | Complete | 2026-01-26 |
| 30. DataFusion S3 Integration | v0.7 | 4/4 | Complete | 2026-01-25 |
| 31. Metadata Foundation | 4/4 | Complete   | 2026-02-23 | - |
| 32. Ingestion Window Enforcement | v0.8 | 0/3 | Not started | - |
| 33. Merge Policy | v0.8 | 0/1 | Not started | - |
| 34. Merge Executor | v0.8 | 0/3 | Not started | - |
| 35. Actor Pipeline & Monitoring | v0.8 | 0/4 | Not started | - |
| 36. Integration Testing & Verification | v0.8 | 0/3 | Not started | - |
