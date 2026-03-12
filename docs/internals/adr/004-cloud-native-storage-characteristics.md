# ADR-004: Cloud-Native Storage Characteristics

## Metadata

- **Status**: Proposed
- **Date**: 2026-02-19
- **Tags**: architecture, storage, cloud-native, observability
- **Components**: all
- **Authors**: gtt@
- **Related**: [ADR-001](./001-parquet-data-model.md), [ADR-002](./002-sort-schema-parquet-splits.md), [ADR-003](./003-time-windowed-sorted-compaction.md)

## Context

To compete with ClickHouse Commercial (SharedMergeTree) in a cloud-native observability context, Quickhouse-Pomsky must exhibit the characteristics that Datadog's internal systems (Husky, Metrics) have demonstrated at scale. These characteristics are derived from operational experience with cloud-native storage for observability workloads and are documented in [Characteristics of Cloud Native Storage relevant to Quickhouse](https://docs.google.com/document/d/...).

This ADR catalogs those characteristics, evaluates Quickwit's current status for each, and identifies gaps that must be closed.

## Decision

We adopt the following characteristics as the target architecture for Quickhouse-Pomsky. Each characteristic is evaluated against Quickwit's current implementation.

### Cloud-Native Storage Characteristics

| # | Characteristic | Quickwit Status | Notes | Gap |
|---|---------------|----------------|-------|-----|
| C1 | **Independent auto-scaling of query, ingest, compaction** | Not yet | Quickwit does not independently auto-scale these components | [GAP-006](./gaps/006-no-independent-auto-scaling.md) |
| C2 | **Object-store-aware file format** | Implemented | Parquet/Tantivy files designed for object storage access patterns | — |
| C3 | **Object storage throughput** | Implemented | Parallelized access to files and columns | — |
| C4 | **Multi-level caching** | Not implemented | No columnar data cache, column header cache, predicate result cache, or query result cache | [GAP-007](./gaps/007-no-multi-level-caching.md) |
| C5 | **Distributed query execution** | Implemented | Two-stage planning + execution with parallel fan-out to searcher nodes | — |
| C6 | **Query affinity** | Partial | Rendezvous hashing on `split_id` assigns splits to searchers. But without exactly-once semantics, dedup may force scatter-gather that undermines affinity | [GAP-007](./gaps/007-no-multi-level-caching.md) |
| C7 | **Query-aware data layout** | Partial | Ingest sharding supports a form of locality. ADR-002/003 introduce sorted splits and compaction to achieve global data layout | [GAP-001](./gaps/001-no-parquet-compaction.md) |
| C8 | **Storage layout decoupled from ingest sharding** | Not yet | Current compaction is node-local (`node_id` in merge scope). ADR-003 excludes `node_id` from scope, enabling future cross-node compaction | [GAP-001](./gaps/001-no-parquet-compaction.md) |
| C9 | **Exactly-once semantics** | Partial | WAL checkpoints provide batch-level exactly-once. No per-point dedup | [GAP-005](./gaps/005-no-per-point-deduplication.md) |
| C10 | **Query pruning before scanning** | Partial | Tag-based and time-range pruning on existing split metadata. Full split-level pruning on sort column min/max/regex requires metadata from ADR-003 | [GAP-004](./gaps/004-incomplete-split-metadata.md) |

### Observability-Specific Characteristics

| # | Characteristic | Quickwit Status | Notes | Gap |
|---|---------------|----------------|-------|-----|
| C11 | **Schema-on-read** | Partial | Quickwit supports dynamic fields. OTel map-based attributes need column extraction (ADR-001 section 5) | ADR-001 §5 |
| C12 | **Minimal ingest-to-query latency** | Implemented | Small splits published quickly; commit timeout bounds latency | — |
| C13 | **High query rate for monitors** | Not implemented | PostgreSQL metadata not optimized for ~800k QPS. No query/predicate result caching | [GAP-008](./gaps/008-no-high-query-rate-optimization.md) |
| C14 | **New data prioritized over updates** | Not implemented | No priority scheduling between fresh ingestion and compaction/backfill | [GAP-009](./gaps/009-no-leading-edge-prioritization.md) |
| C15 | **Time is first-class** | Partial | Splits have time ranges. ADR-003 introduces formal time windowing | [GAP-003](./gaps/003-no-time-window-partitioning.md) |

### KTLO Characteristics

| # | Characteristic | Quickwit Status | Notes | Gap |
|---|---------------|----------------|-------|-----|
| C16 | **Prioritize leading edge** | Not implemented | No compaction priority for recent windows. Too many small files at the leading edge degrades query performance | [GAP-009](./gaps/009-no-leading-edge-prioritization.md) |
| C17 | **Traffic burst handling** | Not implemented | No burst lane or overflow buffering mechanism | [GAP-006](./gaps/006-no-independent-auto-scaling.md) |

### ClickStack Characteristics

| # | Characteristic | Quickwit Status | Notes | Gap |
|---|---------------|----------------|-------|-----|
| C18 | **Materialized views** | Not implemented | No pre-aggregation or materialized view support | — (future) |
| C19 | **SQL** | Not implemented | Planned via ClickHouse integration (remote storage engine) | — (future) |
| C20 | **Available on-prem** | Implemented | Open-source Quickwit is deployable on-prem | — |

### Summary

| Status | Count | Characteristics |
|--------|-------|----------------|
| Implemented | 5 | C2, C3, C5, C12, C20 |
| Partial | 5 | C6, C7, C9, C10, C11, C15 |
| Not yet | 9 | C1, C4, C8, C13, C14, C16, C17, C18, C19 |

The existing ADRs (001-003) and gaps (001-005) address: C7, C8, C9, C10, C11, C15.

New gaps created by this ADR address: C1/C17, C4/C6, C13, C14/C16.

## Consequences

### Positive

- **Clear target architecture.** The characteristics provide a concrete checklist for what Quickhouse-Pomsky must achieve to compete with ClickHouse Commercial and match Datadog's internal systems.
- **Prioritization framework.** Characteristics can be prioritized by impact: C7 (query-aware layout) and C10 (pruning) have the highest query performance impact; C1 (auto-scaling) and C16 (leading edge) have the highest operational impact.
- **Gap tracking.** Each missing characteristic has a corresponding gap document with potential solutions and next steps.

### Negative

- **Large surface area.** 9 characteristics are not yet implemented. Achieving all of them is a multi-quarter effort.
- **Interdependencies.** Some characteristics depend on others (C10 pruning requires C7 data layout; C6 affinity benefits from C9 exactly-once). The implementation order matters.

### Risks

- **PostgreSQL as bottleneck.** Multiple characteristics (C10 pruning, C13 high query rate) require fast metadata access. PostgreSQL may not scale to the required QPS, as noted in ADR-003.
- **ClickHouse Commercial is a moving target.** SharedMergeTree continues to improve. The characteristics identified here represent the current gap, not a static finish line.

## Signal Generalization

All characteristics apply across metrics, traces, and logs. The evaluation in this ADR focuses on the metrics Parquet pipeline (current priority), but the characteristics are signal-agnostic. Query-aware data layout, pruning, caching, and auto-scaling benefit all three signals equally.

## Decision Log

| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-02-19 | Initial ADR created | Catalog cloud-native storage characteristics as target architecture, evaluate Quickwit status, identify gaps |
| 2026-02-19 | 20 characteristics adopted from internal experience | Based on operational lessons from Husky, Metrics, and competitive analysis with ClickHouse Commercial |

## Implementation Status

See the status table in the Decision section above. Implementation is tracked through the referenced ADRs and gaps.

## References

- [Characteristics of Cloud Native Storage relevant to Quickhouse](https://docs.google.com/document/d/...) — source document
- [ADR-001: Parquet Data Model](./001-parquet-data-model.md) — schema-on-read (C11)
- [ADR-002: Sort Schema](./002-sort-schema-parquet-splits.md) — query-aware layout foundation (C7)
- [ADR-003: Time-Windowed Sorted Compaction](./003-time-windowed-sorted-compaction.md) — compaction, time windowing, storage decoupling (C7, C8, C15)
- [Husky Storage Compaction Blog Post](https://www.datadoghq.com/blog/engineering/husky-storage-compaction/) — query-aware data layout reference
