# GAP-010: No Multi-Level Data Caching or Query Affinity Optimization

**Status**: Open
**Discovered**: 2026-02-19
**Context**: Cloud-native storage characteristics analysis (data caching, query affinity). Split from [GAP-007](./007-no-multi-level-caching.md), which now focuses on metadata caching only.

## Problem

Beyond metadata caching (GAP-007), Quickwit lacks the higher-level caching infrastructure that production observability systems use to hide object storage latency and reduce repeated computation:

- **Columnar data cache**: Recently-read column data from Parquet files, avoiding repeated S3 fetches for hot splits. Without this, every query re-fetches column pages from object storage even when the same splits were just queried moments ago.
- **Predicate evaluation cache**: Results of evaluating common predicates against splits. Monitor workloads re-evaluate the same predicate every evaluation cycle (typically 15s-5min). Caching predicate results avoids redundant computation.
- **Query result cache**: Full or partial query results for repeated queries. Dashboard refresh and monitor evaluation patterns produce highly repetitive queries.

**Query affinity** (characteristic C6) exists partially — Quickwit uses rendezvous hashing on `split_id` to assign splits to searcher nodes, which promotes cache locality. However, without the caches themselves, affinity provides no benefit. And without exactly-once semantics (GAP-005), query-time deduplication may require scatter-gather across nodes, undermining the affinity pattern.

## Evidence

Quickwit's search path (`quickwit-search`) downloads split data from object storage on each query. There is a split cache for warming (`report_splits()`), but no columnar data cache, no predicate cache, and no query result cache.

At production scale, a single S3 download thread delivers ~90 MiB/s. Without caching and parallelization, query latency is dominated by object storage round-trips. For monitor workloads that re-evaluate the same query every 15 seconds, the lack of any result or predicate caching means the full cost is paid on every cycle.

## State of the Art

- **Husky**: Multi-level caches (columnar data, predicate results) with query affinity via consistent hashing. Cache hit rates >90% for typical monitor workloads.
- **Mimir/Cortex**: Chunk cache, query result cache. Memcached-backed. Query-frontend handles result caching and splitting.

## Potential Solutions

### Columnar Data Cache
- **Option A**: Local LRU cache on each searcher node, keyed by `(split_id, column_name, page_range)`. Combined with existing rendezvous hashing for affinity.
- **Option B**: Distributed cache layer (Redis/Memcached) for column data. Higher complexity but enables cache sharing across searcher nodes.

### Predicate / Query Result Cache
- **Option C**: Predicate result cache — cache the set of split IDs that match a given predicate, keyed by predicate hash + time range. Lightweight, high-value for monitor workloads.
- **Option D**: Query result cache at the query-frontend level, keyed by query hash + time range. Can serve dashboard refreshes and repeated monitor evaluations without touching searchers.

### Query Affinity
- **Option E**: Enhance existing rendezvous hashing to be affinity-aware — consider cache state when routing queries. Requires cache hit rate metrics per node.

## Signal Impact

All signals equally affected. Columnar data caching benefits metrics (Parquet) and logs/traces (Tantivy segments) alike. Predicate and query result caching is signal-agnostic.

## Impact

- **Severity**: High
- **Frequency**: Every query pays the cost; monitor workloads pay it repeatedly
- **Affected Areas**: `quickwit-search`, `quickwit-storage`, query planner

## Next Steps

- [ ] Resolve GAP-007 (metadata caching) first — prerequisite for effective data caching
- [ ] Measure cache hit rates and object storage fetch frequency for representative query workloads
- [ ] Design columnar data cache (local LRU per searcher node)
- [ ] Evaluate predicate result caching for monitor-style repeated queries
- [ ] Assess query result caching at the query-frontend level
- [ ] Assess impact of storage tiering on cold file access cost

## References

- [GAP-007: No Parquet Metadata Caching](./007-no-multi-level-caching.md)
- [GAP-005: No Per-Point Deduplication](./005-no-per-point-deduplication.md)
