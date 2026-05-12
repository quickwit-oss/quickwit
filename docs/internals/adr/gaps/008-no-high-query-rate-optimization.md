# GAP-008: No High Query Rate Optimization

**Status**: Open
**Discovered**: 2026-02-19
**Context**: Cloud-native storage characteristics analysis (high query rate optimization)

## Problem

Quickwit's metadata and query infrastructure is not optimized for the high query rates required by monitor evaluation at scale. Large-scale observability platforms can require hundreds of thousands of queries per second for monitor evaluation alone. At this scale, the metadata service must respond to split-listing queries with sub-millisecond latency, and query results for repeated predicates must be cacheable to avoid redundant computation.

The current metadata service is PostgreSQL. While PostgreSQL handles moderate query planning loads, it is not designed for very high QPS of metadata lookups with the per-split min/max/regex filtering that split-level pruning requires (see [GAP-004](./004-incomplete-split-metadata.md)). Each monitor evaluation cycle queries the metastore for relevant splits, evaluates the query, and discards the result. The next cycle repeats the same work.

## Evidence

The metrics pipeline stores split metadata in PostgreSQL (`metrics_splits` table). Query planning queries this table to find splits matching a time range and tag predicates. PostgreSQL handles this at current scale (low query rate, limited metadata), but the design does not address:

- Metadata volume at high ingestion rates (~921K splits per 15-minute window before compaction)
- High QPS metadata lookups (monitor evaluation at 800k QPS)
- Repeated identical queries (monitors re-evaluate the same predicate every cycle)

ADR-003 explicitly acknowledges the PostgreSQL scalability concern: "At high ingestion rates, PostgreSQL metadata volume can exceed what a single OLTP database handles efficiently."

## State of the Art

- **Husky**: Dedicated metadata service optimized for high-rate pruning queries. Predicate and query result caching for monitor evaluation.
- **Mimir/Cortex**: Index cache (memcached-backed) for label/series lookups. Query result cache for repeated queries.

## Potential Solutions

- **Option A**: Query result cache. Cache the results of repeated monitor queries, keyed by (query_hash, time_range). Invalidate when new splits are published in the relevant time range. This is the highest-leverage optimization for monitor workloads.
- **Option B**: Dedicated metadata service. Replace PostgreSQL for split-level metadata with a purpose-built service optimized for range queries and high QPS. The self-describing Parquet files (ADR-003) make this migration feasible — PostgreSQL is an index, not the sole source of truth.
- **Option C**: In-memory metadata index on query nodes. Each searcher node maintains an in-memory copy of split metadata for its assigned splits (via rendezvous hashing), updated via a change feed from the metastore. Eliminates per-query metadata round-trips.

## Signal Impact

**Metrics**: Most affected. Monitor evaluation drives the highest query rate.

**Traces and logs**: Less affected for monitors, but dashboard queries at scale face the same metadata bottleneck.

## Impact

- **Severity**: High (for production monitor workloads)
- **Frequency**: Proportional to monitor count and evaluation frequency
- **Affected Areas**: `quickwit-metastore`, `quickwit-search` (query planning), metadata infrastructure

## Next Steps

- [ ] Measure current metadata query latency and throughput at representative scale
- [ ] Design query result cache for monitor-style repeated queries
- [ ] Evaluate in-memory metadata index on query nodes vs dedicated metadata service
- [ ] Prototype split-level pruning using existing PostgreSQL metadata to measure pruning effectiveness before optimizing throughput

## References

- [GAP-004: Incomplete Split Metadata](./004-incomplete-split-metadata.md)
- [ADR-003: Time-Windowed Sorted Compaction](../003-time-windowed-sorted-compaction.md) — PostgreSQL scalability note
