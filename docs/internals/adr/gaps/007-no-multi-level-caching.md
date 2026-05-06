# GAP-007: No Parquet Metadata Caching

**Status**: Open
**Discovered**: 2026-02-19
**Context**: Cloud-native storage characteristics analysis (metadata caching). Split from original multi-level caching gap; broader caching concerns tracked in [GAP-010](./010-no-data-caching-or-query-affinity.md).

## Problem

Quickwit does not cache Parquet file footers or column chunk metadata. Every file access pays the full latency-to-first-byte penalty to read the footer from object storage before any column data can be located and fetched.

Parquet footers contain the schema, row group metadata, column chunk offsets, min/max statistics, and encoding information. Column chunk headers contain page-level metadata (data page offsets, dictionary page locations, compression codec). These are small (typically a few KB per file) but are read on every access to every split.

At production scale with thousands of splits per time range, the aggregate cost of repeated footer fetches is significant — both in latency (sequential S3 round-trips) and in dollars (S3 GET request pricing). Storage tiering (e.g., S3 Intelligent Tiering) makes this worse: reading even one byte from a cold file promotes it to hot tier for 30 days at higher cost.

## Evidence

Quickwit's search path (`quickwit-search`) downloads split data from object storage on each query. There is a split cache for warming (`report_splits()`), but no dedicated cache for Parquet footers or column chunk metadata. Each query that touches a split must fetch the footer before it can locate and read any column data.

A single S3 GET request has ~50-100ms latency-to-first-byte. For a query spanning 100 splits, footer fetches alone contribute 5-10 seconds of serial latency (or significant parallelism overhead).

## State of the Art

- **Husky**: Caches file metadata separately from data, enabling fast query planning without data fetches.
- **Mimir/Cortex**: Index cache (backed by Memcached) caches block-level metadata separately from chunk data.
- **Apache Spark**: Parquet footer caching is a built-in feature (`spark.sql.parquet.footerCache`).

## Potential Solutions

- **Option A**: Local in-memory LRU cache on each searcher node, keyed by `(split_id, footer_offset)`. Footers are immutable (splits are immutable once written), so cache invalidation is trivial — evict on LRU pressure only.
- **Option B**: Pre-fetch and store footer bytes in the metastore alongside split metadata. Adds metastore storage cost but eliminates the S3 round-trip entirely for query planning.
- **Option C**: Embed essential footer statistics (row count, column min/max, size) in split metadata at ingestion time, reducing the need to read the full footer for query planning. Full footer still fetched on demand.

## Signal Impact

All signals equally affected. Parquet footer caching benefits metrics queries directly. For logs/traces using Tantivy, the analogous concern is segment metadata caching — the same pattern applies.

## Impact

- **Severity**: High
- **Frequency**: Every query against every split
- **Affected Areas**: `quickwit-search`, `quickwit-storage`

## Next Steps

- [ ] Measure footer fetch frequency and latency contribution for representative query workloads
- [ ] Design local LRU footer cache (sizing, eviction, key schema)
- [ ] Evaluate embedding footer statistics in split metadata at ingestion time
- [ ] Prototype and benchmark against baseline (no cache)

## References

- [GAP-010: No Multi-Level Data Caching or Query Affinity Optimization](./010-no-data-caching-or-query-affinity.md)
