# Phase 12: Metadata Analysis - Context

**Gathered:** 2026-01-17
**Status:** Ready for planning

<vision>
## How This Should Work

Study existing SplitMetadata pruning patterns and design metadata schema that enables efficient pruning for metrics queries. The goal is a two-tier pruning strategy:

1. **Postgres-level pruning** — Metastore queries filter splits using time range, metric names, and low-cardinality tag key-value pairs. This is the coarse filter that eliminates most irrelevant splits.

2. **Parquet-level pruning** — For high-cardinality tags, bloom filters embedded in Parquet footers handle fine-grained filtering at query execution time.

This hybrid approach keeps the metastore queries simple and Postgres-compatible while still enabling efficient pruning for high-cardinality dimensions.

</vision>

<essential>
## What Must Be Nailed

- **Time range pruning** — Every metrics query filters by time; this must be fast and accurate
- **Metric name pruning** — Queries target specific metrics; prune splits that don't contain requested metrics
- **Tag-based pruning** — Filter by tags (host, env, service) at both Postgres and Parquet levels

All three are equally important — no single dimension dominates metrics query patterns.

</essential>

<specifics>
## Specific Ideas

**Metadata stored in Postgres (metastore):**
- Time range (min/max timestamp)
- Metric names present in split
- Tag key-value pairs for low-cardinality tags (cardinality < 1000)
- Tags for standard metrics and VARIANTs
- Row count and byte size for query planning

**Metadata in Parquet (query-time pruning):**
- Bloom filters for high-cardinality tag values (cardinality >= 1000)
- Row group statistics (already enabled in Phase 11)

**Cardinality threshold:** 1000 unique values determines whether tag values go in Postgres (exact match) or Parquet bloom filters (probabilistic match).

</specifics>

<notes>
## Additional Context

User confirmed the hybrid Postgres/Parquet approach: Postgres doesn't support native bloom filter columns, so high-cardinality tag filtering moves to the Parquet layer. This keeps metastore queries simple SQL while still enabling efficient pruning.

The 10k tag limit mentioned is for total tags stored in metadata, not per tag key.

</notes>

---

*Phase: 12-metadata-analysis*
*Context gathered: 2026-01-17*
