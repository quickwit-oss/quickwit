# ADR-001: Parquet Metrics Data Model

## Metadata

- **Status**: Proposed
- **Date**: 2026-02-19
- **Tags**: storage, metrics, parquet, data-model
- **Components**: quickwit-parquet-engine
- **Authors**: gtt@
- **Related**: [ADR-002](./002-sort-schema-parquet-splits.md), [ADR-003](./003-time-windowed-sorted-compaction.md), [Phase 1 Design](../locality-compaction/phase-1-sorted-splits.md)

## Context

Quickwit's metrics pipeline stores data in Parquet files. A fundamental design question is how metrics data is represented at the row level: what does one row in a Parquet file correspond to?

Two models are in consideration:

1. **Point-per-row**: Each row is a single data point — one metric value at one timestamp for one timeseries (identified by its metric name and tag set).
2. **Timeseries-per-row**: Each row represents an entire timeseries over some time range — the row contains arrays of timestamps and values for a single series, with the tag set stored once.

This decision is foundational because it determines the shape of every downstream system: how compaction merges data, how sort schemas are defined, how DataFusion queries are structured, and what encodings are effective.

## Decision

### 1. Point-Per-Row

Each row in a Parquet split represents a single data point: one metric value at one timestamp for one timeseries.

Point-per-row is chosen because:

- **Simpler compaction.** Sorted k-way merge operates directly on rows. Timeseries-per-row requires both row-level merge (interleaving rows from different splits) *and* intra-row series merge (combining the timestamp/value arrays of the same timeseries across splits). Point-per-row avoids this second level of merge entirely.
- **No last-write-wins (LWW).** We explicitly do not support LWW semantics, where a later write for the same timeseries and timestamp overwrites an earlier one. Without LWW, there is no need for sticky routing or series-level deduplication during compaction. This is a deliberate simplification that avoids the reliability challenges of sticky routing (single-partition overload, constant shuffles on rebalancing) that other systems have encountered.
- **No storage-level interpolation.** Interpolation across points in a timeseries is not performed at the storage layer. If needed in the future, it will operate at query time. This may be slower than storage-level interpolation but avoids coupling the storage format to query semantics.
- **Performance equivalence with good encoding.** With sorted data, columnar encodings like RLE and dictionary encoding produce long runs of repeated values in the sort columns — the same runs that timeseries-per-row would capture by grouping values into arrays. When these encodings are preserved through query execution, point-per-row achieves comparable scan performance to timeseries-per-row without the implementation complexity.
- **Standard DataFusion operators.** Timeseries-per-row requires significant custom DataFusion operator support (nested array types, custom aggregation kernels). Point-per-row uses standard columnar operations, allowing us to contribute generic improvements to DataFusion rather than maintaining timeseries-specific extensions.

### 2. No Last-Write-Wins Semantics

We do not support LWW. If two data points arrive for the same timeseries at the same timestamp in separate ingest requests, both are stored. There is no per-point deduplication at the storage layer. This eliminates:

- Sticky routing requirements (binding a timeseries to a specific shard/node)
- Series-level deduplication during compaction
- Ordering dependencies between ingestion nodes for the same series

**Existing deduplication guarantees.** Quickwit provides deduplication at coarser granularities than individual points:

- **WAL checkpoint exactly-once.** The indexing pipeline publishes each split atomically with a checkpoint delta that records which WAL positions the split covers. On crash recovery, the checkpoint prevents re-indexing the same WAL entries into duplicate splits. This guarantees that a given batch of ingested data produces exactly one set of splits, not that individual points within or across batches are deduplicated. See [compaction-architecture.md](../compaction-architecture.md) for details.
- **File-level deduplication for queue sources.** The SQS/S3 file source tracks ingested files via metastore shard checkpoints with a configurable deduplication window, preventing re-ingestion of the same file.

**Per-point deduplication is not implemented.** If the same metric data point (identical metric name, tags, timestamp, and value) arrives in two separate ingest requests — due to client retries, overlapping sources, or upstream replay — both copies are stored. Per-point deduplication would require either sticky routing (binding a timeseries to a specific shard) or a dedup index (tracking recently-seen points), both of which add significant complexity and reliability risk. If per-point deduplication becomes a product requirement, it should be designed as a separate capability rather than baked into the storage data model. See [GAP-005](./gaps/005-no-per-point-deduplication.md).

### 3. No Storage-Level Interpolation

Interpolation (filling gaps in a timeseries, aligning timestamps across series) is not performed during ingestion or compaction. The storage layer stores raw points. Interpolation is a query-time operation.

This decouples storage format from query semantics. Different query patterns may require different interpolation strategies (linear, last-value, none), and embedding one strategy in the storage format would constrain future flexibility.

### 4. Timeseries ID (Optional Synthetic Column)

When data is sorted by a sort schema (see [ADR-002](./002-sort-schema-parquet-splits.md)), the explicit sort columns may not be granular enough to distinguish individual point sources. For example, if the sort schema is `metric_name|env`, hundreds of hosts within the same environment produce interleaved points within each `(metric_name, env)` group.

To improve locality, the data model includes an optional **`timeseries_id`** column: a hash of all tag names and values. When present, it is placed after explicit sort columns and before `timestamp` in the sort schema, acting as a tiebreaker that clusters points from the same tag combination.

**Properties:**

- **Synthetic column.** Not present in incoming data. Computed at ingestion by hashing the canonicalized (sorted by key name) set of all tag key/value pairs.
- **Hash function**: xxHash64 or SipHash. Deterministic and fast.
- **Persists through compaction.** Once computed and stored in the Parquet file, it does not need recomputation during merges.
- **Purely a physical layout optimization.** Not a semantic concept, not part of the query model. Nothing in the query path or system correctness depends on it.
- **Optional.** If explicit sort columns already provide sufficient granularity (e.g., include `host` or `container`), `timeseries_id` adds little value and can be omitted. It can be added or removed from the sort schema at any time — this is a schema change handled by the normal transition mechanism (new splits use the new schema, old splits age out via retention).

**Limitations:**

- **Hash collisions** (extremely unlikely with 64-bit hashes) would interleave two distinct point sources, affecting only physical layout, not correctness.
- **Tag flapping.** If a tag value intermittently changes (e.g., enrichment flapping between `NA` and an actual value), the hash changes and points from what a user would consider "the same source" get different `timeseries_id` values. This degrades locality but never correctness. If tag flapping is prevalent, omitting `timeseries_id` and relying on explicit sort columns alone may be preferable.
- **Min/max metadata is meaningless.** The hash value's min/max range has no query pruning utility (nobody filters by hash value). Implementations should emit null for `timeseries_id` in per-column min/max/regex metadata.

### 5. OTel Attribute Schema and Schema-on-Read


Map columns are fundamentally non-columnar: all key-value pairs for a row are packed into a single column value. This has two consequences for Parquet storage:

- **Poor compression.** A map column contains interleaved keys and values from many different attributes. Columnar encodings (RLE, dictionary) cannot exploit the structure of individual attributes because different attributes are mixed together in the same column. A dedicated `host` column with sorted data produces long runs of repeated values; a map column containing `host` alongside `env`, `region`, and dozens of other keys produces no useful runs.
- **No direct column access.** To evaluate a predicate like `attributes['host'] = 'web-01'`, the query engine must deserialize the entire map for each row and search for the key. There is no way to seek to `host` values specifically, and Parquet page-level statistics are meaningless for a map column (the min/max of a serialized map has no relationship to the values of individual keys within it).

**Schema-on-read: attributes as columns.** A more effective storage representation is to extract each attribute into its own Parquet column at write time. Rather than storing `Map{"host": "web-01", "env": "prod", "region": "us-east-1"}` as one map value, we store three separate columns: `attr.host = "web-01"`, `attr.env = "prod"`, `attr.region = "us-east-1"`. Each column is independently typed, independently compressed, and independently accessible for predicate evaluation and page-level pruning.

This is a **schema-on-read** approach: the storage layer stores data in whatever shape arrives, creating columns as needed, and any schema interpretation happens at query time. There is no requirement for the schema to be specified up front — new attribute keys that appear in incoming data produce new columns automatically. This is a key characteristic of cloud-native observability storage: "store all the data that the user sends, in whatever types they send it, resolving any ambiguity at query time."

**Dense vs sparse columns.** Not every attribute needs its own column. Attributes that appear in <1% of rows produce extremely sparse columns that waste storage on null markers and add schema complexity. A practical threshold is to extract attributes as dedicated columns when they are **dense** (present in >1% of rows) and keep rare attributes in a residual map column. The density threshold is a tunable parameter. Over time, compaction could consolidate: an attribute that starts sparse (few sources report it) but becomes dense (adopted widely) can be promoted to its own column.

**Implications for the sort schema.** Extracting attributes into columns is a prerequisite for effective sorting. The sort schema ([ADR-002](./002-sort-schema-parquet-splits.md)) references column names like `host`, `env`, `metric_name`. If these values are buried inside a map column, the sort is impossible — the writer cannot extract sort keys from a serialized map efficiently. Columnar attributes enable the sort schema to reference any attribute by name, and enable page-level statistics on sort columns that make intra-file pruning effective.

**Transition.** The current OTel map-based ingestion format is the starting point. The indexing pipeline can extract attributes into columns at write time, presenting the original OTel map interface at the API boundary while storing columnar data internally. This is transparent to ingest clients — they continue sending OTel-format data. Queries can access attributes either by the original map path (for compatibility) or by direct column access (for performance). The storage representation is an internal optimization, not a change to the external data model.

### 6. RLE/Dictionary Encoding Preservation

The point-per-row model's performance depends on columnar encodings being preserved through the query pipeline. Currently, RLE and dictionary encoding are decoded to plain arrays early in DataFusion's execution. As DataFusion grows operator-level support for these encodings, the performance benefits of sorted point-per-row data increase: longer runs in sorted columns translate directly to better RLE compression ratios that are maintained through query execution. This makes point-per-row a bet that improves over time rather than a static trade-off.

## Invariants

These invariants must hold across all code paths (ingestion, compaction, query).

| ID | Invariant | Rationale |
|----|-----------|-----------|
| **DM-1** | Each row in a Parquet split is exactly one data point: one metric value at one timestamp for one timeseries | Foundational data model. Enables row-level sorted merge without series-level merge logic |
| **DM-2** | No last-write-wins. If two data points with the same (metric name, tags, timestamp) arrive in separate ingest requests, both are stored | Eliminates sticky routing, series-level dedup, and ordering dependencies between nodes |
| **DM-3** | The storage layer does not perform interpolation. Points are stored as received; interpolation is a query-time operation | Decouples storage format from query semantics |
| **DM-4** | `timeseries_id`, if present, is deterministic: the same canonicalized tag set always produces the same hash value | Required for locality grouping to be consistent across ingestion and compaction |
| **DM-5** | `timeseries_id` persists through compaction without recomputation. The column is written once at ingestion and carried through all subsequent merges | Avoids recomputing hashes during merge (tags may not all be available as separate columns at merge time) |

## Consequences

### Positive

- **Simple, composable storage format.** Standard Parquet rows with no nested types. Every tool in the Parquet/Arrow ecosystem works out of the box.
- **Straightforward compaction.** K-way merge is row-level only. No series-level merge logic.
- **No routing constraints.** Any node can ingest any point for any series. Load balancing is unconstrained.
- **Query flexibility.** No interpolation baked into storage. Batch-level dedup at ingest handles the common cases; storage and query layers are not coupled to a dedup strategy.
- **Encoding-friendly.** Sorted point-per-row produces long columnar runs that compress well and benefit from RLE/dictionary preservation.

### Negative

- **Tag redundancy.** Every row for the same timeseries repeats all tag values. In timeseries-per-row, tags are stored once per series. With good columnar encoding on sorted data, this redundancy compresses away, but it is still present in the uncompressed representation and affects memory usage during query execution until DataFusion preserves dictionary/RLE encoding through more operators.
- **OTel map attributes defeat columnar benefits.** The current OTel ingest schema stores attributes as key-value maps. Until schema-on-read column extraction is implemented, attributes cannot participate in sorting, page-level pruning, or efficient columnar compression. This is the most significant near-term limitation of the data model.
- **No intra-series locality guarantee.** Without `timeseries_id` in the sort schema, points from the same series may be interleaved with points from other series that share the same sort-column values. This is a configuration choice, not an inherent limitation.
- **Duplicate points are stored.** Without LWW or per-point dedup, retried ingestion or overlapping sources can produce duplicate points. Existing batch-level dedup (WAL checkpoints, file-level tracking) prevents most duplicates, but cross-request duplicates are possible. See [GAP-005](./gaps/005-no-per-point-deduplication.md).

### Risks

- **Encoding-preservation dependency for performance parity.** Until RLE/dictionary encoding is preserved through DataFusion, point-per-row may scan more data than timeseries-per-row for series-centric queries (e.g., "plot CPU for host X"). The magnitude depends on the encoding-preservation timeline.
- **Wide tables (future research).** Metrics from the same source share nearly identical tags. Multiple metric names could be stored as separate value columns in a single wide row (e.g., `k8s.cpu.usage`, `k8s.cpu.limit`, `k8s.mem.usage` as columns sharing one tag set). This is the approach taken by TimescaleDB's hypertables. It would amortize tag storage further but requires significant compactor changes. Worth investigating as future research; it is compatible with point-per-row as an evolution, not a replacement.

## Signal Generalization

This ADR applies to **metrics** (Parquet pipeline). The data model decisions generalize as follows:

- **Traces**: Point-per-row maps naturally to span-per-row (each span is one row). No LWW applies (spans are immutable). `timeseries_id` equivalent would be a hash of trace attributes for locality grouping.
- **Logs**: Point-per-row maps to log-entry-per-row (already the Quickwit model). No LWW. `timeseries_id` equivalent could group log entries by service/host.

The no-LWW and no-storage-interpolation decisions are universal across signals. The `timeseries_id` concept generalizes to any signal where grouping related records improves compression.

## Decision Log

| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-02-19 | Initial ADR created | Establish foundational data model for Parquet metrics pipeline |
| 2026-02-19 | Point-per-row chosen over timeseries-per-row | Simpler compaction, no LWW, standard DataFusion operators. Performance parity via columnar encoding and dictionary/RLE preservation through more operators |
| 2026-02-19 | No LWW semantics | Eliminates sticky routing and series-level dedup. Simplifies ingestion and compaction |
| 2026-02-19 | Dedup clarified: batch-level exists, per-point does not | WAL checkpoints provide exactly-once at the batch level. File-level dedup for queue sources. Per-point dedup not implemented; identified as GAP-005 if needed |
| 2026-02-19 | timeseries_id defined as optional synthetic column | Provides intra-group locality tiebreaker without adding complexity to the core data model |
| 2026-02-19 | Schema-on-read identified as target for attribute storage | OTel map-based attributes are non-columnar, defeating compression and sort/pruning. Extract dense attributes (>1% non-null) into individual columns at write time, keep rare attributes in residual map |

## Implementation Status

### Implemented

| Component | Location | Status |
|-----------|----------|--------|
| Point-per-row Parquet schema | `quickwit-parquet-engine/src/schema/fields.rs` | Done. Each row is one metric data point |
| Tag columns in Parquet | `quickwit-parquet-engine/src/schema/fields.rs` | Done. Tags stored as dictionary-encoded columns per row |

### Not Yet Implemented

| Component | Notes |
|-----------|-------|
| timeseries_id computation | Hash of canonicalized tag key/value pairs, added as column at ingestion |
| timeseries_id persistence through compaction | Column must survive merge without recomputation |
| Schema-on-read attribute extraction | Extract dense attributes from OTel map columns into individual Parquet columns at write time |
| Dense/sparse column threshold | Determine density threshold (e.g., >1% non-null) for column extraction vs residual map |
| Residual map for sparse attributes | Keep rare attributes in a fallback map column alongside extracted dense columns |

## References

- [Phase 1: Sorted Splits for Parquet](../locality-compaction/phase-1-sorted-splits.md) — full design document
- [ADR-002: Configurable Sort Schema](./002-sort-schema-parquet-splits.md) — sort schema that operates on this data model
- [ADR-003: Time-Windowed Sorted Compaction](./003-time-windowed-sorted-compaction.md) — compaction that relies on this data model
