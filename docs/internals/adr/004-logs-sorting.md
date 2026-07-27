# ADR-004: Log Sorting for Compression

## Metadata

- **Status**: Proposed
- **Date**: 2026-07-10
- **Last updated**: 2026-07-27
- **Tags**: storage, logs, indexing, tantivy, sorting, compression
- **Components**: quickwit-indexing, quickwit-config
- **Authors**: Quickwit contributors
- **Related**: [ADR-002](./002-sort-schema-parquet-splits.md), [ADR-003](./003-time-windowed-sorted-compaction.md)

## Context

Logs are indexed into Tantivy segments in ingestion order. Ingestion order is driven by arrival time, batching, and load balancing; it is not correlated with the structure of the log message or with common dimensions such as `service`. As a result, a single segment often interleaves many unrelated log templates:

```text
api:    "server started at 8080"
worker: "job 123 finished in 42ms"
api:    "server started at 9090"
db:     "connection from 1.2.3.4"
worker: "job 456 finished in 13ms"
```

This layout is convenient for ingestion but suboptimal for compression. Stored log bodies and repeated fields compress better when similar documents are adjacent. When templates, field shapes, and stable dimensions are interleaved, general-purpose compression has shorter repeated runs and less locality to exploit.

The Parquet pipeline already treats physical sort order as a storage-level optimization for compression and query efficiency (see [ADR-002](./002-sort-schema-parquet-splits.md)). Logs use Tantivy rather than Parquet, but the same principle applies: physical order inside a split can improve storage characteristics without changing the logical query model.

## Decision

We introduce optional fingerprint-based sorting for log indexing. A YAML policy in `NodeConfig::docs_sorting_config` enables the feature on restart for indexing pipelines in that process. `QW_ENABLE_DOCS_SORTING=false` acts as a process-level environment override even when the policy is configured. Deployments should therefore enable the policy only on indexers whose workloads are intended to use document sorting. Enabled pipelines compute a lightweight `Fingerprint` for each processed document and use it to assign similar documents nearby Tantivy doc IDs before segment finalization.

Sorting is applied exactly once per fresh split, when that split is finalized from ingested documents. Later merge operations use Tantivy's native merge path: they stack the already-sorted input splits without adding additional remapping and doc-store rewrite costs to merges.

The fingerprint is computed from:

1. The document shape, represented by the sorted set of leaf JSON paths.
2. A YAML-defined grouping policy that selects tokenized and raw document fields and excludes
   configured paths from the document-shape fingerprint.
3. Tokenized signatures, limited to the first 50 tokens, for volatile text fields, so messages with
   the same template but different IDs, ports, UUIDs, or IP addresses hash together.

Example:

```text
"server started at 8080" -> Word Gap Word Gap Word Gap Number
"server started at 9090" -> Word Gap Word Gap Word Gap Number
```

Those two messages can share a fingerprint even though their literal values differ.

During indexing:

1. `DocProcessor` computes `Fingerprint` from the processed JSON document when process-level sorting is enabled.
2. `ProcessedDoc::fingerprint_opt` carries the fingerprint to the `Indexer`.
3. `Indexer` records each split-local doc ID in `DocIdSorter`.
4. At split finalization, `DocIdSorter` builds a Tantivy `DocIdMapping`.
5. Tantivy writes the initial segment with documents grouped by fingerprint, emitting the largest sort groups first and preserving insertion order for unsorted documents.

This is a write-time physical layout optimization. It does not change search semantics, indexing checkpoints, split metadata, or the document schema exposed to users.

## Consequences

### Positive

- **Better compression for repeated log templates.** Similar log messages and field shapes become adjacent, giving the doc store compressor longer repeated runs and better locality.
- **No query model change.** The optimization only remaps internal Tantivy doc IDs during segment finalization; user-visible fields and search behavior remain unchanged.
- **Works with dynamic log schemas.** Hashing document shape plus the configured grouping policy gives a useful sorting key without requiring a fixed log schema.

### Negative

- **Higher local disk write volume.** Tantivy's manual doc ID mapping uses a temporary uncompressed doc store followed by a compressed rewrite in permuted order.
- **Extra ingest CPU.** Each processed document needs hash computation, including tokenization for `message`.
- **Best-effort ordering only.** Fingerprint-based grouping improves locality but does not create a total sort order or a query-pruning structure.
- **Compression benefit depends on workload.** Gains depend on the distribution and entropy of the indexed logs. Workloads with recurring schemas, templates, and stable field values have more redundancy for sorting to expose. Highly unique, high-entropy, or already well-grouped logs may see little improvement.

### Risks

- **Indexer storage must have write headroom.** The higher temporary write volume can exhaust provisioned IOPS or throughput before CPU or memory becomes limiting.
- **Sorter memory is not yet included in the indexer's heap accounting.** High-cardinality fingerprints retain document IDs and hash-map entries outside Tantivy's reported memory usage. This must be measured and accounted for before broad rollout.
- **Fingerprint design can over-group or under-group.** If the configured policy ignores too much, unrelated documents may be grouped together. If it includes too much volatile data, similar templates may fail to group. This impacts the overall compression gain.

## Signal Generalization

This ADR applies to **logs** in the Tantivy indexing path.

The underlying principle generalizes across signals: physically group records that are likely to share values, templates, or query predicates. Metrics use explicit sort schemas in Parquet (ADR-002). Logs use fingerprint-based sorting because log schemas are dynamic and message templates are more important than a fixed column order. Traces could use a related strategy based on service, operation, resource, or trace-local grouping, but would need a separate decision because trace query and reconstruction patterns differ from logs.

## Decision Log

| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-07-10 | Initial ADR created | Document the rationale for sorting logs by fingerprint to improve Tantivy segment compression |
| 2026-07-20 | Scope sorting at deployment level | The environment variable applies process-wide; dedicated indexing processes provide workload isolation |
| 2026-07-21 | Retain write-time-only sorting as the proposed rollout design | Sorting fresh splits preserves the benefit through later merges while avoiding repeated remapping and document-store rewrites |

## Implementation Status

### Implemented

| Component | Location | Status |
|-----------|----------|--------|
| Deployment scope | Indexer node configuration | Enable only on indexers whose workloads should use document sorting |
| Policy configuration | `NodeConfig::docs_sorting_config` | Optional YAML policy; presence enables document sorting and is validated when node configuration is loaded unless disabled by the environment override |
| Environment override | `QW_ENABLE_DOCS_SORTING` | Unset preserves and validates the YAML configuration; `false` disables a configured policy without validating it |
| Fingerprint computation | `quickwit-indexing/src/docs_sorting/fingerprinter.rs` | Implemented on feature branch |
| Tokenization | `quickwit-indexing/src/docs_sorting/tokenizer.rs` | Implemented on feature branch |
| Doc ID sorting | `quickwit-indexing/src/docs_sorting/sorter.rs` | Implemented on feature branch |
| Pipeline wiring | `quickwit-indexing/src/actors/doc_processor.rs`, `quickwit-indexing/src/actors/indexer.rs`, `quickwit-indexing/src/models/indexed_split.rs` | Implemented on feature branch |

### Expected Results

The compression gain is workload-dependent rather than a fixed percentage. Sorting is most effective when many documents share schemas, message templates, or stable field values but arrive interleaved. In that case, grouping similar documents increases local redundancy and gives the document-store compressor longer repeated runs.

The gain decreases as document entropy increases. Workloads dominated by unique schemas, unique messages, encrypted or encoded payloads, or high-cardinality values contain less reusable structure. Logs that already arrive grouped by template or schema also leave less room for sorting to improve compression.

Each deployment should benchmark a representative corpus and compare compressed bytes per document and compressed bytes per uncompressed byte. CPU cost, memory use, and temporary local-disk writes should be measured alongside storage savings because their impact also depends on document shape, fingerprint policy, split size, and ingest concurrency.

### Open Validation and Rollout Work

| Component | Notes |
|-----------|-------|
| Search resource efficiency | Compare identical query streams with searcher CPU, memory, cache, object-storage reads, split fan-out, and concurrency telemetry |
| Disk capacity | Verify that provisioned local-disk IOPS and throughput tolerate the additional temporary write volume |
| Sorter memory accounting | Include `DocIdSorter` allocations in the indexer memory limit and test high-cardinality fingerprints across multiple open partitions |
| Rollout validation | Verify deployment manifests configure `docs_sorting` only on intended indexers and retain the environment override |
| Representative benchmark | Compare identical binaries and input streams on a corpus representative of the target workload |

## References

- [ADR-002: Configurable Sort Schema for Parquet Splits](./002-sort-schema-parquet-splits.md)
- [ADR-003: Time-Windowed Sorted Compaction for Parquet](./003-time-windowed-sorted-compaction.md)
- [Husky Storage Compaction Blog Post](https://www.datadoghq.com/blog/engineering/husky-storage-compaction/) — prior art
