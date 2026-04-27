# Split Compaction Architecture in Quickwit

**Last Updated:** 2026-02-11

## Table of Contents

1. [Overview](#overview)
2. [The Data Path: From Document to Split](#the-data-path-from-document-to-split)
3. [Compaction: How Splits Get Merged](#compaction-how-splits-get-merged)
4. [Data Ownership and Scaling](#data-ownership-and-scaling)
5. [How Queries Find Splits](#how-queries-find-splits)
6. [Document Routing Detail](#document-routing-detail)
7. [Partition IDs: Tenant Isolation](#partition-ids-tenant-isolation)
8. [The Metrics Pipeline](#the-metrics-pipeline)
9. [Configuration Reference](#configuration-reference)

---

## Overview

Quickwit ingests documents into small, immutable units called **splits**. As splits accumulate, background **compaction** merges small splits into larger ones. Larger splits improve query throughput by reducing the number of units a searcher must open and scan.

This document follows the path of data from ingestion through compaction and into queries, then covers the scaling properties that emerge from the architecture.

### Vocabulary

- **Split** -- a self-contained unit of indexed data (a Tantivy segment for logs/traces, or a Parquet file for metrics). Documents within a split are stored in **ingestion order** -- no sort is applied at index time or during merges. Once published, a split is immutably associated with the `node_id` of the node that created it.
- **Shard** -- a logical WAL stream for a particular (index, source) pair, hosted on an ingester node. Ephemeral: can be closed and replaced on a different node at any time.
- **Merge scope** -- the 5-part key `(node_id, index_uid, source_id, partition_id, doc_mapping_uid)`. Each unique combination gets an independent compaction hierarchy with no cross-key coordination.
- **Maturity** -- a split's eligibility for further merging. Immature splits can be merged; mature splits (by size or age) will not be merged again.

---

## The Data Path: From Document to Split

### End-to-End Diagram

```
Client
  │
  ▼
External Load Balancer (K8s Service, nginx, etc.)
  │  distributes across Quickwit nodes
  ▼
IngestRouter (on any node -- every node is a router)
  │  picks a shard (round-robin, local preferred)
  ▼
Ingester Node (shard leader)
  │  appends to shard's WAL (local disk)
  ▼
Indexing Pipeline (usually same node)
  │  reads WAL → builds Tantivy segment
  │  stamps split with (node_id, index_uid, source_id, partition_id, doc_mapping_uid)
  ▼
Object Storage (S3/GCS)     +     PostgreSQL (metadata)
```

### a) Client to Router

Every Quickwit node runs an `IngestRouter` and exposes REST and gRPC ingest endpoints -- there are no dedicated "router" nodes. Clients reach Quickwit through an **external load balancer** (a Kubernetes Service, an nginx proxy, etc.) that distributes requests across the available nodes. Quickwit itself has no client-side routing protocol; it relies on the deployment infrastructure to spread traffic.

Available ingest endpoints include:
- **Native API**: `POST /api/v1/<index-id>/ingest` (NDJSON)
- **Elasticsearch-compatible bulk**: `POST /api/v1/_elastic/_bulk`
- **OTLP**: `POST /api/v1/otlp/v1/{logs,traces,metrics}` (protobuf) and gRPC equivalents

**Location:** `quickwit/quickwit-serve/src/lib.rs:940` -- every node instantiates an `IngestRouter`.

### b) Router to Shard

Once a request lands on a node, its `IngestRouter` picks a shard for the document's (index, source) pair. Shard selection happens **before** the WAL write -- the shard determines which node's WAL receives the data.

A single ingest request's documents all go to **one shard** (and therefore one leader node). Across requests, the router distributes documents across shards via round-robin, preferring shards hosted locally on the router's own node. See [Document Routing Detail](#document-routing-detail) for the full algorithm.

### c) The WAL

Each shard has its own append-only WAL stream, stored on the leader node's local disk via `mrecordlog`. Multiple shards can coexist on one node. Optional replication sends writes to a follower node.

**Physical location:** `{data_dir}/queues/` on the ingester node.

**Persistence:** By default, the WAL flushes every 5 seconds without fsync (`PersistPolicy::OnDelay`). For metrics pipelines, the WAL uses persistent volumes to survive pod restarts.

### d) WAL to Split

An indexing pipeline reads batches from the shard's WAL. Documents are parsed, validated, and written into a Tantivy segment in arrival order. When the batch hits a threshold (`commit_timeout_secs` or size), the split is finalized:

1. The split is stamped with the 5-part key from its pipeline identity and partition
2. The split file is uploaded to object storage (`{split_id}.split`)
3. Split metadata is published atomically to PostgreSQL **in the same transaction as a checkpoint delta** that records which WAL positions the split covers

The checkpoint delta is critical for exactly-once semantics: it ties "these documents are now in a published split" to "the WAL has been consumed up to this position" in a single atomic operation. If the node crashes before publication, the in-progress split is abandoned, but the checkpoint hasn't advanced, so the documents will be re-read from the WAL and re-indexed into a new split on restart. Recovery works by fetching the checkpoint from PostgreSQL on startup -- a small metadata read, not a scan of splits or history. Published splits are write-once artifacts that are never reopened or appended to.

Note that checkpoints are purely a concern of the initial indexing pipeline. Merge (compaction) operations do not interact with checkpoints at all -- the merge executor publishes the merged split with no checkpoint delta, and the metastore skips the checkpoint update.

Initial splits are small -- typically 100K-500K documents. Compaction happens later.

### e) Where Things Physically Live

| Data | Location |
|------|----------|
| WAL | Local disk on ingester nodes (ephemeral unless persistent volume) |
| Split files | Object storage (`{split_id}.split` or `.parquet`) |
| Split metadata | PostgreSQL (`splits` table) |

### Pipeline Actors (detail)

<details>
<summary>Click to expand actor-level detail</summary>

The indexing pipeline is a chain of actors:

1. **Source** (e.g., `IngestV2Source`) -- polls the shard's WAL, fetches document batches
2. **DocProcessor** -- parses and validates JSON documents, computes partition keys
3. **Indexer** -- creates Tantivy segments from document batches
4. **Serializer** -- serializes the segment
5. **Packager** -- packages the split with metadata
6. **Uploader** -- uploads the split file to object storage
7. **Sequencer** -- ensures ordering
8. **Publisher** -- atomically publishes split metadata to PostgreSQL

**Location:** `quickwit/quickwit-indexing/src/actors/indexing_pipeline.rs`

</details>

---

## Compaction: How Splits Get Merged

Small splits accumulate from ingestion. A **merge planner** on each node periodically examines published splits and combines eligible ones into larger splits. The merged split replaces the originals: it is uploaded to storage, old splits are marked for deletion, and PostgreSQL is updated atomically.

### The Merge Scope

Each unique combination of the 5-part key gets an independent compaction hierarchy:

```
(node_id, index_uid, source_id, partition_id, doc_mapping_uid)
 ╰──────── pipeline identity ────────╯  ╰──── merge partition ────╯
```

**Compaction is entirely node-local.** There is no cross-node merge coordination. If N nodes produce data for the same partition, you get N independent hierarchies.

The key is enforced in two stages:

**Stage 1 -- Pipeline identity filtering** (first 3 components). Each node's merge planner filters to only the splits matching its own `(node_id, index_uid, source_id)`:

```rust
// quickwit/quickwit-indexing/src/actors/merge_planner.rs:350-355
fn belongs_to_pipeline(pipeline_id: &MergePipelineId, split: &SplitMetadata) -> bool {
    pipeline_id.node_id == split.node_id
        && pipeline_id.index_uid == split.index_uid
        && pipeline_id.source_id == split.source_id
}
```

**Stage 2 -- Partition grouping** (last 2 components). After pipeline filtering, remaining splits are grouped by `(partition_id, doc_mapping_uid)`. Each group gets its own independent merge policy evaluation.

### Why Each Key Component Matters

**`node_id`** -- The `node_id` constraint exists because of **how the current merge planner is implemented**, not because of a fundamental data integrity requirement. Each node runs its own independent merge planner, and each planner only processes splits stamped with its own `node_id`. This avoids any need for cross-node coordination during compaction.

Importantly, **checkpoints are not a reason for this constraint**. Checkpoints are part of the *initial indexing pipeline* -- they track WAL consumption and are updated atomically when a split is first published (see [WAL to Split](#d-wal-to-split)). But merge operations do not interact with checkpoints at all: the merge executor passes no checkpoint delta (`checkpoint_delta_opt: Default::default()`), and the metastore skips checkpoint updates when no delta is provided. Published splits are write-once artifacts; they don't carry WAL position information and are never read back during recovery. Merging splits from different nodes would not corrupt any checkpoint state.

This means the `node_id` constraint is an **implementation choice for simplicity**, not an architectural invariant. Cross-node compaction is feasible without checkpoint changes -- it's a coordination problem, not a data integrity problem. See [Future: Locality-Aware Compaction](#future-locality-aware-compaction) and [Data Ownership and Scaling](#data-ownership-and-scaling) for the implications.

**`index_uid`** -- Includes the logical index name plus an incarnation ID (ULID). Prevents merging across different indexes or across delete-and-recreate cycles. Different indexes have different schemas, storage prefixes, and merge policies.

**`source_id`** -- A **source** is a configured data ingestion channel for an index. Each source has a type (Kafka, Kinesis, file, IngestV2, etc.), a user-defined `source_id` string, and type-specific parameters (e.g., a Kafka topic and broker list). A single index can have **multiple sources** -- for example, `kafka-us-east` and `kafka-eu-west` feeding the same index, or an `_ingest-source` for real-time API ingestion alongside a `kafka-batch` source for batch processing.

Like `node_id`, the `source_id` constraint in the merge scope is an **implementation choice**, not a checkpoint integrity requirement. Merge operations do not update checkpoints, so merging splits from different sources would not corrupt checkpoint tracking. The constraint exists because each node's merge planner is scoped to a single `(node_id, index_uid, source_id)` pipeline, keeping the implementation simple and coordination-free. In practice, merging across sources would rarely be useful anyway -- splits from different sources typically represent different data feeds (e.g., different Kafka topics or regions).

**Location:** `quickwit/quickwit-config/src/source_config/mod.rs` (source configuration and types)

**`partition_id`** -- Isolates tenants or other logical groupings. Keeping partitions separate enables query-time split pruning. See [Partition IDs](#partition-ids-tenant-isolation).

**`doc_mapping_uid`** -- Identifies the schema version. Prevents merging splits built with incompatible schemas after a mapping change.

### The StableLogMergePolicy

**Location:** `quickwit/quickwit-indexing/src/merge_policy/stable_log_merge_policy.rs`

The default merge policy implements a **logarithmic level-based strategy** inspired by LSM trees. Within each merge scope:

**Step 1: Sort splits by recency.** Most-recent-first by `time_range.end`, then by doc count, then by split ID for determinism.

**Step 2: Build logarithmic levels.** Splits are organized into levels based on document count. Each level is ~3x larger than the previous:

```
Level 0: [0 ... 300K docs)       (min_level_num_docs * 3)
Level 1: [300K ... 900K)          (3x previous)
Level 2: [900K ... 2.7M)          (3x again)
...
Level N: [X ... 10M)              (split_num_docs_target)
```

**Step 3: Find merge candidates per level.** For each level (oldest-first), the policy accumulates splits until a threshold is hit:

| Condition | Threshold | Effect |
|-----------|-----------|--------|
| Too few splits | `merge_factor` (default: 10) | Won't merge yet |
| Too many splits | `max_merge_factor` (default: 12) | Stop accumulating, merge what we have |
| Result too large | `split_num_docs_target` (default: 10M) | Stop accumulating, merge what we have |

**Last-merge rule:** If adding one more split would exceed `split_num_docs_target`, merge with fewer than `merge_factor` splits. This prevents orphan splits that can never find enough merge partners at their level.

**Split maturity exits:** Splits with >= 10M docs (size maturity) or older than 48h (time maturity) graduate out of merge eligibility entirely.

### Physical Merge Process

The merge scheduler coordinates execution. Source splits are downloaded from object storage, merged using Tantivy's `UnionDirectory`, and the result is uploaded as a new split. The merge publisher atomically updates PostgreSQL: publishes the new merged split and marks the old splits as `MarkedForDeletion`.

<details>
<summary>Merge pipeline actors (detail)</summary>

1. **MergePlanner** -- queries PostgreSQL for immature splits, applies merge policy
2. **MergeScheduler** -- coordinates merge operations across indexes
3. **MergeSplitDownloader** -- downloads source splits from storage
4. **MergeExecutor** -- merges using Tantivy `UnionDirectory`
5. **MergePackager** -- packages the merged split
6. **MergeUploader** -- uploads the new split
7. **MergePublisher** -- atomically updates PostgreSQL

**Location:** `quickwit/quickwit-indexing/src/actors/merge_planner.rs`

</details>

### Example

**Setup:** 3 indexer nodes, all ingesting into the same index and source, `partition_id=1`. Each node has produced 13 splits of 100K docs.

Each node's merge planner operates independently on its own 13 splits:

1. All 13 splits are in Level 0 (all < 300K)
2. Walk backwards: 12 splits accumulated (Split 2 through 13)
3. 12 >= `merge_factor` (10), 12 <= `max_merge_factor` (12), total 1.2M < 10M target
4. Merge Splits 2-13 into one 1.2M-doc split (now Level 1)
5. Split 1 remains, will merge with future splits

**Result across the cluster:**

```
Node 1: Split 1 (100K) + Split NEW-1 (1.2M)   ← independent hierarchy
Node 2: Split 1 (100K) + Split NEW-2 (1.2M)   ← independent hierarchy
Node 3: Split 1 (100K) + Split NEW-3 (1.2M)   ← independent hierarchy
```

The same partition has 6 splits across the cluster (3 leftover + 3 merged), not 1 merged split. Each node compacts at its own pace.

---

## Data Ownership and Scaling

This section describes the most important architectural consequence of node-local compaction: **shards are ephemeral, but splits are permanent.**

### The Core Asymmetry

A **shard** can be closed and replaced by a new shard on a different node at any time -- during rebalancing, node departure, or scale-up. The control plane does this routinely.

But once a split is published, its `node_id` is **immutable**. It is baked into the metadata in PostgreSQL. No mechanism exists to reassign a split's `node_id` after publication. The `belongs_to_pipeline()` function in the merge planner performs a strict equality check on `node_id`, so only a merge planner running on the *original* node can ever pick up that split for compaction.

### What Happens When a Node Leaves

1. The control plane detects the departure (via chitchat membership protocol) and triggers `rebalance_shards()`.
2. Old shards on the departed node are closed; new shards are opened on remaining nodes.
3. **New incoming data** flows to the new shards on surviving nodes.
4. **WAL data** on the departed node that was not yet converted to splits is **lost** (unless replication was configured, in which case the follower can take over).
5. **Published splits** from the departed node remain in PostgreSQL and object storage. They are still **fully queryable**.
6. **But they can never be compacted again.** No running merge planner has the departed node's ID, so `belongs_to_pipeline()` will never match those splits.

```
Before:  Node A (alive)          Node B (alive)
         ┌──────────────┐        ┌──────────────┐
         │ Shard 1 (WAL)│        │ Shard 2 (WAL)│
         │ Splits: S1-S5│        │ Splits: S6-S9│
         │ MergePlanner ─┤        │ MergePlanner ─┤
         │ (node_id = A) │        │ (node_id = B) │
         └──────────────┘        └──────────────┘

After:   Node A (alive)          Node B (departed)
         ┌──────────────┐
         │ Shard 1 (WAL)│        Splits S6-S9 still in
         │ Shard 3 (new)│        object storage + Postgres
         │ Splits: S1-S5│        ✓ Queryable
         │ MergePlanner ─┤        ✗ Will never be compacted
         │ (node_id = A) │
         └──────────────┘
```

### What Happens When a Node Joins

1. The control plane triggers rebalancing.
2. New shards may be allocated to the new node.
3. **No existing data migrates.** The new node only receives new incoming data through its new shards.
4. The new node's merge planner only compacts splits it creates itself.

### Queries Are NOT Bound by node_id

This is critical: while `node_id` controls which merge planner owns a split, **queries ignore `node_id` entirely**.

Search uses **rendezvous hashing on `split_id`** to assign splits to searcher nodes, with load-aware balancing:

```rust
// quickwit/quickwit-search/src/search_job_placer.rs:212
sort_by_rendez_vous_hash(&mut candidate_nodes, job.split_id());
```

The `node_id` field is not referenced anywhere in the `quickwit-search` module. Any searcher node can read any split from object storage regardless of which node created it. Orphaned splits from departed nodes are fully queryable -- the only impact is on split count.

### Implications for Long-Running Clusters

Over time, a cluster that experiences node churn accumulates small orphaned splits from departed nodes:

- **They are still queryable** -- in object storage, with metadata in PostgreSQL.
- **They will never be compacted** -- no merge planner will claim them.
- **Query performance degrades proportionally to split count.** More splits means more units per query to open and scan. This is not a routing problem (queries find them fine) but a fan-out cost.
- **Retention policies still apply.** If time-based deletion is configured, orphaned splits will be cleaned up when they expire.

```
Time ──────────────────────────────────────────────►

Node A:  ████ ████ ████████ ████████████████████     (compacting normally)
Node B:  ████ ████ ██── departed ──                  (4 small orphaned splits remain)
Node C:       ████ ████ ████████ ████████████████    (joined later, compacting normally)
Node D:            ████ ██── departed ──             (2 small orphaned splits remain)

Total orphaned small splits grows with each departure.
```

---

## How Queries Find Splits

Queries are completely decoupled from `node_id`. The query path:

1. **Coordinator** (root search) queries PostgreSQL for relevant splits, filtered by time range, tags, index, etc. The `node_id` field is not part of any query filter.
2. **Job placement**: each split becomes a search job. Jobs are assigned to searcher nodes via **rendezvous hashing on `split_id`** combined with load-aware balancing. The hasher sorts candidate nodes by affinity, then the placer assigns jobs to the first node under a ~5% load disparity target.
3. **Execution**: each searcher reads its assigned splits directly from object storage. No node needs to "own" the split to read it.
4. **Cache warming**: indexers can optionally notify searchers of newly published splits via `report_splits()` so the split cache can pre-warm. This uses `split_id` and `storage_uri`, not `node_id`.

**Key point:** `node_id` is purely a compaction concept. It has no role in query routing or execution.

---

## Document Routing Detail

How documents get routed to specific nodes determines the `node_id` that gets stamped on splits, so routing directly shapes compaction topology.

### Three Layers of Routing

#### Layer 1: Control Plane Shard Allocation (strategic)

The control plane decides which ingesters host which shards. When a router needs shards for an (index, source) pair, it requests them from the control plane.

The `allocate_shards()` function selects ingesters using **least-loaded-first**:

1. Count open shards per available ingester
2. Pick the ingester with the fewest open shards
3. Break ties randomly
4. If replication is enabled, pick two different nodes (leader + follower)

**Location:** `quickwit/quickwit-control-plane/src/ingest/ingest_controller.rs`

#### Layer 2: Router Shard Selection (per-request)

When an `IngestRouter` receives documents, it picks a shard from its routing table.

The `next_open_shard_round_robin()` function:

1. **Tries local shards first** -- shards where the router's own node is the leader
2. **Falls back to remote shards** -- shards on other nodes
3. **Round-robins within each category** -- uses an atomic counter
4. Skips closed or rate-limited shards

**Location:** `quickwit/quickwit-ingest/src/ingest_v2/routing_table.rs:141-165`

A single request's documents all go to one shard. Distribution across shards happens across requests.

#### Layer 3: Indexing Pipeline Scheduling (where splits get created)

The control plane decides which node runs the indexing pipeline that consumes from each shard. The `build_physical_indexing_plan()` function:

1. Accounts for CPU capacity of each indexer node
2. Uses **shard affinity** -- prefers scheduling indexing on the node that hosts the shard
3. Inflates capacities to 120% of total load for headroom
4. Works iteratively from the previous solution for stability

**Location:** `quickwit/quickwit-control-plane/src/indexing_scheduler/scheduling/mod.rs`

### How `node_id` Gets Stamped

The split's `node_id` comes from the `IndexingPipelineId` of the node running the indexing pipeline:

```rust
// quickwit/quickwit-indexing/src/models/indexed_split.rs:100-103
split_attrs: SplitAttrs {
    node_id: pipeline_id.node_id,
    index_uid: pipeline_id.index_uid,
    source_id: pipeline_id.source_id,
    ...
}
```

### Can a Split's `node_id` Differ From Its Shard's `leader_id`?

Yes, but rarely. The control plane prefers co-locating the indexing pipeline with the shard leader. However, if the leader node lacks CPU capacity, the indexing pipeline may run on a different node, reading from the shard remotely. In that case, the split's `node_id` is the indexer's node, not the shard leader.

---

## Partition IDs: Tenant Isolation

### What is partition_id?

`partition_id` is a **u64 hash** used to isolate documents based on configurable field(s). It's one of the five components of the merge scope key, so splits with different `partition_id` values get independent compaction hierarchies and are never merged together.

### How partition_id Is Computed

The `partition_key` expression in the index config determines partitioning:

```yaml
# Simple field-based
partition_key: service_name

# Hash with modulo (limits to 100 distinct partitions)
partition_key: hash_mod(service_name, 100)

# Composite fields
partition_key: hash_mod((service,division,city), 50)
```

**Computation flow:**

1. Extract field values from the JSON document
2. Hash using SipHasher (salted with the expression tree) for deterministic partitioning
3. Apply modulo if configured
4. Result: u64 `partition_id`

If no `partition_key` is configured, all documents get `partition_id = 0`.

**Location:** `quickwit/quickwit-doc-mapper/src/doc_mapper/doc_mapper_impl.rs:501`

### The OTHER_PARTITION_ID Overflow

```rust
// quickwit/quickwit-indexing/src/actors/indexer.rs:60
const OTHER_PARTITION_ID: u64 = 3264326757911759461u64;
```

When a single indexing workbench exceeds `max_num_partitions`, additional documents are mapped to this special catch-all partition to prevent memory explosion from tracking too many concurrent splits.

---

## The Metrics Pipeline

The metrics pipeline uses a **completely different implementation** from logs/traces.

### Key Differences

| Aspect | Logs/Traces (Tantivy) | Metrics (Parquet) |
|--------|----------------------|-------------------|
| **Storage format** | Tantivy segments (`.split`) | Parquet files (`.parquet`) |
| **Pipeline actors** | 8 (indexing) + 7 (merge) | 4 (no merge pipeline) |
| **Compaction** | StableLogMergePolicy | Not implemented |
| **WAL** | IngestV2 (ephemeral by default) | IngestV2 only (persistent volume) |
| **Metadata** | `SplitMetadata` (Postgres) | `MetricsSplitMetadata` (Postgres) |
| **Query engine** | Tantivy + custom code | DataFusion + Arrow |

### Pipeline Architecture

```
Source → MetricsDocProcessor → MetricsIndexer → MetricsUploader → MetricsPublisher
```

- **MetricsDocProcessor** -- converts Arrow IPC to RecordBatch
- **MetricsIndexer** -- accumulates batches, writes Parquet splits
- **MetricsUploader** -- stages and uploads Parquet files to storage
- **MetricsPublisher** -- publishes metadata to PostgreSQL

**Location:** `quickwit/quickwit-indexing/src/actors/indexing_pipeline.rs:600-728`

### Metrics Skip IngestV1

Metrics indexes are filtered out of IngestV1 scheduling:

```rust
// quickwit/quickwit-control-plane/src/indexing_scheduler/mod.rs:219-222
if is_metrics_index(&source_uid.index_uid.index_id) {
    continue;  // Skip IngestV1 source for metrics
}
```

### Persistent WAL for Metrics

Metrics use persistent volumes for the WAL to survive pod restarts, preventing data loss during failures:

```yaml
# k8s/eks/metrics.quickwit.dev.yaml
indexer:
  persistentVolume:
    enabled: true
    storage: "10Gi"
    storageClass: "gp3"
```

### Why No Compaction (Yet)?

Metrics splits accumulate without compaction. This is tolerable in the short term because DataFusion can query many small Parquet files, and time-based retention eventually removes old data. But it is not ideal, and metrics compaction is a planned goal.

### The Problem With the Current Architecture for Metrics

The existing log/trace compaction system (StableLogMergePolicy) is a poor fit for metrics even if it were enabled on Parquet splits. The core issue is **data locality**.

Metrics time series emit points on a periodic schedule (e.g., every 10 seconds). With load-balanced routing across nodes and shards, the points for any given time series are scattered across whichever nodes happened to receive them. Each node produces its own splits independently, so a single time series' data ends up fragmented across many small splits on many nodes. The node-local compaction model (merge scope bound by `node_id`) means these fragments can never be merged together -- each node only compacts its own portion.

Logs and traces suffer from the same fundamental scattering -- data is load-balanced across nodes with no content-aware placement, so documents for a given service, trace ID, or tag combination are spread across splits on every node. The current compaction model can *get away with* this because log/trace queries typically scan by time range and filter by tags, so a full scan across all matching splits still produces correct results. But "correct" is not "efficient": every query must fan out to every split in the time range, with no ability to prune splits based on data content. The system works, but it leaves significant query performance on the table.

For metrics the problem is more acute. Metrics queries often need to reconstruct a **single time series** across a time window (e.g., "plot CPU usage for host X over the last hour"), which means reading a point or two from each of many splits. The fan-out cost per query grows with the number of splits, and the lack of cross-node compaction means this never improves.

### Future: Locality-Aware Compaction

The eventual goal is a compaction system designed around data locality, drawing on approaches similar to [Husky's storage compaction](https://www.datadoghq.com/blog/engineering/husky-storage-compaction/). While the immediate motivation is metrics, the same approach would benefit logs and traces by enabling split pruning based on data content rather than requiring full scans. The key ideas include:

- **Cross-node compaction** -- unlike the current model, locality-aware compaction must merge data regardless of which node produced it, since load balancing inherently distributes related data across nodes. This applies equally to metrics time series, log streams from a service, and spans from a trace. As discussed in [Why Each Key Component Matters](#why-each-key-component-matters), the current `node_id` constraint is an implementation choice for simplicity, not a data integrity requirement -- merge operations don't interact with checkpoints, so there is no fundamental obstacle to cross-node merging. The challenge is coordination, not correctness.
- **Sort-key-aware merging** -- reorganizing data by a sort schema (e.g., metric name + tags + timestamp, or service name + timestamp) so that related data is physically co-located within splits
- **Locality compaction** -- an LSM-inspired approach that progressively narrows each split's coverage of the sort-key space, creating non-overlapping segments. This enables **query pruning**: a query for a specific service or metric can skip entire splits whose key range doesn't overlap, rather than scanning everything in the time window.
- **Time bucketing** -- partitioning compaction by time windows, since queries use time as a primary filter and observability data is ephemeral

A detailed design will be covered in a forthcoming document.

---

## Configuration Reference

### Merge Policy

**File:** `quickwit-config/src/merge_policy_config.rs`

```yaml
indexing_settings:
  merge_policy:
    type: stable_log
    min_level_num_docs: 100000      # Minimum docs in Level 0 (default: 100K)
    merge_factor: 10                # Minimum splits to trigger merge (default: 10)
    max_merge_factor: 12            # Maximum splits per merge (default: 12)
    maturation_period: 48h          # Time until split becomes mature (default: 48h)

  split_num_docs_target: 10000000   # Target size for mature splits (default: 10M)
  commit_timeout_secs: 60           # Commit timeout for indexer (default: 60s)
```

### Partition Key

```yaml
doc_mapping:
  partition_key: hash_mod(service_name, 100)
```

### IngestV2

```yaml
ingest_api:
  max_queue_memory_usage: 2GB
  max_queue_disk_usage: 100GB

indexer:
  enable_otlp_endpoint: true
  data_dir: /quickwit/data  # Must be on persistent volume for metrics
```

### Kubernetes Persistent Volume (Metrics WAL)

```yaml
indexer:
  persistentVolume:
    enabled: true
    storage: "10Gi"
    storageClass: "gp3"
```

---

**Document Version:** 2.1
**Last Updated:** 2026-02-11
**Maintainer:** Engineering Team
