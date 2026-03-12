# Architecture Patterns: Parquet Compaction Pipeline

**Domain:** Parquet split compaction for Quickhouse-Pomsky metrics engine
**Researched:** 2026-02-23
**Overall Confidence:** HIGH (based on direct codebase analysis of existing Tantivy merge actors, existing Parquet pipeline actors, metastore integration, and actor framework patterns)

---

## Recommended Architecture

### Overview

The Parquet compaction pipeline introduces 5 new actors that mirror the existing Tantivy merge pipeline but replace Tantivy-specific operations with Parquet/Arrow k-way sorted merge. These actors are supervised by a new `ParquetMergePipeline` actor that follows the exact same supervision pattern as the existing `MergePipeline`.

The key architectural decision is: **follow the Tantivy merge pipeline structure exactly, replacing only the format-specific internals.** This means:

- Same supervision model (pipeline actor with health checks and restart logic)
- Same scheduling model (shared `MergeSchedulerService` with priority queue)
- Same planner loop (planner -> publisher -> planner feedback)
- New merge policy (`WindowedSortMergePolicy`) instead of `StableLogMergePolicy`
- New merge executor (k-way sorted merge via DataFusion instead of Tantivy `UnionDirectory`)
- New message types operating on `MetricsSplitMetadata` instead of `SplitMetadata`

### High-Level Architecture

```
EXISTING INGESTION PIPELINE (no changes except MetricsIndexer window enforcement)
====================================================================================

Source -> ParquetDocProcessor -> MetricsIndexer -> ParquetUploader -> Sequencer -> ParquetPublisher
                                    |
                                    | (NEW: partition rows by time window before writing)
                                    | (produces one split per window boundary crossing)
                                    v
                           Parquet files in S3 + metadata in PostgreSQL


NEW COMPACTION PIPELINE
====================================================================================

ParquetMergePipeline (supervisor)
  |
  |-- ParquetMergePlanner  ----+
  |      |                     |
  |      | MergeTask           | NewMetricsSplits (feedback loop from publisher)
  |      v                     |
  |-- MergeSchedulerService    |  (SHARED with Tantivy merge -- uses existing actor)
  |      |                     |
  |      | MergeTask           |
  |      v                     |
  |-- ParquetMergeSplitDownloader
  |      |
  |      | ParquetMergeScratch
  |      v
  |-- ParquetMergeExecutor
  |      |
  |      | ParquetSplitBatch (reuses existing message type)
  |      v
  |-- ParquetMergeUploader  (reuses ParquetUploader with MergeUploader type)
  |      |
  |      | ParquetSplitsUpdate (with replaced_split_ids populated)
  |      v
  |-- ParquetMergePublisher ----+  (calls publish_metrics_splits with replace semantics)
```

### Component Boundaries

| Component | Responsibility | Communicates With | New vs Existing |
|-----------|---------------|-------------------|-----------------|
| `ParquetMergePipeline` | Supervises all merge actors, handles spawn/restart/shutdown | All merge actors (owns handles) | **NEW** |
| `ParquetMergePlanner` | Decides when to merge splits within a window/sort-schema scope | Receives `NewMetricsSplits` from publisher; sends `ParquetMergeTask` to scheduler | **NEW** |
| `MergeSchedulerService` | Priority-queues merge operations across all indexes | Receives scheduled merges; dispatches to downloaders | **EXISTING** (shared) |
| `ParquetMergeSplitDownloader` | Downloads Parquet files from S3 to scratch directory | Receives `ParquetMergeTask`; sends `ParquetMergeScratch` to executor | **NEW** |
| `ParquetMergeExecutor` | Performs k-way sorted merge using DataFusion | Receives `ParquetMergeScratch`; sends `ParquetSplitBatch` to uploader | **NEW** |
| `ParquetMergeUploader` | Stages and uploads merged Parquet file to S3 | Receives `ParquetSplitBatch`; sends `ParquetSplitsUpdate` to publisher | **REUSE** `ParquetUploader` with `UploaderType::MergeUploader` |
| `ParquetMergePublisher` | Atomically publishes merged split + marks old splits for deletion | Receives `ParquetSplitsUpdate`; sends `NewMetricsSplits` back to planner | **NEW** (variant of `ParquetPublisher`) |
| `WindowedSortMergePolicy` | Determines which splits within a window should be merged | Called by `ParquetMergePlanner` | **NEW** |
| `MetricsIndexer` (modified) | Enforces window boundaries at ingestion time | Existing actor, modified to partition rows | **MODIFIED** |

---

## Data Flow for a Complete Compaction Cycle

### Step-by-step flow

**1. Planner queries metastore for merge candidates**

```
ParquetMergePlanner::fetch_immature_splits()
  -> metastore.list_metrics_splits(
       index_id,
       state = Published,
       window_start = <specific window>,
       sort_schema = <specific schema>,
     )
  -> Returns Vec<MetricsSplitMetadata>
```

The planner groups splits by `(index_id, window_start, sort_schema)` -- this is the Parquet compaction scope, replacing the Tantivy 5-part key. Unlike the Tantivy merge pipeline which is scoped by `(node_id, index_uid, source_id, partition_id, doc_mapping_uid)`, the Parquet pipeline deliberately drops `node_id` scoping to enable cross-node compaction (as described in GAP-001 and the compaction architecture doc).

**2. Merge policy selects merge candidates**

```
WindowedSortMergePolicy::operations(&mut splits_in_scope) -> Vec<ParquetMergeOperation>
```

The policy adapts `StableLogMergePolicy` for Parquet: levels are based on file size (bytes) rather than document count, since Parquet compression ratios vary widely. Maturity is based on file size reaching a target (e.g., 256 MiB) or age (e.g., 48 hours).

**3. Merge operation is scheduled**

```
ParquetMergePlanner::send_merge_ops()
  -> schedule_parquet_merge(&merge_scheduler_service, tracked_operation, downloader_mailbox)
```

This reuses the existing `MergeSchedulerService` priority queue. The score function is adapted: `(delta_num_splits << 48) / total_bytes` -- same formula, applied to Parquet file sizes. The merge scheduler does not need modification because it operates on generic `MergeOperation`/`MergeTask` types (or we introduce a parallel `ParquetMergeTask` type that works with the same scheduling pattern).

**4. Split downloader fetches Parquet files**

```
ParquetMergeSplitDownloader::handle(ParquetMergeTask)
  -> For each split in merge_task.splits:
       storage.copy_to_file(
         Path::new(&split.parquet_files[0]),  // e.g., "metrics_xxx.parquet"
         &local_scratch_dir.join(&split.parquet_files[0])
       )
  -> Send ParquetMergeScratch { merge_task, downloaded_files, scratch_directory }
     to ParquetMergeExecutor
```

Key difference from Tantivy downloader: downloads Parquet files (not `.split` bundles) and does not call `fetch_and_open_split` from `IndexingSplitStore`. Instead, uses raw `Storage::copy_to_file`.

**5. Merge executor performs k-way sorted merge**

```
ParquetMergeExecutor::handle(ParquetMergeScratch)
  -> Open each downloaded Parquet file as a DataFusion table
  -> UNION ALL + ORDER BY sort_schema columns
  -> Write output to new Parquet file with:
     - Column index enabled (page-level statistics)
     - Offset index enabled
     - sorting_columns metadata set
     - key_value_metadata with sort_schema, min/max values
  -> Compute merged MetricsSplitMetadata:
     - Union of metric_names from all input splits
     - Union of tag values
     - Combined time_range (same window_start)
     - size_bytes from new file
     - num_rows = sum(input num_rows)
     - num_merge_ops = max(input num_merge_ops) + 1
  -> Send ParquetSplitBatch to uploader
```

The executor runs on `RuntimeType::Blocking` (same as the Tantivy `MergeExecutor`). The k-way merge uses DataFusion's sort-preserving merge, not an in-memory sort of all data.

**6. Upload merged split**

```
ParquetMergeUploader::handle(ParquetSplitBatch)
  -> stage_metrics_splits(new merged split metadata)
  -> storage.put(new_split.parquet_files[0], file_content)
  -> Send ParquetSplitsUpdate {
       index_id,
       new_splits: vec![merged_split_metadata],
       replaced_split_ids: vec![input_split_id_1, input_split_id_2, ...],
       checkpoint_delta_opt: None,  // Merges don't update checkpoints
       ...
     }
```

This reuses the existing `ParquetUploader` actor. The key difference for merge is: `replaced_split_ids` is populated (during ingestion it is always empty), and `checkpoint_delta_opt` is `None`.

**7. Publish merged split atomically**

```
ParquetMergePublisher::handle(ParquetSplitsUpdate)
  -> metastore.publish_metrics_splits(PublishMetricsSplitsRequest {
       index_id,
       staged_split_ids: vec![merged_split_id],
       replaced_split_ids: vec![old_split_1, old_split_2, ...],
       index_checkpoint_delta_json_opt: None,  // No checkpoint for merges
       publish_token_opt: None,
     })
  -> Send NewMetricsSplits { new_splits: vec![merged_metadata] }
     to ParquetMergePlanner  (feedback loop)
```

The publisher closes the loop by notifying the planner of the new merged split, which may trigger further merge operations (cascading merges up levels).

**8. Garbage collection**

Old splits in `MarkedForDeletion` state are cleaned up by the existing `quickwit-janitor` GC process. The janitor needs to be extended to handle `metrics_splits` table entries alongside the existing `splits` table cleanup. The Parquet files in S3 are deleted based on the `parquet_files` field in `MetricsSplitMetadata`.

---

## Window Boundary Enforcement at Ingestion Time

### Where: MetricsIndexer (ParquetIndexer)

The `ParquetIndexer` must be modified to enforce time-window boundaries. Currently, it writes all accumulated rows into a single split regardless of timestamp distribution. The modification adds a row-partitioning step before writing.

### How

```
ParquetIndexer::process_batch(batch):
  1. Group rows by window assignment:
     window_start = timestamp_secs - (timestamp_secs % window_duration_secs)

  2. For each distinct window in the batch:
     a. Filter rows belonging to this window
     b. Add to window-specific accumulator

  3. For any window accumulator that exceeds threshold:
     a. Sort rows by sort_schema columns
     b. Write Parquet file with window_start in metadata
     c. Emit ParquetSplit with window_start and sort_schema in MetricsSplitMetadata

  4. On force_commit or commit_timeout:
     a. Flush ALL window accumulators
     b. Each produces its own split with proper window_start
```

### Design Decisions

**One accumulator per active window** -- The indexer maintains a `HashMap<u64, ParquetBatchAccumulator>` keyed by `window_start`. With 15-minute windows and 60-second commit timeouts, at most 2-3 windows are active at any time (current window + 1-2 late windows). Memory overhead is negligible.

**Late data acceptance** -- Points older than `late_data_acceptance_window` (configurable, e.g., 1 hour) are dropped at ingestion. This bounds the number of active window accumulators and prevents compaction of already-sealed windows from being disrupted.

**Sort at write time** -- Each window's split is sorted by the configured `sort_schema` (e.g., `metric_name, tag_service, tag_env, timestamp_secs`). This means every split enters the compaction pipeline pre-sorted, enabling efficient k-way merge without full re-sort.

---

## Supervision Model

### Pattern: Follows MergePipeline exactly

The `ParquetMergePipeline` actor follows the exact same supervision pattern as `MergePipeline` in `merge_pipeline.rs`:

```rust
pub struct ParquetMergePipeline {
    params: ParquetMergePipelineParams,
    merge_planner_mailbox: Mailbox<ParquetMergePlanner>,
    merge_planner_inbox: Inbox<ParquetMergePlanner>,
    previous_generations_statistics: ParquetMergeStatistics,
    statistics: ParquetMergeStatistics,
    handles_opt: Option<ParquetMergePipelineHandles>,
    kill_switch: KillSwitch,
    initial_immature_splits_opt: Option<Vec<MetricsSplitMetadata>>,
    shutdown_initiated: bool,
}
```

**Key supervision behaviors (all copied from MergePipeline):**

1. **Initialize** -- spawns all actors via `Spawn` message, starts `SuperviseLoop`
2. **SuperviseLoop** (1-second interval) -- calls `healthcheck()` on all actor handles
3. **FailureOrUnhealthy** -- terminates all actors via `kill_switch`, schedules retry with `Spawn { retry_count }`
4. **Success** -- all actors terminated normally, pipeline exits successfully
5. **FinishPendingMergesAndShutdownPipeline** -- disconnects planner loop, runs finalize merge policy, lets in-flight merges drain
6. **Mailbox recycling** -- planner mailbox is created once and reused across pipeline restarts (same as Tantivy merge pipeline, lines 147-151 of `merge_pipeline.rs`)
7. **Spawn semaphore** -- limits concurrent pipeline spawns (same `SPAWN_PIPELINE_SEMAPHORE` pattern)

### Actor spawn order (bottom-up, following merge_pipeline.rs:265-363)

```
1. ParquetMergePublisher   (no downstream)
2. ParquetMergeUploader    (-> publisher)
3. ParquetMergeExecutor    (-> uploader)  [RuntimeType::Blocking]
4. ParquetMergeSplitDownloader (-> executor)
5. ParquetMergePlanner     (-> scheduler -> downloader, recycled mailbox)
```

Each actor gets the pipeline's `kill_switch.child()` and backpressure metrics.

### Lifecycle

```
IndexingService
  |
  |-- spawns IndexingPipeline (for each index/source)
  |     |-- spawns ParquetIndexingPipeline (if is_metrics_index)
  |
  |-- spawns MergePipeline (for logs/traces, existing)
  |
  |-- spawns ParquetMergePipeline (for metrics, NEW)
        |-- watches for is_metrics_index
        |-- shares MergeSchedulerService with MergePipeline
```

The `IndexingService` needs modification to spawn `ParquetMergePipeline` for metrics indexes, similar to how it spawns `MergePipeline` for log/trace indexes.

---

## Merge Planner Interaction with Metastore

### Fetching merge candidates

The planner queries the metastore for published, immature metrics splits:

```sql
-- Conceptual query the planner triggers via metastore RPC
SELECT * FROM metrics_splits
WHERE index_id = $1
  AND split_state = 'Published'
  AND window_start = $2          -- Scope to specific time window
  AND sort_schema = $3           -- Only merge compatible sort schemas
  AND num_merge_ops < $4         -- Maturity check
  AND created_at < $5            -- Not too recent (give ingestion time to settle)
ORDER BY size_bytes ASC;
```

This requires a new metastore RPC: `list_metrics_splits_for_compaction(request)` that accepts the compaction scope parameters. The existing `list_metrics_splits` may be extended or a new method added.

### Publishing merged splits

The planner loop:

```
1. list_metrics_splits_for_compaction() -> immature splits grouped by (index_id, window_start, sort_schema)
2. For each group: WindowedSortMergePolicy::operations() -> merge operations
3. Schedule merge operations via MergeSchedulerService
4. ... merge pipeline executes ...
5. publish_metrics_splits() with staged_split_ids + replaced_split_ids
6. ParquetMergePublisher sends NewMetricsSplits back to planner
7. Planner re-evaluates with the new merged split
```

### New metastore methods needed

| Method | Purpose |
|--------|---------|
| `list_metrics_splits_for_compaction` | Fetch immature published splits grouped by compaction scope |
| `publish_metrics_splits` (extended) | Already exists, but `replaced_split_ids` handling needs to atomically mark old splits for deletion |

The existing `publish_metrics_splits` in the metastore already accepts `replaced_split_ids` in the `PublishMetricsSplitsRequest`. The PostgreSQL implementation needs to handle the replace atomically: insert new split, update old splits to `MarkedForDeletion` state, in one transaction.

---

## Patterns to Follow

### Pattern 1: Mailbox Recycling for Planner

**What:** Create the planner mailbox once in the pipeline constructor; reuse it across pipeline restarts.

**When:** Always. This is how the Tantivy merge pipeline prevents message loss on restart.

**Why:** When the pipeline crashes and restarts, in-flight `NewMetricsSplits` messages from the publisher need somewhere to go. The recycled mailbox catches them. On the new incarnation, the planner drains stale messages using an incarnation timestamp (see `PlanMerge.incarnation_started_at` in `merge_planner.rs:152`).

```rust
// In ParquetMergePipeline::new()
let (merge_planner_mailbox, merge_planner_inbox) = spawn_ctx
    .create_mailbox::<ParquetMergePlanner>(
        "ParquetMergePlanner",
        ParquetMergePlanner::queue_capacity(),
    );
```

### Pattern 2: Inventory Tracking for Ongoing Merges

**What:** Use `tantivy::Inventory<ParquetMergeOperation>` to track in-flight merge operations.

**When:** In the planner, to prevent scheduling duplicate merges for the same splits.

**Why:** The planner needs to know which splits are currently being merged so it does not schedule them again. The inventory provides weak-reference tracking: when a merge operation completes (the `TrackedObject` is dropped in the publisher), the inventory automatically forgets it. This is the exact same pattern used in `merge_planner.rs:86` (`ongoing_merge_operations_inventory`).

### Pattern 3: Kill Switch Hierarchy

**What:** Each pipeline creates a child kill switch; all actors share the same child.

**When:** On every pipeline spawn.

**Why:** When the supervisor detects failure, `kill_switch.kill()` propagates to all actors simultaneously. Each actor checks `ctx.kill_switch().is_dead()` before expensive operations (e.g., download, merge, upload). This prevents wasted work after a pipeline failure.

### Pattern 4: Protect Zone for Async Operations

**What:** Use `ctx.protect_zone()` around operations that must not be interrupted by liveness checks.

**When:** During S3 downloads, S3 uploads, metastore RPCs.

**Why:** The supervisor checks actor progress every `HEARTBEAT` interval. Without a protect zone, a long S3 download would look like the actor is stuck, triggering a false restart.

---

## Anti-Patterns to Avoid

### Anti-Pattern 1: Cross-window merging

**What:** Merging splits from different time windows into a single output split.

**Why bad:** Destroys time-window isolation. Queries can no longer prune entire windows. Retention becomes per-split instead of per-window. Window boundaries are architectural invariants.

**Instead:** Each merge operation operates strictly within a single `(index_id, window_start, sort_schema)` scope.

### Anti-Pattern 2: tokio::sync::Mutex in merge executor

**What:** Using `tokio::sync::Mutex` to protect shared state during the k-way merge.

**Why bad:** Per GAP-002 and CLAUDE.md, `tokio::sync::Mutex` causes data corruption on cancellation. The merge executor runs CPU-intensive work.

**Instead:** The merge executor runs on `RuntimeType::Blocking` (same as the Tantivy `MergeExecutor`). Use message passing between actors, not shared mutable state.

### Anti-Pattern 3: Restarting merges after failure

**What:** Restarting a failed merge from the beginning without checking if intermediate state was leaked.

**Why bad:** A merge that failed after uploading the new split but before publishing could leave orphan splits in storage.

**Instead:** Follow the Tantivy `MergeExecutor` pattern (lines 106-119): on merge failure, log the error and return `Ok(())` without propagating the error. The splits remain in their pre-merge state and will be retried on the next planner cycle. The orphan uploaded-but-unpublished split will be cleaned up by the janitor's GC.

### Anti-Pattern 4: Blocking the merge planner with metastore queries

**What:** Making synchronous metastore queries in the planner's message handler.

**Why bad:** The planner has `QueueCapacity::Bounded(1)`. Blocking on a metastore query while holding the message slot prevents `NewMetricsSplits` feedback from being processed.

**Instead:** Fetch immature splits during pipeline spawn (via `fetch_immature_splits()` pattern from `merge_pipeline.rs:441-472`), not in the `NewMetricsSplits` handler. The planner operates on its in-memory set of known splits.

---

## Integration Points with Existing Code

### Existing code that needs modification

| File | Change | Reason |
|------|--------|--------|
| `quickwit-indexing/src/actors/parquet_indexer.rs` | Add window partitioning to `process_batch()` | Enforce window boundaries at ingestion |
| `quickwit-indexing/src/actors/indexing_pipeline.rs` | Route `ParquetMergePipeline` spawn for metrics indexes | Pipeline lifecycle |
| `quickwit-indexing/src/actors/indexing_service.rs` | Spawn `ParquetMergePipeline` alongside `IndexingPipeline` | Service-level lifecycle |
| `quickwit-indexing/src/actors/mod.rs` | Export new actor types | Module structure |
| `quickwit-parquet-engine/src/split/metadata.rs` | Add `window_start`, `window_duration_secs`, `sort_schema`, `num_merge_ops` fields | Compaction metadata |
| `quickwit-metastore` (PostgreSQL) | Add new columns to `metrics_splits` table; add `list_metrics_splits_for_compaction` RPC | Merge planner queries |
| `quickwit-metastore` (PostgreSQL) | Implement replace semantics in `publish_metrics_splits` | Atomic replace-on-merge |

### Existing code that is reused unchanged

| Component | Reuse Pattern |
|-----------|---------------|
| `MergeSchedulerService` | Shared singleton, schedules both Tantivy and Parquet merges |
| `ParquetUploader` | Reused directly for merge uploads (already supports `UploaderType`) |
| `Sequencer` | Not needed for merge pipeline (ordering not required for merge publishes) |
| `KillSwitch` / `ActorContext` | Standard actor framework |
| `TempDirectory` / scratch management | Standard pattern for local file staging |

### New files to create

| File | Purpose |
|------|---------|
| `quickwit-indexing/src/actors/parquet_merge_pipeline.rs` | Supervisor for Parquet merge actors |
| `quickwit-indexing/src/actors/parquet_merge_planner.rs` | Merge planning for metrics splits |
| `quickwit-indexing/src/actors/parquet_merge_split_downloader.rs` | Downloads Parquet files for merge |
| `quickwit-indexing/src/actors/parquet_merge_executor.rs` | K-way sorted merge via DataFusion |
| `quickwit-indexing/src/actors/parquet_merge_publisher.rs` | Publishes merged splits with replace semantics |
| `quickwit-indexing/src/merge_policy/windowed_sort_merge_policy.rs` | Merge policy for time-windowed Parquet splits |
| `quickwit-indexing/src/models/parquet_merge_scratch.rs` | Message type for downloaded Parquet files |
| `quickwit-indexing/src/models/new_metrics_splits.rs` | Planner feedback message type |

---

## Suggested Build Order (Dependency-Driven)

The build order is driven by data dependencies: each phase produces artifacts the next phase consumes.

### Phase 1: Metadata Foundation (no actor changes)

**Build:** `MetricsSplitMetadata` extensions, PostgreSQL migration, metastore RPCs

**Rationale:** Everything else depends on having `window_start`, `sort_schema`, and `num_merge_ops` in the metadata. Without these fields, the planner cannot scope merges and the executor cannot determine sort order.

**Deliverables:**
- Extended `MetricsSplitMetadata` with `window_start`, `window_duration_secs`, `sort_schema`, `num_merge_ops`
- PostgreSQL migration adding columns to `metrics_splits`
- `list_metrics_splits_for_compaction` metastore RPC
- Replace semantics in `publish_metrics_splits`

### Phase 2: Ingestion-Time Window Enforcement

**Build:** Modify `ParquetIndexer` to partition by window and sort within windows

**Rationale:** The compaction pipeline needs window-scoped, pre-sorted input splits. Without this, the merge executor would need to do a full sort instead of a merge sort, and window scoping would be impossible.

**Deliverables:**
- Window-aware accumulator in `ParquetIndexer`
- Sort-at-write in the Parquet writer
- Late data rejection
- Parquet column index enablement

### Phase 3: Merge Policy

**Build:** `WindowedSortMergePolicy`

**Rationale:** The planner needs a policy before it can generate merge operations. The policy is pure logic with no actor dependencies.

**Deliverables:**
- `WindowedSortMergePolicy` (adapts `StableLogMergePolicy` for size-based levels on Parquet)
- Maturity model (size + age)
- Unit tests with proptest

### Phase 4: Merge Executor (Core Algorithm)

**Build:** `ParquetMergeExecutor` with k-way sorted merge

**Rationale:** This is the most complex new component. It can be developed and tested in isolation before wiring into the actor pipeline.

**Deliverables:**
- K-way sorted merge using DataFusion's `SortPreservingMergeExec`
- Merged metadata computation (union of metric names, tags, time ranges)
- Column index and sort metadata in output files
- Unit tests with known input/output pairs

### Phase 5: Actor Pipeline (Planner, Downloader, Publisher)

**Build:** All remaining actors and the supervisor

**Rationale:** With the executor and policy tested, wire them into the full actor pipeline.

**Deliverables:**
- `ParquetMergePlanner` (with inventory tracking, incarnation handling)
- `ParquetMergeSplitDownloader` (simple S3 download)
- `ParquetMergePublisher` (publish with replace + planner feedback)
- `ParquetMergePipeline` (supervisor with health checks)
- Integration with `IndexingService` for lifecycle management

### Phase 6: Integration Testing

**Build:** End-to-end tests through the full pipeline

**Rationale:** All components exist; verify they work together.

**Deliverables:**
- E2E test: ingest -> compact -> query correctness
- DST test: crash recovery during merge
- Metrics: compaction throughput, split count reduction

---

## Scalability Considerations

| Concern | At 100 splits/window | At 10K splits/window | At 100K splits/window |
|---------|---------------------|---------------------|----------------------|
| Planner memory | Negligible (in-memory set of metadata) | ~10 MB (metadata per split ~1 KB) | ~100 MB, may need pagination |
| Merge executor I/O | Bottlenecked by S3 download | Need parallel downloads | Need parallel downloads + IO throttling |
| Merge concurrency | Default 3 concurrent merges shared with Tantivy | May need separate semaphore for Parquet | Separate semaphore, configurable |
| Window accumulator memory | 2-3 active accumulators | Same (bounded by late data window) | Same (bounded by late data window) |

### Key scaling knob

The `MergeSchedulerService` has a configurable `merge_concurrency` (default: 3). For high-throughput metrics deployments, this should be tunable separately for Parquet merges vs Tantivy merges. Consider a separate `ParquetMergeSchedulerService` if contention becomes an issue.

---

## Sources

All findings are based on direct codebase analysis:

- `quickwit-indexing/src/actors/merge_planner.rs` -- MergePlanner actor, `belongs_to_pipeline()`, incarnation handling, inventory tracking
- `quickwit-indexing/src/actors/merge_split_downloader.rs` -- MergeSplitDownloader actor, download pattern
- `quickwit-indexing/src/actors/merge_executor.rs` -- MergeExecutor actor, Tantivy merge logic, error handling pattern
- `quickwit-indexing/src/actors/merge_pipeline.rs` -- MergePipeline supervisor, spawn order, health check, shutdown
- `quickwit-indexing/src/actors/publisher.rs` -- Publisher with merge planner feedback loop
- `quickwit-indexing/src/actors/merge_scheduler_service.rs` -- Shared scheduler, priority queue, semaphore permits
- `quickwit-indexing/src/merge_policy/stable_log_merge_policy.rs` -- Level-based merge policy, maturity model
- `quickwit-indexing/src/actors/indexing_pipeline.rs` -- ParquetIndexingPipeline spawn, `PipelineHandles` enum
- `quickwit-indexing/src/actors/parquet_indexer.rs` -- ParquetIndexer actor, accumulator, commit timeout
- `quickwit-indexing/src/actors/parquet_uploader.rs` -- ParquetUploader, staging, S3 upload, sequencer integration
- `quickwit-indexing/src/actors/parquet_publisher.rs` -- ParquetPublisher, publish_metrics_splits, SuggestTruncate
- `quickwit-parquet-engine/src/split/metadata.rs` -- MetricsSplitMetadata, current fields
- `docs/internals/compaction-architecture.md` -- Merge scope, node_id constraint analysis
- `docs/internals/adr/gaps/001-no-parquet-compaction.md` -- GAP analysis confirming no Parquet merge exists
- `docs/internals/adr/gaps/003-no-time-window-partitioning.md` -- GAP analysis for window partitioning
- `docs/internals/adr/gaps/004-incomplete-split-metadata.md` -- GAP analysis for metadata fields
