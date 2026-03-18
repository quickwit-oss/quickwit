# Metrics Durability Design Document

**Version:** 1.0
**Date:** 2026-01-19
**Status:** Approved
**Scope:** Phases 18-22 (WAL Integration, Checkpointing, Cluster Gossip, Retry/Error Handling, Rate Limiting)

## 1. Overview

### Problem Statement

The metrics pipeline currently has no durability guarantees. Metrics data written to the pipeline can be lost on:
- Node crashes or restarts
- Process failures
- Network partitions (incomplete replication)

### Solution

Integrate the metrics pipeline with the existing ingest_v2 durability infrastructure:
- **WAL (mrecordlog):** Persist metrics before acknowledgment
- **Position tracking:** Track what's been indexed and can be truncated
- **Cluster gossip (chitchat):** Share position state across nodes
- **Retry with recovery:** Replay uncommitted records on startup
- **Rate limiting:** Prevent WAL overflow under load

### Scope

| Phase | Focus | Key Deliverables |
|-------|-------|------------------|
| 18 | WAL Integration | MetricsWal, queue management, MRecord append |
| 19 | Checkpointing | MetricsShardPosition, metastore persistence |
| 20 | Cluster Gossip | ShardPositionsService integration, chitchat keys |
| 21 | Retry & Recovery | Error classification, recovery on startup |
| 22 | Rate Limiting | RateLimiter integration, capacity tracking |

---

## 2. Architecture Decision Records (ADRs)

### ADR-1: WAL Directory Separation

**Status:** Decided

**Context:**
Logs use a configurable `wal_dir_path`. Metrics could share this directory or use a separate one.

**Decision:** Use separate `metrics_wal_dir_path` configuration.

**Rationale:**
- Isolation prevents cross-contamination between logs and metrics WAL data
- Independent recovery: metrics WAL issues don't affect logs pipeline
- Easier operational debugging (clear ownership of files)
- Allows different disk allocation for metrics vs logs workloads

**Consequences:**
- New configuration field: `metrics_wal_dir_path` in ingest config
- Separate disk space monitoring for metrics WAL
- Two MultiRecordLog instances running (one for logs, one for metrics)

---

### ADR-2: Queue ID Namespace

**Status:** Decided

**Context:**
Logs use queue IDs formatted as `{index_uid}/{source_id}/{shard_id}`. Metrics need a distinct namespace to prevent collision.

**Decision:** Prefix with `metrics/` - full format: `metrics/{index_uid}/{source_id}/{shard_id}`

**Rationale:**
- Clear namespace separation prevents accidental collision
- Easy to filter metrics queues: `list_queues().filter(|q| q.starts_with("metrics/"))`
- Consistent with hierarchical naming patterns in the codebase
- Recovery logic can distinguish metrics vs logs by prefix

**Consequences:**
- Queue iteration must filter by prefix when operating on metrics only
- Logging should include queue type for clarity
- Migration not needed (new queues only)

---

### ADR-3: MRecord Format

**Status:** Decided

**Context:**
The existing MRecord enum has Doc and Commit variants. Question: do metrics need different record types?

**Decision:** Reuse existing MRecord enum without metrics-specific variants.

**Rationale:**
- `MRecord::Doc(Bytes)` can encode any document, including metrics
- `MRecord::Commit` marks durability checkpoints (same semantics for metrics)
- Same format enables shared tooling (recovery, debugging, inspection)
- Versioned header (HeaderVersion::V0) allows future extension if needed

**Consequences:**
- Metrics bytes encoded same as log bytes (opaque payload)
- No changes to mrecord.rs needed for Phase 18
- Shared recovery logic applies to both logs and metrics

**VERIFIED:** MRecord format confirmed in `quickwit-ingest/src/ingest_v2/mrecord.rs` (lines 36-40):
```rust
pub enum MRecord {
    Doc(Bytes),
    Commit,
}
```

---

### ADR-4: Position Tracking Model

**Status:** Decided

**Context:**
Logs track three positions per shard: replication (WAL), truncation (indexed), publish (cluster). Metrics have different access patterns (time-range queries) - question: same model or different?

**Decision:** Per-shard positions identical to logs model.

**Rationale:**
- Proven pattern that handles all durability requirements
- Reuses ShardPositionsService without modification
- Time-range metadata tracked separately in split metadata (not in positions)
- Position semantics are about durability, not query patterns

**Consequences:**
- `Position` enum from quickwit-proto used as-is
- Time-range information stored in split metadata (existing pattern)
- No new position types needed

**VERIFIED:** Position enum in `quickwit-proto/src/types/position.rs` (lines 92-99):
```rust
pub enum Position {
    Beginning,
    Offset(Offset),
    Eof(Option<Offset>),
}
```

---

### ADR-5: Initial Replication Factor

**Status:** Decided

**Context:**
Logs support configurable replication (1-2 replicas). Should metrics have replication from the start?

**Decision:** Start with replication_factor=1 (no replication) for v0.3.

**Rationale:**
- Simpler MVP: single-node durability is valuable on its own
- Replication adds significant complexity (leader election, stream management)
- Can add replication as separate enhancement in v0.4
- Focus v0.3 on proving durability patterns work for metrics

**Consequences:**
- Single-node durability only for v0.3
- No ReplicationStream or ReplicationTask types needed initially
- Configuration should support future replication_factor > 1
- Document as known limitation in v0.3 release notes

---

## 3. Integration Specifications

### 3.1 WAL Integration (Phase 18)

**Module Location:** `quickwit-ingest/src/metrics/`

**Key Types to Create:**

```rust
// metrics/mod.rs
pub mod metrics_wal;

// metrics/metrics_wal.rs
pub struct MetricsWal {
    mrecordlog: Arc<RwLock<MultiRecordLogAsync>>,
    config: MetricsWalConfig,
}

pub struct MetricsWalConfig {
    pub wal_dir_path: PathBuf,
    pub disk_capacity: ByteSize,
    pub memory_capacity: ByteSize,
}
```

**Integration with MultiRecordLogAsync:**

Share the same async wrapper pattern but with separate instance:

```rust
// VERIFIED: API from mrecordlog_async.rs (lines 94-117)
impl MetricsWal {
    pub async fn create_queue(&mut self, queue_id: &str) -> Result<(), CreateQueueError>;
    pub async fn delete_queue(&mut self, queue_id: &str) -> Result<(), DeleteQueueError>;
    pub async fn append_records<T>(&mut self, queue_id: &str, ...) -> Result<Option<u64>, AppendError>;
    pub async fn truncate(&mut self, queue_id: &str, position: u64) -> Result<usize, TruncateError>;
    pub fn range<R>(&self, queue_id: &str, range: R) -> Result<impl Iterator<Item = Record<'_>>, MissingQueue>;
    pub fn list_queues(&self) -> impl Iterator<Item = &str>;
}
```

**Queue Management:**
- Create queue on first metric for a shard: `metrics/{index_uid}/{source_id}/{shard_id}`
- Delete queue when empty after truncate (use `force_delete_queue` pattern)
- Queue ID prefix filtering for metrics-only operations

**VERIFIED:** Force delete pattern in `mrecordlog_utils.rs` (lines 143-151):
```rust
pub async fn force_delete_queue(
    mrecordlog: &mut MultiRecordLogAsync,
    queue_id: &QueueId,
) -> io::Result<()> {
    match mrecordlog.delete_queue(queue_id).await {
        Ok(_) | Err(DeleteQueueError::MissingQueue(_)) => Ok(()),
        Err(DeleteQueueError::IoError(error)) => Err(error),
    }
}
```

---

### 3.2 Position Tracking (Phase 19)

**Reuse:** `Position` enum from `quickwit-proto/src/types/position.rs`

**New Type:**
```rust
// Wrapper with metrics-specific context (optional, evaluate during implementation)
pub struct MetricsShardPosition {
    pub queue_id: QueueId,
    pub replication_position_inclusive: Position,
    pub truncation_position_inclusive: Position,
}
```

**Persistence:** Checkpoint to metastore using existing `Checkpoint` type.

**Events:**
```rust
// Emit after successful indexing
LocalShardPositionsUpdate {
    source_uid: SourceUid { index_uid, source_id: "metrics" },
    shard_positions: vec![(shard_id, publish_position)],
}
```

**VERIFIED:** LocalShardPositionsUpdate in `shard_positions.rs` (lines 46-51):
```rust
pub(crate) struct LocalShardPositionsUpdate {
    source_uid: SourceUid,
    shard_positions: Vec<(ShardId, Position)>,
}
```

---

### 3.3 Cluster Gossip (Phase 20)

**Reuse:** `ShardPositionsService` as-is (no modifications needed).

**Chitchat Key Format:**
```
indexer.shard_positions:{index_uid}:{source_id}:{shard_id}
```

**VERIFIED:** Key prefix in `shard_positions.rs` (line 31):
```rust
const SHARD_POSITIONS_PREFIX: &str = "indexer.shard_positions:";
```

**Key Format Construction:**
```rust
// VERIFIED: shard_positions.rs (line 266)
let key = format!("{SHARD_POSITIONS_PREFIX}{index_uid}:{source_id}:{shard_id}");
```

**Note:** Same key format works for metrics because `source_id` distinguishes metrics sources from log sources. No prefix modification needed.

**Position Value Format:**
- `""` = Beginning
- `"00000000000000001000"` = Offset(1000) - 20-digit zero-padded
- `"~"` = Eof(None)
- `"~00000000000000001000"` = Eof(Some(1000))

**VERIFIED:** Position serialization in `position.rs` (lines 114-123).

---

### 3.4 Retry & Error Handling (Phase 21)

**Reuse:** `RetryParams` configuration pattern from ingest_v2.

**Error Classification:**
- **Transient (retry):** IO errors, network timeouts, temporary unavailability
- **Permanent (fail):** Invalid data, schema mismatch, authentication failure

**Recovery on Startup:**

```rust
// VERIFIED: state.rs init() pattern (lines 173-209)
for queue_id in mrecordlog.list_queues().filter(|q| q.starts_with("metrics/")) {
    if let Some(position_range) = queue_position_range(&mrecordlog, &queue_id) {
        // Non-empty queue: recover shard in Closed state
        let replication_position_inclusive = Position::offset(*position_range.end());
        let truncation_position_inclusive = if *position_range.start() == 0 {
            Position::Beginning
        } else {
            Position::offset(*position_range.start() - 1)
        };
        // Create metrics shard in Closed state for replay
        let shard = MetricsShard::new_recovered(
            ShardState::Closed,
            replication_position_inclusive,
            truncation_position_inclusive,
        );
        state.metrics_shards.insert(queue_id.clone(), shard);
    } else {
        // Empty queue: delete it
        force_delete_queue(&mut mrecordlog, &queue_id).await?;
    }
}
```

---

### 3.5 Rate Limiting (Phase 22)

**Reuse:** `RateLimiter` and `RateMeter` types from `quickwit-common`.

**Configuration:**
```rust
pub struct MetricsRateLimitConfig {
    pub rate_limit_bytes_per_sec: ByteSize,
    pub burst_limit_bytes: ByteSize,
}
```

**Capacity Tracking:**

```rust
// VERIFIED: mrecordlog_utils.rs (lines 114-140)
pub(super) fn check_enough_capacity(
    mrecordlog: &MultiRecordLogAsync,
    disk_capacity: ByteSize,
    memory_capacity: ByteSize,
    requested_capacity: ByteSize,
) -> Result<(), NotEnoughCapacityError> {
    let wal_usage = mrecordlog.resource_usage();
    let disk_used = ByteSize(wal_usage.disk_used_bytes as u64);

    if disk_used + requested_capacity > disk_capacity {
        return Err(NotEnoughCapacityError::Disk { ... });
    }
    let memory_used = ByteSize(wal_usage.memory_used_bytes as u64);

    if memory_used + requested_capacity > memory_capacity {
        return Err(NotEnoughCapacityError::Memory { ... });
    }
    Ok(())
}
```

**Integration Pattern:**
1. Check capacity before append
2. If insufficient, return backpressure signal to caller
3. Track rate with RateMeter for monitoring
4. Apply RateLimiter for throttling if needed

---

## 4. Code Patterns Reference

### Pattern: Two-Phase Locking

**Purpose:** Prevent deadlock when accessing both WAL and in-memory state.

**Rule:** Always acquire locks in order: mrecordlog THEN inner state.

```rust
// VERIFIED: state.rs (lines 244-270)
pub async fn lock_fully(&self) -> IngestV2Result<FullyLockedIngesterState<'_>> {
    // 1. Lock WAL first (most expensive)
    let mrecordlog_opt_guard = self.mrecordlog.write().await;
    // 2. Then lock inner state
    let inner_guard = self.inner.lock().await;
    // ...
}
```

**For Metrics:** Apply same pattern:
```rust
impl MetricsIngesterState {
    pub async fn lock_fully(&self) -> Result<FullyLockedMetricsState<'_>> {
        let mrecordlog_guard = self.mrecordlog.write().await;
        let inner_guard = self.inner.lock().await;
        Ok(FullyLockedMetricsState { inner: inner_guard, mrecordlog: mrecordlog_guard })
    }
}
```

---

### Pattern: Append with MRecord Encoding

**Purpose:** Write metric documents to WAL in versioned format.

```rust
// VERIFIED: mrecordlog_utils.rs (lines 42-89)
pub(super) async fn append_non_empty_doc_batch(
    mrecordlog: &mut MultiRecordLogAsync,
    queue_id: &QueueId,
    doc_batch: DocBatchV2,
    force_commit: bool,
) -> Result<Position, AppendDocBatchError> {
    let encoded_mrecords = doc_batch
        .into_docs()
        .map(|(_doc_uid, doc)| MRecord::Doc(doc).encode())
        .chain(force_commit.then(|| MRecord::Commit.encode()));

    let position = mrecordlog
        .append_records(queue_id, None, encoded_mrecords)
        .await?;

    Ok(Position::offset(position))
}
```

**For Metrics:** Same pattern with metrics batch:
```rust
pub async fn append_metrics_batch(
    mrecordlog: &mut MultiRecordLogAsync,
    queue_id: &QueueId,
    metrics: impl Iterator<Item = Bytes>,
    force_commit: bool,
) -> Result<Position, AppendDocBatchError> {
    let encoded_mrecords = metrics
        .map(|metric_bytes| MRecord::Doc(metric_bytes).encode())
        .chain(force_commit.then(|| MRecord::Commit.encode()));

    let position = mrecordlog
        .append_records(queue_id, None, encoded_mrecords)
        .await?;

    Ok(Position::offset(position))
}
```

---

### Pattern: Position Publication via Event Broker

**Purpose:** Notify cluster of position updates for gossip.

```rust
// VERIFIED: shard_positions.rs (lines 231-252)
impl Handler<LocalShardPositionsUpdate> for ShardPositionsService {
    async fn handle(&mut self, update: LocalShardPositionsUpdate, _ctx: &ActorContext<Self>) {
        let LocalShardPositionsUpdate { source_uid, shard_positions } = update;
        let updated_shard_positions = self.apply_update(&source_uid, shard_positions);
        if !updated_shard_positions.is_empty() {
            self.publish_positions_into_chitchat(&source_uid, &updated_shard_positions).await;
            self.publish_shard_updates_to_event_broker(source_uid, updated_shard_positions);
        }
    }
}
```

**For Metrics:** Emit same event type:
```rust
// After indexing metrics completes
event_broker.publish(LocalShardPositionsUpdate::new(
    SourceUid { index_uid, source_id: "metrics".to_string() },
    vec![(shard_id, indexed_position)],
));
```

---

### Pattern: Recovery on Init

**Purpose:** Restore state from WAL on startup.

```rust
// VERIFIED: state.rs (lines 161-217)
pub async fn init(&self, wal_dir_path: &Path, rate_limiter_settings: RateLimiterSettings) {
    let mut mrecordlog = MultiRecordLogAsync::open_with_prefs(wal_dir_path, ...).await?;

    for queue_id in mrecordlog.list_queues() {
        if let Some(position_range) = queue_position_range(&mrecordlog, &queue_id) {
            // Non-empty: recover as Closed shard
            let shard = IngesterShard::new_solo(
                ShardState::Closed,
                Position::offset(*position_range.end()),
                truncation_position,
                None,
                now,
                false,
            );
            state.shards.insert(queue_id, shard);
        } else {
            // Empty: delete queue
            force_delete_queue(&mut mrecordlog, &queue_id).await?;
        }
    }
}
```

---

## 5. Implementation Roadmap

| Phase | Focus | Duration Est. | Key Deliverables | Dependencies |
|-------|-------|---------------|------------------|--------------|
| **18** | WAL Integration | 2-3 plans | MetricsWal struct, queue CRUD, MRecord append, basic tests | None |
| **19** | Checkpointing | 1-2 plans | MetricsShardPosition, metastore checkpoint, position events | Phase 18 |
| **20** | Cluster Gossip | 1-2 plans | ShardPositionsService integration, chitchat key setup | Phase 19 |
| **21** | Retry & Recovery | 1-2 plans | Error classification, startup recovery, replay logic | Phase 18 |
| **22** | Rate Limiting | 1-2 plans | RateLimiter integration, capacity checks, backpressure | Phase 18 |

**Notes:**
- Phases 19, 20, 21, 22 can potentially parallelize after Phase 18 completes
- Each phase should have integration tests validating the specific durability guarantee

---

## 6. Testing Strategy

### Phase 18: WAL Integration

**Unit Tests:**
- `test_metrics_wal_create_queue` - queue creation succeeds
- `test_metrics_wal_append_records` - records written and readable
- `test_metrics_wal_truncate` - truncation removes old records
- `test_metrics_wal_delete_queue` - queue deletion cleans up

**Integration Tests:**
- `test_metrics_wal_recovery` - WAL survives process restart
- `test_metrics_wal_concurrent_access` - multiple writers don't corrupt

### Phase 19: Checkpointing

**Unit Tests:**
- `test_metrics_position_tracking` - positions update correctly
- `test_metrics_checkpoint_persistence` - checkpoint saved to metastore

**Integration Tests:**
- `test_metrics_checkpoint_recovery` - positions restored after restart

### Phase 20: Cluster Gossip

**Integration Tests:**
- `test_metrics_position_propagation` - position updates reach other nodes (mock chitchat)
- `test_metrics_cross_node_visibility` - position visible cluster-wide

### Phase 21: Retry & Error Handling

**Unit Tests:**
- `test_metrics_error_classification` - transient vs permanent errors
- `test_metrics_retry_logic` - transient errors retried

**Integration Tests:**
- `test_metrics_recovery_on_startup` - uncommitted records replayed
- `test_metrics_fault_injection` - recovery after simulated failures

### Phase 22: Rate Limiting

**Unit Tests:**
- `test_metrics_capacity_check` - capacity enforced
- `test_metrics_rate_limiter` - rate limiting applied

**Integration Tests:**
- `test_metrics_backpressure` - overload triggers backpressure signal

---

## 7. Migration & Rollout

### Existing Metrics Indices

**No migration needed.** v0.3 adds durability to new indices. Existing indices without WAL continue to work (with no durability guarantee).

### Feature Flag

```rust
pub struct MetricsConfig {
    /// Enable WAL-backed durability for metrics.
    /// Default: false (for gradual rollout)
    pub metrics_wal_enabled: bool,
}
```

### Rollout Plan

1. **Stage 1:** Feature flag off by default. Internal testing only.
2. **Stage 2:** Enable per-index for beta customers.
3. **Stage 3:** Monitor WAL size and recovery time metrics.
4. **Stage 4:** Default to enabled for new indices.

### Monitoring

Key metrics to add:
- `metrics_wal_disk_usage_bytes` - WAL disk consumption
- `metrics_wal_memory_usage_bytes` - WAL memory buffer usage
- `metrics_wal_append_latency_seconds` - append operation latency
- `metrics_wal_recovery_duration_seconds` - startup recovery time
- `metrics_wal_queue_count` - number of active queues

---

## Appendix A: File References

| File | Purpose | Key Functions/Types |
|------|---------|---------------------|
| `quickwit-ingest/src/mrecordlog_async.rs` | Async WAL wrapper | `MultiRecordLogAsync`, `create_queue`, `append_records`, `truncate` |
| `quickwit-ingest/src/ingest_v2/mrecord.rs` | Record format | `MRecord`, `encode`, `decode` |
| `quickwit-ingest/src/ingest_v2/state.rs` | State management | `IngesterState`, `lock_fully`, `init` |
| `quickwit-ingest/src/ingest_v2/mrecordlog_utils.rs` | WAL utilities | `append_non_empty_doc_batch`, `check_enough_capacity`, `force_delete_queue` |
| `quickwit-indexing/src/models/shard_positions.rs` | Position service | `ShardPositionsService`, `LocalShardPositionsUpdate` |
| `quickwit-proto/src/types/position.rs` | Position type | `Position`, `Offset` |

---

## Appendix B: Glossary

| Term | Definition |
|------|------------|
| **WAL** | Write-Ahead Log - durable storage that persists data before acknowledgment |
| **mrecordlog** | Multi-queue record log - the WAL implementation used by Quickwit |
| **MRecord** | Message Record - versioned format for WAL entries (Doc or Commit) |
| **Position** | Offset within a shard (Beginning, Offset(N), Eof) |
| **Replication position** | Latest position written to WAL |
| **Truncation position** | Position up to which records have been indexed and can be deleted |
| **Publish position** | Position shared via cluster gossip |
| **Chitchat** | Gossip protocol for cluster state sharing |
| **ShardPositionsService** | Actor that bridges local positions to cluster-wide visibility |

---

*Document created: 2026-01-19*
*Research source: .planning/phases/17-research-deep-dive/17-RESEARCH.md*
*Validated against source code: 2026-01-19*

---

## Appendix C: Validation Summary

All key patterns verified against actual source code:

| Pattern | Source File | Lines | Status |
|---------|-------------|-------|--------|
| MRecord enum | `mrecord.rs` | 36-40 | VERIFIED |
| Position enum | `position.rs` | 92-99 | VERIFIED |
| WAL API (create/append/truncate) | `mrecordlog_async.rs` | 94-117 | VERIFIED |
| force_delete_queue | `mrecordlog_utils.rs` | 143-151 | VERIFIED |
| check_enough_capacity | `mrecordlog_utils.rs` | 114-140 | VERIFIED |
| append_non_empty_doc_batch | `mrecordlog_utils.rs` | 42-89 | VERIFIED |
| Two-phase locking (lock_fully) | `state.rs` | 244-270 | VERIFIED |
| Recovery init pattern | `state.rs` | 161-217 | VERIFIED |
| LocalShardPositionsUpdate | `shard_positions.rs` | 46-51 | VERIFIED |
| Chitchat key prefix | `shard_positions.rs` | 31 | VERIFIED |
| Position chitchat key format | `shard_positions.rs` | 266 | VERIFIED |
| Handler for LocalShardPositionsUpdate | `shard_positions.rs` | 231-252 | VERIFIED |

**Validation Method:** Direct code reading via Read tool on actual source files.
**Validation Date:** 2026-01-19
