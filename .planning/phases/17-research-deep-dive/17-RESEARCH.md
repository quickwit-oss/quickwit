# Phase 17: Research Deep Dive - Research

**Researched:** 2026-01-19
**Domain:** Quickwit ingest_v2 durability patterns (mrecordlog, chitchat, positions)
**Confidence:** HIGH

<research_summary>
## Summary

Researched the existing Quickwit logs pipeline to understand how durability is achieved via WAL (mrecordlog), position tracking, and cluster-wide gossip (chitchat). The metrics pipeline needs to follow these same patterns for production durability.

Key findings:
- **mrecordlog** is the WAL implementation - one queue per shard, MRecord format for documents
- **Position tracking** has three tiers: replication (WAL), truncation (indexed), publish (gossip)
- **Chitchat gossip** shares shard positions cluster-wide with TTL-based keys
- **ShardPositionsService** bridges local position updates to cluster visibility
- **ingest_v2 architecture** separates routing (IngestRouter) from ingestion (Ingester) with smart retry via IngestWorkbench

**Primary recommendation:** Metrics pipeline should create dedicated mrecordlog queues (separate from logs), emit LocalShardPositionsUpdate events, and leverage existing ShardPositionsService/chitchat infrastructure for cluster coordination.
</research_summary>

<standard_stack>
## Standard Stack

These are the existing internal crates/modules metrics will integrate with.

### Core
| Module | Location | Purpose | Why Standard |
|--------|----------|---------|--------------|
| mrecordlog | External crate (quickwit-oss/mrecordlog) | Write-ahead log with multi-queue support | Proven durability, already powers logs |
| MultiRecordLogAsync | quickwit-ingest/src/mrecordlog_async.rs | Async wrapper around mrecordlog | Consistent async patterns |
| MRecord | quickwit-ingest/src/ingest_v2/mrecord.rs | Document encoding for WAL | Same format enables shared tooling |
| Position | quickwit-proto/src/types/position.rs | Offset tracking (Beginning/Offset/Eof) | Consistent position semantics |

### Supporting
| Module | Location | Purpose | When to Use |
|--------|----------|---------|-------------|
| ShardPositionsService | quickwit-indexing/src/models/shard_positions.rs | Cluster-wide position tracking | Need positions visible to other nodes |
| EventBroker | quickwit-common | Pub/sub for local events | In-process communication |
| chitchat | quickwit-cluster | Gossip protocol for cluster state | Cross-node state sharing |
| PublishTracker | quickwit-ingest/src/ingest_v2/publish_tracker.rs | Track persist-to-publish durability | Wait for data to be indexed |

### Existing Patterns to Reuse
| Pattern | Location | Applicability |
|---------|----------|---------------|
| IngesterState locking | state.rs | Always mrecordlog lock before inner |
| Rate limiting | models.rs | Token bucket per shard |
| Capacity checking | mrecordlog_utils.rs | Disk/memory limits before append |
| Idle shard timeout | idle.rs | Auto-close unused shards |
</standard_stack>

<architecture_patterns>
## Architecture Patterns

### Recommended Integration Structure
```
quickwit-ingest/src/
├── mrecordlog_async.rs          # SHARED - async wrapper (already exists)
├── ingest_v2/
│   ├── mrecord.rs               # Extend for metrics OR use as-is
│   ├── mrecordlog_utils.rs      # REUSE - capacity checks, append utilities
│   └── ...
└── metrics/                     # NEW - metrics-specific pipeline
    ├── mod.rs
    ├── metrics_wal.rs           # Metrics WAL integration
    ├── metrics_positions.rs     # Position tracking for metrics
    └── ...
```

### Pattern 1: WAL Queue-per-Shard
**What:** Each metrics shard gets its own mrecordlog queue
**When to use:** Always for durability
**Example:**
```rust
// Queue ID format matches logs pattern
let queue_id = format!("{index_uid}/{source_id}/{shard_id}");

// Create queue
mrecordlog.create_queue(&queue_id).await?;

// Append with MRecord format
let encoded = MRecord::Doc(metric_bytes).encode();
mrecordlog.append_records(&queue_id, None, iter::once(encoded)).await?;
```

### Pattern 2: Two-Phase Locking
**What:** Lock order: mrecordlog → inner state (prevents deadlock)
**When to use:** Any mutation involving both WAL and state
**Example:**
```rust
// From state.rs - ALWAYS follow this pattern
pub async fn lock_fully(&self) -> Result<FullyLockedState<'_>> {
    // 1. Lock WAL first
    let mrecordlog = self.mrecordlog.write().await;
    // 2. Then lock inner state
    let inner = self.inner.lock().await;
    Ok(FullyLockedState { inner, mrecordlog })
}
```

### Pattern 3: Position-Based Durability
**What:** Track three positions: replication (WAL), truncation (indexed), publish (cluster)
**When to use:** Full durability tracking
**Example:**
```rust
// After appending to WAL
shard.replication_position_inclusive = Position::Offset(new_offset);

// After indexing completes
shard.truncation_position_inclusive = indexed_up_to;

// Publish via chitchat
event_broker.publish(LocalShardPositionsUpdate {
    source_uid,
    shard_positions: vec![(shard_id, publish_position)],
});
```

### Pattern 4: Event-Driven Position Propagation
**What:** Local events → ShardPositionsService → chitchat → cluster
**When to use:** Cross-node coordination
**Flow:**
```
LocalShardPositionsUpdate (local event)
    → EventBroker
    → ShardPositionsService (keeps max position)
    → chitchat key: indexer.shard_positions:{index}:{source}:{shard}
    → ShardPositionsUpdate (cluster event)
    → Routers, Indexers receive update
```

### Anti-Patterns to Avoid
- **Custom WAL implementation:** Use mrecordlog, don't roll your own
- **Global lock for all shards:** Lock per-shard, not globally
- **Polling for position updates:** Use watch channels and events
- **Skipping capacity checks:** Always check disk/memory before append
</architecture_patterns>

<dont_hand_roll>
## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Write-ahead log | Custom file-based WAL | mrecordlog crate | Handles fsync, recovery, truncation |
| Cluster gossip | Custom broadcast | chitchat protocol | Battle-tested, TTL support, failure detection |
| Position tracking | Manual offset tracking | Position enum + ShardPositionsService | Consistent semantics, cluster visibility |
| Rate limiting | Simple counters | RateLimiter + RateMeter | Token bucket with sliding window |
| Async locking | std::sync primitives | tokio Mutex/RwLock | Deadlock-safe, async-friendly |
| Document encoding | Raw bytes | MRecord format | Versioned, forward-compatible |
| Event propagation | Direct function calls | EventBroker | Decoupled, testable, extensible |

**Key insight:** The logs pipeline has solved every durability problem metrics will face. Use the same infrastructure - don't diverge.
</dont_hand_roll>

<common_pitfalls>
## Common Pitfalls

### Pitfall 1: Lock Order Violation
**What goes wrong:** Deadlock between threads acquiring locks in different order
**Why it happens:** mrecordlog and inner state have separate locks
**How to avoid:** Always use `lock_fully()` pattern - mrecordlog THEN inner
**Warning signs:** Timeout on lock acquisition, thread hangs

### Pitfall 2: Missing Capacity Checks
**What goes wrong:** WAL fills disk, node becomes unhealthy
**Why it happens:** Appending without checking available capacity
**How to avoid:** Call `check_enough_capacity()` before every append
**Warning signs:** Disk usage alerts, append errors

### Pitfall 3: Stale Position in Chitchat
**What goes wrong:** Truncation on stale position deletes unindexed data
**Why it happens:** Not waiting for position to propagate through cluster
**How to avoid:** Use PublishTracker to wait for durability, respect TTL
**Warning signs:** Data loss after node restart

### Pitfall 4: Replication Stream Timeout
**What goes wrong:** Replication fails silently, data on leader only
**Why it happens:** Network partition or slow follower
**How to avoid:** Monitor replication lag, handle AckTimeout properly
**Warning signs:** Increasing replication_position gap between leader/follower

### Pitfall 5: Queue ID Collision
**What goes wrong:** Metrics and logs write to same WAL queue
**Why it happens:** Using same queue_id format without namespace
**How to avoid:** Use distinct prefix: `metrics/{index_uid}/{source_id}/{shard_id}`
**Warning signs:** Mixed record types in recovery, parse errors
</common_pitfalls>

<code_examples>
## Code Examples

Verified patterns from existing logs pipeline.

### WAL Append with Capacity Check
```rust
// Source: mrecordlog_utils.rs
pub async fn append_non_empty_doc_batch(
    mrecordlog: &mut MultiRecordLogAsync,
    queue_id: &QueueId,
    doc_batch: DocBatchV2,
    force_commit: bool,
) -> Result<Position, AppendDocBatchError> {
    let encoded_mrecords = doc_batch
        .into_docs()
        .map(|(_uid, doc)| MRecord::Doc(doc).encode())
        .chain(force_commit.then(|| MRecord::Commit.encode()));

    let position = mrecordlog
        .append_records(queue_id, None, encoded_mrecords)
        .await?
        .expect("batch is not empty");

    Ok(Position::offset(position))
}
```

### Position Publication via Event Broker
```rust
// Source: ingest_v2/mod.rs pattern
pub fn suggest_truncate(&self, checkpoint: &Checkpoint) {
    let shard_positions: Vec<_> = checkpoint
        .iter()
        .map(|(shard_id, position)| (*shard_id, position.clone()))
        .collect();

    let update = LocalShardPositionsUpdate {
        source_uid: self.source_uid.clone(),
        shard_positions,
    };

    self.event_broker.publish(update);
}
```

### State Recovery on Startup
```rust
// Source: state.rs init()
for queue_id in mrecordlog.list_queues() {
    if let Some(range) = queue_position_range(&mrecordlog, &queue_id) {
        // Non-empty queue: recover shard in Closed state
        let shard = IngesterShard::new_solo(
            ShardState::Closed,
            Position::offset(*range.end()),
            Position::offset(*range.start().saturating_sub(1)),
            None, // doc_mapper
            false, // validate_docs
        );
        state.shards.insert(queue_id, shard);
    } else {
        // Empty queue: delete it
        force_delete_queue(&mut mrecordlog, &queue_id).await?;
    }
}
```

### Chitchat Position Key Format
```rust
// Source: shard_positions.rs
// Key format for cluster-wide position sharing
let chitchat_key = format!(
    "indexer.shard_positions:{index_uid}:{source_id}:{shard_id}"
);

// Value: Position as string (lexicographically sortable)
// "00000000000000001000" = Offset(1000)
// "~" = Eof(None)
// "~00000000000000001000" = Eof(Some(1000))
```
</code_examples>

<sota_updates>
## State of the Art (2025-2026)

This is internal codebase - no external ecosystem changes to track.

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Custom per-service WAL | Shared mrecordlog | Original design | Consistent durability |
| Polling for positions | Event-driven + watch | ingest_v2 design | Lower latency |
| Manual gossip | chitchat integration | Cluster redesign | Reliable propagation |

**Internal improvements to consider:**
- mrecordlog could support batched fsync for higher throughput
- Position tracking could use CRDT for better conflict resolution

**Stable patterns (don't change):**
- MRecord encoding format (versioned, backward compatible)
- Position enum semantics (Beginning < Offset < Eof ordering)
- chitchat key format (must match for routing)
</sota_updates>

<open_questions>
## Open Questions

Questions to resolve during Phase 17 planning and Phases 18-22 execution.

1. **Shared vs Separate WAL Directory**
   - What we know: Logs use configurable `wal_dir_path`
   - What's unclear: Should metrics use same directory or separate?
   - Recommendation: Separate directory `metrics_wal_dir_path` for isolation

2. **Queue ID Namespace**
   - What we know: Logs use `{index_uid}/{source_id}/{shard_id}`
   - What's unclear: How to distinguish metrics queues?
   - Recommendation: Prefix with `metrics/` - `metrics/{index_uid}/{source_id}/{shard_id}`

3. **Metrics-Specific MRecord Types**
   - What we know: MRecord has Doc and Commit variants
   - What's unclear: Do metrics need different record types?
   - Recommendation: Start with same MRecord, extend only if needed

4. **Position Tracking Granularity**
   - What we know: Logs track per-shard positions
   - What's unclear: Metrics have different access patterns (time-range)
   - Recommendation: Same per-shard model, add time-range metadata separately

5. **Replication Factor for Metrics**
   - What we know: Logs support configurable replication (1-2)
   - What's unclear: Do metrics need replication?
   - Recommendation: Start with replication_factor=1 (simpler), add later
</open_questions>

<sources>
## Sources

### Primary (HIGH confidence)
- `/quickwit-ingest/src/mrecordlog_async.rs` - WAL wrapper API
- `/quickwit-ingest/src/ingest_v2/mrecord.rs` - MRecord encoding
- `/quickwit-ingest/src/ingest_v2/state.rs` - Locking patterns
- `/quickwit-ingest/src/ingest_v2/mrecordlog_utils.rs` - Append utilities
- `/quickwit-ingest/src/ingest_v2/ingester.rs` - Full persist flow
- `/quickwit-indexing/src/models/shard_positions.rs` - ShardPositionsService
- `/quickwit-proto/src/types/position.rs` - Position enum
- `/quickwit-cluster/src/cluster.rs` - Chitchat integration

### Secondary (MEDIUM confidence)
- `/quickwit-ingest/src/ingest_v2/router.rs` - Routing patterns
- `/quickwit-ingest/src/ingest_v2/workbench.rs` - Retry state machine
- `/quickwit-ingest/src/ingest_v2/publish_tracker.rs` - Durability tracking

### Tertiary (LOW confidence - needs validation)
- None - all findings from codebase analysis
</sources>

<metadata>
## Metadata

**Research scope:**
- Core technology: mrecordlog WAL, chitchat gossip
- Ecosystem: ingest_v2 service architecture
- Patterns: Position tracking, event-driven coordination, locking
- Pitfalls: Lock order, capacity, stale positions

**Confidence breakdown:**
- Standard stack: HIGH - direct codebase analysis
- Architecture: HIGH - traced actual code paths
- Pitfalls: HIGH - documented in code comments
- Code examples: HIGH - copied from source files

**Research date:** 2026-01-19
**Valid until:** Indefinite (internal patterns, not external ecosystem)
</metadata>

---

*Phase: 17-research-deep-dive*
*Research completed: 2026-01-19*
*Ready for planning: yes*
