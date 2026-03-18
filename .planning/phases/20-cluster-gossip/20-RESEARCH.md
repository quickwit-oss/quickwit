# Phase 20: Cluster Gossip - Research

**Researched:** 2026-01-19
**Domain:** Chitchat gossip integration for metrics shard positions
**Confidence:** HIGH

<research_summary>
## Summary

Researched the Quickwit chitchat gossip infrastructure for shard positions. The existing ShardPositionsService in the logs pipeline provides a complete pattern to follow. Metrics should use the same infrastructure with a separate gossip key prefix.

Key findings:
- ShardPositionsService already handles local-to-cluster position propagation
- Chitchat uses prefix-filtered subscriptions — metrics can use `metrics.shard_positions:` prefix
- Event broker broadcasts `ShardPositionsUpdate` for local observers
- Two-phase position flow: LocalShardPositionsUpdate → ShardPositionsService → chitchat + EventBroker

**Primary recommendation:** Create MetricsShardPositionsService mirroring ShardPositionsService exactly. Use prefix `metrics.shard_positions:` for chitchat keys. Emit same event types for full ecosystem compatibility.

</research_summary>

<standard_stack>
## Standard Stack

### Core
| Component | Location | Purpose | Why Standard |
|-----------|----------|---------|--------------|
| ShardPositionsService | quickwit-indexing/src/models/shard_positions.rs | Position→cluster propagation | Proven pattern for logs |
| Cluster | quickwit-cluster/src/cluster.rs | Chitchat wrapper API | Built-in gossip infrastructure |
| EventBroker | quickwit-common/src/event_broker.rs | Local pub/sub | Decoupled event propagation |

### Supporting
| Component | Location | Purpose | When to Use |
|-----------|----------|---------|-------------|
| LocalShardPositionsUpdate | quickwit-indexing/src/models/shard_positions.rs | Local→service event | Trigger gossip from local updates |
| ShardPositionsUpdate | quickwit-proto/src/indexing/mod.rs | Cluster-wide event | Notify local observers of any position change |
| Position | quickwit-proto/src/types/position.rs | Position type | Consistent position representation |

### Existing Metrics Components (Phase 19)
| Component | Location | Purpose |
|-----------|----------|---------|
| MetricsShardPosition | quickwit-ingest/src/metrics/shard_position.rs | Per-shard position tracking |
| MetricsIngesterState | quickwit-ingest/src/metrics/state.rs | Two-phase locked state |
| truncation.rs | quickwit-ingest/src/metrics/truncation.rs | Safe WAL truncation |

</standard_stack>

<architecture_patterns>
## Architecture Patterns

### Recommended Project Structure
```
quickwit-ingest/src/metrics/
├── mod.rs                    # Public exports
├── wal.rs                    # MetricsWal (Phase 18)
├── shard_position.rs         # MetricsShardPosition (Phase 19)
├── state.rs                  # MetricsIngesterState (Phase 19)
├── truncation.rs             # Truncation utilities (Phase 19)
└── shard_positions_service.rs  # NEW: MetricsShardPositionsService (Phase 20)
```

### Pattern 1: Separate Gossip Prefix for Metrics
**What:** Use distinct chitchat key prefix to isolate metrics from logs
**When to use:** Always — prevents cross-contamination
**Implementation:**
```rust
// Logs prefix (existing)
const SHARD_POSITIONS_PREFIX: &str = "indexer.shard_positions:";

// Metrics prefix (new)
const METRICS_SHARD_POSITIONS_PREFIX: &str = "metrics.shard_positions:";
```
**Key format:** `metrics.shard_positions:{index_uid}:{source_id}:{shard_id}`

### Pattern 2: Mirror ShardPositionsService Actor
**What:** Create MetricsShardPositionsService with identical structure
**When to use:** For metrics position gossip
**Key methods to implement:**
```rust
impl MetricsShardPositionsService {
    // Static factory - spawns as actor
    pub fn spawn(cluster: Cluster, event_broker: EventBroker) -> ActorHandle<Self>;

    // Initialize - subscribe to chitchat, replay existing state
    async fn initialize(&mut self);

    // Apply monotonic position update
    fn apply_update(&mut self, source_uid: SourceUid, updates: Vec<(ShardId, Position)>);

    // Publish to chitchat (cluster-wide)
    async fn publish_positions_into_chitchat(&self, ...);

    // Publish to event broker (local observers)
    fn publish_shard_updates_to_event_broker(&self, ...);
}

// Handler for local position updates (from metrics ingester)
impl Handler<LocalShardPositionsUpdate> for MetricsShardPositionsService { ... }

// Handler for cluster position updates (from chitchat)
impl Handler<ClusterShardPositionsUpdate> for MetricsShardPositionsService { ... }
```

### Pattern 3: Event-Driven Position Flow
**What:** Use EventBroker for loose coupling between components
**Flow:**
```
1. MetricsIngester advances truncation_position
   ↓
2. Emits LocalShardPositionsUpdate via EventBroker
   ↓
3. MetricsShardPositionsService receives event
   ↓
4. Service updates internal state (monotonic max)
   ↓
5a. Publishes to chitchat (cluster visibility)
5b. Publishes ShardPositionsUpdate to EventBroker (local observers)
   ↓
6. Other nodes receive via chitchat subscription
   ↓
7. Their MetricsShardPositionsService updates + broadcasts locally
```

### Anti-Patterns to Avoid
- **Custom gossip implementation:** Use existing chitchat infrastructure
- **Direct cluster calls from ingester:** Use EventBroker for decoupling
- **Shared prefix with logs:** Use `metrics.shard_positions:` not `indexer.shard_positions:`
- **Skipping initialization replay:** Must sync with existing chitchat state on startup
</architecture_patterns>

<dont_hand_roll>
## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Cluster gossip | Custom UDP/gRPC sync | Chitchat + Cluster API | Proven, handles failures, TTL |
| Position monotonicity | Manual tracking | apply_update pattern | Edge cases around concurrent updates |
| Local event dispatch | Direct function calls | EventBroker | Decoupled, testable |
| Chitchat subscription | Manual polling | cluster.subscribe(prefix) | Built-in, efficient |
| Position serialization | Custom format | Position::to_string() | 20-char zero-padded, lexicographic |

**Key insight:** The entire gossip infrastructure exists. MetricsShardPositionsService is essentially ShardPositionsService with a different prefix. Don't diverge from the proven pattern.
</dont_hand_roll>

<common_pitfalls>
## Common Pitfalls

### Pitfall 1: Missing Initialization Replay
**What goes wrong:** Service misses positions gossiped before it started
**Why it happens:** Skipping initial sync with existing chitchat state
**How to avoid:** In `initialize()`, replay all existing keys matching the prefix
**Warning signs:** Positions out of sync after node restart

### Pitfall 2: Using Wrong Prefix
**What goes wrong:** Metrics positions overwrite logs positions or vice versa
**Why it happens:** Copy-paste error, using `indexer.shard_positions:` for metrics
**How to avoid:** Use `metrics.shard_positions:` prefix consistently
**Warning signs:** Logs showing metrics shard IDs, position corruption

### Pitfall 3: Publishing Non-Monotonic Updates
**What goes wrong:** Position goes backward, confuses truncation logic
**Why it happens:** Not checking if new position > current position
**How to avoid:** apply_update only keeps max position per shard
**Warning signs:** Truncation positions moving backward

### Pitfall 4: Forgetting to Broadcast Both Ways
**What goes wrong:** Local observers don't see cluster updates, or cluster doesn't see local
**Why it happens:** Only publishing to one of chitchat OR EventBroker
**How to avoid:**
- Local updates → chitchat + EventBroker
- Cluster updates → EventBroker only (already in chitchat)
**Warning signs:** Incomplete position visibility

### Pitfall 5: TTL Expiration Without Refresh
**What goes wrong:** Positions disappear from chitchat after TTL
**Why it happens:** Not refreshing positions periodically
**How to avoid:** Re-publish on position update; TTL is long enough (2 hours default)
**Warning signs:** Positions missing after idle period
</common_pitfalls>

<code_examples>
## Code Examples

Verified patterns from existing ShardPositionsService.

### Chitchat Key Format and Parsing
```rust
// Source: quickwit-indexing/src/models/shard_positions.rs
const METRICS_SHARD_POSITIONS_PREFIX: &str = "metrics.shard_positions:";

fn parse_shard_positions_from_kv(key: &str, value: &str) -> anyhow::Result<ClusterShardPositionsUpdate> {
    // key format: metrics.shard_positions:{index_uid}:{source_id}:{shard_id}
    let key_without_prefix = key.strip_prefix(METRICS_SHARD_POSITIONS_PREFIX)
        .context("invalid key prefix")?;

    let (source_uid_str, shard_id_str) = key_without_prefix
        .rsplit_once(':')
        .context("missing shard_id separator")?;

    let (index_uid_str, source_id) = source_uid_str
        .rsplit_once(':')
        .context("missing source_id separator")?;

    let shard_id = ShardId::from(shard_id_str);
    let index_uid: IndexUid = index_uid_str.parse()?;
    let source_uid = SourceUid { index_uid, source_id: source_id.to_string() };
    let position = Position::from(value.to_string());

    Ok(ClusterShardPositionsUpdate {
        source_uid,
        shard_positions: vec![(shard_id, position)],
    })
}
```

### Publishing to Chitchat
```rust
// Source: quickwit-indexing/src/models/shard_positions.rs
async fn publish_positions_into_chitchat(
    &self,
    source_uid: &SourceUid,
    shard_positions: &[(ShardId, Position)],
) {
    for (shard_id, position) in shard_positions {
        let key = format!(
            "{}{}:{}:{}",
            METRICS_SHARD_POSITIONS_PREFIX,
            source_uid.index_uid,
            source_uid.source_id,
            shard_id
        );
        self.cluster
            .set_self_key_value_delete_after_ttl(key, position.to_string())
            .await;
    }
}
```

### Chitchat Subscription in Initialize
```rust
// Source: quickwit-indexing/src/models/shard_positions.rs
async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
    let mailbox = ctx.mailbox().clone();

    // Subscribe to chitchat changes with prefix filter
    self.cluster_listener_handle_opt = Some(
        self.cluster
            .subscribe(METRICS_SHARD_POSITIONS_PREFIX, move |event| {
                let update = parse_shard_positions_from_kv(&event.key, &event.value);
                if let Ok(update) = update {
                    let _ = mailbox.send_message(update);
                }
            })
            .await,
    );

    // Replay existing state (idempotent)
    for (key, value) in self.cluster.iter_prefix(METRICS_SHARD_POSITIONS_PREFIX) {
        if let Ok(update) = parse_shard_positions_from_kv(&key, &value) {
            self.apply_update(update.source_uid, update.shard_positions);
            // Publish to local EventBroker so observers catch up
            self.publish_shard_updates_to_event_broker(
                update.source_uid,
                update.shard_positions,
            );
        }
    }

    Ok(())
}
```

### Monotonic Position Update
```rust
// Source: quickwit-indexing/src/models/shard_positions.rs
fn apply_update(
    &mut self,
    source_uid: SourceUid,
    shard_positions: Vec<(ShardId, Position)>,
) -> Vec<(ShardId, Position)> {
    let mut changed = Vec::new();

    let source_positions = self
        .shard_positions_per_source
        .entry(source_uid)
        .or_default();

    for (shard_id, new_position) in shard_positions {
        let current = source_positions.entry(shard_id.clone()).or_insert(Position::Beginning);
        // Only update if new position is greater (monotonic)
        if new_position > *current {
            *current = new_position.clone();
            changed.push((shard_id, new_position));
        }
    }

    changed
}
```

### Handler for Local Updates
```rust
// Source: quickwit-indexing/src/models/shard_positions.rs
#[async_trait]
impl Handler<LocalShardPositionsUpdate> for MetricsShardPositionsService {
    type Reply = ();

    async fn handle(
        &mut self,
        message: LocalShardPositionsUpdate,
        _ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        let changed = self.apply_update(
            message.source_uid.clone(),
            message.shard_positions,
        );

        if !changed.is_empty() {
            // Publish to cluster (chitchat)
            self.publish_positions_into_chitchat(&message.source_uid, &changed).await;
            // Publish to local observers (EventBroker)
            self.publish_shard_updates_to_event_broker(message.source_uid, changed);
        }

        Ok(())
    }
}
```
</code_examples>

<integration_points>
## Integration Points

### Where Metrics Emits LocalShardPositionsUpdate

The metrics truncation flow (from Phase 19) should emit position updates:

```rust
// In metrics ingester, after indexer confirms records indexed:
pub async fn advance_truncation_position(
    &self,
    source_uid: SourceUid,
    shard_id: ShardId,
    new_position: Position,
) {
    // Update local state
    let mut locked = self.state.lock_fully().await;
    if let Some(shard_pos) = locked.inner.shard_positions.get_mut(&queue_id) {
        shard_pos.update_truncation_position(new_position.clone());
    }

    // Emit event for gossip propagation
    self.event_broker.publish(LocalShardPositionsUpdate {
        source_uid,
        shard_positions: vec![(shard_id, new_position)],
    });
}
```

### Where Observers Consume ShardPositionsUpdate

Components that need to react to position changes:
1. **Truncation service** — triggers WAL cleanup when safe
2. **Routers** — know which positions are durable for routing decisions
3. **Recovery** — knows minimum positions for replay

```rust
// Example observer registration
let _subscription = event_broker.subscribe(|update: ShardPositionsUpdate| {
    for (shard_id, position) in &update.updated_shard_positions {
        // React to position change
    }
});
```
</integration_points>

<sources>
## Sources

### Primary (HIGH confidence)
- quickwit-indexing/src/models/shard_positions.rs — ShardPositionsService implementation
- quickwit-cluster/src/cluster.rs — Cluster API for chitchat
- quickwit-proto/src/indexing/mod.rs — ShardPositionsUpdate event definition
- quickwit-ingest/src/metrics/ — Existing metrics infrastructure (Phase 18-19)

### Secondary (MEDIUM confidence)
- .planning/phases/17-research-deep-dive/17-RESEARCH.md — Prior durability research
- quickwit-cluster/src/grpc_gossip.rs — gRPC catch-up for convergence

### Verified Patterns
- Chitchat prefix subscription — used by ShardPositionsService
- TTL-based key expiration — cluster.set_self_key_value_delete_after_ttl()
- Monotonic position updates — apply_update pattern
- Event-driven architecture — EventBroker throughout codebase
</sources>

<metadata>
## Metadata

**Research scope:**
- Core technology: Chitchat gossip + quickwit-actors
- Ecosystem: ShardPositionsService, EventBroker, Cluster API
- Patterns: Actor-based service, prefix subscriptions, event-driven updates
- Pitfalls: Initialization replay, prefix collision, monotonicity

**Confidence breakdown:**
- Standard stack: HIGH — direct codebase analysis
- Architecture: HIGH — mirrors existing ShardPositionsService
- Pitfalls: HIGH — observed in logs pipeline patterns
- Code examples: HIGH — adapted from actual ShardPositionsService code

**Research date:** 2026-01-19
**Valid until:** N/A (internal codebase patterns, not external ecosystem)
</metadata>

---

*Phase: 20-cluster-gossip*
*Research completed: 2026-01-19*
*Ready for planning: yes*
