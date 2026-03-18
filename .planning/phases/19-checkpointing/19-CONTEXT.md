# Phase 19: Checkpointing - Context

**Gathered:** 2026-01-19
**Status:** Ready for planning

<vision>
## How This Should Work

Mirror the logs ingest_v2 pipeline exactly. Full position tracking with replication position, truncation position, and publish position — the complete ShardPositionsService pattern adapted for metrics.

On restart, the system immediately knows exactly where to resume from each shard. Truncation only happens after splits are safely published and replicated. All the machinery that makes the logs pipeline durable should be present for metrics.

</vision>

<essential>
## What Must Be Nailed

- **Safe truncation** — Never lose data. Only truncate WAL after splits are safely published and replicated
- **Fast recovery** — On restart, immediately know exactly where to resume from each shard
- **Metastore persistence** — Positions must survive restarts via metastore storage

</essential>

<specifics>
## Specific Ideas

- Follow the existing logs pipeline patterns exactly — replication position, truncation position, publish position
- Focus on local position tracking + metastore persistence in this phase
- Chitchat gossip integration comes in Phase 20 (Cluster Gossip)

</specifics>

<notes>
## Additional Context

This phase is the foundation for durability. All three aspects (safe truncation, fast recovery, cluster coordination) need to work as a unit, but gossip coordination is deferred to Phase 20 as per the roadmap.

</notes>

---

*Phase: 19-checkpointing*
*Context gathered: 2026-01-19*
