# Phase 20: Cluster Gossip - Context

**Gathered:** 2026-01-19
**Status:** Ready for planning

<vision>
## How This Should Work

Metrics gossip should mirror the logs pipeline exactly. When a metrics shard advances its position, that information flows through chitchat the same way log shard positions do — using ShardPositionsService, the same observer pattern, the same update cadence.

Other nodes in the cluster see metrics shard positions just like they see log shard positions. This enables coordinated truncation (nodes know when it's safe to clean up WAL) and position visibility (routers and ingesters stay coordinated).

The goal is behavioral consistency: if you understand how logs gossip works, you understand how metrics gossip works.

</vision>

<essential>
## What Must Be Nailed

- **Full parity with logs gossip** — Same patterns, same infrastructure, same behavior
- **Position visibility** — Nodes can see where each metrics shard is at
- **Truncation coordination** — Safe WAL cleanup across the cluster

</essential>

<specifics>
## Specific Ideas

- Follow existing ShardPositionsService patterns exactly
- Use the same chitchat key format conventions (adapted for metrics)
- Same observer/subscription model for position change notifications
- Metrics should "just plug in" to existing gossip infrastructure

</specifics>

<notes>
## Additional Context

No special handling needed for metrics — the assumption is metrics shards behave like log shards from a gossip perspective. Higher throughput or different sharding patterns aren't concerns for this phase; the focus is clean integration with existing patterns.

</notes>

---

*Phase: 20-cluster-gossip*
*Context gathered: 2026-01-19*
