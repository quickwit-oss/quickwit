# Phase 17: Research Deep Dive - Context

**Gathered:** 2026-01-19
**Status:** Ready for planning

<vision>
## How This Should Work

Deep code reading of the existing logs pipeline durability implementation. The goal is to thoroughly understand how ingest_v2, mrecordlog, and chitchat work together before implementing similar patterns for metrics.

This is pure research — no implementation. Read the code, trace the flows, understand the patterns. The output informs all subsequent durability phases (18-22).

</vision>

<essential>
## What Must Be Nailed

- **WAL mechanics** — How mrecordlog persists records, handles truncation, and recovers on restart
- **Position tracking** — How shard positions flow through the system (replication, publish, truncation checkpoints)
- **Integration points** — Where metrics pipeline needs to hook into existing ingest_v2 infrastructure

All three areas are equally important. Need comprehensive understanding, not just surface-level patterns.

</essential>

<specifics>
## Specific Ideas

No specific requirements — open to standard approaches. Trust judgment on what's important to document and understand.

</specifics>

<notes>
## Additional Context

This research phase sets the foundation for the entire v0.3 Durability milestone. Phases 18-22 all depend on understanding the patterns discovered here. Take time to understand thoroughly rather than rushing to implementation.

</notes>

---

*Phase: 17-research-deep-dive*
*Context gathered: 2026-01-19*
