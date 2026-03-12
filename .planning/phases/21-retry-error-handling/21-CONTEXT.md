# Phase 21: Retry & Error Handling - Context

**Gathered:** 2026-01-19
**Status:** Ready for research

<vision>
## How This Should Work

Mirror the logs pipeline retry patterns exactly. When the metrics ingester encounters failures — whether publishing to metastore, writing to storage, or communicating with other cluster nodes — it should behave identically to how the logs ingester handles the same situations.

The system should be invisible when things work, and graceful when they don't. Transient failures get retried with appropriate backoff. Permanent failures get surfaced rather than retried forever. And when an ingester restarts, it picks up exactly where it left off.

</vision>

<essential>
## What Must Be Nailed

- **Recovery on startup** — When an ingester restarts after crash or deployment, it resumes from exactly where it left off using WAL and checkpoint positions. No data loss, no duplicate processing.
- **Match logs pipeline patterns** — Same error classification, same backoff timing, same retry semantics. One system to understand, not two.

</essential>

<specifics>
## Specific Ideas

No specific requirements — follow existing logs pipeline patterns exactly as the reference implementation.

</specifics>

<notes>
## Additional Context

This phase builds on the WAL (Phase 18), checkpointing (Phase 19), and cluster gossip (Phase 20) work. Those phases provide the foundation; this phase ensures the system actually uses that foundation to recover gracefully from failures.

</notes>

---

*Phase: 21-retry-error-handling*
*Context gathered: 2026-01-19*
