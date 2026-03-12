# Phase 26: Metrics Sequencer - Context

**Gathered:** 2026-01-21
**Status:** Ready for research

<vision>
## How This Should Work

The metrics sequencer should mirror how the logs sequencer works — same architectural patterns, same guarantees, same feel. When metrics flow through the pipeline, they should have the same ordering and delivery guarantees that logs have.

This isn't about inventing something new for metrics. It's about bringing metrics to parity with the existing logs infrastructure so the system behaves consistently regardless of data type.

</vision>

<essential>
## What Must Be Nailed

- **Ordering guarantees** — Metrics arrive in the same order they were sent, no reordering
- **Exactly-once delivery** — No duplicates, no drops, every metric point counted once
- **Actor integration** — Clean integration with the existing actor pipeline, just like logs

All three are equally important. Full parity with logs is the goal.

</essential>

<specifics>
## Specific Ideas

No specific requirements — open to standard approaches. The key directive is: make it work like the logs sequencer. Implementation details are open as long as the end result matches logs behavior.

</specifics>

<notes>
## Additional Context

This phase continues the pattern established in v0.3 Durability (WAL, checkpointing, retry, rate limiting) by adding the sequencer component. The metrics pipeline should ultimately have feature parity with the logs pipeline for production readiness.

</notes>

---

*Phase: 26-metrics-sequencer*
*Context gathered: 2026-01-21*
