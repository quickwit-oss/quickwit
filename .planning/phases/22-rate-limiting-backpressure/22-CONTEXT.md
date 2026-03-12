# Phase 22: Rate Limiting & Backpressure - Context

**Gathered:** 2026-01-19
**Status:** Ready for planning

<vision>
## How This Should Work

Match the logs pipeline exactly. The metrics pipeline should use the same rate limiting and backpressure patterns that already exist in the logs ingest path — token bucket per shard, memory capacity tracking, bounded channels. This isn't about innovation; it's about consistency and proven patterns.

When the system is under pressure, it should behave identically to how logs handles pressure: push back cleanly, don't crash, don't let one hot shard starve others.

</vision>

<essential>
## What Must Be Nailed

- **Protect against OOM** - Memory capacity tracking ensures the system never crashes from memory pressure
- **Fair shard distribution** - Per-shard rate limiting prevents one hot shard from starving others
- **Graceful degradation** - When limits hit, push back cleanly rather than fail

All three are equally essential — can't ship without all of them.

</essential>

<specifics>
## Specific Ideas

- Reuse quickwit's existing rate limiting primitives (RateLimiter and related types)
- Token bucket: 10MB burst, 5MB/s rate (as noted in roadmap)
- Follow the same patterns as logs for bounded channels and backpressure signaling

</specifics>

<notes>
## Additional Context

This is the final phase of v0.3 Durability. The goal is production-grade reliability matching the logs pipeline. No need for metrics-specific innovation here — just solid, proven patterns.

</notes>

---

*Phase: 22-rate-limiting-backpressure*
*Context gathered: 2026-01-19*
