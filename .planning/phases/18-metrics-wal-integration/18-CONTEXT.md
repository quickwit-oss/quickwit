# Phase 18: Metrics WAL Integration - Context

**Gathered:** 2026-01-19
**Status:** Ready for planning

<vision>
## How This Should Work

Mirror the logs WAL pattern exactly. MetricsWal wraps MultiRecordLogAsync just like the logs pipeline does — same locking patterns, same queue lifecycle, same MRecord format. If you know how logs WAL works, metrics WAL should feel identical.

The WAL should have its own directory (separate from logs), its own MultiRecordLogAsync instance, and use a `metrics/` queue prefix to namespace metrics shards. But the API, the locking discipline, and the overall structure should be a direct parallel to what exists for logs.

This phase focuses on core WAL operations: queue CRUD, MRecord append/read, and truncation. Recovery logic (init-time queue discovery, shard state restoration) is explicitly deferred to Phase 21 — keeps this phase focused on the foundation.

</vision>

<essential>
## What Must Be Nailed

- **Data safety** — Metrics written to WAL must be recoverable after crash/restart. This is the entire point of durability.
- **Clean separation** — Metrics WAL completely isolated from logs WAL. Independent failure domains, independent disk allocation, independent recovery.
- **Match existing API** — MetricsWal should feel identical to working with the logs WAL. No surprises, no new patterns to learn. Same locking discipline, same error handling approach.

All three are equally important for this foundational phase.

</essential>

<specifics>
## Specific Ideas

- Separate `metrics_wal_dir_path` configuration (ADR-1 from design doc)
- Queue ID format: `metrics/{index_uid}/{source_id}/{shard_id}` (ADR-2)
- Reuse existing `MRecord::Doc(Bytes)` and `MRecord::Commit` — no new variants
- Two-phase locking: mrecordlog first, then inner state (match `lock_fully` pattern)
- Test coverage must match what logs WAL has — same confidence level

</specifics>

<notes>
## Additional Context

Phase 17 research produced a detailed design document (METRICS-DURABILITY-DESIGN.md) with:
- 5 ADRs covering directory separation, queue namespacing, MRecord format, position tracking, and replication factor
- Verified code patterns from the logs pipeline
- Integration specifications for all durability phases (18-22)

This phase is the foundation — Phases 19-22 all depend on MetricsWal being solid.

Recovery on startup is explicitly Phase 21 scope, not Phase 18.

</notes>

---

*Phase: 18-metrics-wal-integration*
*Context gathered: 2026-01-19*
