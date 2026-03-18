# Phase 14: Metastore Extension - Context

**Gathered:** 2026-01-17
**Status:** Ready for research

<vision>
## How This Should Work

Metrics splits should integrate into the metastore following the same patterns as Tantivy splits — same APIs, same behavioral flow. Where the same code paths can be used, use them. Where metrics-specific paths are needed, replicate the Tantivy behavior so the experience is consistent.

The core idea is invisible integration: MetricsSplitWriter creates a split, and it flows through the metastore the same way a Tantivy split would. The system shouldn't need special handling for "oh this is a metrics split" — it should just work within the established patterns.

</vision>

<essential>
## What Must Be Nailed

- **New protobuf message types** — MetricsSplitMetadata as its own protobuf message alongside SplitMetadata, keeping the separation clean
- **New metrics_splits table** — Use the Postgres schema from Phase 13-02 with GIN indexes and metrics-specific structure, not extending the existing splits table
- **New index type** — Metrics as a distinct index type alongside existing ones
- **API compatibility** — Existing metastore APIs should work with metrics splits following established patterns

All three pieces (protobuf, database, APIs) need to work together as a complete unit.

</essential>

<specifics>
## Specific Ideas

- Foundation only for this phase — protobuf definitions, migrations, conversion traits
- Staging and publishing wiring stays in Phase 15-16 as originally planned
- Research how existing splits connect to the metastore and apply that pattern to metrics

</specifics>

<notes>
## Additional Context

The goal is behavioral consistency — even if implementation details differ due to the metrics metadata structure, the way splits flow through the system should feel the same as Tantivy. This reduces cognitive load for anyone working in the codebase and leverages proven patterns.

</notes>

---

*Phase: 14-metastore-extension*
*Context gathered: 2026-01-17*
