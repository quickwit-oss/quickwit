# Phase 25: Full Pipeline E2E Tests - Context

**Gathered:** 2026-01-20
**Status:** Ready for planning

<vision>
## How This Should Work

Two complementary test approaches working together:

1. **End-to-end journey tests** — Single tests that push metrics in and query them out, verifying the complete Ingest → WAL → Parquet → metastore → query flow works as a whole.

2. **Stage-by-stage validation tests** — Tests that verify each stage individually with checkpoints between, so when something breaks you know exactly where.

Each stage checkpoint should validate:
- Data integrity (what goes in comes out unchanged)
- Metadata correctness (split metadata, pruning info, statistics)
- Error handling (failures handled gracefully with clear reporting)

</vision>

<essential>
## What Must Be Nailed

- **Confidence to ship** — These tests should give definitive proof that the pipeline works end-to-end. After they pass, you should feel confident deploying.

</essential>

<specifics>
## Specific Ideas

Key scenarios to cover:

- **High volume ingest** — Push lots of metrics through to verify throughput handles the load
- **Query accuracy** — Aggregations, time ranges, and tag filtering return correct results
- **Recovery scenarios** — WAL replay, checkpoint restoration after restart (validating v0.3 durability work)

</specifics>

<notes>
## Additional Context

This is the final phase of v0.4 Local Testing milestone. The Docker Compose environment (Phase 23) and test infrastructure module (Phase 24) are already in place — this phase is about writing the actual tests that exercise everything.

</notes>

---

*Phase: 25-full-pipeline-e2e-tests*
*Context gathered: 2026-01-20*
