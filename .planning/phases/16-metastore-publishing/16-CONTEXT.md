# Phase 16: Metastore Publishing - Context

**Gathered:** 2026-01-18
**Status:** Ready for planning

<vision>
## How This Should Work

Mirror the logs publishing flow exactly. The metrics pipeline should follow the same actor pattern as logs — MetricsUploader sends to Sequencer, Sequencer forwards to Publisher, Publisher calls publish_splits on the metastore.

After publish_splits completes, metrics splits transition from "staged" to "published" state and become visible to queries. This completes the staging-to-queryable lifecycle started in Phase 15.

</vision>

<essential>
## What Must Be Nailed

- **Splits become queryable** — After publish_splits, metrics splits must appear in list_splits for queries
- **Mirror logs flow exactly** — Same actor wiring (Uploader → Sequencer → Publisher), same message pattern as logs pipeline
- **Correctness over speed** — Ensure no splits get lost or published incorrectly

</essential>

<specifics>
## Specific Ideas

- Use Sequencer even though metrics don't strictly need ordering (consistency with logs pattern)
- Reuse existing Publisher actor if possible, or adapt it for metrics
- MetricsSplitsUpdate message needs to convert/integrate with what Publisher expects

</specifics>

<notes>
## Additional Context

The existing MetricsUploader has a TODO placeholder for Phase 16 — it currently logs that splits were staged/uploaded but doesn't send to Publisher yet. This phase wires up that final connection.

Key difference from logs: no checkpoint delta tracking for metrics (yet). The publish call just moves splits from staged → published state.

</notes>

---

*Phase: 16-metastore-publishing*
*Context gathered: 2026-01-18*
