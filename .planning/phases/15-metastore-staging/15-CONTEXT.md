# Phase 15: Metastore Staging - Context

**Gathered:** 2026-01-18
**Status:** Ready for planning

<vision>
## How This Should Work

Metrics staging should work exactly like logs staging. When the MetricsSplitWriter creates a split, it gets staged in the metastore automatically through the same actor-based flow that logs use.

The existing Uploader actor handles: PackagedSplitBatch → stage_splits() → upload files → SplitsUpdate → Sequencer → Publisher.

For metrics, a separate MetricsUploader actor follows the same pattern: MetricsSplitBatch → stage_metrics_splits() → upload Parquet → MetricsSplitsUpdate → Sequencer → Publisher.

Same flow, metrics-native types.

</vision>

<essential>
## What Must Be Nailed

- **Correctness over performance** — Get the flow right first, optimize later
- **Match logs exactly** — Same actor lifecycle, same error handling, same upload semaphores
- **Stage-before-upload ordering** — Metadata in metastore before files hit storage

</essential>

<specifics>
## Specific Ideas

- Separate MetricsUploader actor (not generalizing existing Uploader)
- Same semaphore-based upload limiting as logs
- Same Sequencer integration for ordered publishing
- Uses `stage_metrics_splits()` with `MetricsSplitMetadata` (from Phase 14)
- Clean separation from Tantivy code paths

</specifics>

<notes>
## Additional Context

The existing Uploader is deeply coupled to Tantivy's `PackagedSplit` and `SplitMetadata`. Metrics splits are fundamentally different (Parquet files, different metadata structure). A separate actor following the same pattern is cleaner than trying to abstract over both split types.

</notes>

---

*Phase: 15-metastore-staging*
*Context gathered: 2026-01-18*
