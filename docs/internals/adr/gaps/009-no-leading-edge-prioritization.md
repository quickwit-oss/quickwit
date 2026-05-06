# GAP-009: No Leading Edge Prioritization or New Data Prioritization

**Status**: Open
**Discovered**: 2026-02-19
**Context**: Cloud-native storage characteristics analysis (leading edge prioritization, freshness)

## Problem

Quickwit's compaction does not prioritize recent time windows over older ones. The merge planner treats all eligible windows equally, processing them in whatever order it discovers merge candidates. This means that the **leading edge** — the most recent time windows where small files accumulate fastest and queries are most frequent — competes for compaction resources with older windows that are less query-hot.

At high ingestion rates, the leading edge accumulates hundreds of thousands of small splits per 15-minute window (see ADR-003). If compaction doesn't keep up with this accumulation, query performance on recent data degrades because every query must fan out to all those small splits. This is the most visible and impactful degradation, because observability queries overwhelmingly target recent data (dashboards, alerts, incident investigation).

**New data visibility** is also affected. The system does not prioritize making freshly-ingested data queryable over performing compaction on older data. In extremis, compaction of old windows should yield resources to ensure new data is visible within the ingest-to-query latency SLO (e.g., 30s p99.9).

## Evidence

The `StableLogMergePolicy` (which ADR-003 proposes adapting for metrics) does not have a concept of window priority. It evaluates merge candidates based on maturity and document count, not based on the age of the time window or the urgency of compaction for query performance.

There is no mechanism to:
- Prioritize compaction of recent windows over old windows
- Back off compaction of old windows when the leading edge has a backlog
- Signal that a particular window needs urgent compaction (e.g., many small files degrading queries)

## State of the Art

- **Husky**: Compactor prioritizes the leading edge. Recent time buckets are compacted first. Compaction auto-scales based on the backlog of uncompacted files at the leading edge.
- **Prometheus/Mimir**: Head block compaction runs on a tight schedule (every 2 hours). Vertical compaction of overlapping blocks is lower priority.

## Potential Solutions

- **Option A**: Priority queue for compaction scheduling. Assign priority based on window recency and split count. Recent windows with high split counts get compacted first. Older, already-compacted windows get lower priority.
- **Option B**: Separate leading-edge compaction from background compaction. Dedicate a portion of compaction resources to the most recent N windows (e.g., last 1 hour), with remaining resources for background compaction of older windows.
- **Option C**: Event-driven compaction hints. When a window's split count exceeds a threshold (e.g., affecting query latency), emit a compaction hint that bumps that window's priority. Similar to Husky's hinting mechanism mentioned in the Phase 1 design doc.

## Signal Impact

All signals equally affected. Leading edge prioritization is signal-agnostic — any signal with high write rates and time-range queries benefits.

## Impact

- **Severity**: High (directly affects query latency on recent data)
- **Frequency**: Constant under production load
- **Affected Areas**: Merge planner, compaction scheduler, resource allocation

## Next Steps

- [ ] Measure split count accumulation rate at the leading edge for representative ingestion rates
- [ ] Design priority-based compaction scheduling (window recency + split count)
- [ ] Define leading edge compaction SLO (e.g., max split count per window, max age before first compaction)
- [ ] Evaluate event-driven compaction hints vs polling-based priority

## References

- [ADR-003: Time-Windowed Sorted Compaction](../003-time-windowed-sorted-compaction.md)
- [Phase 1: Sorted Splits for Parquet](../../locality-compaction/phase-1-sorted-splits.md) — mentions hinting mechanism for compaction
