# Architecture Decision Records (ADR) Index

This directory serves as the **central knowledge base** for Quickwit architecture.

## Knowledge Map (Agent Context)

For AI agents and developers, here is how the system is organized by domain:

### Core Architecture

ADRs will be created here as we implement new systems. Start with the metrics pipeline and work outward.

### Signal Priority

**Metrics first**, then traces, then logs. Architectural decisions must generalize across all three signals.

---

## Master Index

| ADR | Title | Status | Tags | Key Components |
|-----|-------|--------|------|----------------|
| [000](./000-template.md) | Template | - | `meta` | - |
| [001](./001-parquet-data-model.md) | Parquet Metrics Data Model | Proposed | `storage`, `metrics`, `parquet`, `data-model` | quickwit-parquet-engine |
| [002](./002-sort-schema-parquet-splits.md) | Configurable Sort Schema for Parquet Splits | Proposed | `storage`, `metrics`, `compaction`, `parquet`, `sorting` | quickwit-parquet-engine, quickwit-indexing |
| [003](./003-time-windowed-sorted-compaction.md) | Time-Windowed Sorted Compaction for Parquet | Proposed | `storage`, `metrics`, `compaction`, `parquet`, `time-windowing` | quickwit-parquet-engine, quickwit-indexing, quickwit-metastore |

## Supplements & Roadmaps

Detailed implementation plans and reports linked to ADRs.

| Parent ADR | Supplement | Description |
|------------|------------|-------------|
| [000](./000-template.md) | [Supplement Template](./supplements/000-supplement-template.md) | Template for new supplements |

## Architecture Evolution

Quickwit tracks architectural change through three lenses. See **[EVOLUTION.md](./EVOLUTION.md)** for the full process.

```
                    Architecture Evolution
                            │
       ┌────────────────────┼────────────────────┐
       ▼                    ▼                    ▼
 Characteristics          Gaps              Deviations
  (Proactive)          (Reactive)          (Pragmatic)
```

### Characteristics (What we need)

Product requirements and capabilities we must have.

### Gaps (What we learned)

| Gap | Title | Status | Severity |
|-----|-------|--------|----------|
| [001](./gaps/001-no-parquet-compaction.md) | No Parquet Split Compaction | Open | High |
| [002](./gaps/002-fixed-sort-schema.md) | Fixed Hardcoded Sort Schema | Open | Medium |
| [003](./gaps/003-no-time-window-partitioning.md) | No Time-Window Partitioning at Ingestion | Open | High |
| [004](./gaps/004-incomplete-split-metadata.md) | Incomplete Split Metadata for Compaction | Open | High |
| [005](./gaps/005-no-per-point-deduplication.md) | No Per-Point Deduplication | Open | Medium |
| [006](./gaps/006-no-independent-auto-scaling.md) | No Independent Auto-Scaling | Open | High |
| [007](./gaps/007-no-multi-level-caching.md) | No Parquet Metadata Caching | Open | High |
| [008](./gaps/008-no-high-query-rate-optimization.md) | No High Query Rate Optimization | Open | High |
| [009](./gaps/009-no-leading-edge-prioritization.md) | No Leading Edge Prioritization | Open | High |
| [010](./gaps/010-no-data-caching-or-query-affinity.md) | No Multi-Level Data Caching or Query Affinity Optimization | Open | High |

**Create a gap** when you discover a design limitation from production, incidents, or research. See [gaps/README.md](./gaps/README.md).

### Deviations (What we accepted)

| Deviation | Title | Related ADR | Priority |
|-----------|-------|-------------|----------|

*No deviations recorded yet.*

**Create a deviation** when implementation intentionally differs from ADR intent. See [deviations/README.md](./deviations/README.md).

## Decision Logs (How to use)

We do not have a separate "Decision Log" file. **Decision Logs are embedded in each ADR.**

When you need to understand *why* a decision was made:
1. Find the relevant ADR in the Knowledge Map above.
2. Scroll to the **Decision Log** section at the bottom of that ADR.
3. If making a NEW decision, update that table.

## Status Definitions

- **Proposed**: Under discussion, awaiting prototype or review.
- **Accepted**: Approved plan of record. Implementation should follow this.
- **Deprecated**: Replaced or abandoned. Kept for history.
- **Superseded**: Replaced by a newer ADR (see link).
