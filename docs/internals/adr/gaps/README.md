# Architecture Gaps

> **Part of [Architecture Evolution](../EVOLUTION.md)** - the reactive lens for tracking design limitations discovered through production.

This directory tracks **architecture gaps** - observations and learnings that may warrant future ADRs but aren't yet ready for formal treatment.

## Role in Architecture Evolution

| Lens | This Directory |
|------|----------------|
| **Characteristics** (Proactive) | "What we need" - see ADRs |
| **Gaps** (Reactive) | **You are here** - "What we learned" |
| **Deviations** (Pragmatic) | "What we accepted" - see [deviations/](../deviations/) |

A gap may:
- Explain why a **Characteristic** is partially implemented
- Evolve into an **ADR** when a solution is designed
- Become a **Deviation** if we accept the limitation

## Purpose

Gaps capture:
- Problems discovered during incidents, scale tests, or code reviews
- Comparisons with state-of-the-art systems
- Potential solutions worth investigating
- Impact assessments to prioritize work

## When to Create a Gap

Create a gap document when you observe:
- A design limitation exposed by production behavior
- A pattern used by other systems that we're missing
- Technical debt that affects reliability or performance
- A recurring problem that needs architectural attention

**Don't create a gap for**: bugs (use issues), feature requests (use roadmap), or decisions already made (use ADRs).

## Gap Lifecycle

```
Discovered → Open → Investigating → ADR-Drafted → Closed
                         ↓
                   (Won't Fix) → Closed
```

| Status | Meaning |
|--------|---------|
| **Open** | Problem identified, not yet investigated |
| **Investigating** | Actively researching solutions |
| **ADR-Drafted** | Solution chosen, ADR written |
| **Closed** | Resolved (via ADR) or Won't Fix |

## Template

```markdown
# GAP-XXX: [Title]

**Status**: Open | Investigating | ADR-Drafted | Closed
**Discovered**: YYYY-MM-DD
**Context**: [Incident/Scale test/Review that surfaced this]

## Problem

[1-2 paragraphs describing the issue]

## Evidence

[Metrics, logs, commands, observations that demonstrate the problem]

## State of the Art


## Potential Solutions

- **Option A**: [Description]
- **Option B**: [Description]
- **Option C**: [Description]

## Signal Impact

Which signals are affected (metrics, traces, logs)? Does the gap affect all three or just one?

## Impact

- **Severity**: Low | Medium | High | Critical
- **Frequency**: Rare | Occasional | Common
- **Affected Areas**: [Components]

## Next Steps

- [ ] Action item 1
- [ ] Action item 2

## References

- [Related ADR](../NNN-title.md)
- [External link](https://...)
```

## Naming Convention

Gap files use sequential numbering: `001-short-description.md`

## Index

| Gap | Title | Status | Severity |
|-----|-------|--------|----------|
| [001](./001-no-parquet-compaction.md) | No Parquet Split Compaction | Open | High |
| [002](./002-fixed-sort-schema.md) | Fixed Hardcoded Sort Schema | Open | Medium |
| [003](./003-no-time-window-partitioning.md) | No Time-Window Partitioning at Ingestion | Open | High |
| [004](./004-incomplete-split-metadata.md) | Incomplete Split Metadata for Compaction | Open | High |
| [005](./005-no-per-point-deduplication.md) | No Per-Point Deduplication | Open | Medium |
| [006](./006-no-independent-auto-scaling.md) | No Independent Auto-Scaling | Open | High |
| [007](./007-no-multi-level-caching.md) | No Parquet Metadata Caching | Open | High |
| [008](./008-no-high-query-rate-optimization.md) | No High Query Rate Optimization | Open | High |
| [009](./009-no-leading-edge-prioritization.md) | No Leading Edge Prioritization | Open | High |
| [010](./010-no-data-caching-or-query-affinity.md) | No Multi-Level Data Caching or Query Affinity Optimization | Open | High |
