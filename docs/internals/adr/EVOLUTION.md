# Architecture Evolution

This document defines how Quickwit tracks architectural change through three complementary lenses.

## The Three Lenses

```
                        Architecture Evolution
                                 │
        ┌────────────────────────┼────────────────────────┐
        │                        │                        │
        ▼                        ▼                        ▼
  Characteristics              Gaps                  Deviations
   (Proactive)              (Reactive)              (Pragmatic)
        │                        │                        │
  "What we need"          "What we learned"      "What we accepted"
        │                        │                        │
  From requirements       From production         From trade-offs
```

| Lens | Question | Trigger | Outcome |
|------|----------|---------|---------|
| **Characteristics** | What capabilities must we have? | Product requirements, competitive analysis | Feature roadmap, ADR targets |
| **Gaps** | What limitations have we discovered? | Incidents, scale tests, code reviews | Potential ADRs, system improvements |
| **Deviations** | Where did we intentionally diverge? | Implementation trade-offs, PoC scope | Documented tech debt, migration plans |

## Signal Priority

Quickwit handles three observability signals: **metrics** (current priority), **traces**, and **logs**. Architectural decisions should generalize across all three, but metrics drives the initial implementation.

## Characteristics (Proactive)

**Location:** Tracked in ADRs as they are created.

**Purpose:** Track implementation status of cloud-native storage and query capabilities required for production observability at scale.

**Status Legend:**
- Implemented - Production ready
- Partial - Some aspects implemented, gaps remain
- Implicit - Achieved as side effect
- Proposed - ADR exists but not implemented
- Deviation/Not Planned - Gap or intentional omission

## Gaps (Reactive)

**Location:** [gaps/](./gaps/)

**Purpose:** Capture design limitations discovered through production behavior, incidents, or research. Lightweight pre-ADR documents that may evolve into formal ADRs.

**Lifecycle:**
```
Discovered → Open → Investigating → ADR-Drafted → Closed
                         ↓
                   (Won't Fix) → Closed
```

**When to Create:**
- Design limitation exposed by production behavior
- Pattern used by other systems that we're missing
- Recurring problem needing architectural attention

**Current Gaps:**

| Gap | Title | Status | Severity |
|-----|-------|--------|----------|
| [001](./gaps/001-no-parquet-compaction.md) | No Parquet Split Compaction | Open | High |
| [002](./gaps/002-fixed-sort-schema.md) | Fixed Hardcoded Sort Schema | Open | Medium |
| [003](./gaps/003-no-time-window-partitioning.md) | No Time-Window Partitioning at Ingestion | Open | High |
| [004](./gaps/004-incomplete-split-metadata.md) | Incomplete Split Metadata for Compaction | Open | High |
| [005](./gaps/005-no-per-point-deduplication.md) | No Per-Point Deduplication | Open | Medium |
| [006](./gaps/006-no-independent-auto-scaling.md) | No Independent Auto-Scaling | Open | High |
| [007](./gaps/007-no-multi-level-caching.md) | No Multi-Level Caching | Open | High |
| [008](./gaps/008-no-high-query-rate-optimization.md) | No High Query Rate Optimization | Open | High |
| [009](./gaps/009-no-leading-edge-prioritization.md) | No Leading Edge Prioritization | Open | High |

## Deviations (Pragmatic)

**Location:** [deviations/](./deviations/)

**Purpose:** Document where actual implementation intentionally differs from ADR intent. These are known, accepted trade-offs - not bugs.

**When to Create:**
- Implementation takes a different approach than ADR described
- PoC simplification that will need future work
- Architectural compromise due to time/resource constraints

**Current Deviations:**

| Deviation | Title | Related ADR | Priority |
|-----------|-------|-------------|----------|

*No deviations recorded yet.*

## Relationships

### Gaps → Characteristics
A gap may provide evidence for a characteristic's partial status.

### Gaps → ADRs
A gap may evolve into a formal ADR when the solution is designed.

### Gaps → Deviations
A gap may become a deviation if we decide to accept the limitation.

### Characteristics → Deviations
A characteristic marked as not planned should have a corresponding deviation explaining why.

## Decision Flow

```
Problem Discovered
       │
       ▼
  Is it a known requirement?
       │
  ┌────┴────┐
  │ Yes     │ No
  ▼         ▼
Update    Create Gap
Characteristic   │
Status           ▼
            Can we fix it?
                 │
           ┌─────┴─────┐
           │ Yes       │ No (or not now)
           ▼           ▼
      Draft ADR    Create Deviation
      (close gap)  (document trade-off)
```

## Maintenance

| Document Type | Review Cadence | Owner |
|---------------|----------------|-------|
| Characteristics | Quarterly (roadmap sync) | Product/Architecture |
| Gaps | After incidents, scale tests | Engineering |
| Deviations | Before major releases | Tech Lead |

## References

- [ADR Index](./README.md)
- [Gaps Directory](./gaps/README.md)
- [Deviations Directory](./deviations/README.md)
