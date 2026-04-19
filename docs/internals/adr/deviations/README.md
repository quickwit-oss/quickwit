# Architecture Deviations

> **Part of [Architecture Evolution](../EVOLUTION.md)** - the pragmatic lens for documenting intentional divergence from ADR intent.

This directory tracks **architecture deviations** - places where actual implementation intentionally differs from what an ADR describes. These are known, accepted trade-offs, not bugs.

## Role in Architecture Evolution

| Lens | This Directory |
|------|----------------|
| **Characteristics** (Proactive) | "What we need" - see ADRs |
| **Gaps** (Reactive) | "What we learned" - see [gaps/](../gaps/) |
| **Deviations** (Pragmatic) | **You are here** - "What we accepted" |

A deviation:
- Documents why a **Characteristic** is marked as not planned
- May originate from a **Gap** we decided to accept
- Requires a migration plan for eventual resolution

## When to Create a Deviation

Create a deviation document when:
- Implementation takes a different approach than ADR described
- PoC simplification that will need future work
- Architectural compromise due to time/resource constraints
- Intentional scope reduction with documented rationale

**Don't create a deviation for**: bugs (use issues), unfinished features (use roadmap), or design limitations not yet decided (use gaps).

## Template

```markdown
# Deviation XXX: [Title]

## Summary

[1-2 sentences describing the divergence]

## Related ADR

- **ADR**: [ADR-NNN](../NNN-title.md)
- **Section**: [Which part of the ADR this deviates from]

## ADR States

> [Quote the relevant section from the ADR]

## Current Implementation

[Describe what was actually built and why]

## Signal Impact

Which signals are affected (metrics, traces, logs)? Does the deviation apply to all three or just one?

## Impact

| Aspect | ADR Target | Current Reality |
|--------|------------|-----------------|
| ... | ... | ... |

## Why This Exists

[Explain the trade-off decision]

## Priority Assessment

[When should this be resolved? Is it acceptable for PoC/MVP/Production?]

## Work Required to Match ADR

| Change | Difficulty | Description |
|--------|------------|-------------|
| ... | ... | ... |

## Recommendation

[Accept for now? Fix before X milestone?]

## References

- [Related Gap](../gaps/NNN-*.md) (if applicable)

## Date

YYYY-MM-DD
```

## Naming Convention

Deviation files use sequential numbering: `001-short-description.md`

## Index

| Deviation | Title | Related ADR | Priority |
|-----------|-------|-------------|----------|

*No deviations recorded yet.*

## Lifecycle

```
Identified → Documented → Accepted → (Eventually) Resolved
                              ↓
                        Permanent (rare)
```

Unlike gaps which may become ADRs, deviations are typically resolved by updating the implementation to match the ADR, or by updating the ADR to match reality.
