# Architecture Decision Records (ADR) Index

This directory serves as the **central knowledge base** for Quickwit architecture.

## Knowledge Map (Agent Context)

For AI agents and developers, here is how the system is organized by domain:

## Master Index

| ADR | Title | Status | Tags | Key Components |
|-----|-------|--------|------|----------------|
| [000](./000-template.md) | Template | - | `meta` | - |

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
