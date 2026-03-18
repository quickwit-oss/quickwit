# ADR Supplements

This directory contains supplementary documentation that supports the main ADRs but is too detailed or dynamic to include in the ADR itself.

## Naming Convention

Supplements are named after the ADR they support:
- `NNN-adr-title-supplement-name.md`

For example:
- `001-metrics-pipeline-implementation-roadmap.md` - Detailed roadmap for ADR-001

## Agent Workflow

Agents (and humans) should use supplements to track progress without losing context.

### 1. Finding Work
- Check the **Knowledge Map** in `../README.md` to find the relevant ADR.
- Look for linked **Supplements** (Roadmaps/Reports) in that ADR or the index below.

### 2. Tracking Progress
- **Read**: Supplements use the [Template](./000-supplement-template.md) format.
- **Update**:
  - Change `- [ ]` to `- [x]` when tasks are completed.
  - Update "Last Updated" date.
  - Add notes/context to completed items (e.g., "Fixed in PR #123").
  - Update status tables/metrics if applicable.

### 3. Creating New Supplements
- Copy `000-supplement-template.md`.
- Name it `NNN-parent-adr-name-supplement-type.md`.
- Link it in the table below and in the parent ADR.

---

## Supplement Index

| File | Related ADR | Description | Status |
|------|-------------|-------------|--------|
| [000-supplement-template.md](000-supplement-template.md) | - | Template for new supplements | - |

*Supplements will be added as ADRs are created and implementation progresses.*

## When to Use Supplements

Create a supplement when:
- The ADR needs detailed implementation plans that change frequently (Roadmaps)
- Test results or metrics that get updated over time (Reports)
- Function/feature catalogs that grow incrementally
- Migration guides or compatibility matrices

The main ADR should remain stable and capture the architectural decision, while supplements can be updated as implementation progresses.
