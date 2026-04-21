# Roadmap: Pomsky Intake -- Metric Metadata Transform

## Overview

Build a single custom `TaskTransform` in `pomsky-intake` that replaces the Go sidecar (`byoc-metrics-metadata`) and five Vector YAML nodes. The transform runs in-process after `preprocess_metric`, maintaining an in-memory known-metrics map with TTL expiry, persisting state to CSV, and flushing new metric metadata to the SaaS ingest endpoint via HTTP POST. Four phases deliver the architecture skeleton, state+persistence, HTTP submission, and full stream integration in that order.

## Milestones

- [v0.1 Metric Metadata Transform](milestones/v0.1-ROADMAP.md) (Phases 1-4) -- SHIPPED 2026-04-20

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

Decimal phases appear between their surrounding integers in numeric order.

<details>
<summary>v0.1 Metric Metadata Transform (Phases 1-4) -- SHIPPED 2026-04-20</summary>

- [x] **Phase 1: Foundation** - Architecture skeleton, type definitions, and config wiring (1/1 plan) -- completed 2026-04-17
- [x] **Phase 2: State and Persistence** - In-memory known-metrics map with TTL expiry and atomic CSV persistence (2/2 plans) -- completed 2026-04-20
- [x] **Phase 3: HTTP Submission** - Async flush client, response-driven state updates (2/2 plans) -- completed 2026-04-20
- [x] **Phase 4: Stream Integration** - Full select! loop wiring, interval/size triggers, graceful shutdown flush (2/2 plans) -- completed 2026-04-20

</details>

## Progress

**Execution Order:**
Phases execute in numeric order: 1 -> 2 -> 3 -> 4

| Phase | Milestone | Plans Complete | Status | Completed |
|-------|-----------|----------------|--------|-----------|
| 1. Foundation | v0.1 | 1/1 | Complete | 2026-04-17 |
| 2. State and Persistence | v0.1 | 2/2 | Complete | 2026-04-20 |
| 3. HTTP Submission | v0.1 | 2/2 | Complete | 2026-04-20 |
| 4. Stream Integration | v0.1 | 2/2 | Complete | 2026-04-20 |
