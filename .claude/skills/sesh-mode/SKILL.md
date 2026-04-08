---
description: "Verification-first development workflow with TLA+ specs, DST, and formal methods. Use when you want Claude to follow the rigorous plan→spec→test→implement sequence."
user-invocable: true
---

# Sesh Mode — Verification-First Development

Activate this mode when working on features that touch state machines, protocols, or critical data paths. This adds formal verification requirements on top of the base CLAUDE.md.

## Before Writing Any Code

**MUST** follow this sequence before implementation:

1. **Define the plan**: What are you doing and why? What invariants must hold?
2. **Check ADR/roadmap**: `docs/internals/adr/README.md` → find relevant supplement
3. **Read the spec**: If touching state machines or protocols, read `docs/internals/specs/tla/*.tla`
4. **Write tests first**: DST tests define correctness, write them before code
5. **Only then**: Start implementation

## Three Engineering Pillars

Every code change **MUST** respect all three:

| Pillar | Location | Purpose |
|--------|----------|---------|
| **Code Quality** | [CODE_STYLE.md](../../../CODE_STYLE.md) + CLAUDE.md | Coding standards & reliability |
| **Formal Specs** | `docs/internals/specs/tla/`, `stateright_*.rs` | Protocol correctness |
| **DST** | DST crate (when created) | Fault tolerance |

**Priority**: Safety > Performance > Developer Experience

## The Verification Pyramid

All verification layers share the same invariants:

```
         TLA+ Specs (docs/internals/specs/tla/*.tla)
                    │ mirrors
         Shared Invariants (invariants/)  ← SINGLE SOURCE
                    │ used by
    ┌───────────────┼───────────────┐
    ▼               ▼               ▼
Stateright      DST Tests      Production Metrics
(exhaustive)    (simulation)   (Observability)
```

## Testing Through Production Path

**MUST NOT** claim a feature works unless tested through the actual network stack.

```bash
# 1. Start quickwit
cargo run -p quickwit-cli -- run --config ../config/quickwit.yaml

# 2. Ingest via OTLP
# (send logs/traces to localhost:4317)

# 3. Query via REST API
curl http://localhost:7280/api/v1/<index>/search -d '{"query": "*"}'
```

**Bypasses to AVOID**: Testing indexing pipeline without the HTTP/gRPC server, testing search without the REST API layer.

## DST (Deterministic Simulation Testing)

- DST tests define correctness for stateful components
- Write DST tests before implementation for new state machines
- Shared invariants are the single source of truth across all verification layers

## Architecture Evolution

Quickwit tracks architectural change through three lenses. See `docs/internals/adr/EVOLUTION.md` for the full process.

```
                    Architecture Evolution
                            │
       ┌────────────────────┼────────────────────┐
       ▼                    ▼                    ▼
 Characteristics          Gaps              Deviations
  (Proactive)          (Reactive)          (Pragmatic)
 "What we need"      "What we learned"   "What we accepted"
```

| Lens | Location | When to Use |
|------|----------|-------------|
| **Characteristics** | `docs/internals/adr/` | Track cloud-native requirements |
| **Gaps** | `docs/internals/adr/gaps/` | Design limitation from incident/production |
| **Deviations** | `docs/internals/adr/deviations/` | Intentional divergence from ADR intent |

**Before implementing, check for**:
- Open gaps (design limitations to be aware of)
- Deviations (intentional divergence from ADRs)
- Characteristic status (what's implemented vs planned)

## Additional Commit Checklist (on top of CLAUDE.md MUST items)

These are expected unless justified:
- [ ] Functions under 70 lines
- [ ] Explanatory variables for complex expressions
- [ ] Documentation explains "why"
- [ ] ADR/roadmap updated if applicable
- [ ] DST test for new state transitions
- [ ] Integration test for new API endpoints
- [ ] Tests through production path (HTTP/gRPC)

## Reference Documentation

| Topic | Location |
|-------|----------|
| Verification & DST | [docs/internals/VERIFICATION.md](../../../docs/internals/VERIFICATION.md) |
| Verification philosophy | [docs/internals/VERIFICATION_STACK.md](../../../docs/internals/VERIFICATION_STACK.md) |
| Simulation workflow | [docs/internals/SIMULATION_FIRST_WORKFLOW.md](../../../docs/internals/SIMULATION_FIRST_WORKFLOW.md) |
| Benchmarking | [docs/internals/BENCHMARKING.md](../../../docs/internals/BENCHMARKING.md) |
| Rust style patterns | [docs/internals/RUST_STYLE.md](../../../docs/internals/RUST_STYLE.md) |
| ADR index | [docs/internals/adr/README.md](../../../docs/internals/adr/README.md) |
| Architecture evolution | [docs/internals/adr/EVOLUTION.md](../../../docs/internals/adr/EVOLUTION.md) |
| Compaction architecture | [docs/internals/compaction-architecture.md](../../../docs/internals/compaction-architecture.md) |
| Tantivy + Parquet design | [docs/internals/tantivy-parquet-architecture.md](../../../docs/internals/tantivy-parquet-architecture.md) |
| Locality compaction | [docs/internals/locality-compaction/](../../../docs/internals/locality-compaction/) |
