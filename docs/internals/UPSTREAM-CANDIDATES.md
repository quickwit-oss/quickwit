# Upstream Contribution Candidates

Items in pomsky that could be contributed back to quickwit-oss/quickwit in separate PRs.

## Testing Infrastructure

- **Metrics E2E test framework**: Makefile targets (`docker-metrics-up`, `test-metrics-e2e`) for running Parquet metrics pipeline end-to-end tests against Docker Compose (Minio + Postgres). Currently referenced in pomsky's CLAUDE.md but the targets don't exist yet — needs implementation.
- **Parquet integration tests**: `quickwit-indexing/src/actors/parquet_e2e_test.rs` exercises the full write path. Could be generalized into a proper integration test suite.

## Documentation

- **ADR-004 (Cloud-Native Storage Characteristics)**: Contains analysis of cloud-native storage patterns relevant to observability engines. Currently pomsky-only (`docs/internals/adr/004-cloud-native-storage-characteristics.md`) because it references internal systems. Could be rewritten to focus on public information and contributed.

## Aspirational Items in Upstream Docs

These are described in contributed documentation but not yet implemented in code. They represent the target architecture.

- **`/sesh-mode` skill**: The `.claude/skills/sesh-mode/SKILL.md` describes a verification-first development workflow. The skill file is contributed but the workflow (TLA+ specs, DST tests before implementation) is a target, not current practice.
- **TLA+ specs**: `docs/internals/specs/tla/` directory structure is contributed but no `.tla` spec files exist yet. The ADRs and verification docs reference TLA+ model checking as a goal.
- **DST (Deterministic Simulation Testing)**: The `quickwit-dst` crate has shared invariants and Stateright model stubs, but full simulation testing infrastructure (SimClock, FaultInjector, simulated storage/network) is not yet implemented.
- **Kani proofs**: Referenced in VERIFICATION_STACK.md as a verification layer but no Kani proof harnesses exist in the codebase.
- **Bloodhound integration**: Referenced in verification docs but not implemented.
- **Performance baselines framework**: BENCHMARKING.md describes a `PerformanceBaseline` struct and observability metrics pattern that doesn't exist in code yet.
- **`quickwit-metrics-engine` benchmarks**: BENCHMARKING.md references benchmark binaries (`ingestion_profile_bench`, `high_cardinality_bench`, etc.) that don't exist yet.

## Build & Deployment

- **`http-body` duplicate dependency fix**: Fixed in pomsky's staging branch (`quickwit-serve` had both `http-body = { workspace = true }` and `http-body = "0.4"` in dev-dependencies). Should be contributed upstream if not already fixed.

## Last Updated

2026-04-08
