# Phase 24: E2E Test Infrastructure - Context

**Gathered:** 2026-01-20
**Status:** Ready for planning

<vision>
## How This Should Work

Standard Rust integration tests using `#[test]` that spin up Docker containers (Minio + Postgres from Phase 23's compose setup) and validate the full metrics pipeline. Tests should follow the same patterns used elsewhere in Quickwit for integration testing with real backends.

The flow: test starts → containers spin up → run pipeline operations → verify results → containers tear down cleanly.

</vision>

<essential>
## What Must Be Nailed

- **Reliable container lifecycle** - Tests must cleanly start/stop Minio+Postgres, handle failures gracefully. If a test fails mid-execution, cleanup still happens. No orphaned containers.

</essential>

<specifics>
## Specific Ideas

- Follow existing Quickwit test patterns for integration testing with real backends
- Standard Rust test harness (`#[test]` attributes, cargo test)
- Leverage the Docker Compose setup from Phase 23

</specifics>

<notes>
## Additional Context

This phase builds the test infrastructure foundation. Phase 25 will implement the actual E2E test cases that exercise the full Ingest → WAL → Parquet → metastore → query pipeline.

</notes>

---

*Phase: 24-e2e-test-infrastructure*
*Context gathered: 2026-01-20*
