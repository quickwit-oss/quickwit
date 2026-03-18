# Phase 23: Docker Compose Setup - Context

**Gathered:** 2026-01-20
**Status:** Ready for planning

<vision>
## How This Should Work

A Docker Compose environment that provides production-like fidelity for local testing. The key is real network paths — S3 calls should go through actual HTTP to Minio, Postgres connections over real TCP. No mocking the storage layer.

When you run the compose environment, you get the same infrastructure topology as production: Minio for S3-compatible storage, Postgres for the metastore. The metrics pipeline talks to these services the same way it would in a real deployment.

</vision>

<essential>
## What Must Be Nailed

- **Infrastructure containers actually work** — Minio and Postgres start reliably and accept connections
- **Metrics pipeline connects to them** — The full pipeline (ingest → WAL → Parquet → metastore → query) can talk to compose services over real network paths

</essential>

<specifics>
## Specific Ideas

No specific requirements — open to standard Docker Compose patterns.

</specifics>

<notes>
## Additional Context

This is the foundation for v0.4 Local Testing milestone. Phase 24 will build the test harness on top of this, and Phase 25 will implement the actual E2E tests. The compose environment needs to be solid before those phases can proceed.

</notes>

---

*Phase: 23-docker-compose-setup*
*Context gathered: 2026-01-20*
