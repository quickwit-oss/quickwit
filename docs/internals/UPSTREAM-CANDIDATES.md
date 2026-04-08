# Upstream Contribution Candidates

Items in pomsky that could be contributed back to quickwit-oss/quickwit in separate PRs.

## Testing Infrastructure

- **Metrics E2E test framework**: Makefile targets (`docker-metrics-up`, `test-metrics-e2e`) for running Parquet metrics pipeline end-to-end tests against Docker Compose (Minio + Postgres). Currently referenced in pomsky's CLAUDE.md but the targets don't exist yet — needs implementation.
- **Parquet integration tests**: `quickwit-indexing/src/actors/parquet_e2e_test.rs` exercises the full write path. Could be generalized into a proper integration test suite.

## Documentation

- **ADR-004 (Cloud-Native Storage Characteristics)**: Contains analysis of cloud-native storage patterns relevant to observability engines. Currently pomsky-only (`docs/internals/adr/004-cloud-native-storage-characteristics.md`) because it references internal systems. Could be rewritten to focus on public information and contributed.

## Build & Deployment

- **`http-body` duplicate dependency fix**: Fixed in pomsky's staging branch (`quickwit-serve` had both `http-body = { workspace = true }` and `http-body = "0.4"` in dev-dependencies). Should be contributed upstream if not already fixed.

## Last Updated

2026-04-08
