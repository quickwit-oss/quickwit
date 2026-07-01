# Deferred PostgreSQL migrations

Long, degrade-gracefully-only migrations (e.g. `CREATE INDEX CONCURRENTLY`) run in a background task after readiness. Both tracks share the `_sqlx_migrations` table.

- Version must be globally unique across both dirs (continue the single sequence).
- Must be idempotent: `-- no-transaction` first line, then `DROP INDEX CONCURRENTLY IF EXISTS ...; CREATE INDEX CONCURRENTLY IF NOT EXISTS ...`.
- Never depend on an unshipped required migration; required migrations must never depend on a deferred one.