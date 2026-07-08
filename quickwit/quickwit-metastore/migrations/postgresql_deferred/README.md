# Deferred PostgreSQL migrations

Long, degrade-gracefully-only migrations (e.g. `CREATE INDEX CONCURRENTLY`) run in a background task after readiness. Both tracks share the `_sqlx_migrations` table.

- Version must be globally unique across both dirs (continue the single sequence).
- Must be idempotent. A statement that can't run in a transaction (e.g. `CREATE INDEX CONCURRENTLY`) needs `-- no-transaction` as the first line, then the DDL. Each statement auto-commits, so a killed migration must be safe to re-run (`CREATE INDEX CONCURRENTLY` can leave an invalid index, so drop it first). For example:

29_add_foo_index.up.sql
  ```sql
  -- no-transaction
  DROP INDEX CONCURRENTLY IF EXISTS foo_idx;
  CREATE INDEX CONCURRENTLY IF NOT EXISTS foo_idx ON foo (bar);
  ```

29_add_foo_index.down.sql
  ```sql
  -- no-transaction
  DROP INDEX CONCURRENTLY IF EXISTS foo_idx;
  ```

  The `DROP` before the `CREATE` looks like it would drop the index on every run, but it does not: sqlx applies each migration at most once (tracked by version in `_sqlx_migrations`) and never re-runs one that already succeeded. The drop only matters on a retry after a partial failure. If a `CREATE INDEX CONCURRENTLY` is killed midway, Postgres leaves an *invalid* index and sqlx records nothing (the bookkeeping row is written only on success), so the next attempt re-runs the migration. A bare `CREATE INDEX CONCURRENTLY IF NOT EXISTS` would then see that invalid index and skip it forever, so we drop it first to force a clean rebuild.
- Never depend on an unshipped required migration; required migrations must never depend on a deferred one.