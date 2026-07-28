# Deferred PostgreSQL migrations

Long, degrade-gracefully-only migrations (e.g. `CREATE INDEX CONCURRENTLY`) run in a background task after readiness. Both tracks share the `_sqlx_migrations` table.

- Version must be globally unique across both dirs (continue the single sequence).
- Must be idempotent. A statement that can't run in a transaction (e.g. `CREATE INDEX CONCURRENTLY`) needs `-- no-transaction` as the first line, then the DDL. Each statement auto-commits, so a killed migration must be safe to re-run (`CREATE INDEX CONCURRENTLY` can leave an invalid index, so drop it first). For example:

29_add_foo_index.up.sql
  ```sql
  -- no-transaction
  CREATE INDEX CONCURRENTLY IF NOT EXISTS foo_idx ON foo (bar);
  ```

29_add_foo_index.down.sql
  ```sql
  -- no-transaction
  DROP INDEX CONCURRENTLY IF EXISTS foo_idx;
  ```
- If any migrations failed, they'll need to be manually cleaned up before attempting to retry as failed index creation leaves a broken index, without adding an entry into the migrations table.
- We tried to clean up invalid indexes inside the migration SQL, but because it's two separate statements, Postgres implicitly runs them inside a transaction block, overriding `-- no-transaction`, which can't work with `concurrently`.
- Never depend on an unshipped required migration; required migrations must never depend on a deferred one.
