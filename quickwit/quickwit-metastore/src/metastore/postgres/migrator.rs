// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;

use quickwit_proto::metastore::{MetastoreError, MetastoreResult};
use sqlx::migrate::{Migrate, Migrator};
use sqlx::{Acquire, PgConnection, Postgres};
use tracing::{error, info};

use super::pool::TrackedPool;

// The deferred migrations should be attempted by only one metastore pod. We do that by having
// metastore pods try to acquire a lock to run the deferred migrations. Pods that don't get the lock
// ignore deferred migrations. The locks are implemented using Postgres advisory locks.
//
// Advisory locks are Postgres locks on custom defined resources, represented by a key of two ints.
// These two ints are arbitrary magic (but stable) numbers that make up the lock for running deferred
// migrations.
const DEFERRED_MIGRATIONS_LOCK_KEY_1: i32 = 424242;
const DEFERRED_MIGRATIONS_LOCK_KEY_2: i32 = 1;

fn get_migrations() -> Migrator {
    sqlx::migrate!("migrations/postgresql")
}

fn get_deferred_migrations() -> Migrator {
    sqlx::migrate!("migrations/postgresql_deferred")
}

/// Initializes the database and runs the SQL migrations stored in the
/// `quickwit-metastore/migrations` directory.
///
/// Runs on a raw pooled connection -- not wrapped in an outer transaction.
/// sqlx's `Migrator::run_direct` handles per-migration transactionality
/// itself, honoring each migration's `no_tx` flag (set by a
/// `-- no-transaction` directive as the first line of the migration file).
/// Wrapping the run in our own transaction would defeat that for migrations
/// that must execute outside a transaction block (e.g. `CREATE INDEX
/// CONCURRENTLY`).
///
/// Atomicity is per-migration, not per-run: a failure on migration N leaves
/// migrations 1..N-1 applied and committed in `_sqlx_migrations`. The
/// operator fixes the failing migration and re-runs.
// #[instrument(skip_all)]
pub(super) async fn run_migrations(
    pool: &TrackedPool<Postgres>,
    skip_migrations: bool,
    skip_locking: bool,
) -> MetastoreResult<()> {
    let mut conn = pool.acquire().await?;
    let mut migrator = get_migrations();

    // ignore_missing will throw if any migrations applied to the DB are not present in the set of
    // migrations it's given to run. Given that we also have deferred migrations, that will never be
    // true.
    migrator.set_ignore_missing(true);

    if skip_locking {
        migrator.set_locking(false);
    }

    if skip_migrations {
        return check_migrations(migrator, &mut conn).await;
    }

    // this is a hidden function, made to get "around the annoying "implementation of `Acquire`
    // is not general enough" error", which is the error we get otherwise.
    if let Err(migrate_error) = migrator.run_direct(&mut *conn).await {
        error!(error=%migrate_error, "failed to run PostgreSQL migrations");
        return Err(MetastoreError::Internal {
            message: "failed to run PostgreSQL migrations".to_string(),
            cause: migrate_error.to_string(),
        });
    }
    Ok(())
}

async fn check_migrations(migrator: Migrator, conn: &mut PgConnection) -> MetastoreResult<()> {
    let dirty = match conn.dirty_version().await {
        Ok(dirty) => dirty,
        Err(migrate_error) => {
            error!(error=%migrate_error, "failed to validate PostgreSQL migrations");

            return Err(MetastoreError::Internal {
                message: "failed to validate PostgreSQL migrations".to_string(),
                cause: migrate_error.to_string(),
            });
        }
    };
    if let Some(dirty) = dirty {
        error!("migration {dirty} is dirty");

        return Err(MetastoreError::Internal {
            message: "failed to validate PostgreSQL migrations".to_string(),
            cause: format!("migration {dirty} is dirty"),
        });
    };
    let applied_migrations = match conn.list_applied_migrations().await {
        Ok(applied_migrations) => applied_migrations,
        Err(migrate_error) => {
            error!(error=%migrate_error, "failed to validate PostgreSQL migrations");

            return Err(MetastoreError::Internal {
                message: "failed to validate PostgreSQL migrations".to_string(),
                cause: migrate_error.to_string(),
            });
        }
    };
    let applied_by_version: BTreeMap<_, _> = applied_migrations
        .iter()
        .map(|applied_migration| (applied_migration.version, applied_migration))
        .collect();
    for expected_migration in migrator
        .iter()
        .filter(|migration| migration.migration_type.is_up_migration())
    {
        let Some(applied_migration) = applied_by_version.get(&expected_migration.version) else {
            error!("missing required migration {}", expected_migration.version);

            return Err(MetastoreError::Internal {
                message: "failed to validate PostgreSQL migrations".to_string(),
                cause: format!("missing required migration {}", expected_migration.version),
            });
        };
        if expected_migration.checksum != applied_migration.checksum {
            error!(
                "migration {} differs between database and expected value",
                expected_migration.version
            );

            return Err(MetastoreError::Internal {
                message: "failed to validate PostgreSQL migrations".to_string(),
                cause: format!(
                    "migration {} differs between database and expected value",
                    expected_migration.version
                ),
            });
        }
    }
    Ok(())
}

enum DeferredOutcome {
    LockNotAcquired,
    Applied,
}

pub(super) async fn run_deferred_migrations(
    pool: &TrackedPool<Postgres>,
    mut migrator: Migrator,
) -> MetastoreResult<DeferredOutcome> {
    let mut conn: PgConnection = pool.acquire().await?.detach();

    // this query returns either true or false: we got the lock, or we didn't.
    let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1, $2)")
        .bind(DEFERRED_MIGRATIONS_LOCK_KEY_1)
        .bind(DEFERRED_MIGRATIONS_LOCK_KEY_2)
        .fetch_one(&mut conn)
        .await
        .map_err(|error| MetastoreError::Internal {
            message: "failed to query deferred-migration advisory lock".to_string(),
            cause: error.to_string(),
        })?;
    if !acquired {
        return Ok(DeferredOutcome::LockNotAcquired);
    }
    
    // we've taken out our own lock; we don't need sqlx to also try to do so.  
    migrator.set_locking(false);
    migrator.set_ignore_missing(true);
    let run_result = migrator.run_direct(&mut conn).await;

    // dropping the detached connection closes the session, which also releases the lock
    drop(conn);

    if let Err(migrate_error) = run_result {
        error!(error=%migrate_error, "failed to run deferred PostgreSQL migrations");
        return Err(MetastoreError::Internal {
            message: "failed to run deferred PostgreSQL migrations".to_string(),
            cause: migrate_error.to_string(),
        });
    }
    Ok(DeferredOutcome::Applied)
}

/// Spawns the one-shot, detached deferred-migration attempt. Fire-and-forget:
/// self-terminates after one attempt or when the process exits.
/// TODO: Figure out what to do here on error etc.
pub(super) fn spawn_deferred_migrations(pool: TrackedPool<Postgres>) {
    quickwit_common::spawn_named_task(
        async move {
            match run_deferred_migrations(&pool, get_deferred_migrations()).await {
                Ok(DeferredOutcome::Applied) => info!("deferred PostgreSQL migrations applied"),
                Ok(DeferredOutcome::LockNotAcquired) => {
                    info!("deferred PostgreSQL migrations handled by another node")
                }
                Err(error) => error!(%error, "deferred PostgreSQL migrations failed"),
            }
        },
        "postgres_deferred_migrations",
    );
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use quickwit_common::uri::Uri;
    use sqlx::Acquire;
    use sqlx::migrate::{Migrate, Migrator};

    use super::{DeferredOutcome, get_migrations, run_deferred_migrations, run_migrations};
    use crate::metastore::postgres::utils::establish_connection;

    #[tokio::test]
    #[serial_test::file_serial]
    async fn test_metastore_check_migration() {
        let _ = tracing_subscriber::fmt::try_init();

        dotenvy::dotenv().ok();
        let uri: Uri = std::env::var("QW_TEST_DATABASE_URL")
            .expect("environment variable `QW_TEST_DATABASE_URL` should be set")
            .parse()
            .expect("environment variable `QW_TEST_DATABASE_URL` should be a valid URI");

        {
            let connection_pool =
                establish_connection(&uri, 1, 5, Duration::from_secs(2), None, None, false)
                    .await
                    .unwrap();
            // make sure migrations are run
            run_migrations(&connection_pool, false, false)
                .await
                .unwrap();

            // we just ran migration, nothing else to run
            run_migrations(&connection_pool, true, false).await.unwrap();

            // an unknown high-version row (a deferred migration, or a newer rolled-back image) is
            // tolerated: the required-track check only asserts required migrations are present
            sqlx::query(
                "INSERT INTO _sqlx_migrations (version, description, success, checksum, \
                 execution_time) VALUES (999999, 'tolerance test row', true, '\\x00'::bytea, 0)",
            )
            .execute(&connection_pool)
            .await
            .unwrap();
            run_migrations(&connection_pool, true, false).await.unwrap();
            sqlx::query("DELETE FROM _sqlx_migrations WHERE version = 999999")
                .execute(&connection_pool)
                .await
                .unwrap();

            let migrations = get_migrations();
            let last_migration = migrations
                .iter()
                .map(|migration| migration.version)
                .max()
                .expect("no migration exists?");
            let up_migration = migrations
                .iter()
                .find(|migration| {
                    migration.version == last_migration
                        && migration.migration_type.is_up_migration()
                })
                .unwrap();
            let down_migration = migrations
                .iter()
                .find(|migration| {
                    migration.version == last_migration
                        && migration.migration_type.is_down_migration()
                })
                .unwrap();
            let mut conn = connection_pool.acquire().await.unwrap();

            conn.revert(down_migration).await.unwrap();

            run_migrations(&connection_pool, true, false)
                .await
                .unwrap_err();

            conn.apply(up_migration).await.unwrap();
        }

        {
            let connection_pool =
                establish_connection(&uri, 1, 5, Duration::from_secs(2), None, None, true)
                    .await
                    .unwrap();
            // error because we are in read only mode, and we try to run migrations
            run_migrations(&connection_pool, false, false)
                .await
                .unwrap_err();
            // okay because all migrations were already run before
            run_migrations(&connection_pool, true, false).await.unwrap();
        }
    }

    #[tokio::test]
    #[serial_test::file_serial]
    async fn test_deferred_migrations_elect_single_runner() {
        let _ = tracing_subscriber::fmt::try_init();

        dotenvy::dotenv().ok();
        let uri: Uri = std::env::var("QW_TEST_DATABASE_URL")
            .expect("environment variable `QW_TEST_DATABASE_URL` should be set")
            .parse()
            .expect("environment variable `QW_TEST_DATABASE_URL` should be a valid URI");
        let connection_pool =
            establish_connection(&uri, 2, 5, Duration::from_secs(5), None, None, false)
                .await
                .unwrap();

        // in case a previous attempt ran before us
        sqlx::query("DELETE FROM _sqlx_migrations WHERE version = 90001")
            .execute(&connection_pool)
            .await
            .unwrap();

        // keeps the lock winner busy while the other node tries to acquire it
        let migrations_dir = tempfile::tempdir().unwrap();
        std::fs::write(
            migrations_dir.path().join("90001_slow_marker.up.sql"),
            "SELECT pg_sleep(1)",
        )
        .unwrap();

        let (result_1, result_2) = tokio::join!(
            run_deferred_migrations(
                &connection_pool,
                Migrator::new(migrations_dir.path()).await.unwrap()
            ),
            run_deferred_migrations(
                &connection_pool,
                Migrator::new(migrations_dir.path()).await.unwrap()
            ),
        );

        let outcomes = [
            result_1.expect("first deferred run should not error"),
            result_2.expect("second deferred run should not error"),
        ];
        let applied = outcomes
            .iter()
            .filter(|outcome| matches!(outcome, DeferredOutcome::Applied))
            .count();
        let not_acquired = outcomes
            .iter()
            .filter(|outcome| matches!(outcome, DeferredOutcome::LockNotAcquired))
            .count();
        assert_eq!(applied, 1, "exactly one runner should win the lock and apply");
        assert_eq!(not_acquired, 1, "the other runner should not acquire the lock");
    }
}
