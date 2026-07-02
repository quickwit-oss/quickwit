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

use quickwit_common::get_bool_from_env;
use quickwit_proto::metastore::{MetastoreError, MetastoreResult};
use sqlx::migrate::{Migrate, Migrator};
use sqlx::{Acquire, PgConnection, Postgres};
use tracing::{error, info};

use super::metrics::DEFERRED_MIGRATIONS_APPLY_ERRORS;
use super::pool::TrackedPool;
use super::{
    QW_POSTGRES_SKIP_DEFERRED_MIGRATIONS_ENV_KEY, QW_POSTGRES_SKIP_MIGRATION_LOCKING_ENV_KEY,
    QW_POSTGRES_SKIP_MIGRATIONS_ENV_KEY,
};

// The deferred migrations should be attempted by only one metastore pod. We do that by having
// metastore pods try to acquire a lock to run the deferred migrations. Pods that don't get the lock
// ignore deferred migrations. The locks are implemented using Postgres advisory locks.
//
// Advisory locks are Postgres locks on custom defined resources, represented by a key of two ints.
// These two ints are arbitrary magic (but stable) numbers that make up the key for the deferred
// migrations lock.
const DEFERRED_MIGRATIONS_LOCK_KEY_1: i32 = 424242;
const DEFERRED_MIGRATIONS_LOCK_KEY_2: i32 = 1;

fn get_migrations() -> Migrator {
    sqlx::migrate!("migrations/postgresql")
}

fn get_deferred_migrations() -> Migrator {
    sqlx::migrate!("migrations/postgresql_deferred")
}

enum DeferredOutcome {
    LockNotAcquired,
    Applied,
    Failed,
}

pub(super) struct Migrations {
    connection_pool: TrackedPool<Postgres>,
    skip_migrations: bool,
    skip_locking: bool,
    skip_deferred: bool,
}

impl Migrations {
    pub(super) fn new(connection_pool: TrackedPool<Postgres>) -> Self {
        Migrations {
            connection_pool,
            skip_migrations: get_bool_from_env(QW_POSTGRES_SKIP_MIGRATIONS_ENV_KEY, false),
            skip_locking: get_bool_from_env(QW_POSTGRES_SKIP_MIGRATION_LOCKING_ENV_KEY, false),
            skip_deferred: get_bool_from_env(QW_POSTGRES_SKIP_DEFERRED_MIGRATIONS_ENV_KEY, false),
        }
    }

    /// Runs the required migrations, and tries to also run the deferred migrations.
    pub(super) async fn run(&self) -> MetastoreResult<()> {
        self.run_required(get_migrations()).await?;

        if !self.skip_migrations && !self.skip_locking && !self.skip_deferred {
            self.spawn_deferred();
        }
        Ok(())
    }

    /// Required migrations are lightweight and critical to the system, and must be applied.
    async fn run_required(&self, mut migrator: Migrator) -> MetastoreResult<()> {
        let mut conn = self.connection_pool.acquire().await?;

        if self.skip_locking {
            migrator.set_locking(false);
        }
        if self.skip_migrations {
            return check_migrations(&migrator, &mut conn).await;
        }
        do_migrations(migrator, &mut conn).await
    }

    /// Spawns the task to apply deferred migrations. A failure in applying these migrations is not
    /// fatal.
    fn spawn_deferred(&self) {
        let connection_pool = self.connection_pool.clone();
        quickwit_common::spawn_named_task(
            async move {
                match Self::run_deferred(&connection_pool, get_deferred_migrations()).await {
                    DeferredOutcome::Applied => {
                        info!("deferred PostgreSQL migrations applied successfully")
                    }
                    DeferredOutcome::LockNotAcquired => {
                        info!("deferred PostgreSQL migrations handled by another node")
                    }
                    DeferredOutcome::Failed => {}
                }
            },
            "postgres_deferred_migrations",
        );
    }

    /// Apply the deferred migrations. We try to get the lock; if we do, we're the migration leader
    /// and attempt to apply it. If we didn't, that means another pod did, and we can go about our
    /// day.
    async fn run_deferred(pool: &TrackedPool<Postgres>, mut migrator: Migrator) -> DeferredOutcome {
        let mut conn: PgConnection = match pool.acquire().await {
            Ok(connection) => connection.detach(),
            Err(error) => {
                error!(%error, "failed to acquire connection for deferred PostgreSQL migrations");
                return DeferredOutcome::Failed;
            }
        };

        // this query returns either true or false: we got the lock, or we didn't. It's dropped when
        // the connection is dropped.
        let acquired: bool = match sqlx::query_scalar("SELECT pg_try_advisory_lock($1, $2)")
            .bind(DEFERRED_MIGRATIONS_LOCK_KEY_1)
            .bind(DEFERRED_MIGRATIONS_LOCK_KEY_2)
            .fetch_one(&mut conn)
            .await
        {
            Ok(acquired) => acquired,
            Err(error) => {
                error!(%error, "failed to query deferred-migration advisory lock");
                return DeferredOutcome::Failed;
            }
        };
        if !acquired {
            return DeferredOutcome::LockNotAcquired;
        }

        // we've taken out our own lock; we don't need sqlx to also get one
        migrator.set_locking(false);
        match do_migrations(migrator, &mut conn).await {
            Ok(()) => DeferredOutcome::Applied,
            Err(error) => {
                error!(%error, "failed to apply deferred migrations");
                DEFERRED_MIGRATIONS_APPLY_ERRORS.inc();
                DeferredOutcome::Failed
            }
        }
    }
}

fn internal_error(message: &'static str, cause: impl std::fmt::Display) -> MetastoreError {
    MetastoreError::Internal {
        message: message.to_string(),
        cause: cause.to_string(),
    }
}

/// Runs a migrator's pending up-migrations on `conn`.
/// `ignore_missing` is how we are able to ensure forwards/backwards compatibility - we don't error
/// in the case a migration in the db is not present in our migration set.
async fn do_migrations(mut migrator: Migrator, conn: &mut PgConnection) -> MetastoreResult<()> {
    migrator.set_ignore_missing(true);
    // run_direct takes a Migrate connection directly, sidestepping run()'s Acquire bound that our
    // pooled connection type doesn't satisfy.
    migrator.run_direct(conn).await.map_err(|migrate_error| {
        error!(error=%migrate_error, "failed to run PostgreSQL migrations");
        internal_error("failed to run PostgreSQL migrations", migrate_error)
    })
}

async fn check_migrations(migrator: &Migrator, conn: &mut PgConnection) -> MetastoreResult<()> {
    let dirty = conn.dirty_version().await.map_err(|migrate_error| {
        error!(error=%migrate_error, "failed to validate PostgreSQL migrations");
        internal_error("failed to validate PostgreSQL migrations", migrate_error)
    })?;
    if let Some(dirty) = dirty {
        error!("migration {dirty} is dirty");
        return Err(internal_error(
            "failed to validate PostgreSQL migrations",
            format!("migration {dirty} is dirty"),
        ));
    }
    let applied_migrations = conn
        .list_applied_migrations()
        .await
        .map_err(|migrate_error| {
            error!(error=%migrate_error, "failed to validate PostgreSQL migrations");
            internal_error("failed to validate PostgreSQL migrations", migrate_error)
        })?;
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
            return Err(internal_error(
                "failed to validate PostgreSQL migrations",
                format!("missing required migration {}", expected_migration.version),
            ));
        };
        if expected_migration.checksum != applied_migration.checksum {
            error!(
                "migration {} differs between database and expected value",
                expected_migration.version
            );
            return Err(internal_error(
                "failed to validate PostgreSQL migrations",
                format!(
                    "migration {} differs between database and expected value",
                    expected_migration.version
                ),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use quickwit_common::uri::Uri;
    use sqlx::migrate::{Migrate, Migrator};
    use sqlx::{Acquire, Postgres};

    use super::{DeferredOutcome, Migrations, TrackedPool};
    use crate::metastore::postgres::utils::establish_connection;

    fn test_uri() -> Uri {
        dotenvy::dotenv().ok();
        std::env::var("QW_TEST_DATABASE_URL")
            .expect("environment variable `QW_TEST_DATABASE_URL` should be set")
            .parse()
            .expect("environment variable `QW_TEST_DATABASE_URL` should be a valid URI")
    }

    async fn test_pool(read_only: bool) -> TrackedPool<Postgres> {
        establish_connection(
            &test_uri(),
            1,
            5,
            Duration::from_secs(2),
            None,
            None,
            read_only,
        )
        .await
        .unwrap()
    }

    fn migrations(connection_pool: &TrackedPool<Postgres>, skip_migrations: bool) -> Migrations {
        Migrations {
            connection_pool: connection_pool.clone(),
            skip_migrations,
            skip_locking: false,
            skip_deferred: false,
        }
    }

    // A single controlled migration so these tests don't depend on the production migration set.
    // Migrator::new loads the SQL into memory, so the tempdir can be dropped afterward.
    async fn test_migrator() -> Migrator {
        let migrations_dir = tempfile::tempdir().unwrap();
        std::fs::write(
            migrations_dir.path().join("90000_test.up.sql"),
            "CREATE TABLE IF NOT EXISTS qw_test_migration_marker (id INT)",
        )
        .unwrap();
        std::fs::write(
            migrations_dir.path().join("90000_test.down.sql"),
            "DROP TABLE IF EXISTS qw_test_migration_marker",
        )
        .unwrap();
        Migrator::new(migrations_dir.path()).await.unwrap()
    }

    // Removes a bookkeeping row, tolerating a fresh database where the table doesn't exist yet.
    async fn delete_migration_row(connection_pool: &TrackedPool<Postgres>, version: i64) {
        let migrations_table_exists: bool =
            sqlx::query_scalar("SELECT to_regclass('_sqlx_migrations') IS NOT NULL")
                .fetch_one(connection_pool)
                .await
                .unwrap();
        if migrations_table_exists {
            sqlx::query("DELETE FROM _sqlx_migrations WHERE version = $1")
                .bind(version)
                .execute(connection_pool)
                .await
                .unwrap();
        }
    }

    async fn reset_test_migration(connection_pool: &TrackedPool<Postgres>) {
        sqlx::query("DROP TABLE IF EXISTS qw_test_migration_marker")
            .execute(connection_pool)
            .await
            .unwrap();
        delete_migration_row(connection_pool, 90000).await;
    }

    #[tokio::test]
    #[serial_test::file_serial]
    async fn test_run_required_applies_migrations() {
        let _ = tracing_subscriber::fmt::try_init();
        let pool = test_pool(false).await;
        reset_test_migration(&pool).await;

        migrations(&pool, false)
            .run_required(test_migrator().await)
            .await
            .unwrap();

        // the migration actually ran: its marker table now exists (we dropped it above, so this
        // proves run_required recreated it)
        sqlx::query("SELECT 1 FROM qw_test_migration_marker LIMIT 1")
            .fetch_optional(&pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    #[serial_test::file_serial]
    async fn test_run_required_validates_applied_migrations() {
        let _ = tracing_subscriber::fmt::try_init();
        let pool = test_pool(false).await;
        reset_test_migration(&pool).await;
        migrations(&pool, false)
            .run_required(test_migrator().await)
            .await
            .unwrap();
        // nothing left to run; validation passes
        migrations(&pool, true)
            .run_required(test_migrator().await)
            .await
            .unwrap();
    }

    #[tokio::test]
    #[serial_test::file_serial]
    async fn test_run_required_tolerates_unknown_migration() {
        let _ = tracing_subscriber::fmt::try_init();
        let pool = test_pool(false).await;
        reset_test_migration(&pool).await;
        migrations(&pool, false)
            .run_required(test_migrator().await)
            .await
            .unwrap();

        // an unknown high-version row (a deferred migration, or a newer rolled-back image) is
        // tolerated: the required-track check only asserts required migrations are present
        sqlx::query(
            "INSERT INTO _sqlx_migrations (version, description, success, checksum, \
             execution_time) VALUES (999999, 'tolerance test row', true, '\\x00'::bytea, 0)",
        )
        .execute(&pool)
        .await
        .unwrap();
        let result = migrations(&pool, true)
            .run_required(test_migrator().await)
            .await;
        sqlx::query("DELETE FROM _sqlx_migrations WHERE version = 999999")
            .execute(&pool)
            .await
            .unwrap();
        result.unwrap();
    }

    #[tokio::test]
    #[serial_test::file_serial]
    async fn test_run_required_fails_on_missing_required_migration() {
        let _ = tracing_subscriber::fmt::try_init();
        let pool = test_pool(false).await;
        reset_test_migration(&pool).await;
        migrations(&pool, false)
            .run_required(test_migrator().await)
            .await
            .unwrap();

        // revert the only migration so validation finds it missing
        let migrator = test_migrator().await;
        let down_migration = migrator
            .iter()
            .find(|migration| migration.migration_type.is_down_migration())
            .unwrap();
        let mut conn = pool.acquire().await.unwrap();
        conn.revert(down_migration).await.unwrap();

        let result = migrations(&pool, true)
            .run_required(test_migrator().await)
            .await;
        reset_test_migration(&pool).await;
        result.unwrap_err();
    }

    #[tokio::test]
    #[serial_test::file_serial]
    async fn test_run_required_apply_fails_when_read_only() {
        let _ = tracing_subscriber::fmt::try_init();
        let pool = test_pool(true).await;
        // writing migrations fails in read-only mode
        migrations(&pool, false)
            .run_required(test_migrator().await)
            .await
            .unwrap_err();
    }

    #[tokio::test]
    #[serial_test::file_serial]
    async fn test_run_required_validates_when_read_only() {
        let _ = tracing_subscriber::fmt::try_init();
        let writable_pool = test_pool(false).await;
        reset_test_migration(&writable_pool).await;
        // apply the test migration first, using a writable connection
        migrations(&writable_pool, false)
            .run_required(test_migrator().await)
            .await
            .unwrap();
        // validation only reads, so it succeeds even when read-only
        migrations(&test_pool(true).await, true)
            .run_required(test_migrator().await)
            .await
            .unwrap();
        reset_test_migration(&writable_pool).await;
    }

    #[tokio::test]
    #[serial_test::file_serial]
    async fn test_deferred_migrations_elect_single_runner() {
        let _ = tracing_subscriber::fmt::try_init();
        let connection_pool =
            establish_connection(&test_uri(), 2, 5, Duration::from_secs(5), None, None, false)
                .await
                .unwrap();

        // in case a previous attempt left state behind
        sqlx::query("DROP TABLE IF EXISTS qw_test_deferred_marker")
            .execute(&connection_pool)
            .await
            .unwrap();
        delete_migration_row(&connection_pool, 90001).await;

        // the migration creates a marker table (to prove it ran) and sleeps (to keep the lock
        // winner busy while the other node tries to acquire the lock)
        let migrations_dir = tempfile::tempdir().unwrap();
        std::fs::write(
            migrations_dir.path().join("90001_slow_marker.up.sql"),
            "CREATE TABLE IF NOT EXISTS qw_test_deferred_marker (id INT); SELECT pg_sleep(1)",
        )
        .unwrap();

        let (result_1, result_2) = tokio::join!(
            Migrations::run_deferred(
                &connection_pool,
                Migrator::new(migrations_dir.path()).await.unwrap()
            ),
            Migrations::run_deferred(
                &connection_pool,
                Migrator::new(migrations_dir.path()).await.unwrap()
            ),
        );

        let outcomes = [result_1, result_2];
        let applied = outcomes
            .iter()
            .filter(|outcome| matches!(outcome, DeferredOutcome::Applied))
            .count();
        let not_acquired = outcomes
            .iter()
            .filter(|outcome| matches!(outcome, DeferredOutcome::LockNotAcquired))
            .count();
        assert_eq!(
            applied, 1,
            "exactly one runner should win the lock and apply"
        );
        assert_eq!(
            not_acquired, 1,
            "the other runner should not acquire the lock"
        );

        // the winning runner actually applied the migration: its marker table now exists
        sqlx::query("SELECT 1 FROM qw_test_deferred_marker LIMIT 1")
            .fetch_optional(&connection_pool)
            .await
            .unwrap();
    }
}
