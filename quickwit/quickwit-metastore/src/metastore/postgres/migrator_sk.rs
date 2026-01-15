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
use tracing::{error, instrument};

use super::pool::TrackedPool;

fn get_migrations() -> Migrator {
    sqlx::migrate!("migrations_sk/postgresql")
}

/// Configure the search path to a separate schema
///
/// This is necessary to create a separate migration table for schema changes
/// belonging to the fork.
async fn set_schema_to_fork_migrations_temporarily(conn: &mut PgConnection) -> MetastoreResult<()> {
    sqlx::query("SET LOCAL search_path TO sk_fork_migrations")
        .execute(conn)
        .await?;
    Ok(())
}

/// Runs migrations in parallel to the upstream migrations
///
/// We should restrain to simple schema changes that do not interfere with
/// upstream migrations (e.g., adding columns).
#[instrument(skip_all)]
pub(super) async fn run_fork_migrations(
    pool: &TrackedPool<Postgres>,
    skip_migrations: bool,
    skip_locking: bool,
) -> MetastoreResult<()> {
    let mut tx = pool.begin().await?;
    let conn = tx.acquire().await?;

    if !skip_migrations {
        sqlx::query("CREATE SCHEMA IF NOT EXISTS sk_fork_migrations")
            .execute(&mut *conn)
            .await?;
    }

    set_schema_to_fork_migrations_temporarily(&mut *conn).await?;

    let mut migrator = get_migrations();

    if skip_locking {
        migrator.set_locking(false);
    }

    if !skip_migrations {
        // this is an hidden function, made to get "around the annoying "implementation of `Acquire`
        // is not general enough" error", which is the error we get otherwise.
        let migrate_result = migrator.run_direct(conn).await;

        let Err(migrate_error) = migrate_result else {
            tx.commit().await?;
            return Ok(());
        };
        tx.rollback().await?;
        error!(error=%migrate_error, "failed to run PostgreSQL migrations");

        Err(MetastoreError::Internal {
            message: "failed to run PostgreSQL migrations".to_string(),
            cause: migrate_error.to_string(),
        })
    } else {
        check_migrations(migrator, conn).await
    }
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
    let expected_migrations: BTreeMap<_, _> = migrator
        .iter()
        .filter(|migration| migration.migration_type.is_up_migration())
        .map(|migration| (migration.version, migration))
        .collect();
    if applied_migrations.len() < expected_migrations.len() {
        error!(
            "missing migrations, expected {} migrations, only {} present in database",
            expected_migrations.len(),
            applied_migrations.len()
        );

        return Err(MetastoreError::Internal {
            message: "failed to validate PostgreSQL migrations".to_string(),
            cause: format!(
                "missing migrations, expected {} migrations, only {} present in database",
                expected_migrations.len(),
                applied_migrations.len()
            ),
        });
    }
    for applied_migration in applied_migrations {
        let Some(migration) = expected_migrations.get(&applied_migration.version) else {
            error!(
                "found unknown migration {} in database",
                applied_migration.version
            );

            return Err(MetastoreError::Internal {
                message: "failed to validate PostgreSQL migrations".to_string(),
                cause: format!(
                    "found unknown migration {} in database",
                    applied_migration.version
                ),
            });
        };
        if migration.checksum != applied_migration.checksum {
            error!(
                "migration {} differ between database and expected value",
                applied_migration.version
            );

            return Err(MetastoreError::Internal {
                message: "failed to validate PostgreSQL migrations".to_string(),
                cause: format!(
                    "migration {} differ between database and expected value",
                    applied_migration.version
                ),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use quickwit_common::uri::Uri;
    use sqlx::migrate::Migrate;
    use sqlx::{Acquire, Postgres};

    use super::{get_migrations, run_fork_migrations};
    use crate::metastore::postgres::migrator::run_migrations;
    use crate::metastore::postgres::migrator_sk::set_schema_to_fork_migrations_temporarily;
    use crate::metastore::postgres::pool::TrackedPool;
    use crate::metastore::postgres::utils::establish_connection;

    async fn get_search_path(pool: &TrackedPool<Postgres>) -> String {
        let mut conn = pool.acquire().await.unwrap();
        sqlx::query_scalar("SHOW search_path")
            .fetch_one(&mut *conn)
            .await
            .unwrap()
    }

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
                establish_connection(&uri, 1, 1, Duration::from_secs(2), None, None, false)
                    .await
                    .unwrap();

            let original_search_path = get_search_path(&connection_pool).await;

            // make sure upstream migrations are run
            run_migrations(&connection_pool, false, false)
                .await
                .unwrap();

            run_fork_migrations(&connection_pool, false, false)
                .await
                .unwrap();

            // we just ran migration, nothing else to run
            run_fork_migrations(&connection_pool, true, false)
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

            // verify that the search path modification doesn't leak
            let current_search_path = get_search_path(&connection_pool).await;
            assert_eq!(
                original_search_path, current_search_path,
                "search_path should not be modified after migrations"
            );

            // check that a read only migration that wasn't already applied fails
            {
                let mut tx = connection_pool.begin().await.unwrap();
                let conn = tx.acquire().await.unwrap();
                set_schema_to_fork_migrations_temporarily(&mut *conn)
                    .await
                    .unwrap();
                conn.revert(down_migration).await.unwrap();
                tx.commit().await.unwrap();
            }

            run_fork_migrations(&connection_pool, true, false)
                .await
                .unwrap_err();

            {
                let mut tx = connection_pool.begin().await.unwrap();
                let conn = tx.acquire().await.unwrap();
                set_schema_to_fork_migrations_temporarily(&mut *conn)
                    .await
                    .unwrap();
                conn.apply(up_migration).await.unwrap();
                tx.commit().await.unwrap();
            }
        }

        {
            let connection_pool =
                establish_connection(&uri, 1, 5, Duration::from_secs(2), None, None, true)
                    .await
                    .unwrap();
            // error because we are in read only mode, and we try to run migrations
            run_fork_migrations(&connection_pool, false, false)
                .await
                .unwrap_err();
            // okay because all migrations were already run before
            run_fork_migrations(&connection_pool, true, false)
                .await
                .unwrap();
        }
    }
}
