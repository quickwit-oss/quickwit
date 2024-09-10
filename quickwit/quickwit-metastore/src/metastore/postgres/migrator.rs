// Copyright (C) 2024 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::collections::BTreeMap;

use quickwit_proto::metastore::{MetastoreError, MetastoreResult};
use sqlx::migrate::{Migrate, Migrator};
use sqlx::{Acquire, PgConnection, Postgres};
use tracing::{error, instrument};

use super::pool::TrackedPool;

fn get_migrations() -> Migrator {
    sqlx::migrate!("migrations/postgresql")
}

/// Initializes the database and runs the SQL migrations stored in the
/// `quickwit-metastore/migrations` directory.
#[instrument(skip_all)]
pub(super) async fn run_migrations(
    pool: &TrackedPool<Postgres>,
    skip_migrations: bool,
    skip_locking: bool,
) -> MetastoreResult<()> {
    let mut tx = pool.begin().await?;
    let conn = tx.acquire().await?;

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
    use sqlx::Acquire;

    use super::{get_migrations, run_migrations};
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
}
