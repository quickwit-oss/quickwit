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

use quickwit_common::get_bool_from_env;
use quickwit_proto::metastore::{MetastoreError, MetastoreResult};
use sqlx::migrate::{Migrate, Migrator};
use sqlx::{Acquire, Postgres};
use tracing::{error, instrument};

use super::pool::TrackedPool;
use super::{QW_POSTGRES_SKIP_MIGRATIONS_ENV_KEY, QW_POSTGRES_SKIP_MIGRATION_LOCKING_ENV_KEY};

/// Initializes the database and runs the SQL migrations stored in the
/// `quickwit-metastore/migrations` directory.
#[instrument(skip_all)]
pub(super) async fn run_migrations(pool: &TrackedPool<Postgres>) -> MetastoreResult<()> {
    let mut tx = pool.begin().await?;
    let conn = tx.acquire().await?;

    let skip_migrations = get_bool_from_env(QW_POSTGRES_SKIP_MIGRATIONS_ENV_KEY, false);
    let skip_locking = get_bool_from_env(QW_POSTGRES_SKIP_MIGRATION_LOCKING_ENV_KEY, false);

    let mut migrator: Migrator = sqlx::migrate!("migrations/postgresql");

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
}
