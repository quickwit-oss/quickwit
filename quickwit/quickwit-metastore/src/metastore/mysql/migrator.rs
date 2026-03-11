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
use sqlx::{Acquire, MySql, MySqlConnection};
use tracing::{error, instrument};

use super::pool::TrackedPool;

fn get_migrations() -> Migrator {
    sqlx::migrate!("migrations/mysql")
}

/// Initializes the database and runs the SQL migrations stored in the
/// `quickwit-metastore/migrations/mysql` directory.
#[instrument(skip_all)]
pub(super) async fn run_migrations(
    pool: &TrackedPool<MySql>,
    skip_migrations: bool,
    skip_locking: bool,
) -> MetastoreResult<()> {
    let mut migrator = get_migrations();

    if skip_locking {
        migrator.set_locking(false);
    }

    if !skip_migrations {
        // MySQL DDL statements (CREATE TABLE, ALTER TABLE, etc.) cause implicit
        // commits, which breaks transaction-wrapped migrations. We must use a plain
        // connection instead of a transaction.
        let mut conn = pool.acquire().await?;
        let migrate_result = migrator.run_direct(&mut *conn).await;

        if let Err(migrate_error) = migrate_result {
            error!(error=%migrate_error, "failed to run MySQL migrations");
            return Err(MetastoreError::Internal {
                message: "failed to run MySQL migrations".to_string(),
                cause: migrate_error.to_string(),
            });
        }
        Ok(())
    } else {
        let mut conn = pool.acquire().await?;
        check_migrations(migrator, &mut conn).await
    }
}

async fn check_migrations(migrator: Migrator, conn: &mut MySqlConnection) -> MetastoreResult<()> {
    let dirty = match conn.dirty_version().await {
        Ok(dirty) => dirty,
        Err(migrate_error) => {
            error!(error=%migrate_error, "failed to validate MySQL migrations");

            return Err(MetastoreError::Internal {
                message: "failed to validate MySQL migrations".to_string(),
                cause: migrate_error.to_string(),
            });
        }
    };
    if let Some(dirty) = dirty {
        error!("migration {dirty} is dirty");

        return Err(MetastoreError::Internal {
            message: "failed to validate MySQL migrations".to_string(),
            cause: format!("migration {dirty} is dirty"),
        });
    };
    let applied_migrations = match conn.list_applied_migrations().await {
        Ok(applied_migrations) => applied_migrations,
        Err(migrate_error) => {
            error!(error=%migrate_error, "failed to validate MySQL migrations");

            return Err(MetastoreError::Internal {
                message: "failed to validate MySQL migrations".to_string(),
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
            message: "failed to validate MySQL migrations".to_string(),
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
                message: "failed to validate MySQL migrations".to_string(),
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
                message: "failed to validate MySQL migrations".to_string(),
                cause: format!(
                    "migration {} differ between database and expected value",
                    applied_migration.version
                ),
            });
        }
    }
    Ok(())
}
