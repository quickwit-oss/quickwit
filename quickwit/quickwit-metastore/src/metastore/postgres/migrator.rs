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

use quickwit_proto::metastore::{MetastoreError, MetastoreResult};
use sqlx::migrate::Migrator;
use sqlx::{Pool, Postgres};
use tracing::{error, instrument};

static MIGRATOR: Migrator = sqlx::migrate!("migrations/postgresql");

/// Initializes the database and runs the SQL migrations stored in the
/// `quickwit-metastore/migrations` directory.
#[instrument(skip_all)]
pub(super) async fn run_migrations(pool: &Pool<Postgres>) -> MetastoreResult<()> {
    let tx = pool.begin().await?;
    let migrate_result = MIGRATOR.run(pool).await;

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
}
