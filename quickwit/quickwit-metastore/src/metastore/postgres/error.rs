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

use quickwit_proto::metastore::{EntityKind, MetastoreError};
use sqlx::postgres::PgDatabaseError;
use tracing::error;

// https://www.postgresql.org/docs/current/errcodes-appendix.html
mod pg_error_codes {
    pub const FOREIGN_KEY_VIOLATION: &str = "23503";
    pub const UNIQUE_VIOLATION: &str = "23505";
}

pub(super) fn convert_sqlx_err(index_id: &str, sqlx_error: sqlx::Error) -> MetastoreError {
    match &sqlx_error {
        sqlx::Error::Database(boxed_db_error) => {
            let pg_db_error = boxed_db_error.downcast_ref::<PgDatabaseError>();
            let pg_error_code = pg_db_error.code();
            let pg_error_table = pg_db_error.table();

            match (pg_error_code, pg_error_table) {
                (pg_error_codes::FOREIGN_KEY_VIOLATION, _) => {
                    MetastoreError::NotFound(EntityKind::Index {
                        index_id: index_id.to_string(),
                    })
                }
                (pg_error_codes::UNIQUE_VIOLATION, Some(table)) if table.starts_with("indexes") => {
                    MetastoreError::AlreadyExists(EntityKind::Index {
                        index_id: index_id.to_string(),
                    })
                }
                (pg_error_codes::UNIQUE_VIOLATION, _) => {
                    error!(error=?boxed_db_error, "postgresql-error");
                    MetastoreError::Internal {
                        message: "unique key violation".to_string(),
                        cause: format!("DB error {boxed_db_error:?}"),
                    }
                }
                _ => {
                    error!(error=?boxed_db_error, "postgresql-error");
                    MetastoreError::Db {
                        message: boxed_db_error.to_string(),
                    }
                }
            }
        }
        _ => {
            error!(error=?sqlx_error, "an error has occurred in the database operation");
            MetastoreError::Db {
                message: sqlx_error.to_string(),
            }
        }
    }
}
