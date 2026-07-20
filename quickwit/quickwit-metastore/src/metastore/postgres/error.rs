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

use quickwit_proto::metastore::{EntityKind, MetastoreError};
use tracing::error;

// https://www.postgresql.org/docs/current/errcodes-appendix.html
mod pg_error_codes {
    pub const FOREIGN_KEY_VIOLATION: &str = "23503";
    pub const UNIQUE_VIOLATION: &str = "23505";
    pub const READ_ONLY_SQL_TRANSACTION: &str = "25006";
}

pub(super) fn convert_sqlx_err(index_id: &str, sqlx_error: sqlx::Error) -> MetastoreError {
    match &sqlx_error {
        sqlx::Error::Database(boxed_db_error) => {
            let pg_error_code = boxed_db_error.code();
            let pg_error_table = boxed_db_error.table();

            match (pg_error_code.as_deref(), pg_error_table) {
                (Some(pg_error_codes::FOREIGN_KEY_VIOLATION), _) => {
                    MetastoreError::NotFound(EntityKind::Index {
                        index_id: index_id.to_string(),
                    })
                }
                (Some(pg_error_codes::UNIQUE_VIOLATION), Some(table))
                    if table.starts_with("indexes") =>
                {
                    MetastoreError::AlreadyExists(EntityKind::Index {
                        index_id: index_id.to_string(),
                    })
                }
                (Some(pg_error_codes::UNIQUE_VIOLATION), _) => {
                    error!(error=?boxed_db_error, "postgresql-error");
                    MetastoreError::Internal {
                        message: "unique key violation".to_string(),
                        cause: format!("DB error {boxed_db_error:?}"),
                    }
                }
                (Some(pg_error_codes::READ_ONLY_SQL_TRANSACTION), _) => MetastoreError::Forbidden {
                    message: format!(
                        "cannot issue write request on a read-only replica: {boxed_db_error}"
                    ),
                },
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
