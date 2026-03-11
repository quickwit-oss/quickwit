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
use sqlx::mysql::MySqlDatabaseError;
use tracing::error;

// https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html
mod mysql_error_codes {
    pub const DUPLICATE_ENTRY: u16 = 1062;
    pub const FOREIGN_KEY_VIOLATION: u16 = 1452;
}

pub(super) fn convert_sqlx_err(index_id: &str, sqlx_error: sqlx::Error) -> MetastoreError {
    match &sqlx_error {
        sqlx::Error::Database(boxed_db_error) => {
            let Some(mysql_db_error) = boxed_db_error.try_downcast_ref::<MySqlDatabaseError>()
            else {
                error!(error=?boxed_db_error, "mysql-error");
                return MetastoreError::Db {
                    message: boxed_db_error.to_string(),
                };
            };
            let mysql_error_code = mysql_db_error.number();

            match mysql_error_code {
                mysql_error_codes::FOREIGN_KEY_VIOLATION => {
                    MetastoreError::NotFound(EntityKind::Index {
                        index_id: index_id.to_string(),
                    })
                }
                mysql_error_codes::DUPLICATE_ENTRY => {
                    let message = boxed_db_error.message();
                    if message.contains("indexes_index_id_unique")
                        || message.contains("PRIMARY") && message.contains("indexes")
                    {
                        MetastoreError::AlreadyExists(EntityKind::Index {
                            index_id: index_id.to_string(),
                        })
                    } else {
                        error!(error=?boxed_db_error, "mysql-error");
                        MetastoreError::Internal {
                            message: "unique key violation".to_string(),
                            cause: format!("DB error {boxed_db_error:?}"),
                        }
                    }
                }
                _ => {
                    error!(error=?boxed_db_error, "mysql-error");
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
