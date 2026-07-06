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
                    message: format!("postgresql metastore is read-only: {boxed_db_error}"),
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

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::error::Error as StdError;
    use std::fmt;

    use sqlx::error::{DatabaseError, ErrorKind};

    use super::*;

    #[derive(Debug)]
    struct TestDatabaseError {
        message: &'static str,
        code: Option<&'static str>,
        table: Option<&'static str>,
    }

    impl fmt::Display for TestDatabaseError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str(self.message)
        }
    }

    impl StdError for TestDatabaseError {}

    impl DatabaseError for TestDatabaseError {
        fn message(&self) -> &str {
            self.message
        }

        fn code(&self) -> Option<Cow<'_, str>> {
            self.code.map(Cow::Borrowed)
        }

        fn as_error(&self) -> &(dyn StdError + Send + Sync + 'static) {
            self
        }

        fn as_error_mut(&mut self) -> &mut (dyn StdError + Send + Sync + 'static) {
            self
        }

        fn into_error(self: Box<Self>) -> Box<dyn StdError + Send + Sync + 'static> {
            self
        }

        fn table(&self) -> Option<&str> {
            self.table
        }

        fn kind(&self) -> ErrorKind {
            ErrorKind::Other
        }
    }

    #[test]
    fn test_convert_sqlx_err() {
        let convert_test_database_err =
            |code: Option<&'static str>, table: Option<&'static str>| {
                convert_sqlx_err(
                    "test-index",
                    sqlx::Error::database(TestDatabaseError {
                        message: "database error",
                        code,
                        table,
                    }),
                )
            };

        let error = convert_test_database_err(Some(pg_error_codes::FOREIGN_KEY_VIOLATION), None);
        assert!(matches!(error, MetastoreError::NotFound(_)));

        let error =
            convert_test_database_err(Some(pg_error_codes::UNIQUE_VIOLATION), Some("indexes"));
        assert!(matches!(error, MetastoreError::AlreadyExists(_)));

        let error =
            convert_test_database_err(Some(pg_error_codes::UNIQUE_VIOLATION), Some("sources"));
        assert!(matches!(error, MetastoreError::Internal { .. }));

        let error =
            convert_test_database_err(Some(pg_error_codes::READ_ONLY_SQL_TRANSACTION), None);
        assert!(matches!(error, MetastoreError::Forbidden { .. }));

        let error = convert_test_database_err(None, None);
        assert!(matches!(error, MetastoreError::Db { .. }));
    }
}
