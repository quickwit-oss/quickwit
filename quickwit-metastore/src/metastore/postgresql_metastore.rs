// Copyright (C) 2022 Quickwit, Inc.
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

use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use itertools::Itertools;
use quickwit_common::uri::Uri;
use quickwit_config::SourceConfig;
use quickwit_doc_mapper::tag_pruning::TagFilterAst;
use sqlx::migrate::Migrator;
use sqlx::postgres::{PgConnectOptions, PgDatabaseError, PgPoolOptions};
use sqlx::{ConnectOptions, Pool, Postgres, Row, Transaction};
use tokio::sync::Mutex;
use tracing::log::LevelFilter;
use tracing::{debug, error, instrument, warn};

use crate::metastore::postgresql_model::{Index, IndexIdSplitIdRow};
use crate::metastore::{postgresql_model, CheckpointDelta};
use crate::{
    IndexMetadata, Metastore, MetastoreError, MetastoreFactory, MetastoreResolverError,
    MetastoreResult, Split, SplitMetadata, SplitState,
};

static MIGRATOR: Migrator = sqlx::migrate!("migrations/postgresql");

const CONNECTION_POOL_MAX_SIZE: u32 = 10;

// https://www.postgresql.org/docs/current/errcodes-appendix.html
mod pg_error_code {
    pub const FOREIGN_KEY_VIOLATION: &str = "23503";
    pub const UNIQUE_VIOLATION: &str = "23505";
}

/// Establishes a connection to the given database URI.
async fn establish_connection(connection_uri: &Uri) -> MetastoreResult<Pool<Postgres>> {
    let pool_options = PgPoolOptions::new()
        .max_connections(CONNECTION_POOL_MAX_SIZE)
        .idle_timeout(Duration::from_secs(1))
        .acquire_timeout(Duration::from_secs(2));
    let mut pg_connect_options: PgConnectOptions = connection_uri.as_str().parse()?;
    pg_connect_options.log_statements(LevelFilter::Info);
    pool_options
        .connect_with(pg_connect_options)
        .await
        .map_err(|err| {
            error!(connection_uri=%connection_uri, err=?err, "Failed to establish connection to database.");
            MetastoreError::ConnectionError {
                message: err.to_string(),
            }
        })
}

/// Initialize the database.
/// The sql used for the initialization is stored in quickwit-metastore/migrations directory.
#[instrument(skip_all)]
async fn run_postgres_migrations(pool: &Pool<Postgres>) -> MetastoreResult<()> {
    let tx = pool.begin().await?;
    let migration_res = MIGRATOR.run(pool).await;
    if let Err(migration_err) = migration_res {
        tx.rollback().await?;
        error!(err=?migration_err, "Database migrations failed");
        return Err(MetastoreError::InternalError {
            message: "Failed to run migrator on Postgresql database.".to_string(),
            cause: anyhow::anyhow!(migration_err),
        });
    }
    tx.commit().await?;
    Ok(())
}

/// PostgreSQL metastore implementation.
#[derive(Clone)]
pub struct PostgresqlMetastore {
    uri: Uri,
    connection_pool: Pool<Postgres>,
}

impl PostgresqlMetastore {
    /// Creates a meta store given a database URI.
    pub async fn new(connection_uri: Uri) -> MetastoreResult<Self> {
        let connection_pool = establish_connection(&connection_uri).await?;
        run_postgres_migrations(&connection_pool).await?;
        Ok(PostgresqlMetastore {
            uri: connection_uri,
            connection_pool,
        })
    }
}

/// Returns an Index object given an index_id or None if it does not exists.
async fn index_opt<'a>(
    tx: &mut Transaction<'a, Postgres>,
    index_id: &str,
) -> MetastoreResult<Option<Index>> {
    let index_opt: Option<Index> = sqlx::query_as::<_, Index>(
        r#"
        SELECT *
        FROM indexes
        WHERE index_id = $1
    "#,
    )
        .bind(index_id)
        .fetch_optional(tx)
        .await
        .map_err(MetastoreError::DbError)?;
    Ok(index_opt)
}

async fn index_metadata(
    tx: &mut Transaction<'_, Postgres>,
    index_id: &str,
) -> MetastoreResult<IndexMetadata> {
    index_opt(tx, index_id)
        .await?
        .ok_or_else(|| MetastoreError::IndexDoesNotExist {
            index_id: index_id.to_string(),
        })?
        .index_metadata()
}

/// Publishes mutiple splits.
/// Returns the IDs of the splits successfully published.
#[instrument(skip(tx))]
async fn mark_splits_as_published_helper(
    tx: &mut Transaction<'_, Postgres>,
    index_id: &str,
    split_ids: &[&str],
) -> MetastoreResult<Vec<String>> {
    let publishable_states = [SplitState::Staged.as_str(), SplitState::Published.as_str()];
    let published_split_ids: Vec<String> = sqlx::query(
        r#"
        UPDATE splits
        SET split_state = $1
        WHERE
                index_id = $2
            AND split_id = ANY($3)
            AND split_state = ANY($4)
        RETURNING split_id
    "#,
    )
        .bind(SplitState::Published.as_str())
        .bind(index_id)
        .bind(split_ids)
        .bind(&publishable_states[..])
        .map(|row| row.get(0))
        .fetch_all(tx)
        .await?;
    Ok(published_split_ids)
}

/// Marks mutiple splits for deletion.
/// Returns the IDs of the splits successfully marked for deletion.
#[instrument(skip(tx))]
async fn mark_splits_for_deletion(
    tx: &mut Transaction<'_, Postgres>,
    index_id: &str,
    split_ids: &[&str],
    deletable_states: &[&str],
) -> MetastoreResult<Vec<String>> {
    if split_ids.is_empty() {
        return Ok(Vec::new());
    }
    let marked_split_ids: Vec<String> = sqlx::query(
        r#"
        UPDATE splits
        SET split_state = $1
        WHERE
                index_id = $2
            AND split_id = ANY($3)
            AND split_state = ANY($4)
        RETURNING split_id
    "#,
    )
    .bind(SplitState::MarkedForDeletion.as_str())
    .bind(index_id)
    .bind(split_ids)
    .bind(deletable_states)
    .map(|row| row.get(0))
    .fetch_all(tx)
    .await?;

    Ok(marked_split_ids)
}

async fn list_splits_helper(
    tx: &mut Transaction<'_, Postgres>,
    index_id: &str,
    state_opt: Option<SplitState>,
    time_range_opt: Option<Range<i64>>,
    tags_opt: Option<TagFilterAst>,
) -> MetastoreResult<Vec<Split>> {
    let mut sql = r#"
        SELECT *
        FROM splits
        WHERE index_id = $1
    "#
        .to_string();
    if let Some(state) = state_opt {
        sql.push_str(&format!(" AND split_state = '{}'", state.as_str()));
    }
    if let Some(time_range) = time_range_opt {
        sql.push_str(&format!(
            " AND (time_range_end >= {} OR time_range_end IS NULL) ",
            time_range.start
        ));
        sql.push_str(&format!(
            " AND (time_range_start < {} OR time_range_start IS NULL) ",
            time_range.end
        ));
    }

    if let Some(tags) = tags_opt {
        sql.push_str(" AND (");
        sql.push_str(&tags_filter_expression_helper(tags));
        sql.push_str(") ");
    }

    let splits = sqlx::query_as::<_, postgresql_model::Split>(&sql)
        .bind(index_id)
        .fetch_all(&mut *tx)
        .await?;

    // If no splits was returned, maybe the index itself does not exist
    // in the first place?
    if splits.is_empty() && index_opt(&mut *tx, index_id).await?.is_none() {
        return Err(MetastoreError::IndexDoesNotExist {
            index_id: index_id.to_string(),
        });
    }

    splits.into_iter().map(|split| split.try_into()).collect()
}

/// Query the database to find out if:
/// - index exists?
/// - splits exist?
/// Returns split that are not in valid state.
async fn get_splits_with_invalid_state<'a>(
    tx: &mut Transaction<'_, Postgres>,
    index_id: &str,
    split_ids: &[&'a str],
    affected_split_ids: &[String],
) -> MetastoreResult<Vec<String>> {
    let affected_ids_set: HashSet<&str> = affected_split_ids
        .iter()
        .map(|split_id| split_id.as_str())
        .collect();
    let unaffected_ids_set: HashSet<&str> = split_ids
        .iter()
        .copied()
        .filter(|&split_id| !affected_ids_set.contains(split_id))
        .collect();

    // SQL query that helps figure out if index exist, non-existent
    // splits and not deletable splits.
    const SELECT_SPLITS_FOR_INDEX: &str = r#"
        SELECT i.index_id, s.split_id
        FROM indexes AS i
        LEFT JOIN (
            SELECT index_id, split_id
            FROM splits
            WHERE split_id = ANY ($1)
        ) AS s
        ON i.index_id = s.index_id
        WHERE i.index_id = $2"#;

    let index_split_rows: Vec<IndexIdSplitIdRow> =
        sqlx::query_as::<_, IndexIdSplitIdRow>(SELECT_SPLITS_FOR_INDEX)
            .bind(
                unaffected_ids_set
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<String>>(),
            )
            .bind(index_id)
            .fetch_all(tx)
            .await?;

    // Index does not exist if empty.
    if index_split_rows.is_empty() {
        return Err(MetastoreError::IndexDoesNotExist {
            index_id: index_id.to_string(),
        });
    }

    // None of the unaffected splits exist if we have a single row
    // with the split_id being `null`
    if index_split_rows.len() == 1 && index_split_rows[0].split_id.is_none() {
        error!("none of the unaffected split exists");
        return Err(MetastoreError::SplitsDoNotExist {
            split_ids: unaffected_ids_set
                .iter()
                .map(|split_id| split_id.to_string())
                .collect(),
        });
    }

    // The unaffected splits might be a mix of non-existant splits and splits in non valid
    // state.
    let not_in_correct_state_ids_set: HashSet<&str> = index_split_rows
        .iter()
        .flat_map(|item| item.split_id.as_deref())
        .collect();
    let not_found_ids_set: HashSet<&str> = &unaffected_ids_set - &not_in_correct_state_ids_set;

    if !not_found_ids_set.is_empty() {
        return Err(MetastoreError::SplitsDoNotExist {
            split_ids: not_found_ids_set
                .iter()
                .map(|split_id| split_id.to_string())
                .collect(),
        });
    }

    Ok(not_in_correct_state_ids_set
        .iter()
        .map(|split_id| split_id.to_string())
        .collect())
}

fn convert_sqlx_err(index_id: &str, sqlx_err: sqlx::Error) -> MetastoreError {
    match &sqlx_err {
        sqlx::Error::Database(boxed_db_err) => {
            error!(pg_db_err=?boxed_db_err, "postgresql-error");
            let pg_error_code = boxed_db_err.downcast_ref::<PgDatabaseError>().code();
            match pg_error_code {
                pg_error_code::FOREIGN_KEY_VIOLATION => MetastoreError::IndexDoesNotExist {
                    index_id: index_id.to_string(),
                },
                pg_error_code::UNIQUE_VIOLATION => MetastoreError::InternalError {
                    message: "Unique key violation.".to_string(),
                    cause: anyhow::anyhow!("DB error {:?}", boxed_db_err),
                },
                _ => MetastoreError::DbError(sqlx_err),
            }
        }
        _ => {
            error!(err=?sqlx_err, "An error has occurred in the database operation.");
            MetastoreError::DbError(sqlx_err)
        }
    }
}

/// This macro is used to systematically wrap the metastore
/// into transaction, commit them on Result::Ok and rollback on Error.
///
/// Note this is suboptimal.
/// Some of the methods actually did not require a transaction.
///
/// We still use this macro for them in order to make the code
/// "trivially correct".
macro_rules! run_with_tx {
    ($connection_pool:expr, $tx_refmut:ident, $x:block) => {{
        let mut tx: Transaction<'_, Postgres> = $connection_pool.begin().await?;
        let $tx_refmut = &mut tx;
        let op_fut = move || async move { $x };
        let op_result: MetastoreResult<_> = op_fut().await;
        if op_result.is_ok() {
            debug!("commit");
            tx.commit().await?;
        } else {
            warn!("rollback");
            tx.rollback().await?;
        }
        op_result
    }};
}

async fn mutate_index_metadata<E, M: FnOnce(&mut IndexMetadata) -> Result<(), E>>(
    tx: &mut Transaction<'_, Postgres>,
    index_id: &str,
    mutation: M,
) -> MetastoreResult<()>
    where
        MetastoreError: From<E>,
{
    let mut index_metadata = index_metadata(tx, index_id).await?;
    mutation(&mut index_metadata)?;
    let index_metadata_json =
        serde_json::to_string(&index_metadata).map_err(|err| MetastoreError::InternalError {
            message: "Failed to serialize index metadata.".to_string(),
            cause: anyhow::anyhow!(err),
        })?;
    let update_index_res = sqlx::query(
        r#"
        UPDATE indexes
        SET index_metadata_json = $1
        WHERE index_id = $2
    "#,
    )
        .bind(index_metadata_json)
        .bind(&index_id)
        .execute(tx)
        .await?;
    if update_index_res.rows_affected() == 0 {
        return Err(MetastoreError::IndexDoesNotExist {
            index_id: index_id.to_string(),
        });
    }
    Ok(())
}

#[async_trait]
impl Metastore for PostgresqlMetastore {
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        self.connection_pool.acquire().await?;
        Ok(())
    }

    async fn list_indexes_metadatas(&self) -> MetastoreResult<Vec<IndexMetadata>> {
        run_with_tx!(self.connection_pool, tx, {
            let indexes: Vec<Index> = sqlx::query_as::<_, Index>("SELECT * FROM indexes")
                .fetch_all(tx)
                .await?;
            let index_metadata: MetastoreResult<Vec<IndexMetadata>> = indexes
                .into_iter()
                .map(|index| index.index_metadata())
                .collect::<MetastoreResult<_>>();
            index_metadata
        })
    }

    #[instrument(skip(self), fields(index_id = index_metadata.index_id.as_str()))]
    async fn create_index(&self, index_metadata: IndexMetadata) -> MetastoreResult<()> {
        run_with_tx!(self.connection_pool, tx, {
            // Serialize the index metadata to fit the database model.
            let index_metadata_json = serde_json::to_string(&index_metadata).map_err(|err| {
                MetastoreError::InternalError {
                    message: "Failed to serialize index metadata.".to_string(),
                    cause: anyhow::anyhow!(err),
                }
            })?;
            // Create index.
            let create_index_statement_res =
                sqlx::query("INSERT INTO indexes (index_id, index_metadata_json) VALUES ($1, $2)")
                    .bind(&index_metadata.index_id)
                    .bind(&index_metadata_json)
                    .execute(tx)
                    .await;
            create_index_statement_res
                .map_err(|err| convert_sqlx_err(&index_metadata.index_id, err))?;
            Ok(())
        })
    }

    #[instrument(skip(self))]
    async fn delete_index(&self, index_id: &str) -> MetastoreResult<()> {
        run_with_tx!(self.connection_pool, tx, {
            let query_res = sqlx::query("DELETE FROM indexes WHERE index_id = $1")
                .bind(index_id)
                .execute(tx)
                .await?;
            if query_res.rows_affected() == 0 {
                return Err(MetastoreError::IndexDoesNotExist {
                    index_id: index_id.to_string(),
                });
            }
            Ok(())
        })
    }

    #[instrument(skip(self, metadata), fields(split_id = metadata.split_id.as_str()))]
    async fn stage_split(&self, index_id: &str, metadata: SplitMetadata) -> MetastoreResult<()> {
        run_with_tx!(self.connection_pool, tx, {
            // Fit the time_range to the database model.
            let time_range_start = metadata.time_range.clone().map(|range| *range.start());
            let time_range_end = metadata.time_range.clone().map(|range| *range.end());

            // Serialize the split metadata and footer offsets to fit the database model.
            let split_metadata_json =
                serde_json::to_string(&metadata).map_err(|err| MetastoreError::InternalError {
                    message: "Failed to serialize split metadata and footer offsets".to_string(),
                    cause: anyhow::anyhow!(err),
                })?;

            let tags: Vec<String> = metadata.tags.into_iter().collect();
            // Insert a new split metadata as `Staged` state.
            let split_id = metadata.split_id.clone();
            sqlx::query(r#"
                INSERT INTO splits
                    (split_id, split_state, time_range_start, time_range_end, tags, split_metadata_json, index_id)
                VALUES
                    ($1, $2, $3, $4, $5, $6, $7)
            "#)
            .bind(&metadata.split_id)
            .bind(&SplitState::Staged.as_str())
            .bind(time_range_start)
            .bind(time_range_end)
            .bind(tags)
            .bind(split_metadata_json)
            .bind(index_id)
            .execute(tx)
            .await
                .map_err(|err| convert_sqlx_err(index_id, err))?;

            debug!(index_id=?index_id, split_id=?split_id, "The split has been staged");
            Ok(())
        })
    }

    #[instrument(skip(self, checkpoint_delta))]
    async fn publish_splits<'a>(
        &self,
        index_id: &str,
        source_id: &str,
        split_ids: &[&'a str],
        checkpoint_delta: CheckpointDelta,
    ) -> MetastoreResult<()> {
        run_with_tx!(self.connection_pool, tx, {
            // Update the index checkpoint.
            mutate_index_metadata(tx, index_id, |index_metadata| {
                index_metadata
                    .checkpoint
                    .try_apply_delta(source_id, checkpoint_delta)
            })
            .await?;

            if split_ids.is_empty() {
                return Ok(());
            }

            let published_split_ids =
                mark_splits_as_published_helper(tx, index_id, split_ids).await?;

            if published_split_ids.len() == split_ids.len() {
                return Ok(());
            }

            error!(
                num_split_ids = split_ids.len(),
                published = published_split_ids.len(),
                "published_splits_ids_not_match"
            );

            // Investigate and report the error.
            let not_staged_ids =
                get_splits_with_invalid_state(tx, index_id, split_ids, &published_split_ids)
                    .await?;

            Err(MetastoreError::SplitsNotStaged {
                split_ids: not_staged_ids,
            })
        })
    }

    #[instrument(skip(self))]
    async fn replace_splits<'a>(
        &self,
        index_id: &str,
        new_split_ids: &[&'a str],
        replaced_split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        run_with_tx!(self.connection_pool, tx, {
            // Publish splits.
            let published_split_ids =
                mark_splits_as_published_helper(tx, index_id, new_split_ids).await?;

            // Mark splits for deletion
            let marked_split_ids = mark_splits_for_deletion(
                tx,
                index_id,
                replaced_split_ids,
                &[SplitState::Published.as_str()],
            )
            .await?;

            if published_split_ids.len() != new_split_ids.len() {
                let affected_split_ids: Vec<String> = published_split_ids
                    .into_iter()
                    .chain(marked_split_ids.into_iter())
                    .collect();
                let split_ids: Vec<&str> = new_split_ids
                    .iter()
                    .chain(replaced_split_ids.iter())
                    .copied()
                    .collect();

                let not_staged_ids =
                    get_splits_with_invalid_state(tx, index_id, &split_ids, &affected_split_ids)
                        .await?;

                return Err(MetastoreError::SplitsNotStaged {
                    split_ids: not_staged_ids,
                });
            }
            if marked_split_ids.len() != replaced_split_ids.len() {
                let non_deletable_split_ids = replaced_split_ids
                    .iter()
                    .filter(|replaced_split_id| {
                        marked_split_ids
                            .iter()
                            .all(|marked_split_id| &marked_split_id != replaced_split_id)
                    })
                    .map(|split_id| split_id.to_string())
                    .collect();
                return Err(MetastoreError::SplitsNotDeletable {
                    split_ids: non_deletable_split_ids,
                });
            }
            Ok(())
        })
    }

    #[instrument(skip(self))]
    async fn list_splits(
        &self,
        index_id: &str,
        state: SplitState,
        time_range_opt: Option<Range<i64>>,
        tags: Option<TagFilterAst>,
    ) -> MetastoreResult<Vec<Split>> {
        run_with_tx!(self.connection_pool, tx, {
            list_splits_helper(tx, index_id, Some(state), time_range_opt, tags).await
        })
    }

    #[instrument(skip(self))]
    async fn list_all_splits(&self, index_id: &str) -> MetastoreResult<Vec<Split>> {
        run_with_tx!(self.connection_pool, tx, {
            list_splits_helper(tx, index_id, None, None, None).await
        })
    }

    #[instrument(skip(self))]
    async fn mark_splits_for_deletion<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        run_with_tx!(self.connection_pool, tx, {
            let marked_split_ids: Vec<String> = mark_splits_for_deletion(
                tx,
                index_id,
                split_ids,
                &[
                    SplitState::Staged.as_str(),
                    SplitState::Published.as_str(),
                    SplitState::MarkedForDeletion.as_str(),
                ],
            )
            .await?;

            if marked_split_ids.len() == split_ids.len() {
                return Ok(());
            }

            get_splits_with_invalid_state(tx, index_id, split_ids, &marked_split_ids).await?;

            let err_msg = format!("Failed to mark splits for deletion for index {index_id}.");
            let cause = anyhow::anyhow!(err_msg.clone());
            Err(MetastoreError::InternalError {
                message: err_msg,
                cause,
            })
        })
    }

    #[instrument(skip(self))]
    async fn delete_splits<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        run_with_tx!(self.connection_pool, tx, {
            let deletable_states = [
                SplitState::Staged.as_str(),
                SplitState::MarkedForDeletion.as_str(),
            ];
            let deleted_split_ids: Vec<String> = sqlx::query(
                r#"
                DELETE FROM splits
                WHERE
                        split_id = ANY($1)
                    AND split_state = ANY($2)
                RETURNING split_id
            "#,
            )
            .bind(split_ids)
            .bind(&deletable_states[..])
            .map(|pg_row| pg_row.get(0))
            .fetch_all(&mut *tx)
            .await?;

            if deleted_split_ids.len() == split_ids.len() {
                return Ok(());
            }

            // There is an error, but we want to investigate and return a meaningful error.
            // From this point, we always have to return `Err` to abort the transaction.
            let not_deletable_ids =
                get_splits_with_invalid_state(tx, index_id, split_ids, &deleted_split_ids).await?;

            Err(MetastoreError::SplitsNotDeletable {
                split_ids: not_deletable_ids,
            })
        })
    }

    #[instrument(skip(self))]
    async fn index_metadata(&self, index_id: &str) -> MetastoreResult<IndexMetadata> {
        run_with_tx!(self.connection_pool, tx, {
            index_metadata(tx, index_id).await
        })
    }

    #[instrument(skip(self, source), fields(source_id = source.source_id.as_str()))]
    async fn add_source(&self, index_id: &str, source: SourceConfig) -> MetastoreResult<()> {
        run_with_tx!(self.connection_pool, tx, {
            mutate_index_metadata(tx, index_id, |index_metadata| {
                index_metadata.add_source(source)
            })
            .await
        })
    }

    #[instrument(skip(self))]
    async fn delete_source(&self, index_id: &str, source_id: &str) -> MetastoreResult<()> {
        run_with_tx!(self.connection_pool, tx, {
            mutate_index_metadata(tx, index_id, |index_metadata| {
                index_metadata.delete_source(source_id)
            })
            .await
        })
    }

    fn uri(&self) -> &Uri {
        &self.uri
    }
}

// We use dollar-quoted strings in Postgresql.
//
// In order to ensure that we do not risk SQL injection,
// we need to generate a string that does not appear in
// the literal we want to dollar quote.
fn generate_dollar_guard(s: &str) -> String {
    if !s.contains('$') {
        // That's our happy path here.
        return String::new();
    }
    let mut dollar_guard = String::new();
    loop {
        dollar_guard.push_str("Quickwit!");
        // This terminates because `dollar_guard`
        // will eventually be longer than s.
        if !s.contains(&dollar_guard) {
            return dollar_guard;
        }
    }
}

/// Takes a tag filters AST and returns a sql expression that can be used as
/// a filter.
fn tags_filter_expression_helper(tags: TagFilterAst) -> String {
    match tags {
        TagFilterAst::And(child_asts) => {
            if child_asts.is_empty() {
                return "TRUE".to_string();
            }
            let expr_without_parenthesis = child_asts
                .into_iter()
                .map(tags_filter_expression_helper)
                .join(" AND ");
            format!("({expr_without_parenthesis})")
        }
        TagFilterAst::Or(child_asts) => {
            if child_asts.is_empty() {
                return "TRUE".to_string();
            }
            let expr_without_parenthesis = child_asts
                .into_iter()
                .map(tags_filter_expression_helper)
                .join(" OR ");
            format!("({expr_without_parenthesis})")
        }
        TagFilterAst::Tag { is_present, tag } => {
            let dollar_guard = generate_dollar_guard(&tag);
            if is_present {
                format!("${dollar_guard}${tag}${dollar_guard}$ = ANY(tags)")
            } else {
                format!("NOT (${dollar_guard}${tag}${dollar_guard}$ = ANY(tags))")
            }
        }
    }
}

/// A postgres metastore factory
#[derive(Clone, Default)]
pub struct PostgresqlMetastoreFactory {
    // In a normal run, this cache will contain a single Metastore.
    //
    // In contrast to the file backe metastore, we use a strong pointer here, so that Metastore
    // doesn't get dropped. This is done in order to keep the underlying connection pool to
    // postgres alive.
    cache: Arc<Mutex<HashMap<Uri, Arc<dyn Metastore>>>>,
}

impl PostgresqlMetastoreFactory {
    async fn get_from_cache(&self, uri: &Uri) -> Option<Arc<dyn Metastore>> {
        let cache_lock = self.cache.lock().await;
        cache_lock.get(uri).map(Arc::clone)
    }

    /// If there is a valid entry in the cache to begin with, we trash the new
    /// one and return the old one.
    ///
    /// This way we make sure that we keep only one instance associated
    /// to the key `uri` outside of this struct.
    async fn cache_metastore(&self, uri: Uri, metastore: Arc<dyn Metastore>) -> Arc<dyn Metastore> {
        let mut cache_lock = self.cache.lock().await;
        if let Some(metastore) = cache_lock.get(&uri) {
            return metastore.clone();
        }
        cache_lock.insert(uri, metastore.clone());
        metastore
    }
}

#[async_trait]
impl MetastoreFactory for PostgresqlMetastoreFactory {
    async fn resolve(&self, uri: &Uri) -> Result<Arc<dyn Metastore>, MetastoreResolverError> {
        if let Some(metastore) = self.get_from_cache(uri).await {
            debug!("using metastore from cache");
            return Ok(metastore);
        }
        debug!("metastore not found in cache");
        let metastore = PostgresqlMetastore::new(uri.clone())
            .await
            .map_err(MetastoreResolverError::FailedToOpenMetastore)?;
        let metastore = self.cache_metastore(uri.clone(), Arc::new(metastore)).await;
        Ok(metastore)
    }
}

#[cfg(test)]
#[async_trait]
impl crate::tests::test_suite::DefaultForTest for PostgresqlMetastore {
    async fn default_for_test() -> Self {
        // We cannot use a singleton here,
        // because sqlx needs the runtime used to create a connection to
        // not being dropped.
        //
        // Each unit test runs its own tokio Runtime, so a singleton would mean
        // tying the connection pool to the runtime of one unit test.
        // Concretely this results in a "IO driver has terminated"
        // once the first unit test finishes and its runtime is dropped.
        //
        // The number of connections to Postgres should not be
        // too catastrophic, as it is limited by the number of concurrent
        // unit tests running (= number of test-threads).
        dotenv::dotenv().ok();
        let uri = Uri::try_new(&std::env::var("TEST_DATABASE_URL").unwrap())
            .expect("Failed to parse test database URL.");
        PostgresqlMetastore::new(uri)
            .await
            .expect("Failed to initialize test PostgreSQL metastore.")
    }
}

metastore_test_suite!(crate::PostgresqlMetastore);

#[cfg(test)]
mod tests {
    use quickwit_doc_mapper::tag_pruning::{no_tag, tag, TagFilterAst};

    use super::tags_filter_expression_helper;

    fn test_tags_filter_expression_helper(tags_ast: TagFilterAst, expected: &str) {
        assert_eq!(tags_filter_expression_helper(tags_ast), expected);
    }

    #[test]
    fn test_tags_filter_expression_single_tag() {
        let tags_ast = tag("my_field:titi");
        test_tags_filter_expression_helper(tags_ast, r#"$$my_field:titi$$ = ANY(tags)"#);
    }

    #[test]
    fn test_tags_filter_expression_not_tag() {
        test_tags_filter_expression_helper(
            no_tag("my_field:titi"),
            r#"NOT ($$my_field:titi$$ = ANY(tags))"#,
        );
    }

    #[test]
    fn test_tags_filter_expression_ands() {
        let tags_ast = TagFilterAst::And(vec![tag("tag:val1"), tag("tag:val2"), tag("tag:val3")]);
        test_tags_filter_expression_helper(
            tags_ast,
            "($$tag:val1$$ = ANY(tags) AND $$tag:val2$$ = ANY(tags) AND $$tag:val3$$ = ANY(tags))",
        );
    }

    #[test]
    fn test_tags_filter_expression_and_or() {
        let tags_ast = TagFilterAst::Or(vec![
            TagFilterAst::And(vec![tag("tag:val1"), tag("tag:val2")]),
            tag("tag:val3"),
        ]);
        test_tags_filter_expression_helper(
            tags_ast,
            "(($$tag:val1$$ = ANY(tags) AND $$tag:val2$$ = ANY(tags)) OR $$tag:val3$$ = ANY(tags))",
        );
    }

    #[test]
    fn test_tags_filter_expression_and_or_correct_parenthesis() {
        let tags_ast = TagFilterAst::And(vec![
            TagFilterAst::Or(vec![tag("tag:val1"), tag("tag:val2")]),
            tag("tag:val3"),
        ]);
        test_tags_filter_expression_helper(
            tags_ast,
            r#"(($$tag:val1$$ = ANY(tags) OR $$tag:val2$$ = ANY(tags)) AND $$tag:val3$$ = ANY(tags))"#,
        );
    }

    #[test]
    fn test_tags_sql_injection_attempt() {
        let tags_ast = tag("tag:$$;DELETE FROM something_evil");
        test_tags_filter_expression_helper(
            tags_ast,
            "$Quickwit!$tag:$$;DELETE FROM something_evil$Quickwit!$ = ANY(tags)",
        );
    }
}
