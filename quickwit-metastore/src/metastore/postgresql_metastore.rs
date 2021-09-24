// Copyright (C) 2021 Quickwit, Inc.
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

use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use diesel::pg::Pg;
use diesel::r2d2::{ConnectionManager, Pool, PooledConnection};
use diesel::result::DatabaseErrorKind;
use diesel::result::Error::DatabaseError;
use diesel::{
    debug_query, BoolExpressionMethods, Connection, ExpressionMethods, PgConnection, QueryDsl,
    RunQueryDsl,
};
use tracing::{debug, error, info, warn};

use crate::metastore::{match_tags_filter, CheckpointDelta};
use crate::postgresql::{model, schema};
use crate::{
    IndexMetadata, Metastore, MetastoreError, MetastoreFactory, MetastoreResolverError,
    MetastoreResult, SplitMetadataAndFooterOffsets, SplitState,
};

embed_migrations!("migrations/postgresql");

const CONNECTION_POOL_MAX_SIZE: u32 = 10;
const CONNECTION_POOL_TIMEOUT: Duration = Duration::from_secs(10);
const CONNECTION_POOL_MAX_RETRY_COUNT: u32 = 10;
const CONNECTION_STATUS_CHECK_MAX_RETRY_COUNT: u32 = 3;
const CONNECTION_STATUS_CHECK_INTERVAL: Duration = Duration::from_secs(2);

/// Establishes a connection to the given database URI.
fn establish_connection(
    database_uri: &str,
) -> anyhow::Result<Pool<ConnectionManager<PgConnection>>> {
    let mut retry_cnt = 0;
    while retry_cnt <= CONNECTION_POOL_MAX_RETRY_COUNT {
        let connection_manager: ConnectionManager<PgConnection> =
            ConnectionManager::new(database_uri);
        match Pool::builder()
            .max_size(CONNECTION_POOL_MAX_SIZE)
            .connection_timeout(CONNECTION_POOL_TIMEOUT)
            .build(connection_manager)
        {
            Ok(pool) => {
                return Ok(pool);
            }
            Err(err) => {
                warn!(err=?err, "Failed to connect to postgres. Trying again");
                retry_cnt += 1;
            }
        }
    }

    anyhow::bail!(
        "The retry count has exceeded the limit ({})",
        CONNECTION_POOL_MAX_RETRY_COUNT
    );
}

/// Initialize the database.
/// The sql used for the initialization is stored in quickwit-metastore/migrations directory.
fn initialize_db(pool: &Pool<ConnectionManager<PgConnection>>) -> anyhow::Result<()> {
    let db_conn = pool.get()?;
    let mut migrations_log_buffer = Vec::new();

    match embedded_migrations::run_with_output(&*db_conn, &mut migrations_log_buffer) {
        Ok(_) => {
            let migrations_log = String::from_utf8_lossy(&migrations_log_buffer);
            info!(
                migrations_log = migrations_log.as_ref(),
                "Database migrations succeeded"
            );
            Ok(())
        }
        Err(err) => {
            let migrations_log = String::from_utf8_lossy(&migrations_log_buffer);
            error!(
                migrations_log = migrations_log.as_ref(),
                "Database migrations failed"
            );
            Err(anyhow::anyhow!(err))
        }
    }
}

/// Check the difference between the given split IDs to be modified and the actual modified split
/// IDs. If there is a difference, it returns SplitDoesNotExist.
fn check_all_splits_were_modified<'a>(
    split_ids_to_modify: &[&'a str],
    modified_split_ids: &[&'a str],
) -> Result<(), MetastoreError> {
    for split_id in split_ids_to_modify.iter() {
        if !modified_split_ids
            .iter()
            .any(|modified_split_id| modified_split_id == split_id)
        {
            return Err(MetastoreError::SplitDoesNotExist {
                split_id: split_id.to_string(),
            });
        }
    }

    Ok(())
}

/// PostgreSQL metastore implementation.
#[derive(Clone)]
pub struct PostgresqlMetastore {
    uri: String,
    connection_pool: Arc<Pool<ConnectionManager<PgConnection>>>,
}

impl PostgresqlMetastore {
    /// Creates a meta store given a database URI.
    pub async fn new(database_uri: &str) -> MetastoreResult<Self> {
        let connection_pool = Arc::new(establish_connection(database_uri).map_err(|err| {
            error!(err=?err, "Failed to establish connection");
            MetastoreError::ConnectionError {
                message: err.to_string(),
            }
        })?);

        // Check the connection pool.
        let mut is_status_ok = false;
        let mut retry_cnt = 0;
        while retry_cnt <= CONNECTION_STATUS_CHECK_MAX_RETRY_COUNT {
            let connection_pool_state = connection_pool.state();
            debug!(
                connections = connection_pool_state.connections,
                idle_connections = connection_pool_state.idle_connections,
                "Connection pool state"
            );
            match connection_pool.get() {
                Ok(_conn) => {
                    info!("The connection pool works fine");
                    is_status_ok = true;
                    break;
                }
                Err(err) => {
                    warn!(err=?err, "Failed to get connection from the connection pool. Trying again");
                    retry_cnt += 1;
                    tokio::time::sleep(CONNECTION_STATUS_CHECK_INTERVAL).await;
                }
            }
        }
        if !is_status_ok {
            error!(
                "The retry count has exceeded the limit ({})",
                CONNECTION_STATUS_CHECK_MAX_RETRY_COUNT
            );
            return Err(MetastoreError::ConnectionError {
                message: "The connection pool does not work fine".to_string(),
            });
        }

        initialize_db(&*connection_pool).map_err(|err| MetastoreError::InternalError {
            message: "Failed to initialize database".to_string(),
            cause: anyhow::anyhow!(err),
        })?;

        Ok(PostgresqlMetastore {
            uri: database_uri.to_string(),
            connection_pool,
        })
    }

    /// Check index existence.
    /// Returns true if the index exists.
    fn is_index_exist(
        &self,
        conn: &PooledConnection<ConnectionManager<PgConnection>>,
        index_id: &str,
    ) -> MetastoreResult<bool> {
        let check_index_existence_statement = diesel::select(diesel::dsl::exists(
            schema::indexes::dsl::indexes.filter(schema::indexes::dsl::index_id.eq(index_id)),
        ));
        debug!(sql=%debug_query::<Pg, _>(&check_index_existence_statement).to_string());
        let index_exists: bool = check_index_existence_statement
            .get_result(conn)
            .map_err(MetastoreError::DbError)?;

        Ok(index_exists)
    }

    /// Publish splits.
    /// Returns the successful split IDs.
    fn publish_splits(
        &self,
        conn: &PooledConnection<ConnectionManager<PgConnection>>,
        index_id: &str,
        split_ids: &[&str],
    ) -> MetastoreResult<Vec<String>> {
        // Select splits to publish.
        let select_splits_statement = schema::splits::dsl::splits.filter(
            schema::splits::dsl::index_id
                .eq(index_id)
                .and(schema::splits::dsl::split_id.eq_any(split_ids)),
        );
        debug!(sql=%debug_query::<Pg, _>(&select_splits_statement).to_string());
        let model_splits: Vec<model::Split> = select_splits_statement
            .get_results(conn)
            .map_err(MetastoreError::DbError)?;

        let now_timestamp = Utc::now().timestamp();
        let mut succeeded_split_ids = Vec::new();
        for model_split in model_splits {
            // Check for the inclusion of non-publishable split IDs.
            // Except for SplitState::Staged and SplitState::Published, you cannot publish.
            if model_split.split_state != SplitState::Staged.to_string()
                && model_split.split_state != SplitState::Published.to_string()
            {
                return Err(MetastoreError::SplitIsNotStaged {
                    split_id: model_split.split_id,
                });
            }

            // Deserialize the target split metadata.
            let mut split_metadata_and_footer_offsets = match model_split
                .make_split_metadata_and_footer_offsets()
            {
                Ok(split_metadata_and_footer_offsets) => split_metadata_and_footer_offsets,
                Err(err) => {
                    let msg = format!(
                        "Failed to deserialize JSON to SplitMetadataAndFooterOffsets split_id={:?}",
                        model_split.split_id
                    );
                    error!("{:?} {:?}", msg, err);
                    return Err(MetastoreError::InternalError {
                        message: msg,
                        cause: anyhow::anyhow!(err),
                    });
                }
            };

            // Update its split_state and update_timestamp.
            split_metadata_and_footer_offsets.split_metadata.split_state = SplitState::Published;
            split_metadata_and_footer_offsets
                .split_metadata
                .update_timestamp = now_timestamp;

            // Serialize to JSON.
            let split_metadata_and_footer_offsets_json =
                match serde_json::to_string(&split_metadata_and_footer_offsets) {
                    Ok(json_str) => json_str,
                    Err(err) => {
                        let msg = format!(
                            "Failed to serialize from JSON to SplitMetadataAndFooterOffsets \
                             split_id={:?}",
                            model_split.split_id
                        );
                        error!("{:?} {:?}", msg, err);
                        return Err(MetastoreError::InternalError {
                            message: msg,
                            cause: anyhow::anyhow!(err),
                        });
                    }
                };

            // Update database.
            let update_splits_statement = diesel::update(
                schema::splits::dsl::splits.filter(
                    schema::splits::dsl::index_id
                        .eq(index_id)
                        .and(schema::splits::dsl::split_id.eq(model_split.split_id)),
                ),
            )
            .set((
                schema::splits::dsl::split_state.eq(SplitState::Published.to_string()),
                schema::splits::dsl::split_metadata_json.eq(split_metadata_and_footer_offsets_json),
            ));
            debug!(sql=%debug_query::<Pg, _>(&update_splits_statement).to_string());
            let updated_split: model::Split = update_splits_statement
                .get_result(&*conn)
                .map_err(MetastoreError::DbError)?;

            succeeded_split_ids.push(updated_split.split_id);
        }

        debug!(index_id=?index_id, split_ids=?succeeded_split_ids, "Published");

        Ok(succeeded_split_ids)
    }

    /// Mark splits as deleted.
    /// Returns the successful split IDs.
    fn mark_as_deleted(
        &self,
        conn: &PooledConnection<ConnectionManager<PgConnection>>,
        index_id: &str,
        split_ids: &[&str],
    ) -> MetastoreResult<Vec<String>> {
        // Select splits to matk as deleted.
        let select_splits_statement = schema::splits::dsl::splits.filter(
            schema::splits::dsl::index_id
                .eq(index_id)
                .and(schema::splits::dsl::split_id.eq_any(split_ids)),
        );
        debug!(sql=%debug_query::<Pg, _>(&select_splits_statement).to_string());
        let model_splits: Vec<model::Split> = select_splits_statement
            .get_results(conn)
            .map_err(MetastoreError::DbError)?;

        let now_timestamp = Utc::now().timestamp();
        let mut succeeded_split_ids = Vec::new();
        for model_split in model_splits {
            // Deserialize the target split metadata.
            let mut split_metadata_and_footer_offsets = match model_split
                .make_split_metadata_and_footer_offsets()
            {
                Ok(split_metadata_and_footer_offsets) => split_metadata_and_footer_offsets,
                Err(err) => {
                    let msg = format!(
                        "Failed to deserialize JSON to SplitMetadataAndFooterOffsets split_id={:?}",
                        model_split.split_id
                    );
                    error!("{:?} {:?}", msg, err);
                    return Err(MetastoreError::InternalError {
                        message: msg,
                        cause: anyhow::anyhow!(err),
                    });
                }
            };

            // Update its split_state and update_timestamp.
            split_metadata_and_footer_offsets.split_metadata.split_state =
                SplitState::ScheduledForDeletion;
            split_metadata_and_footer_offsets
                .split_metadata
                .update_timestamp = now_timestamp;

            // Serialize to JSON.
            let split_metadata_and_footer_offsets_json =
                match serde_json::to_string(&split_metadata_and_footer_offsets) {
                    Ok(json_str) => json_str,
                    Err(err) => {
                        let msg = format!(
                            "Failed to serialize from JSON to SplitMetadataAndFooterOffsets \
                             split_id={:?}",
                            model_split.split_id
                        );
                        error!("{:?} {:?}", msg, err);
                        return Err(MetastoreError::InternalError {
                            message: msg,
                            cause: anyhow::anyhow!(err),
                        });
                    }
                };

            // Update database.
            let update_splits_statement = diesel::update(
                schema::splits::dsl::splits.filter(
                    schema::splits::dsl::index_id
                        .eq(index_id)
                        .and(schema::splits::dsl::split_id.eq(model_split.split_id)),
                ),
            )
            .set((
                schema::splits::dsl::split_state.eq(SplitState::ScheduledForDeletion.to_string()),
                schema::splits::dsl::split_metadata_json.eq(split_metadata_and_footer_offsets_json),
            ));
            debug!(sql=%debug_query::<Pg, _>(&update_splits_statement).to_string());
            let updated_split: model::Split = update_splits_statement
                .get_result(&*conn)
                .map_err(MetastoreError::DbError)?;

            succeeded_split_ids.push(updated_split.split_id);
        }

        debug!(succeeded_split_ids=?succeeded_split_ids, "Mark as deleted");

        Ok(succeeded_split_ids)
    }

    fn delete_splits(
        &self,
        conn: &PooledConnection<ConnectionManager<PgConnection>>,
        index_id: &str,
        split_ids: &[&str],
    ) -> MetastoreResult<Vec<String>> {
        // Select splits to delete.
        let select_splits_statement = schema::splits::dsl::splits.filter(
            schema::splits::dsl::index_id
                .eq(index_id)
                .and(schema::splits::dsl::split_id.eq_any(split_ids)),
        );
        debug!(sql=%debug_query::<Pg, _>(&select_splits_statement).to_string());
        let model_splits: Vec<model::Split> = select_splits_statement
            .get_results(conn)
            .map_err(MetastoreError::DbError)?;

        let mut succeeded_split_ids = Vec::new();
        for model_split in model_splits {
            // Check for the inclusion of non-deletable split IDs.
            // Except for SplitState::Staged and SplitState::ScheduledForDeletion, you cannot
            // delete.
            if model_split.split_state != SplitState::Staged.to_string()
                && model_split.split_state != SplitState::ScheduledForDeletion.to_string()
            {
                return Err(MetastoreError::Forbidden {
                    message: format!(
                        "This split {:?} is not in a deletable state",
                        model_split.split_id
                    ),
                });
            }

            // Update database.
            let delete_splits_statement = diesel::delete(
                schema::splits::dsl::splits.filter(
                    schema::splits::dsl::index_id
                        .eq(index_id)
                        .and(schema::splits::dsl::split_id.eq(model_split.split_id)),
                ),
            );
            debug!(sql=%debug_query::<Pg, _>(&delete_splits_statement).to_string());
            let deleted_split: model::Split = delete_splits_statement
                .get_result(&*conn)
                .map_err(MetastoreError::DbError)?;

            succeeded_split_ids.push(deleted_split.split_id);
        }

        debug!(index_id=?index_id, split_ids=?succeeded_split_ids, "Deleted");

        Ok(succeeded_split_ids)
    }

    /// Apply checkpoint delta.
    fn apply_checkpoint_delta(
        &self,
        conn: &PooledConnection<ConnectionManager<PgConnection>>,
        index_id: &str,
        checkpoint_delta: CheckpointDelta,
    ) -> MetastoreResult<()> {
        // Get index metadata.
        let select_index_statement =
            schema::indexes::dsl::indexes.filter(schema::indexes::dsl::index_id.eq(index_id));
        debug!(sql=%debug_query::<Pg, _>(&select_index_statement).to_string());
        let model_index = select_index_statement
            .first::<model::Index>(conn)
            .map_err(|err| match err {
                diesel::result::Error::NotFound => MetastoreError::IndexDoesNotExist {
                    index_id: index_id.to_string(),
                },
                _ => MetastoreError::InternalError {
                    message: "Failed to select index".to_string(),
                    cause: anyhow::anyhow!(err),
                },
            })?;

        // Deserialize the checkpoint from the database model.
        let mut index_metadata =
            model_index
                .make_index_metadata()
                .map_err(|err| MetastoreError::InternalError {
                    message: "Failed to deserialize index metadata".to_string(),
                    cause: anyhow::anyhow!(err),
                })?;

        // Apply checkpoint_delta
        index_metadata
            .checkpoint
            .try_apply_delta(checkpoint_delta)?;

        // Serialize the checkpoint to fit the database model.
        let index_metadata_json = serde_json::to_string(&index_metadata).map_err(|err| {
            MetastoreError::InternalError {
                message: "Failed to serialize index metadata".to_string(),
                cause: anyhow::anyhow!(err),
            }
        })?;

        // Update the index checkpoint.
        let update_index_statement = diesel::update(schema::indexes::dsl::indexes.find(index_id))
            .set(schema::indexes::dsl::index_metadata_json.eq(index_metadata_json));
        debug!(sql=%debug_query::<Pg, _>(&update_index_statement).to_string());
        update_index_statement
            .execute(&*conn)
            .map_err(MetastoreError::DbError)?;

        Ok(())
    }
}

#[async_trait]
impl Metastore for PostgresqlMetastore {
    async fn create_index(&self, index_metadata: IndexMetadata) -> MetastoreResult<()> {
        // Serialize the index metadata to fit the database model.
        let index_metadata_json = serde_json::to_string(&index_metadata).map_err(|err| {
            MetastoreError::InternalError {
                message: "Failed to serialize checkpoint".to_string(),
                cause: anyhow::anyhow!(err),
            }
        })?;

        let model_index = model::Index {
            index_id: index_metadata.index_id.clone(),
            index_metadata_json,
        };

        let conn = self.connection_pool.get().map_err(|err| {
            error!(err=?err, "Failed to get connection");
            MetastoreError::ConnectionError {
                message: format!("Failed to get connection {:?}", err),
            }
        })?;

        conn.transaction::<_, MetastoreError, _>(|| {
            // Create index.
            let create_index_statement =
                diesel::insert_into(schema::indexes::dsl::indexes).values(&model_index);
            debug!(sql=%debug_query::<Pg, _>(&create_index_statement).to_string());
            // create_index_statement.execute(&*conn)?;
            create_index_statement
                .execute(&*conn)
                .map_err(|err| match err {
                    DatabaseError(db_err_kind, ref err_info) => match db_err_kind {
                        DatabaseErrorKind::UniqueViolation => {
                            error!(index_id=?index_metadata.index_id, "Index already exists");
                            MetastoreError::IndexAlreadyExists {
                                index_id: index_metadata.index_id.clone(),
                            }
                        }
                        _ => {
                            error!(index_id=?index_metadata.index_id, "An error has occurred in the database operation. {:?}", err_info.message());
                            MetastoreError::DbError(err)
                        }
                    },
                    _ => {
                        error!(index_id=?index_metadata.index_id, "An error has occurred in the database operation. {:?}", err);
                        MetastoreError::DbError(err)
                    }
                })?;

            debug!(index_id=?model_index.index_id, "The index has been created");

            Ok(())
        })?;

        Ok(())
    }

    async fn delete_index(&self, index_id: &str) -> MetastoreResult<()> {
        let conn = self.connection_pool.get().map_err(|err| {
            error!(err=?err, "Failed to get connection");
            MetastoreError::ConnectionError {
                message: format!("Failed to get connection {:?}", err),
            }
        })?;

        conn.transaction::<_, MetastoreError, _>(|| {
            // Delete index.
            let delete_index_statement =
                diesel::delete(schema::indexes::dsl::indexes.find(index_id));
            debug!(sql=%debug_query::<Pg, _>(&delete_index_statement).to_string());
            let num = delete_index_statement
                .execute(&*conn)
                .map_err(MetastoreError::DbError)?;

            if num == 0 {
                return Err(MetastoreError::IndexDoesNotExist {
                    index_id: index_id.to_string(),
                });
            }

            debug!(index_id=?index_id, "The index has been deleted");

            Ok(())
        })?;

        Ok(())
    }

    async fn stage_split(
        &self,
        index_id: &str,
        mut metadata: SplitMetadataAndFooterOffsets,
    ) -> MetastoreResult<()> {
        // Modify split state to Staged.
        metadata.split_metadata.split_state = SplitState::Staged;
        metadata.split_metadata.update_timestamp = Utc::now().timestamp();

        // Fit the time_range to the database model.
        let start_time_range = metadata
            .split_metadata
            .time_range
            .clone()
            .map(|range| *range.start());
        let end_time_range = metadata
            .split_metadata
            .time_range
            .clone()
            .map(|range| *range.end());

        // Serialize the split metadata and footer offsets to fit the database model.
        let split_metadata_and_footer_offsets_json =
            serde_json::to_string(&metadata).map_err(|err| MetastoreError::InternalError {
                message: "Failed to serialize split metadata and footer offsets".to_string(),
                cause: anyhow::anyhow!(err),
            })?;

        let model_split = model::Split {
            split_id: metadata.split_metadata.split_id,
            split_state: metadata.split_metadata.split_state.to_string(),
            start_time_range,
            end_time_range,
            tags: metadata
                .split_metadata
                .tags
                .into_iter()
                .collect::<Vec<String>>(),
            split_metadata_json: split_metadata_and_footer_offsets_json,
            index_id: index_id.to_string(),
        };

        let conn = self.connection_pool.get().map_err(|err| {
            error!(err=?err, "Failed to get connection");
            MetastoreError::ConnectionError {
                message: format!("Failed to get connection {:?}", err),
            }
        })?;

        conn.transaction::<_, MetastoreError, _>(|| {
            // Insert a new split metadata as `Staged` state.
            let insert_staged_split_statement =
                diesel::insert_into(schema::splits::dsl::splits).values(&model_split);
            debug!(sql=%debug_query::<Pg, _>(&insert_staged_split_statement).to_string());
            insert_staged_split_statement.execute(&*conn).map_err(|err| {
                match err {
                    DatabaseError(err_kind, ref err_info) => match err_kind {
                        DatabaseErrorKind::ForeignKeyViolation => {
                            error!(index_id=?index_id, split_id=?model_split.split_id, "Index does not exist");
                            MetastoreError::IndexDoesNotExist {
                                index_id: index_id.to_string(),
                            }
                        },
                        DatabaseErrorKind::UniqueViolation => {
                            error!(index_id=?index_id, split_id=?model_split.split_id, "Split already exists");
                            MetastoreError::InternalError {
                                message: format!(
                                    "Try to stage split that already exists ({})",
                                    model_split.split_id
                                ),
                                cause: anyhow::anyhow!(err),
                            }
                        }
                        _ => {
                            error!(index_id=?index_id, split_id=?model_split.split_id, "An error has occurred in the database operation. {:?}", err_info.message());
                            MetastoreError::DbError(err)
                        }
                    },
                    _ => {
                        error!(index_id=?index_id, split_id=?model_split.split_id, "An error has occurred in the database operation. {:?}", err);
                        MetastoreError::DbError(err)
                    }
                }
            })?;

            debug!(index_id=?index_id, spliet_id=?model_split.split_id, "The split has been staged");

            Ok(())
        })?;

        Ok(())
    }

    async fn publish_splits<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
        checkpoint_delta: CheckpointDelta,
    ) -> MetastoreResult<()> {
        let conn = self.connection_pool.get().map_err(|err| {
            error!(err=?err, "Failed to get connection");
            MetastoreError::ConnectionError {
                message: format!("Failed to get connection {:?}", err),
            }
        })?;

        // Check for the existence of index.
        let index_exists: bool = self.is_index_exist(&conn, index_id)?;
        if !index_exists {
            return Err(MetastoreError::IndexDoesNotExist {
                index_id: index_id.to_string(),
            });
        }

        conn.transaction::<_, MetastoreError, _>(|| {
            // Update the index checkpoint.
            self.apply_checkpoint_delta(&conn, index_id, checkpoint_delta)?;

            // Publish splits.
            let published_split_ids = self.publish_splits(&conn, index_id, split_ids)?;

            if published_split_ids.len() < split_ids.len() {
                // Return an error if there are any splits that could not be published.
                check_all_splits_were_modified(
                    split_ids,
                    &published_split_ids
                        .iter()
                        .map(String::as_str)
                        .collect::<Vec<_>>(),
                )?;
            }

            Ok(())
        })?;

        Ok(())
    }

    async fn replace_splits<'a>(
        &self,
        index_id: &str,
        new_split_ids: &[&'a str],
        replaced_split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        let conn = self.connection_pool.get().map_err(|err| {
            error!(err=?err, "Failed to get connection");
            MetastoreError::ConnectionError {
                message: format!("Failed to get connection {:?}", err),
            }
        })?;

        // Check for the existence of index.
        let index_exists: bool = self.is_index_exist(&conn, index_id)?;
        if !index_exists {
            return Err(MetastoreError::IndexDoesNotExist {
                index_id: index_id.to_string(),
            });
        }

        conn.transaction::<_, MetastoreError, _>(|| {
            // Publish splits.
            let published_split_ids = self.publish_splits(&conn, index_id, new_split_ids)?;

            if published_split_ids.len() < new_split_ids.len() {
                // Return an error if there are any splits that could not be published.
                check_all_splits_were_modified(
                    new_split_ids,
                    &published_split_ids
                        .iter()
                        .map(String::as_str)
                        .collect::<Vec<_>>(),
                )?;
            }

            // Mark as deleted.
            let mark_as_deleted_split_ids =
                self.mark_as_deleted(&conn, index_id, replaced_split_ids)?;

            if mark_as_deleted_split_ids.len() < replaced_split_ids.len() {
                // Return an error if there are any splits that could not be marked as deleted.
                check_all_splits_were_modified(
                    replaced_split_ids,
                    &mark_as_deleted_split_ids
                        .iter()
                        .map(String::as_str)
                        .collect::<Vec<_>>(),
                )?;
            }

            Ok(())
        })?;

        Ok(())
    }

    async fn list_splits(
        &self,
        index_id: &str,
        state: SplitState,
        time_range_opt: Option<Range<i64>>,
        tags: &[String],
    ) -> MetastoreResult<Vec<SplitMetadataAndFooterOffsets>> {
        let conn = self.connection_pool.get().map_err(|err| {
            error!(err=?err, "Failed to get connection");
            MetastoreError::ConnectionError {
                message: format!("Failed to get connection {:?}", err),
            }
        })?;

        // Check for the existence of index.
        let index_exists: bool = self.is_index_exist(&conn, index_id)?;
        if !index_exists {
            return Err(MetastoreError::IndexDoesNotExist {
                index_id: index_id.to_string(),
            });
        }

        let list_splits_statement = if let Some(time_range) = time_range_opt {
            schema::splits::dsl::splits
                .filter(
                    schema::splits::dsl::index_id
                        .eq(index_id)
                        .and(schema::splits::dsl::split_state.eq(state.to_string()))
                        .and(
                            schema::splits::dsl::end_time_range.is_null().or(
                                schema::splits::dsl::end_time_range
                                    .ge(time_range.start)
                                    .and(schema::splits::dsl::start_time_range.lt(time_range.end)),
                            ),
                        ),
                )
                .into_boxed()
        } else {
            schema::splits::dsl::splits
                .filter(
                    schema::splits::dsl::index_id
                        .eq(index_id)
                        .and(schema::splits::dsl::split_state.eq(state.to_string())),
                )
                .into_boxed()
        };
        debug!(sql=%debug_query::<Pg, _>(&list_splits_statement).to_string());
        let model_splits: Vec<model::Split> = list_splits_statement
            .load(&conn)
            .map_err(MetastoreError::DbError)?;

        // Make the split metadata and footer offsets from database model.
        let mut split_metadata_footer_offset_list: Vec<SplitMetadataAndFooterOffsets> = Vec::new();
        for model_split in model_splits {
            let split_metadata_and_footer_offsets =
                match model_split.make_split_metadata_and_footer_offsets() {
                    Ok(metadata) => metadata,
                    Err(err) => {
                        let msg = format!(
                            "Failed to make split metadata and footer offsets split_id={:?}",
                            model_split.split_id
                        );
                        error!("{:?} {:?}", msg, err);
                        return Err(MetastoreError::InternalError {
                            message: msg,
                            cause: err,
                        });
                    }
                };
            let split_tags = split_metadata_and_footer_offsets
                .split_metadata
                .tags
                .clone()
                .into_iter()
                .collect::<Vec<String>>();
            if match_tags_filter(split_tags.as_slice(), tags) {
                split_metadata_footer_offset_list.push(split_metadata_and_footer_offsets);
            }
        }

        Ok(split_metadata_footer_offset_list)
    }

    async fn list_all_splits(
        &self,
        index_id: &str,
    ) -> MetastoreResult<Vec<SplitMetadataAndFooterOffsets>> {
        let conn = self
            .connection_pool
            .get()
            .map_err(|err| MetastoreError::InternalError {
                message: "Failed to get connection".to_string(),
                cause: anyhow::anyhow!(err),
            })?;

        // Check for the existence of index.
        let index_exists: bool = self.is_index_exist(&conn, index_id)?;
        if !index_exists {
            return Err(MetastoreError::IndexDoesNotExist {
                index_id: index_id.to_string(),
            });
        }

        let list_all_splits_statement =
            schema::splits::dsl::splits.filter(schema::splits::dsl::index_id.eq(index_id));
        debug!(sql=%debug_query::<Pg, _>(&list_all_splits_statement).to_string());
        let model_splits: Vec<model::Split> = list_all_splits_statement
            .load(&conn)
            .map_err(MetastoreError::DbError)?;

        // Make the split metadata and footer offsets from database model.
        let mut split_metadata_footer_offset_list: Vec<SplitMetadataAndFooterOffsets> = Vec::new();
        for model_split in model_splits {
            let split_metadata_and_footer_offsets =
                match model_split.make_split_metadata_and_footer_offsets() {
                    Ok(metadata) => metadata,
                    Err(err) => {
                        let msg = format!(
                            "Failed to make split metadata and footer offsets split_id={:?}",
                            model_split.split_id
                        );
                        error!("{:?} {:?}", msg, err);
                        return Err(MetastoreError::InternalError {
                            message: msg,
                            cause: err,
                        });
                    }
                };
            split_metadata_footer_offset_list.push(split_metadata_and_footer_offsets);
        }

        Ok(split_metadata_footer_offset_list)
    }

    async fn mark_splits_as_deleted<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        let conn = self.connection_pool.get().map_err(|err| {
            error!(err=?err, "Failed to get connection");
            MetastoreError::ConnectionError {
                message: format!("Failed to get connection {:?}", err),
            }
        })?;

        // Check for the existence of index.
        let index_exists: bool = self.is_index_exist(&conn, index_id)?;
        if !index_exists {
            return Err(MetastoreError::IndexDoesNotExist {
                index_id: index_id.to_string(),
            });
        }

        conn.transaction::<_, MetastoreError, _>(|| {
            // Mark as deleted.
            let mark_as_deleted_split_ids = self.mark_as_deleted(&conn, index_id, split_ids)?;

            if mark_as_deleted_split_ids.len() < split_ids.len() {
                // Return an error if there are any splits that could not be marked as deleted.
                check_all_splits_were_modified(
                    split_ids,
                    &mark_as_deleted_split_ids
                        .iter()
                        .map(String::as_str)
                        .collect::<Vec<_>>(),
                )?;
            }

            Ok(())
        })?;

        Ok(())
    }

    async fn delete_splits<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        let conn = self.connection_pool.get().map_err(|err| {
            error!(err=?err, "Failed to get connection");
            MetastoreError::ConnectionError {
                message: format!("Failed to get connection {:?}", err),
            }
        })?;

        // Check for the existence of index.
        let index_exists: bool = self.is_index_exist(&conn, index_id)?;
        if !index_exists {
            return Err(MetastoreError::IndexDoesNotExist {
                index_id: index_id.to_string(),
            });
        }

        conn.transaction::<_, MetastoreError, _>(|| {
            // Delete splits.
            let deleted_split_ids = self.delete_splits(&conn, index_id, split_ids)?;

            if deleted_split_ids.len() < split_ids.len() {
                // Return an error if there are any splits that could not be deleted.
                check_all_splits_were_modified(
                    split_ids,
                    &deleted_split_ids
                        .iter()
                        .map(String::as_str)
                        .collect::<Vec<_>>(),
                )?;
            }

            Ok(())
        })?;

        Ok(())
    }

    async fn index_metadata(&self, index_id: &str) -> MetastoreResult<IndexMetadata> {
        let conn = self.connection_pool.get().map_err(|err| {
            error!(err=?err, "Failed to get connection");
            MetastoreError::ConnectionError {
                message: format!("Failed to get connection {:?}", err),
            }
        })?;

        let select_index_statement =
            schema::indexes::dsl::indexes.filter(schema::indexes::dsl::index_id.eq(index_id));
        debug!(sql=%debug_query::<Pg, _>(&select_index_statement).to_string());
        let model_index = select_index_statement
            .first::<model::Index>(&conn)
            .map_err(|err| match err {
                diesel::result::Error::NotFound => MetastoreError::IndexDoesNotExist {
                    index_id: index_id.to_string(),
                },
                _ => MetastoreError::DbError(err),
            })?;

        let index_metadata =
            model_index
                .make_index_metadata()
                .map_err(|err| MetastoreError::InternalError {
                    message: "Failed to make index metadata".to_string(),
                    cause: anyhow::anyhow!(err),
                })?;

        Ok(index_metadata)
    }

    fn uri(&self) -> String {
        self.uri.clone()
    }
}

/// A single file metastore factory
#[derive(Clone)]
pub struct PostgresqlMetastoreFactory {}

impl Default for PostgresqlMetastoreFactory {
    fn default() -> Self {
        PostgresqlMetastoreFactory {}
    }
}

#[async_trait]
impl MetastoreFactory for PostgresqlMetastoreFactory {
    async fn resolve(&self, uri: &str) -> Result<Arc<dyn Metastore>, MetastoreResolverError> {
        let metastore = PostgresqlMetastore::new(uri)
            .await
            .map_err(MetastoreResolverError::FailedToOpenMetastore)?;

        Ok(Arc::new(metastore))
    }
}

#[cfg(test)]
/// Get the PostgreSQL-based metastore for testing.
pub async fn get_or_init_postgresql_metastore_for_test() -> &'static PostgresqlMetastore {
    use std::env;

    use dotenv::dotenv;
    use tokio::sync::OnceCell;

    static POSTGRESQL_METASTORE: OnceCell<PostgresqlMetastore> = OnceCell::const_new();

    POSTGRESQL_METASTORE
        .get_or_init(|| async {
            dotenv().ok();
            let uri = env::var("TEST_DATABASE_URL").unwrap();

            PostgresqlMetastore::new(&uri)
                .await
                .expect("PostgreSQL metastore is not initialized")
        })
        .await
}

#[cfg(test)]
#[async_trait]
impl crate::tests::test_suite::DefaultForTest for PostgresqlMetastore {
    async fn default_for_test() -> Self {
        let metastore = get_or_init_postgresql_metastore_for_test().await;
        (*metastore).clone()
    }
}

#[cfg(feature = "postgres")]
metastore_test_suite_for_postgresql!(crate::PostgresqlMetastore);
