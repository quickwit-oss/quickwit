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

use std::collections::HashMap;
use std::fmt::{self, Write};
use std::str::FromStr;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use itertools::Itertools;
use quickwit_common::pretty::PrettySample;
use quickwit_common::uri::Uri;
use quickwit_common::{ServiceStream, get_bool_from_env, rate_limited_error};
use quickwit_config::{
    IndexTemplate, IndexTemplateId, MysqlMetastoreConfig, validate_index_id_pattern,
};
use quickwit_proto::ingest::{Shard, ShardState};
use quickwit_proto::metastore::{
    AcquireShardsRequest, AcquireShardsResponse, AddSourceRequest, CreateIndexRequest,
    CreateIndexResponse, CreateIndexTemplateRequest, DeleteIndexRequest,
    DeleteIndexTemplatesRequest, DeleteQuery, DeleteShardsRequest, DeleteShardsResponse,
    DeleteSourceRequest, DeleteSplitsRequest, DeleteTask, EmptyResponse, EntityKind,
    FindIndexTemplateMatchesRequest, FindIndexTemplateMatchesResponse, GetClusterIdentityRequest,
    GetClusterIdentityResponse, GetIndexTemplateRequest, GetIndexTemplateResponse,
    IndexMetadataFailure, IndexMetadataFailureReason, IndexMetadataRequest, IndexMetadataResponse,
    IndexStats, IndexTemplateMatch, IndexesMetadataRequest, IndexesMetadataResponse,
    LastDeleteOpstampRequest, LastDeleteOpstampResponse, ListDeleteTasksRequest,
    ListDeleteTasksResponse, ListIndexStatsRequest, ListIndexStatsResponse,
    ListIndexTemplatesRequest, ListIndexTemplatesResponse, ListIndexesMetadataRequest,
    ListIndexesMetadataResponse, ListShardsRequest, ListShardsResponse, ListShardsSubresponse,
    ListSplitsRequest, ListSplitsResponse, ListStaleSplitsRequest, MarkSplitsForDeletionRequest,
    MetastoreError, MetastoreResult, MetastoreService, MetastoreServiceStream, OpenShardSubrequest,
    OpenShardSubresponse, OpenShardsRequest, OpenShardsResponse, PruneShardsRequest,
    PublishSplitsRequest, ResetSourceCheckpointRequest, SplitStats, StageSplitsRequest,
    ToggleSourceRequest, UpdateIndexRequest, UpdateSourceRequest, UpdateSplitsDeleteOpstampRequest,
    UpdateSplitsDeleteOpstampResponse, serde_utils,
};
use quickwit_proto::types::{IndexId, IndexUid, Position, PublishToken, ShardId, SourceId};
use sea_query::{Asterisk, Expr, MysqlQueryBuilder, Query, UnionType};
use sea_query_binder::SqlxBinder;
use sqlx::{Acquire, Executor, MySql, Transaction};
use time::OffsetDateTime;
use tracing::{debug, info, instrument, warn};
use uuid::Uuid;

use super::error::convert_sqlx_err;
use super::migrator::run_migrations;
use super::model::{
    MysqlDeleteTask, MysqlIndex, MysqlIndexTemplate, MysqlShard, MysqlSplit, Splits,
};
use super::pool::TrackedPool;
use super::split_stream::SplitStream;
use super::utils::{append_query_filters_and_order_by, establish_connection};
use super::{
    QW_MYSQL_READ_ONLY_ENV_KEY, QW_MYSQL_SKIP_MIGRATION_LOCKING_ENV_KEY,
    QW_MYSQL_SKIP_MIGRATIONS_ENV_KEY,
};
use crate::checkpoint::{
    IndexCheckpointDelta, PartitionId, SourceCheckpoint, SourceCheckpointDelta,
};
use crate::file_backed::MutationOccurred;
use crate::metastore::mysql::model::Shards;
use crate::metastore::mysql::utils::split_maturity_timestamp;
use crate::metastore::{
    IndexesMetadataResponseExt, PublishSplitsRequestExt, STREAM_SPLITS_CHUNK_SIZE,
    UpdateSourceRequestExt, use_shard_api,
};
use crate::{
    AddSourceRequestExt, CreateIndexRequestExt, IndexMetadata, IndexMetadataResponseExt,
    ListIndexesMetadataResponseExt, ListSplitsRequestExt, ListSplitsResponseExt,
    MetastoreServiceExt, Split, SplitState, StageSplitsRequestExt, UpdateIndexRequestExt,
};

/// MySQL metastore implementation.
#[derive(Clone)]
pub struct MysqlMetastore {
    uri: Uri,
    connection_pool: TrackedPool<MySql>,
}

impl fmt::Debug for MysqlMetastore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MysqlMetastore")
            .field("uri", &self.uri)
            .finish()
    }
}

impl MysqlMetastore {
    /// Creates a metastore given a database URI.
    pub async fn new(
        mysql_metastore_config: &MysqlMetastoreConfig,
        connection_uri: &Uri,
    ) -> MetastoreResult<Self> {
        let min_connections = mysql_metastore_config.min_connections;
        let max_connections = mysql_metastore_config.max_connections.get();
        let acquire_timeout = mysql_metastore_config
            .acquire_connection_timeout()
            .expect("MySQL metastore config should have been validated");
        let idle_timeout_opt = mysql_metastore_config
            .idle_connection_timeout_opt()
            .expect("MySQL metastore config should have been validated");
        let max_lifetime_opt = mysql_metastore_config
            .max_connection_lifetime_opt()
            .expect("MySQL metastore config should have been validated");

        let read_only = get_bool_from_env(QW_MYSQL_READ_ONLY_ENV_KEY, false);
        let skip_migrations = get_bool_from_env(QW_MYSQL_SKIP_MIGRATIONS_ENV_KEY, false);
        let skip_locking = get_bool_from_env(QW_MYSQL_SKIP_MIGRATION_LOCKING_ENV_KEY, false);

        let connection_pool = establish_connection(
            connection_uri,
            min_connections,
            max_connections,
            acquire_timeout,
            idle_timeout_opt,
            max_lifetime_opt,
            read_only,
            &mysql_metastore_config.auth_mode,
            mysql_metastore_config.aws_region.as_deref(),
        )
        .await?;

        run_migrations(&connection_pool, skip_migrations, skip_locking).await?;

        let metastore = MysqlMetastore {
            uri: connection_uri.clone(),
            connection_pool,
        };
        Ok(metastore)
    }
}

/// Returns an Index object given an index_id or None if it does not exist.
async fn index_opt<'a, E>(
    executor: E,
    index_id: &str,
    lock: bool,
) -> MetastoreResult<Option<MysqlIndex>>
where
    E: sqlx::Executor<'a, Database = MySql>,
{
    let index_opt: Option<MysqlIndex> = sqlx::query_as::<_, MysqlIndex>(&format!(
        r#"
        SELECT *
        FROM indexes
        WHERE index_id = ?
        {}
        "#,
        if lock { "FOR UPDATE" } else { "" }
    ))
    .bind(index_id)
    .fetch_optional(executor)
    .await?;
    Ok(index_opt)
}

/// Returns an Index object given an index_uid or None if it does not exist.
async fn index_opt_for_uid<'a, E>(
    executor: E,
    index_uid: IndexUid,
    lock: bool,
) -> MetastoreResult<Option<MysqlIndex>>
where
    E: sqlx::Executor<'a, Database = MySql>,
{
    let index_opt: Option<MysqlIndex> = sqlx::query_as::<_, MysqlIndex>(&format!(
        r#"
        SELECT *
        FROM indexes
        WHERE index_uid = ?
        {}
        "#,
        if lock { "FOR UPDATE" } else { "" }
    ))
    .bind(&index_uid)
    .fetch_optional(executor)
    .await?;
    Ok(index_opt)
}

async fn index_metadata(
    tx: &mut Transaction<'_, MySql>,
    index_id: &str,
    lock: bool,
) -> MetastoreResult<IndexMetadata> {
    index_opt(tx.as_mut(), index_id, lock)
        .await?
        .ok_or_else(|| {
            MetastoreError::NotFound(EntityKind::Index {
                index_id: index_id.to_string(),
            })
        })?
        .index_metadata()
}

async fn try_apply_delta_v2(
    tx: &mut Transaction<'_, MySql>,
    index_uid: &IndexUid,
    source_id: &SourceId,
    checkpoint_delta: SourceCheckpointDelta,
    publish_token: PublishToken,
) -> MetastoreResult<()> {
    let num_partitions = checkpoint_delta.num_partitions();
    let shard_ids: Vec<String> = checkpoint_delta
        .partitions()
        .map(|partition_id| partition_id.to_string())
        .collect();

    if shard_ids.is_empty() {
        return Ok(());
    }

    let placeholders = vec!["?"; shard_ids.len()].join(", ");
    let sql = format!(
        r#"
        SELECT
            shard_id, publish_position_inclusive, publish_token
        FROM
            shards
        WHERE
            index_uid = ?
            AND source_id = ?
            AND shard_id IN ({placeholders})
        FOR UPDATE
        "#,
    );
    let mut query = sqlx::query_as::<_, (String, String, Option<PublishToken>)>(&sql)
        .bind(index_uid)
        .bind(source_id);
    for shard_id in &shard_ids {
        query = query.bind(shard_id);
    }
    let shards: Vec<(String, String, Option<PublishToken>)> = query.fetch_all(tx.as_mut()).await?;

    if shards.len() != num_partitions {
        let queue_id = format!("{index_uid}/{source_id}");
        let entity_kind = EntityKind::Shard { queue_id };
        return Err(MetastoreError::NotFound(entity_kind));
    }
    let mut current_checkpoint = SourceCheckpoint::default();

    for (shard_id, current_position, current_publish_token_opt) in shards {
        if current_publish_token_opt.is_none()
            || current_publish_token_opt.unwrap() != publish_token
        {
            let message = "failed to apply checkpoint delta: invalid publish token".to_string();
            return Err(MetastoreError::InvalidArgument { message });
        }
        let partition_id = PartitionId::from(shard_id);
        let current_position = Position::from(current_position);
        current_checkpoint.add_partition(partition_id, current_position);
    }
    current_checkpoint
        .try_apply_delta(checkpoint_delta)
        .map_err(|error| MetastoreError::InvalidArgument {
            message: error.to_string(),
        })?;

    let now = OffsetDateTime::now_utc();

    for (partition_id, new_position) in current_checkpoint.iter() {
        let shard_id = partition_id.to_string();
        let new_position = new_position.to_string();
        sqlx::query(
            r#"
            UPDATE shards
            SET
                publish_position_inclusive = ?,
                shard_state = CASE WHEN ? LIKE '~%' THEN 'closed' ELSE shard_state END,
                update_timestamp = ?
            WHERE
                index_uid = ?
                AND source_id = ?
                AND shard_id = ?
            "#,
        )
        .bind(&new_position)
        .bind(&new_position)
        .bind(now)
        .bind(index_uid)
        .bind(source_id)
        .bind(&shard_id)
        .execute(tx.as_mut())
        .await?;
    }
    Ok(())
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
    ($connection_pool:expr, $tx_refmut:ident, $label:literal, $x:block) => {{
        let mut tx: Transaction<'_, MySql> = $connection_pool.begin().await?;
        let $tx_refmut = &mut tx;
        let op_fut = move || async move { $x };
        let op_result: MetastoreResult<_> = op_fut().await;
        match &op_result {
            Ok(_) => {
                debug!("committing transaction");
                tx.commit().await?;
            }
            Err(error) => {
                rate_limited_error!(limit_per_min = 60, error=%error, "failed to {}, rolling transaction back", $label);
                tx.rollback().await?;
            }
        }
        op_result
    }};
}

async fn mutate_index_metadata<E, M>(
    tx: &mut Transaction<'_, MySql>,
    index_uid: IndexUid,
    mutate_fn: M,
) -> MetastoreResult<IndexMetadata>
where
    MetastoreError: From<E>,
    M: FnOnce(&mut IndexMetadata) -> Result<MutationOccurred<()>, E>,
{
    let index_id = &index_uid.index_id;
    let mut index_metadata = index_metadata(tx, index_id, true).await?;

    if index_metadata.index_uid != index_uid {
        return Err(MetastoreError::NotFound(EntityKind::Index {
            index_id: index_id.to_string(),
        }));
    }
    if let MutationOccurred::No(()) = mutate_fn(&mut index_metadata)? {
        return Ok(index_metadata);
    }
    let index_metadata_json = serde_utils::to_json_str(&index_metadata)?;

    let update_index_res = sqlx::query(
        r#"
        UPDATE indexes
        SET index_metadata_json = ?
        WHERE index_uid = ?
        "#,
    )
    .bind(index_metadata_json)
    .bind(&index_uid)
    .execute(tx.as_mut())
    .await?;
    if update_index_res.rows_affected() == 0 {
        return Err(MetastoreError::NotFound(EntityKind::Index {
            index_id: index_id.to_string(),
        }));
    }
    Ok(index_metadata)
}

#[async_trait]
impl MetastoreService for MysqlMetastore {
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        self.connection_pool.acquire().await?;
        Ok(())
    }

    fn endpoints(&self) -> Vec<quickwit_common::uri::Uri> {
        vec![self.uri.clone()]
    }

    // Index API

    #[instrument(skip(self))]
    async fn create_index(
        &self,
        request: CreateIndexRequest,
    ) -> MetastoreResult<CreateIndexResponse> {
        let index_config = request.deserialize_index_config()?;
        let mut index_metadata = IndexMetadata::new(index_config);

        let source_configs = request.deserialize_source_configs()?;

        for source_config in source_configs {
            index_metadata.add_source(source_config)?;
        }
        let index_metadata_json = serde_utils::to_json_str(&index_metadata)?;

        sqlx::query(
            "INSERT INTO indexes (index_uid, index_id, index_metadata_json) VALUES (?, ?, ?)",
        )
        .bind(index_metadata.index_uid.to_string())
        .bind(&index_metadata.index_uid.index_id)
        .bind(&index_metadata_json)
        .execute(&self.connection_pool)
        .await
        .map_err(|sqlx_error| convert_sqlx_err(index_metadata.index_id(), sqlx_error))?;

        let response = CreateIndexResponse {
            index_uid: index_metadata.index_uid.into(),
            index_metadata_json,
        };
        Ok(response)
    }

    async fn update_index(
        &self,
        request: UpdateIndexRequest,
    ) -> MetastoreResult<IndexMetadataResponse> {
        let doc_mapping = request.deserialize_doc_mapping()?;
        let indexing_settings = request.deserialize_indexing_settings()?;
        let ingest_settings = request.deserialize_ingest_settings()?;
        let search_settings = request.deserialize_search_settings()?;
        let retention_policy_opt = request.deserialize_retention_policy()?;

        let index_uid: IndexUid = request.index_uid().clone();
        let updated_index_metadata = run_with_tx!(self.connection_pool, tx, "update index", {
            mutate_index_metadata::<MetastoreError, _>(tx, index_uid, |index_metadata| {
                let mutation_occurred = index_metadata.update_index_config(
                    doc_mapping,
                    indexing_settings,
                    ingest_settings,
                    search_settings,
                    retention_policy_opt,
                )?;
                Ok(MutationOccurred::from(mutation_occurred))
            })
            .await
        })?;
        IndexMetadataResponse::try_from_index_metadata(&updated_index_metadata)
    }

    #[instrument(skip(self))]
    async fn index_metadata(
        &self,
        request: IndexMetadataRequest,
    ) -> MetastoreResult<IndexMetadataResponse> {
        let mysql_index_opt = if let Some(index_uid) = &request.index_uid {
            index_opt_for_uid(&self.connection_pool, index_uid.clone(), false).await?
        } else if let Some(index_id) = &request.index_id {
            index_opt(&self.connection_pool, index_id, false).await?
        } else {
            let message = "invalid request: neither `index_id` nor `index_uid` is set".to_string();
            return Err(MetastoreError::Internal {
                message,
                cause: "".to_string(),
            });
        };
        let index_metadata = mysql_index_opt
            .ok_or(MetastoreError::NotFound(EntityKind::Index {
                index_id: request
                    .into_index_id()
                    .expect("`index_id` or `index_uid` should be set"),
            }))?
            .index_metadata()?;
        let response = IndexMetadataResponse::try_from_index_metadata(&index_metadata)?;
        Ok(response)
    }

    #[instrument(skip(self))]
    async fn indexes_metadata(
        &self,
        request: IndexesMetadataRequest,
    ) -> MetastoreResult<IndexesMetadataResponse> {
        let num_subrequests = request.subrequests.len();

        if num_subrequests == 0 {
            return Ok(Default::default());
        }
        let mut index_ids: Vec<IndexId> = Vec::new();
        let mut index_uids: Vec<IndexUid> = Vec::with_capacity(num_subrequests);
        let mut failures: Vec<IndexMetadataFailure> = Vec::new();

        for subrequest in request.subrequests {
            if let Some(index_id) = subrequest.index_id {
                index_ids.push(index_id);
            } else if let Some(index_uid) = subrequest.index_uid {
                index_uids.push(index_uid);
            } else {
                let failure = IndexMetadataFailure {
                    index_id: subrequest.index_id,
                    index_uid: subrequest.index_uid,
                    reason: IndexMetadataFailureReason::Internal as i32,
                };
                failures.push(failure);
            }
        }

        // Build dynamic query with IN (?, ?, ...) for both index_ids and index_uids
        let mut conditions = Vec::new();
        let mut bind_values: Vec<String> = Vec::new();

        if !index_ids.is_empty() {
            let placeholders = vec!["?"; index_ids.len()].join(", ");
            conditions.push(format!("index_id IN ({placeholders})"));
            bind_values.extend(index_ids.iter().cloned());
        }
        if !index_uids.is_empty() {
            let placeholders = vec!["?"; index_uids.len()].join(", ");
            conditions.push(format!("index_uid IN ({placeholders})"));
            bind_values.extend(index_uids.iter().map(|uid| uid.to_string()));
        }

        let sql = format!("SELECT * FROM indexes WHERE {}", conditions.join(" OR "));
        let mut query = sqlx::query_as::<_, MysqlIndex>(&sql);
        for val in &bind_values {
            query = query.bind(val);
        }
        let mysql_indexes: Vec<MysqlIndex> = query.fetch_all(&self.connection_pool).await?;

        let indexes_metadata: Vec<IndexMetadata> = mysql_indexes
            .iter()
            .map(|mysql_index| mysql_index.index_metadata())
            .collect::<MetastoreResult<_>>()?;

        if mysql_indexes.len() + failures.len() < num_subrequests {
            for index_id in index_ids {
                if mysql_indexes
                    .iter()
                    .all(|mysql_index| mysql_index.index_id != index_id)
                {
                    let failure = IndexMetadataFailure {
                        index_id: Some(index_id),
                        index_uid: None,
                        reason: IndexMetadataFailureReason::NotFound as i32,
                    };
                    failures.push(failure);
                }
            }
            for index_uid in index_uids {
                if mysql_indexes
                    .iter()
                    .all(|mysql_index| mysql_index.index_uid != index_uid)
                {
                    let failure = IndexMetadataFailure {
                        index_id: None,
                        index_uid: Some(index_uid),
                        reason: IndexMetadataFailureReason::NotFound as i32,
                    };
                    failures.push(failure);
                }
            }
        }
        let response =
            IndexesMetadataResponse::try_from_indexes_metadata(indexes_metadata, failures).await?;
        Ok(response)
    }

    #[instrument(skip(self))]
    async fn list_indexes_metadata(
        &self,
        request: ListIndexesMetadataRequest,
    ) -> MetastoreResult<ListIndexesMetadataResponse> {
        let sql =
            build_index_id_patterns_sql_query(&request.index_id_patterns).map_err(|error| {
                MetastoreError::Internal {
                    message: "failed to build `list_indexes_metadata` SQL query".to_string(),
                    cause: error.to_string(),
                }
            })?;
        let mysql_indexes = sqlx::query_as::<_, MysqlIndex>(&sql)
            .fetch_all(&self.connection_pool)
            .await?;
        let indexes_metadata: Vec<IndexMetadata> = mysql_indexes
            .into_iter()
            .map(|mysql_index| mysql_index.index_metadata())
            .collect::<MetastoreResult<_>>()?;
        let response =
            ListIndexesMetadataResponse::try_from_indexes_metadata(indexes_metadata).await?;
        Ok(response)
    }

    #[instrument(skip_all, fields(index_id=%request.index_uid()))]
    async fn delete_index(&self, request: DeleteIndexRequest) -> MetastoreResult<EmptyResponse> {
        let index_uid: IndexUid = request.index_uid().clone();
        let delete_result = sqlx::query("DELETE FROM indexes WHERE index_uid = ?")
            .bind(&index_uid)
            .execute(&self.connection_pool)
            .await?;
        if delete_result.rows_affected() == 0 {
            return Err(MetastoreError::NotFound(EntityKind::Index {
                index_id: index_uid.index_id,
            }));
        }
        info!(index_id = index_uid.index_id, "deleted index successfully");
        Ok(EmptyResponse {})
    }

    // Split Lifecycle

    #[instrument(skip_all, fields(split_ids))]
    async fn stage_splits(&self, request: StageSplitsRequest) -> MetastoreResult<EmptyResponse> {
        let index_uid: IndexUid = request.index_uid().clone();
        let splits_metadata = request.deserialize_splits_metadata()?;

        if splits_metadata.is_empty() {
            return Ok(Default::default());
        }

        let mut split_ids = Vec::with_capacity(splits_metadata.len());
        let mut time_range_start_list = Vec::with_capacity(splits_metadata.len());
        let mut time_range_end_list = Vec::with_capacity(splits_metadata.len());
        let mut tags_list = Vec::with_capacity(splits_metadata.len());
        let mut splits_metadata_json = Vec::with_capacity(splits_metadata.len());
        let mut delete_opstamps = Vec::with_capacity(splits_metadata.len());
        let mut maturity_timestamps = Vec::with_capacity(splits_metadata.len());
        let mut node_ids = Vec::with_capacity(splits_metadata.len());

        for split_metadata in splits_metadata {
            let split_metadata_json = serde_utils::to_json_str(&split_metadata)?;
            splits_metadata_json.push(split_metadata_json);

            let time_range_start = split_metadata
                .time_range
                .as_ref()
                .map(|range| *range.start());
            time_range_start_list.push(time_range_start);
            maturity_timestamps.push(split_maturity_timestamp(&split_metadata));

            let time_range_end = split_metadata.time_range.map(|range| *range.end());
            time_range_end_list.push(time_range_end);

            let tags: Vec<String> = split_metadata.tags.into_iter().collect();
            tags_list.push(tags);
            split_ids.push(split_metadata.split_id);
            delete_opstamps.push(split_metadata.delete_opstamp as i64);
            node_ids.push(split_metadata.node_id);
        }
        tracing::Span::current().record("split_ids", format!("{split_ids:?}"));

        run_with_tx!(self.connection_pool, tx, "stage splits", {
            let split_state_str = SplitState::Staged.as_str();

            // Lock the index row first to ensure consistent lock ordering
            // (indexes → splits) with publish_splits, preventing deadlocks.
            let _ = index_opt_for_uid(tx.as_mut(), index_uid.clone(), true)
                .await?
                .ok_or_else(|| {
                    MetastoreError::NotFound(EntityKind::Index {
                        index_id: index_uid.index_id.to_string(),
                    })
                })?;

            // Check if any of the splits exist in a non-Staged state.
            let placeholders = vec!["?"; split_ids.len()].join(", ");
            let check_sql = format!(
                "SELECT split_id FROM splits WHERE index_uid = ? AND split_id IN ({placeholders}) \
                 AND split_state != ? FOR UPDATE"
            );
            let mut check_query = sqlx::query_as::<_, (String,)>(&check_sql).bind(&index_uid);
            for split_id in &split_ids {
                check_query = check_query.bind(split_id);
            }
            check_query = check_query.bind(split_state_str);
            let non_staged: Vec<(String,)> = check_query.fetch_all(tx.as_mut()).await?;

            if !non_staged.is_empty() {
                let failed_split_ids: Vec<String> =
                    non_staged.into_iter().map(|(id,)| id).collect();
                let entity = EntityKind::Splits {
                    split_ids: failed_split_ids,
                };
                let message = "splits are not staged".to_string();
                return Err(MetastoreError::FailedPrecondition { entity, message });
            }

            for idx in 0..split_ids.len() {
                let tags_json = serde_json::to_string(&tags_list[idx]).map_err(|error| {
                    MetastoreError::Internal {
                        message: "failed to serialize tags".to_string(),
                        cause: error.to_string(),
                    }
                })?;

                sqlx::query(
                    r#"
                    INSERT INTO splits
                        (split_id, time_range_start, time_range_end, tags,
                         split_metadata_json, delete_opstamp, maturity_timestamp,
                         split_state, index_uid, node_id)
                    VALUES (?, ?, ?, ?, ?, ?, FROM_UNIXTIME(?), ?, ?, ?)
                        AS new
                    ON DUPLICATE KEY UPDATE
                        time_range_start = new.time_range_start,
                        time_range_end = new.time_range_end,
                        tags = new.tags,
                        split_metadata_json = new.split_metadata_json,
                        delete_opstamp = new.delete_opstamp,
                        maturity_timestamp = new.maturity_timestamp,
                        node_id = new.node_id,
                        update_timestamp = UTC_TIMESTAMP(),
                        create_timestamp = UTC_TIMESTAMP()
                    "#,
                )
                .bind(&split_ids[idx])
                .bind(time_range_start_list[idx])
                .bind(time_range_end_list[idx])
                .bind(&tags_json)
                .bind(&splits_metadata_json[idx])
                .bind(delete_opstamps[idx])
                .bind(maturity_timestamps[idx])
                .bind(split_state_str)
                .bind(&index_uid)
                .bind(&node_ids[idx])
                .execute(tx.as_mut())
                .await
                .map_err(|sqlx_error| convert_sqlx_err(&index_uid.index_id, sqlx_error))?;
            }

            info!(
                %index_uid,
                "staged `{}` splits successfully", split_ids.len()
            );
            Ok(EmptyResponse {})
        })
    }

    #[instrument(skip(self))]
    async fn publish_splits(
        &self,
        request: PublishSplitsRequest,
    ) -> MetastoreResult<EmptyResponse> {
        let checkpoint_delta_opt: Option<IndexCheckpointDelta> =
            request.deserialize_index_checkpoint()?;
        let index_uid: IndexUid = request.index_uid().clone();
        let staged_split_ids = request.staged_split_ids;
        let replaced_split_ids = request.replaced_split_ids;

        run_with_tx!(self.connection_pool, tx, "publish splits", {
            let mut index_metadata = index_metadata(tx, &index_uid.index_id, true).await?;
            if index_metadata.index_uid != index_uid {
                return Err(MetastoreError::NotFound(EntityKind::Index {
                    index_id: index_uid.index_id,
                }));
            }
            if let Some(checkpoint_delta) = checkpoint_delta_opt {
                let source_id = checkpoint_delta.source_id.clone();
                let source = index_metadata.sources.get(&source_id).ok_or_else(|| {
                    MetastoreError::NotFound(EntityKind::Source {
                        index_id: index_uid.index_id.to_string(),
                        source_id: source_id.to_string(),
                    })
                })?;

                if use_shard_api(&source.source_params) {
                    let publish_token = request.publish_token_opt.ok_or_else(|| {
                        let message = format!(
                            "publish token is required for publishing splits for source \
                             `{source_id}`"
                        );
                        MetastoreError::InvalidArgument { message }
                    })?;
                    try_apply_delta_v2(
                        tx,
                        &index_uid,
                        &source_id,
                        checkpoint_delta.source_delta,
                        publish_token,
                    )
                    .await?;
                } else {
                    index_metadata
                        .checkpoint
                        .try_apply_delta(checkpoint_delta)
                        .map_err(|error| {
                            let entity = EntityKind::CheckpointDelta {
                                index_id: index_uid.index_id.to_string(),
                                source_id,
                            };
                            let message = error.to_string();
                            MetastoreError::FailedPrecondition { entity, message }
                        })?;
                }
            }
            let index_metadata_json = serde_utils::to_json_str(&index_metadata)?;

            // Collect all split_ids we need to validate, lock them.
            let all_split_ids: Vec<&str> = staged_split_ids
                .iter()
                .chain(replaced_split_ids.iter())
                .map(|s| s.as_str())
                .collect();

            if !all_split_ids.is_empty() {
                let placeholders = vec!["?"; all_split_ids.len()].join(", ");
                let select_sql = format!(
                    "SELECT split_id, split_state FROM splits WHERE index_uid = ? AND split_id IN \
                     ({placeholders}) FOR UPDATE"
                );
                let mut select_query =
                    sqlx::query_as::<_, (String, String)>(&select_sql).bind(&index_uid);
                for split_id in &all_split_ids {
                    select_query = select_query.bind(split_id);
                }
                let found_splits: Vec<(String, String)> =
                    select_query.fetch_all(tx.as_mut()).await?;

                let found_map: HashMap<&str, &str> = found_splits
                    .iter()
                    .map(|(id, state)| (id.as_str(), state.as_str()))
                    .collect();

                // Check for missing splits.
                let not_found_split_ids: Vec<String> = all_split_ids
                    .iter()
                    .filter(|id| !found_map.contains_key(**id))
                    .map(|id| id.to_string())
                    .unique()
                    .collect();
                if !not_found_split_ids.is_empty() {
                    return Err(MetastoreError::NotFound(EntityKind::Splits {
                        split_ids: not_found_split_ids,
                    }));
                }

                // Validate staged splits are in 'Staged' state.
                let not_staged_split_ids: Vec<String> = staged_split_ids
                    .iter()
                    .filter(|id| found_map.get(id.as_str()) != Some(&"Staged"))
                    .cloned()
                    .unique()
                    .collect();
                if !not_staged_split_ids.is_empty() {
                    let entity = EntityKind::Splits {
                        split_ids: not_staged_split_ids,
                    };
                    let message = "splits are not staged".to_string();
                    return Err(MetastoreError::FailedPrecondition { entity, message });
                }

                // Validate replaced splits are in 'Published' state.
                let not_published_split_ids: Vec<String> = replaced_split_ids
                    .iter()
                    .filter(|id| found_map.get(id.as_str()) != Some(&"Published"))
                    .cloned()
                    .unique()
                    .collect();
                if !not_published_split_ids.is_empty() {
                    let entity = EntityKind::Splits {
                        split_ids: not_published_split_ids,
                    };
                    let message = "splits are not marked for deletion".to_string();
                    return Err(MetastoreError::FailedPrecondition { entity, message });
                }
            }

            // Update index metadata (checkpoint).
            sqlx::query("UPDATE indexes SET index_metadata_json = ? WHERE index_uid = ?")
                .bind(&index_metadata_json)
                .bind(&index_uid)
                .execute(tx.as_mut())
                .await?;

            // Publish staged splits.
            if !staged_split_ids.is_empty() {
                let placeholders = vec!["?"; staged_split_ids.len()].join(", ");
                let update_sql = format!(
                    "UPDATE splits SET split_state = 'Published', update_timestamp = \
                     UTC_TIMESTAMP(), publish_timestamp = UTC_TIMESTAMP() WHERE index_uid = ? AND \
                     split_id IN ({placeholders}) AND split_state = 'Staged'"
                );
                let mut update_query = sqlx::query(&update_sql).bind(&index_uid);
                for split_id in &staged_split_ids {
                    update_query = update_query.bind(split_id);
                }
                let num_published_splits = update_query.execute(tx.as_mut()).await?.rows_affected();

                // Mark replaced splits for deletion.
                if !replaced_split_ids.is_empty() {
                    let placeholders = vec!["?"; replaced_split_ids.len()].join(", ");
                    let mark_sql = format!(
                        "UPDATE splits SET split_state = 'MarkedForDeletion', update_timestamp = \
                         UTC_TIMESTAMP() WHERE index_uid = ? AND split_id IN ({placeholders}) AND \
                         split_state = 'Published'"
                    );
                    let mut mark_query = sqlx::query(&mark_sql).bind(&index_uid);
                    for split_id in &replaced_split_ids {
                        mark_query = mark_query.bind(split_id);
                    }
                    let num_marked_splits = mark_query.execute(tx.as_mut()).await?.rows_affected();
                    info!(
                        %index_uid,
                        "published {num_published_splits} splits and marked {num_marked_splits} for deletion successfully"
                    );
                } else {
                    info!(
                        %index_uid,
                        "published {num_published_splits} splits successfully"
                    );
                }
            } else if !replaced_split_ids.is_empty() {
                let placeholders = vec!["?"; replaced_split_ids.len()].join(", ");
                let mark_sql = format!(
                    "UPDATE splits SET split_state = 'MarkedForDeletion', update_timestamp = \
                     UTC_TIMESTAMP() WHERE index_uid = ? AND split_id IN ({placeholders}) AND \
                     split_state = 'Published'"
                );
                let mut mark_query = sqlx::query(&mark_sql).bind(&index_uid);
                for split_id in &replaced_split_ids {
                    mark_query = mark_query.bind(split_id);
                }
                let num_marked_splits = mark_query.execute(tx.as_mut()).await?.rows_affected();
                info!(
                    %index_uid,
                    "published 0 splits and marked {num_marked_splits} for deletion successfully"
                );
            }
            Ok(EmptyResponse {})
        })
    }

    #[instrument(skip(self))]
    async fn list_splits(
        &self,
        request: ListSplitsRequest,
    ) -> MetastoreResult<MetastoreServiceStream<ListSplitsResponse>> {
        let list_splits_query = request.deserialize_list_splits_query()?;
        let mut sql_query_builder = Query::select();
        sql_query_builder.column(Asterisk).from(Splits::Table);
        append_query_filters_and_order_by(&mut sql_query_builder, &list_splits_query);

        let (sql_query, values) = sql_query_builder.build_sqlx(MysqlQueryBuilder);
        let mysql_split_stream = SplitStream::new(
            self.connection_pool.clone(),
            sql_query,
            |connection_pool: &TrackedPool<MySql>, sql_query: &String| {
                sqlx::query_as_with::<_, MysqlSplit, _>(sql_query, values).fetch(connection_pool)
            },
        );
        let split_stream =
            mysql_split_stream
                .chunks(STREAM_SPLITS_CHUNK_SIZE)
                .map(|mysql_splits_results| {
                    let mut splits = Vec::with_capacity(mysql_splits_results.len());
                    for mysql_split_result in mysql_splits_results {
                        let mysql_split = match mysql_split_result {
                            Ok(mysql_split) => mysql_split,
                            Err(error) => {
                                return Err(MetastoreError::Internal {
                                    message: "failed to fetch splits".to_string(),
                                    cause: error.to_string(),
                                });
                            }
                        };
                        let split: Split = match mysql_split.try_into() {
                            Ok(split) => split,
                            Err(error) => {
                                return Err(MetastoreError::Internal {
                                    message: "failed to convert `MysqlSplit` to `Split`"
                                        .to_string(),
                                    cause: error.to_string(),
                                });
                            }
                        };
                        splits.push(split);
                    }
                    ListSplitsResponse::try_from_splits(splits)
                });
        let service_stream = ServiceStream::new(Box::pin(split_stream));
        Ok(service_stream)
    }

    async fn list_index_stats(
        &self,
        request: ListIndexStatsRequest,
    ) -> MetastoreResult<ListIndexStatsResponse> {
        let index_pattern_sql = build_index_id_patterns_sql_query(&request.index_id_patterns)
            .map_err(|error| MetastoreError::Internal {
                message: "failed to build `list_index_stats` SQL query".to_string(),
                cause: error.to_string(),
            })?;
        let sql = format!(
            "SELECT
                i.index_uid,
                s.split_state,
                COUNT(s.split_state) AS num_splits,
                COALESCE(CAST(SUM(s.split_size_bytes) AS SIGNED), 0) AS total_size_bytes
            FROM ({index_pattern_sql}) i
            LEFT JOIN splits s ON s.index_uid = i.index_uid
            GROUP BY i.index_uid, s.split_state"
        );

        let rows: Vec<(String, Option<String>, i64, i64)> = sqlx::query_as(&sql)
            .fetch_all(&self.connection_pool)
            .await?;

        let mut index_stats = HashMap::new();
        for (index_uid_str, split_state, num_splits, total_size_bytes) in rows {
            let Ok(index_uid) = IndexUid::from_str(&index_uid_str) else {
                return Err(MetastoreError::Internal {
                    message: "failed to parse index_uid".to_string(),
                    cause: index_uid_str.to_string(),
                });
            };
            let stats = index_stats
                .entry(index_uid_str)
                .or_insert_with(|| IndexStats {
                    index_uid: Some(index_uid),
                    staged: Some(SplitStats::default()),
                    published: Some(SplitStats::default()),
                    marked_for_deletion: Some(SplitStats::default()),
                });
            let num_splits = num_splits as u64;
            let total_size_bytes = total_size_bytes as u64;
            match split_state.as_deref() {
                Some("Staged") => {
                    stats.staged = Some(SplitStats {
                        num_splits,
                        total_size_bytes,
                    });
                }
                Some("Published") => {
                    stats.published = Some(SplitStats {
                        num_splits,
                        total_size_bytes,
                    });
                }
                Some("MarkedForDeletion") => {
                    stats.marked_for_deletion = Some(SplitStats {
                        num_splits,
                        total_size_bytes,
                    });
                }
                None => {}
                Some(split_state) => {
                    return Err(MetastoreError::Internal {
                        message: "invalid split state".to_string(),
                        cause: split_state.to_string(),
                    });
                }
            }
        }

        Ok(ListIndexStatsResponse {
            index_stats: index_stats.into_values().collect(),
        })
    }

    #[instrument(skip(self))]
    async fn mark_splits_for_deletion(
        &self,
        request: MarkSplitsForDeletionRequest,
    ) -> MetastoreResult<EmptyResponse> {
        let index_uid: IndexUid = request.index_uid().clone();
        let split_ids = request.split_ids;

        if split_ids.is_empty() {
            if index_opt_for_uid(&self.connection_pool, index_uid.clone(), false)
                .await?
                .is_none()
            {
                return Err(MetastoreError::NotFound(EntityKind::Index {
                    index_id: index_uid.index_id,
                }));
            }
            return Ok(EmptyResponse {});
        }

        run_with_tx!(self.connection_pool, tx, "mark splits for deletion", {
            // Lock the candidate splits.
            let placeholders = vec!["?"; split_ids.len()].join(", ");
            let select_sql = format!(
                "SELECT split_id, split_state FROM splits WHERE index_uid = ? AND split_id IN \
                 ({placeholders}) FOR UPDATE"
            );
            let mut select_query =
                sqlx::query_as::<_, (String, String)>(&select_sql).bind(&index_uid);
            for split_id in &split_ids {
                select_query = select_query.bind(split_id);
            }
            let found_splits: Vec<(String, String)> = select_query.fetch_all(tx.as_mut()).await?;

            let num_found_splits = found_splits.len() as i64;
            let not_found_split_ids: Vec<String> = split_ids
                .iter()
                .filter(|id| found_splits.iter().all(|(fid, _)| fid != *id))
                .cloned()
                .collect();

            // Mark the staged and published splits for deletion.
            let update_sql = format!(
                "UPDATE splits SET split_state = 'MarkedForDeletion', update_timestamp = \
                 UTC_TIMESTAMP() WHERE index_uid = ? AND split_id IN ({placeholders}) AND \
                 split_state IN ('Staged', 'Published')"
            );
            let mut update_query = sqlx::query(&update_sql).bind(&index_uid);
            for split_id in &split_ids {
                update_query = update_query.bind(split_id);
            }
            let num_marked_splits = update_query.execute(tx.as_mut()).await?.rows_affected();

            if num_found_splits == 0
                && index_opt(tx.as_mut(), &index_uid.index_id, false)
                    .await?
                    .is_none()
            {
                return Err(MetastoreError::NotFound(EntityKind::Index {
                    index_id: index_uid.index_id.clone(),
                }));
            }
            info!(
                %index_uid,
                "marked {} splits for deletion, among which {} were newly marked",
                split_ids.len() - not_found_split_ids.len(),
                num_marked_splits
            );
            if !not_found_split_ids.is_empty() {
                warn!(
                    %index_uid,
                    split_ids=?PrettySample::new(&not_found_split_ids, 5),
                    "{} splits were not found and could not be marked for deletion",
                    not_found_split_ids.len()
                );
            }
            Ok(EmptyResponse {})
        })
    }

    #[instrument(skip(self))]
    async fn delete_splits(&self, request: DeleteSplitsRequest) -> MetastoreResult<EmptyResponse> {
        let index_uid: IndexUid = request.index_uid().clone();
        let split_ids = request.split_ids;

        if split_ids.is_empty() {
            if index_opt_for_uid(&self.connection_pool, index_uid.clone(), false)
                .await?
                .is_none()
            {
                return Err(MetastoreError::NotFound(EntityKind::Index {
                    index_id: index_uid.index_id,
                }));
            }
            return Ok(EmptyResponse {});
        }

        run_with_tx!(self.connection_pool, tx, "delete splits", {
            let placeholders = vec!["?"; split_ids.len()].join(", ");
            // Lock and read all candidate splits.
            let select_sql = format!(
                "SELECT split_id, split_state FROM splits WHERE index_uid = ? AND split_id IN \
                 ({placeholders}) FOR UPDATE"
            );
            let mut select_query =
                sqlx::query_as::<_, (String, String)>(&select_sql).bind(&index_uid);
            for split_id in &split_ids {
                select_query = select_query.bind(split_id);
            }
            let found_splits: Vec<(String, String)> = select_query.fetch_all(tx.as_mut()).await?;

            let num_found_splits = found_splits.len() as i64;
            let not_found_split_ids: Vec<String> = split_ids
                .iter()
                .filter(|id| found_splits.iter().all(|(fid, _)| fid != *id))
                .cloned()
                .collect();

            let not_deletable_split_ids: Vec<String> = found_splits
                .iter()
                .filter(|(_, state)| state == "Staged" || state == "Published")
                .map(|(id, _)| id.clone())
                .collect();

            if num_found_splits == 0
                && index_opt_for_uid(tx.as_mut(), index_uid.clone(), false)
                    .await?
                    .is_none()
            {
                return Err(MetastoreError::NotFound(EntityKind::Index {
                    index_id: index_uid.index_id.clone(),
                }));
            }
            if !not_deletable_split_ids.is_empty() {
                let message = format!(
                    "splits `{}` are not deletable",
                    not_deletable_split_ids.join(", ")
                );
                let entity = EntityKind::Splits {
                    split_ids: not_deletable_split_ids,
                };
                return Err(MetastoreError::FailedPrecondition { entity, message });
            }

            // Delete only MarkedForDeletion splits.
            if !split_ids.is_empty() {
                let delete_sql = format!(
                    "DELETE FROM splits WHERE index_uid = ? AND split_id IN ({placeholders}) AND \
                     split_state = 'MarkedForDeletion'"
                );
                let mut delete_query = sqlx::query(&delete_sql).bind(&index_uid);
                for split_id in &split_ids {
                    delete_query = delete_query.bind(split_id);
                }
                let num_deleted_splits = delete_query.execute(tx.as_mut()).await?.rows_affected();
                info!(%index_uid, "deleted {} splits from index", num_deleted_splits);
            }

            if !not_found_split_ids.is_empty() {
                warn!(
                    %index_uid,
                    split_ids=?PrettySample::new(&not_found_split_ids, 5),
                    "{} splits were not found and could not be deleted",
                    not_found_split_ids.len()
                );
            }
            Ok(EmptyResponse {})
        })
    }

    // Source API

    #[instrument(skip(self))]
    async fn add_source(&self, request: AddSourceRequest) -> MetastoreResult<EmptyResponse> {
        let source_config = request.deserialize_source_config()?;
        let index_uid: IndexUid = request.index_uid().clone();
        run_with_tx!(self.connection_pool, tx, "add source", {
            mutate_index_metadata::<MetastoreError, _>(tx, index_uid, |index_metadata| {
                index_metadata.add_source(source_config)?;
                Ok(MutationOccurred::Yes(()))
            })
            .await?;
            Ok(())
        })?;
        Ok(EmptyResponse {})
    }

    #[instrument(skip(self))]
    async fn update_source(&self, request: UpdateSourceRequest) -> MetastoreResult<EmptyResponse> {
        let source_config = request.deserialize_source_config()?;
        let index_uid: IndexUid = request.index_uid().clone();
        run_with_tx!(self.connection_pool, tx, "update source", {
            mutate_index_metadata::<MetastoreError, _>(tx, index_uid, |index_metadata| {
                let mutation_occurred = index_metadata.update_source(source_config)?;
                Ok(MutationOccurred::from(mutation_occurred))
            })
            .await?;
            Ok(())
        })?;
        Ok(EmptyResponse {})
    }

    #[instrument(skip(self))]
    async fn toggle_source(&self, request: ToggleSourceRequest) -> MetastoreResult<EmptyResponse> {
        let index_uid: IndexUid = request.index_uid().clone();
        run_with_tx!(self.connection_pool, tx, "toggle source", {
            mutate_index_metadata(tx, index_uid, |index_metadata| {
                if index_metadata.toggle_source(&request.source_id, request.enable)? {
                    Ok::<_, MetastoreError>(MutationOccurred::Yes(()))
                } else {
                    Ok::<_, MetastoreError>(MutationOccurred::No(()))
                }
            })
            .await?;
            Ok(())
        })?;
        Ok(EmptyResponse {})
    }

    #[instrument(skip(self))]
    async fn delete_source(&self, request: DeleteSourceRequest) -> MetastoreResult<EmptyResponse> {
        let index_uid: IndexUid = request.index_uid().clone();
        let source_id = request.source_id.clone();
        run_with_tx!(self.connection_pool, tx, "delete source", {
            mutate_index_metadata(tx, index_uid.clone(), |index_metadata| {
                index_metadata.delete_source(&source_id)?;
                Ok::<_, MetastoreError>(MutationOccurred::Yes(()))
            })
            .await?;
            sqlx::query(
                r#"
                    DELETE FROM shards
                    WHERE
                        index_uid = ?
                        AND source_id = ?
                "#,
            )
            .bind(&index_uid)
            .bind(source_id)
            .execute(tx.as_mut())
            .await?;
            Ok(())
        })?;
        Ok(EmptyResponse {})
    }

    #[instrument(skip(self))]
    async fn reset_source_checkpoint(
        &self,
        request: ResetSourceCheckpointRequest,
    ) -> MetastoreResult<EmptyResponse> {
        let index_uid: IndexUid = request.index_uid().clone();
        run_with_tx!(self.connection_pool, tx, "reset source checkpoint", {
            mutate_index_metadata(tx, index_uid, |index_metadata| {
                if index_metadata.checkpoint.reset_source(&request.source_id) {
                    Ok::<_, MetastoreError>(MutationOccurred::Yes(()))
                } else {
                    Ok::<_, MetastoreError>(MutationOccurred::No(()))
                }
            })
            .await?;
            Ok(())
        })?;
        Ok(EmptyResponse {})
    }

    // Delete Tasks API

    #[instrument(skip(self))]
    async fn last_delete_opstamp(
        &self,
        request: LastDeleteOpstampRequest,
    ) -> MetastoreResult<LastDeleteOpstampResponse> {
        let max_opstamp: i64 = sqlx::query_scalar(
            r#"
            SELECT COALESCE(MAX(opstamp), 0)
            FROM delete_tasks
            WHERE index_uid = ?
        "#,
        )
        .bind(request.index_uid())
        .fetch_one(&self.connection_pool)
        .await
        .map_err(|error| MetastoreError::Db {
            message: error.to_string(),
        })?;

        Ok(LastDeleteOpstampResponse::new(max_opstamp as u64))
    }

    /// Creates a delete task from a delete query.
    /// CRITICAL: INSERT + SELECT must use same connection (LAST_INSERT_ID is connection-scoped).
    #[instrument(skip(self))]
    async fn create_delete_task(&self, delete_query: DeleteQuery) -> MetastoreResult<DeleteTask> {
        let delete_query_json = serde_utils::to_json_str(&delete_query)?;

        let mut conn = self.connection_pool.acquire().await?;

        sqlx::query("INSERT INTO delete_tasks (index_uid, delete_query_json) VALUES (?, ?)")
            .bind(delete_query.index_uid().to_string())
            .bind(&delete_query_json)
            .execute(&mut *conn)
            .await
            .map_err(|error| convert_sqlx_err(&delete_query.index_uid().index_id, error))?;

        let (create_timestamp, opstamp): (sqlx::types::time::PrimitiveDateTime, i64) =
            sqlx::query_as(
                "SELECT create_timestamp, opstamp FROM delete_tasks WHERE opstamp = \
                 LAST_INSERT_ID()",
            )
            .fetch_one(&mut *conn)
            .await
            .map_err(|error| convert_sqlx_err(&delete_query.index_uid().index_id, error))?;

        Ok(DeleteTask {
            create_timestamp: create_timestamp.assume_utc().unix_timestamp(),
            opstamp: opstamp as u64,
            delete_query: Some(delete_query),
        })
    }

    #[instrument(skip(self))]
    async fn update_splits_delete_opstamp(
        &self,
        request: UpdateSplitsDeleteOpstampRequest,
    ) -> MetastoreResult<UpdateSplitsDeleteOpstampResponse> {
        let index_uid: IndexUid = request.index_uid().clone();
        let split_ids = request.split_ids;
        if split_ids.is_empty() {
            return Ok(UpdateSplitsDeleteOpstampResponse {});
        }
        let placeholders = vec!["?"; split_ids.len()].join(", ");
        let sql = format!(
            r#"
            UPDATE splits
            SET
                delete_opstamp = ?,
                update_timestamp = CASE
                    WHEN delete_opstamp != ? THEN UTC_TIMESTAMP()
                    ELSE update_timestamp
                END
            WHERE
                index_uid = ?
                AND split_id IN ({placeholders})
        "#,
        );
        let mut query = sqlx::query(&sql)
            .bind(request.delete_opstamp as i64)
            .bind(request.delete_opstamp as i64)
            .bind(&index_uid);
        for split_id in &split_ids {
            query = query.bind(split_id);
        }
        let update_result = query.execute(&self.connection_pool).await?;

        if update_result.rows_affected() == 0
            && index_opt_for_uid(&self.connection_pool, index_uid.clone(), false)
                .await?
                .is_none()
        {
            return Err(MetastoreError::NotFound(EntityKind::Index {
                index_id: index_uid.index_id,
            }));
        }
        Ok(UpdateSplitsDeleteOpstampResponse {})
    }

    #[instrument(skip(self))]
    async fn list_delete_tasks(
        &self,
        request: ListDeleteTasksRequest,
    ) -> MetastoreResult<ListDeleteTasksResponse> {
        let index_uid: IndexUid = request.index_uid().clone();
        let mysql_delete_tasks: Vec<MysqlDeleteTask> = sqlx::query_as::<_, MysqlDeleteTask>(
            r#"
                SELECT * FROM delete_tasks
                WHERE
                    index_uid = ?
                    AND opstamp > ?
                "#,
        )
        .bind(&index_uid)
        .bind(request.opstamp_start as i64)
        .fetch_all(&self.connection_pool)
        .await?;
        let delete_tasks: Vec<DeleteTask> = mysql_delete_tasks
            .into_iter()
            .map(|mysql_delete_task| mysql_delete_task.try_into())
            .collect::<MetastoreResult<_>>()?;
        Ok(ListDeleteTasksResponse { delete_tasks })
    }

    #[instrument(skip(self))]
    async fn list_stale_splits(
        &self,
        request: ListStaleSplitsRequest,
    ) -> MetastoreResult<ListSplitsResponse> {
        let index_uid: IndexUid = request.index_uid().clone();
        let stale_mysql_splits: Vec<MysqlSplit> = sqlx::query_as::<_, MysqlSplit>(
            r#"
                SELECT *
                FROM splits
                WHERE
                    index_uid = ?
                    AND delete_opstamp < ?
                    AND split_state = ?
                    AND (maturity_timestamp = FROM_UNIXTIME(0) OR UTC_TIMESTAMP() >= maturity_timestamp)
                ORDER BY delete_opstamp ASC, publish_timestamp ASC
                LIMIT ?
            "#,
        )
        .bind(&index_uid)
        .bind(request.delete_opstamp as i64)
        .bind(SplitState::Published.as_str())
        .bind(request.num_splits as i64)
        .fetch_all(&self.connection_pool)
        .await?;

        let stale_splits: Vec<Split> = stale_mysql_splits
            .into_iter()
            .map(|mysql_split| mysql_split.try_into())
            .collect::<MetastoreResult<_>>()?;
        let response = ListSplitsResponse::try_from_splits(stale_splits)?;
        Ok(response)
    }

    // Shard API

    async fn open_shards(&self, request: OpenShardsRequest) -> MetastoreResult<OpenShardsResponse> {
        let mut subresponses = Vec::with_capacity(request.subrequests.len());

        for subrequest in request.subrequests {
            let open_shard: Shard = open_or_fetch_shard(&self.connection_pool, &subrequest).await?;
            let subresponse = OpenShardSubresponse {
                subrequest_id: subrequest.subrequest_id,
                open_shard: Some(open_shard),
            };
            subresponses.push(subresponse);
        }
        Ok(OpenShardsResponse { subresponses })
    }

    async fn acquire_shards(
        &self,
        request: AcquireShardsRequest,
    ) -> MetastoreResult<AcquireShardsResponse> {
        if request.shard_ids.is_empty() {
            return Ok(Default::default());
        }

        // MySQL doesn't support RETURNING, so we UPDATE then SELECT.
        let placeholders = vec!["?"; request.shard_ids.len()].join(", ");

        let update_sql = format!(
            "UPDATE shards SET publish_token = ? WHERE index_uid = ? AND source_id = ? AND \
             shard_id IN ({placeholders})"
        );
        let mut update_query = sqlx::query(&update_sql)
            .bind(&request.publish_token)
            .bind(request.index_uid())
            .bind(&request.source_id);
        for shard_id in &request.shard_ids {
            update_query = update_query.bind(shard_id);
        }
        update_query.execute(&self.connection_pool).await?;

        let select_sql = format!(
            "SELECT * FROM shards WHERE index_uid = ? AND source_id = ? AND shard_id IN \
             ({placeholders})"
        );
        let mut select_query = sqlx::query_as::<_, MysqlShard>(&select_sql)
            .bind(request.index_uid())
            .bind(&request.source_id);
        for shard_id in &request.shard_ids {
            select_query = select_query.bind(shard_id);
        }
        let mysql_shards: Vec<MysqlShard> = select_query.fetch_all(&self.connection_pool).await?;

        let acquired_shards = mysql_shards
            .into_iter()
            .map(|mysql_shard| mysql_shard.into())
            .collect();
        let response = AcquireShardsResponse { acquired_shards };
        Ok(response)
    }

    async fn list_shards(&self, request: ListShardsRequest) -> MetastoreResult<ListShardsResponse> {
        if request.subrequests.is_empty() {
            return Ok(Default::default());
        }
        let mut sql_query_builder = Query::select();

        for (idx, subrequest) in request.subrequests.iter().enumerate() {
            let mut sql_subquery_builder = Query::select();

            sql_subquery_builder
                .column(Asterisk)
                .from(Shards::Table)
                .and_where(Expr::col(Shards::IndexUid).eq(subrequest.index_uid()))
                .and_where(Expr::col(Shards::SourceId).eq(&subrequest.source_id));

            let shard_state = subrequest.shard_state();

            if shard_state != ShardState::Unspecified {
                let shard_state_str = shard_state.as_json_str_name();
                // MySQL ENUM accepts string values directly — no CAST needed.
                sql_subquery_builder.and_where(Expr::col(Shards::ShardState).eq(shard_state_str));
            }
            if idx == 0 {
                sql_query_builder = sql_subquery_builder;
            } else {
                sql_query_builder.union(UnionType::All, sql_subquery_builder);
            }
        }
        let (sql_query, values) = sql_query_builder.build_sqlx(MysqlQueryBuilder);

        let mysql_shards: Vec<MysqlShard> =
            sqlx::query_as_with::<_, MysqlShard, _>(&sql_query, values)
                .fetch_all(&self.connection_pool)
                .await?;

        let mut per_source_subresponses: HashMap<(IndexUid, SourceId), ListShardsSubresponse> =
            request
                .subrequests
                .into_iter()
                .map(|subrequest| {
                    let index_uid = subrequest.index_uid().clone();
                    let source_id = subrequest.source_id.clone();
                    (
                        (index_uid, source_id),
                        ListShardsSubresponse {
                            index_uid: subrequest.index_uid,
                            source_id: subrequest.source_id,
                            shards: Vec::new(),
                        },
                    )
                })
                .collect();

        for mysql_shard in mysql_shards {
            let shard: Shard = mysql_shard.into();
            let source_key = (shard.index_uid().clone(), shard.source_id.clone());

            let Some(subresponse) = per_source_subresponses.get_mut(&source_key) else {
                warn!(
                    index_uid=%shard.index_uid(),
                    source_id=%shard.source_id,
                    "could not find source in subresponses: this should never happen, please report"
                );
                continue;
            };
            subresponse.shards.push(shard);
        }
        let subresponses = per_source_subresponses.into_values().collect();
        let response = ListShardsResponse { subresponses };
        Ok(response)
    }

    async fn delete_shards(
        &self,
        request: DeleteShardsRequest,
    ) -> MetastoreResult<DeleteShardsResponse> {
        if request.shard_ids.is_empty() {
            return Ok(Default::default());
        }

        let placeholders = vec!["?"; request.shard_ids.len()].join(", ");

        // Delete shards that are fully indexed (position starts with '~') or forced.
        let delete_sql = format!(
            "DELETE FROM shards WHERE index_uid = ? AND source_id = ? AND shard_id IN \
             ({placeholders}) AND (? OR publish_position_inclusive LIKE '~%')"
        );
        let mut delete_query = sqlx::query(&delete_sql)
            .bind(request.index_uid())
            .bind(&request.source_id);
        for shard_id in &request.shard_ids {
            delete_query = delete_query.bind(shard_id);
        }
        delete_query = delete_query.bind(request.force);
        let query_result = delete_query.execute(&self.connection_pool).await?;

        // Happy path: all shards were deleted.
        if request.force || query_result.rows_affected() == request.shard_ids.len() as u64 {
            let response = DeleteShardsResponse {
                index_uid: request.index_uid,
                source_id: request.source_id,
                successes: request.shard_ids,
                failures: Vec::new(),
            };
            return Ok(response);
        }

        // Unhappy path: find shards that are not deletable.
        let find_sql = format!(
            "SELECT * FROM shards WHERE index_uid = ? AND source_id = ? AND shard_id IN \
             ({placeholders}) AND publish_position_inclusive NOT LIKE '~%'"
        );
        let mut find_query = sqlx::query_as::<_, MysqlShard>(&find_sql)
            .bind(request.index_uid())
            .bind(&request.source_id);
        for shard_id in &request.shard_ids {
            find_query = find_query.bind(shard_id);
        }
        let not_deletable_mysql_shards: Vec<MysqlShard> =
            find_query.fetch_all(&self.connection_pool).await?;

        if not_deletable_mysql_shards.is_empty() {
            let response = DeleteShardsResponse {
                index_uid: request.index_uid,
                source_id: request.source_id,
                successes: request.shard_ids,
                failures: Vec::new(),
            };
            return Ok(response);
        }
        let failures: Vec<ShardId> = not_deletable_mysql_shards
            .into_iter()
            .map(|mysql_shard| mysql_shard.shard_id)
            .collect();
        warn!(
            index_uid=%request.index_uid(),
            source_id=%request.source_id,
            "failed to delete shards `{}`: shards are not fully indexed",
            failures.iter().join(", ")
        );
        let successes: Vec<ShardId> = request
            .shard_ids
            .into_iter()
            .filter(|shard_id| !failures.contains(shard_id))
            .collect();
        let response = DeleteShardsResponse {
            index_uid: request.index_uid,
            source_id: request.source_id,
            successes,
            failures,
        };
        Ok(response)
    }

    async fn prune_shards(&self, request: PruneShardsRequest) -> MetastoreResult<EmptyResponse> {
        const PRUNE_AGE_SHARDS_QUERY: &str = include_str!("queries/shards/prune_age.sql");
        const PRUNE_COUNT_SHARDS_QUERY: &str = include_str!("queries/shards/prune_count.sql");

        if let Some(max_age_secs) = request.max_age_secs {
            let limit_datetime =
                OffsetDateTime::now_utc() - Duration::from_secs(max_age_secs as u64);
            sqlx::query(PRUNE_AGE_SHARDS_QUERY)
                .bind(request.index_uid())
                .bind(&request.source_id)
                .bind(limit_datetime)
                .execute(&self.connection_pool)
                .await?;
        }

        if let Some(max_count) = request.max_count {
            sqlx::query(PRUNE_COUNT_SHARDS_QUERY)
                .bind(request.index_uid())
                .bind(&request.source_id)
                .bind(request.index_uid())
                .bind(&request.source_id)
                .bind(max_count as i64)
                .execute(&self.connection_pool)
                .await?;
        }
        Ok(EmptyResponse {})
    }

    // Index Template API

    async fn create_index_template(
        &self,
        request: CreateIndexTemplateRequest,
    ) -> MetastoreResult<EmptyResponse> {
        const INSERT_INDEX_TEMPLATE_QUERY: &str =
            include_str!("queries/index_templates/insert.sql");
        const UPSERT_INDEX_TEMPLATE_QUERY: &str =
            include_str!("queries/index_templates/upsert.sql");

        let index_template: IndexTemplate =
            serde_utils::from_json_str(&request.index_template_json)?;

        index_template
            .validate()
            .map_err(|error| MetastoreError::InvalidArgument {
                message: format!(
                    "invalid index template `{}`: `{error}`",
                    index_template.template_id
                ),
            })?;

        let mut positive_patterns = Vec::new();
        let mut negative_patterns = Vec::new();

        for pattern in &index_template.index_id_patterns {
            if let Some(negative_pattern) = pattern.strip_prefix('-') {
                negative_patterns.push(negative_pattern.replace('*', "%"));
            } else {
                positive_patterns.push(pattern.replace('*', "%"));
            }
        }

        let positive_json = serde_json::to_string(&positive_patterns).map_err(|error| {
            MetastoreError::Internal {
                message: "failed to serialize positive patterns".to_string(),
                cause: error.to_string(),
            }
        })?;
        let negative_json = serde_json::to_string(&negative_patterns).map_err(|error| {
            MetastoreError::Internal {
                message: "failed to serialize negative patterns".to_string(),
                cause: error.to_string(),
            }
        })?;

        if request.overwrite {
            sqlx::query(UPSERT_INDEX_TEMPLATE_QUERY)
                .bind(&index_template.template_id)
                .bind(&positive_json)
                .bind(&negative_json)
                .bind(index_template.priority as i32)
                .bind(&request.index_template_json)
                .execute(&self.connection_pool)
                .await?;

            return Ok(EmptyResponse {});
        }
        let insert_result = sqlx::query(INSERT_INDEX_TEMPLATE_QUERY)
            .bind(&index_template.template_id)
            .bind(&positive_json)
            .bind(&negative_json)
            .bind(index_template.priority as i32)
            .bind(&request.index_template_json)
            .execute(&self.connection_pool)
            .await;

        match insert_result {
            Ok(_) => Ok(EmptyResponse {}),
            Err(sqlx::Error::Database(ref db_error))
                if db_error
                    .try_downcast_ref::<sqlx::mysql::MySqlDatabaseError>()
                    .map(|err| err.number() == 1062)
                    .unwrap_or(false) =>
            {
                Err(MetastoreError::AlreadyExists(EntityKind::IndexTemplate {
                    template_id: index_template.template_id,
                }))
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn get_index_template(
        &self,
        request: GetIndexTemplateRequest,
    ) -> MetastoreResult<GetIndexTemplateResponse> {
        let mysql_index_template: MysqlIndexTemplate =
            sqlx::query_as("SELECT * FROM index_templates WHERE template_id = ?")
                .bind(&request.template_id)
                .fetch_optional(&self.connection_pool)
                .await?
                .ok_or({
                    MetastoreError::NotFound(EntityKind::IndexTemplate {
                        template_id: request.template_id,
                    })
                })?;
        let response = GetIndexTemplateResponse {
            index_template_json: mysql_index_template.index_template_json,
        };
        Ok(response)
    }

    async fn find_index_template_matches(
        &self,
        request: FindIndexTemplateMatchesRequest,
    ) -> MetastoreResult<FindIndexTemplateMatchesResponse> {
        if request.index_ids.is_empty() {
            return Ok(Default::default());
        }
        const FIND_INDEX_TEMPLATE_MATCHES_QUERY: &str =
            include_str!("queries/index_templates/find.sql");

        let index_ids_json = serde_json::to_string(&request.index_ids).map_err(|error| {
            MetastoreError::Internal {
                message: "failed to serialize index_ids".to_string(),
                cause: error.to_string(),
            }
        })?;

        let sql_matches: Vec<(IndexId, IndexTemplateId, String)> =
            sqlx::query_as(FIND_INDEX_TEMPLATE_MATCHES_QUERY)
                .bind(&index_ids_json)
                .fetch_all(&self.connection_pool)
                .await?;

        let matches = sql_matches
            .into_iter()
            .map(
                |(index_id, template_id, index_template_json)| IndexTemplateMatch {
                    index_id,
                    template_id,
                    index_template_json,
                },
            )
            .collect();
        let response = FindIndexTemplateMatchesResponse { matches };
        Ok(response)
    }

    async fn list_index_templates(
        &self,
        _request: ListIndexTemplatesRequest,
    ) -> MetastoreResult<ListIndexTemplatesResponse> {
        let mysql_index_templates_json: Vec<(String,)> = sqlx::query_as(
            "SELECT index_template_json FROM index_templates ORDER BY template_id ASC",
        )
        .fetch_all(&self.connection_pool)
        .await?;
        let index_templates_json: Vec<String> = mysql_index_templates_json
            .into_iter()
            .map(|(index_template_json,)| index_template_json)
            .collect();
        let response = ListIndexTemplatesResponse {
            index_templates_json,
        };
        Ok(response)
    }

    async fn delete_index_templates(
        &self,
        request: DeleteIndexTemplatesRequest,
    ) -> MetastoreResult<EmptyResponse> {
        if request.template_ids.is_empty() {
            return Ok(EmptyResponse {});
        }
        let placeholders = vec!["?"; request.template_ids.len()].join(", ");
        let sql = format!("DELETE FROM index_templates WHERE template_id IN ({placeholders})");
        let mut query = sqlx::query(&sql);
        for template_id in &request.template_ids {
            query = query.bind(template_id);
        }
        query.execute(&self.connection_pool).await?;
        Ok(EmptyResponse {})
    }

    // Cluster Identity

    async fn get_cluster_identity(
        &self,
        _: GetClusterIdentityRequest,
    ) -> MetastoreResult<GetClusterIdentityResponse> {
        // MySQL: INSERT + SELECT on same connection (no RETURNING).
        let mut conn = self.connection_pool.acquire().await?;
        let new_uuid = Uuid::new_v4().hyphenated().to_string();

        sqlx::query(
            r#"
            INSERT INTO kv (`key`, value)
            VALUES ('cluster_identity', ?)
            ON DUPLICATE KEY UPDATE `key` = `key`
            "#,
        )
        .bind(&new_uuid)
        .execute(&mut *conn)
        .await?;

        let (uuid,): (String,) =
            sqlx::query_as("SELECT value FROM kv WHERE `key` = 'cluster_identity'")
                .fetch_one(&mut *conn)
                .await?;

        Ok(GetClusterIdentityResponse { uuid })
    }
}

async fn open_or_fetch_shard<'e>(
    executor: impl Executor<'e, Database = MySql> + Clone,
    subrequest: &OpenShardSubrequest,
) -> MetastoreResult<Shard> {
    // MySQL: INSERT ... ON DUPLICATE KEY UPDATE pk = pk + separate SELECT
    // instead of INSERT ... ON CONFLICT DO NOTHING RETURNING *
    let insert_result = sqlx::query(
        r#"
        INSERT INTO shards(index_uid, source_id, shard_id, leader_id, follower_id,
                           doc_mapping_uid, publish_token, update_timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE index_uid = index_uid
        "#,
    )
    .bind(subrequest.index_uid())
    .bind(&subrequest.source_id)
    .bind(subrequest.shard_id().as_str())
    .bind(&subrequest.leader_id)
    .bind(&subrequest.follower_id)
    .bind(subrequest.doc_mapping_uid)
    .bind(&subrequest.publish_token)
    .bind(OffsetDateTime::now_utc())
    .execute(executor.clone())
    .await;

    match insert_result {
        Ok(result) => {
            // Fetch the shard (whether newly inserted or already existing).
            const FETCH_SHARD_QUERY: &str = include_str!("queries/shards/fetch.sql");
            let mysql_shard_opt: Option<MysqlShard> = sqlx::query_as(FETCH_SHARD_QUERY)
                .bind(subrequest.index_uid())
                .bind(&subrequest.source_id)
                .bind(subrequest.shard_id().as_str())
                .fetch_optional(executor.clone())
                .await?;

            if let Some(mysql_shard) = mysql_shard_opt {
                let shard: Shard = mysql_shard.into();
                if result.rows_affected() == 1 {
                    info!(
                        index_uid=%shard.index_uid(),
                        source_id=%shard.source_id,
                        shard_id=%shard.shard_id(),
                        leader_id=%shard.leader_id,
                        follower_id=?shard.follower_id,
                        "opened shard"
                    );
                }
                return Ok(shard);
            }
        }
        Err(sqlx_error) => {
            // FK violation means the source/index doesn't exist.
            if let sqlx::Error::Database(ref db_error) = sqlx_error
                && let Some(mysql_error) =
                    db_error.try_downcast_ref::<sqlx::mysql::MySqlDatabaseError>()
                && mysql_error.number() == 1452
            {
                return Err(MetastoreError::NotFound(EntityKind::Source {
                    index_id: subrequest.index_uid().to_string(),
                    source_id: subrequest.source_id.clone(),
                }));
            }
            return Err(sqlx_error.into());
        }
    }

    Err(MetastoreError::NotFound(EntityKind::Source {
        index_id: subrequest.index_uid().to_string(),
        source_id: subrequest.source_id.clone(),
    }))
}

impl MetastoreServiceExt for MysqlMetastore {}

/// Builds the SQL query that returns indexes matching at least one pattern in
/// `index_id_patterns`, and none of the patterns starting with '-'
fn build_index_id_patterns_sql_query(index_id_patterns: &[String]) -> anyhow::Result<String> {
    let mut positive_patterns = Vec::new();
    let mut negative_patterns = Vec::new();
    for pattern in index_id_patterns {
        if let Some(negative_pattern) = pattern.strip_prefix('-') {
            negative_patterns.push(negative_pattern.to_string());
        } else {
            positive_patterns.push(pattern);
        }
    }

    if positive_patterns.is_empty() {
        anyhow::bail!("The list of index id patterns may not be empty.");
    }

    if index_id_patterns.iter().any(|pattern| pattern == "*") && negative_patterns.is_empty() {
        return Ok("SELECT * FROM indexes".to_string());
    }

    let mut where_like_query = String::new();
    for (index_id_pattern_idx, index_id_pattern) in positive_patterns.iter().enumerate() {
        validate_index_id_pattern(index_id_pattern, false).map_err(|error| {
            MetastoreError::Internal {
                message: "failed to build list indexes query".to_string(),
                cause: error.to_string(),
            }
        })?;
        if index_id_pattern_idx != 0 {
            where_like_query.push_str(" OR ");
        }
        if index_id_pattern.contains('*') {
            let sql_pattern = index_id_pattern.replace('*', "%");
            let _ = write!(where_like_query, "index_id LIKE '{sql_pattern}'");
        } else {
            let _ = write!(where_like_query, "index_id = '{index_id_pattern}'");
        }
    }
    let mut negative_like_query = String::new();
    for index_id_pattern in negative_patterns.iter() {
        validate_index_id_pattern(index_id_pattern, false).map_err(|error| {
            MetastoreError::Internal {
                message: "failed to build list indexes query".to_string(),
                cause: error.to_string(),
            }
        })?;
        negative_like_query.push_str(" AND ");
        if index_id_pattern.contains('*') {
            let sql_pattern = index_id_pattern.replace('*', "%");
            let _ = write!(negative_like_query, "index_id NOT LIKE '{sql_pattern}'");
        } else {
            let _ = write!(negative_like_query, "index_id <> '{index_id_pattern}'");
        }
    }

    Ok(format!(
        "SELECT * FROM indexes WHERE ({where_like_query}){negative_like_query}"
    ))
}

#[cfg(test)]
#[async_trait]
impl crate::tests::DefaultForTest for MysqlMetastore {
    async fn default_for_test() -> Self {
        dotenvy::dotenv().ok();
        let uri: Uri = std::env::var("QW_TEST_MYSQL_DATABASE_URL")
            .expect("environment variable `QW_TEST_MYSQL_DATABASE_URL` should be set")
            .parse()
            .expect("environment variable `QW_TEST_MYSQL_DATABASE_URL` should be a valid URI");
        MysqlMetastore::new(&MysqlMetastoreConfig::default(), &uri)
            .await
            .expect("failed to initialize MySQL metastore test")
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use quickwit_proto::ingest::{Shard, ShardState};
    use quickwit_proto::types::{IndexUid, SourceId};
    use time::OffsetDateTime;

    use super::*;
    use crate::metastore::mysql::model::{MysqlShard, shard_state_to_str};
    use crate::metastore_test_suite;
    use crate::tests::DefaultForTest;
    use crate::tests::shard::ReadWriteShardsForTest;

    #[async_trait]
    impl ReadWriteShardsForTest for MysqlMetastore {
        async fn insert_shards(
            &self,
            index_uid: &IndexUid,
            source_id: &SourceId,
            shards: Vec<Shard>,
        ) {
            const INSERT_SHARD_QUERY: &str = include_str!("queries/shards/insert.sql");

            for shard in shards {
                assert_eq!(&shard.source_id, source_id);
                assert_eq!(shard.index_uid(), index_uid);
                let Shard {
                    doc_mapping_uid,
                    follower_id,
                    index_uid,
                    leader_id,
                    publish_position_inclusive,
                    publish_token,
                    shard_id,
                    shard_state,
                    source_id,
                    update_timestamp,
                } = shard;
                let shard_state_name = ShardState::try_from(shard_state)
                    .unwrap()
                    .as_json_str_name();
                let update_timestamp = OffsetDateTime::from_unix_timestamp(update_timestamp)
                    .expect("bad timestamp format");
                sqlx::query(INSERT_SHARD_QUERY)
                    .bind(index_uid)
                    .bind(source_id)
                    .bind(shard_id.unwrap())
                    .bind(shard_state_name)
                    .bind(leader_id)
                    .bind(follower_id)
                    .bind(doc_mapping_uid)
                    .bind(publish_position_inclusive.unwrap().to_string())
                    .bind(publish_token)
                    .bind(update_timestamp)
                    .execute(&self.connection_pool)
                    .await
                    .unwrap();
            }
        }

        async fn list_all_shards(&self, index_uid: &IndexUid, source_id: &SourceId) -> Vec<Shard> {
            let mysql_shards: Vec<MysqlShard> = sqlx::query_as(
                r#"
                SELECT *
                FROM shards
                WHERE
                    index_uid = ?
                    AND source_id = ?
                "#,
            )
            .bind(index_uid)
            .bind(source_id)
            .fetch_all(&self.connection_pool)
            .await
            .unwrap();

            mysql_shards
                .into_iter()
                .map(|mysql_shard| mysql_shard.into())
                .collect()
        }
    }

    metastore_test_suite!(crate::MysqlMetastore);
}
