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
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use itertools::Itertools;
use quickwit_common::pretty::PrettySample;
use quickwit_common::uri::Uri;
use quickwit_common::{ServiceStream, get_bool_from_env, rate_limited_error};
use quickwit_config::{
    IndexTemplate, IndexTemplateId, PostgresMetastoreConfig, validate_index_id_pattern,
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
    IndexTemplateMatch, IndexesMetadataRequest, IndexesMetadataResponse, LastDeleteOpstampRequest,
    LastDeleteOpstampResponse, ListDeleteTasksRequest, ListDeleteTasksResponse,
    ListIndexTemplatesRequest, ListIndexTemplatesResponse, ListIndexesMetadataRequest,
    ListIndexesMetadataResponse, ListShardsRequest, ListShardsResponse, ListShardsSubresponse,
    ListSplitsRequest, ListSplitsResponse, ListStaleSplitsRequest, MarkSplitsForDeletionRequest,
    MetastoreError, MetastoreResult, MetastoreService, MetastoreServiceStream, OpenShardSubrequest,
    OpenShardSubresponse, OpenShardsRequest, OpenShardsResponse, PruneShardsRequest,
    PublishSplitsRequest, ResetSourceCheckpointRequest, StageSplitsRequest, ToggleSourceRequest,
    UpdateIndexRequest, UpdateSourceRequest, UpdateSplitsDeleteOpstampRequest,
    UpdateSplitsDeleteOpstampResponse, serde_utils,
};
use quickwit_proto::types::{IndexId, IndexUid, Position, PublishToken, ShardId, SourceId};
use sea_query::{Alias, Asterisk, Expr, Func, PostgresQueryBuilder, Query, UnionType};
use sea_query_binder::SqlxBinder;
use sqlx::{Acquire, Executor, Postgres, Transaction};
use time::OffsetDateTime;
use tracing::{debug, info, instrument, warn};
use uuid::Uuid;

use super::error::convert_sqlx_err;
use super::migrator::run_migrations;
use super::model::{PgDeleteTask, PgIndex, PgIndexTemplate, PgShard, PgSplit, Splits};
use super::pool::TrackedPool;
use super::split_stream::SplitStream;
use super::utils::{append_query_filters_and_order_by, establish_connection};
use super::{
    QW_POSTGRES_READ_ONLY_ENV_KEY, QW_POSTGRES_SKIP_MIGRATION_LOCKING_ENV_KEY,
    QW_POSTGRES_SKIP_MIGRATIONS_ENV_KEY,
};
use crate::checkpoint::{
    IndexCheckpointDelta, PartitionId, SourceCheckpoint, SourceCheckpointDelta,
};
use crate::file_backed::MutationOccurred;
use crate::metastore::postgres::model::Shards;
use crate::metastore::postgres::utils::split_maturity_timestamp;
use crate::metastore::{
    IndexesMetadataResponseExt, PublishSplitsRequestExt, STREAM_SPLITS_CHUNK_SIZE,
    UpdateSourceRequestExt, use_shard_api,
};
use crate::{
    AddSourceRequestExt, CreateIndexRequestExt, IndexMetadata, IndexMetadataResponseExt,
    ListIndexesMetadataResponseExt, ListSplitsRequestExt, ListSplitsResponseExt,
    MetastoreServiceExt, Split, SplitState, StageSplitsRequestExt, UpdateIndexRequestExt,
};

/// PostgreSQL metastore implementation.
#[derive(Clone)]
pub struct PostgresqlMetastore {
    uri: Uri,
    connection_pool: TrackedPool<Postgres>,
}

impl fmt::Debug for PostgresqlMetastore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresqlMetastore")
            .field("uri", &self.uri)
            .finish()
    }
}

impl PostgresqlMetastore {
    /// Creates a metastore given a database URI.
    pub async fn new(
        postgres_metastore_config: &PostgresMetastoreConfig,
        connection_uri: &Uri,
    ) -> MetastoreResult<Self> {
        let min_connections = postgres_metastore_config.min_connections;
        let max_connections = postgres_metastore_config.max_connections.get();
        let acquire_timeout = postgres_metastore_config
            .acquire_connection_timeout()
            .expect("PostgreSQL metastore config should have been validated");
        let idle_timeout_opt = postgres_metastore_config
            .idle_connection_timeout_opt()
            .expect("PostgreSQL metastore config should have been validated");
        let max_lifetime_opt = postgres_metastore_config
            .max_connection_lifetime_opt()
            .expect("PostgreSQL metastore config should have been validated");

        let read_only = get_bool_from_env(QW_POSTGRES_READ_ONLY_ENV_KEY, false);
        let skip_migrations = get_bool_from_env(QW_POSTGRES_SKIP_MIGRATIONS_ENV_KEY, false);
        let skip_locking = get_bool_from_env(QW_POSTGRES_SKIP_MIGRATION_LOCKING_ENV_KEY, false);

        let connection_pool = establish_connection(
            connection_uri,
            min_connections,
            max_connections,
            acquire_timeout,
            idle_timeout_opt,
            max_lifetime_opt,
            read_only,
        )
        .await?;

        run_migrations(&connection_pool, skip_migrations, skip_locking).await?;

        let metastore = PostgresqlMetastore {
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
) -> MetastoreResult<Option<PgIndex>>
where
    E: sqlx::Executor<'a, Database = Postgres>,
{
    let index_opt: Option<PgIndex> = sqlx::query_as::<_, PgIndex>(&format!(
        r#"
        SELECT *
        FROM indexes
        WHERE index_id = $1
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
) -> MetastoreResult<Option<PgIndex>>
where
    E: sqlx::Executor<'a, Database = Postgres>,
{
    let index_opt: Option<PgIndex> = sqlx::query_as::<_, PgIndex>(&format!(
        r#"
        SELECT *
        FROM indexes
        WHERE index_uid = $1
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
    tx: &mut Transaction<'_, Postgres>,
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
    tx: &mut Transaction<'_, Postgres>,
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

    let shards: Vec<(String, String, Option<PublishToken>)> = sqlx::query_as(
        r#"
        SELECT
            shard_id, publish_position_inclusive, publish_token
        FROM
            shards
        WHERE
            index_uid = $1
            AND source_id = $2
            AND shard_id = ANY($3)
        FOR UPDATE
        "#,
    )
    .bind(index_uid)
    .bind(source_id)
    .bind(shard_ids)
    .fetch_all(tx.as_mut())
    .await?;

    if shards.len() != num_partitions {
        let queue_id = format!("{index_uid}/{source_id}"); // FIXME
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

    let mut shard_ids = Vec::with_capacity(num_partitions);
    let mut new_positions = Vec::with_capacity(num_partitions);

    for (partition_id, new_position) in current_checkpoint.iter() {
        let shard_id = partition_id.to_string();
        shard_ids.push(shard_id.to_string());
        new_positions.push(new_position.to_string());
    }

    sqlx::query(
        r#"
            UPDATE
                shards
            SET
                publish_position_inclusive = new_positions.position,
                shard_state = CASE WHEN new_positions.position LIKE '~%' THEN 'closed' ELSE shards.shard_state END,
                update_timestamp = $5
            FROM
                UNNEST($3, $4)
                AS new_positions(shard_id, position)
            WHERE
                index_uid = $1
                AND source_id = $2
                AND shards.shard_id = new_positions.shard_id
            "#,
    )
    .bind(index_uid)
    .bind(source_id)
    .bind(shard_ids)
    .bind(new_positions)
    // Use a timestamp generated by the metastore node to avoid clock drift issues
    .bind(OffsetDateTime::now_utc())
    .execute(tx.as_mut())
    .await?;
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
        let mut tx: Transaction<'_, Postgres> = $connection_pool.begin().await?;
        let $tx_refmut = &mut tx;
        let op_fut = move || async move { $x };
        let op_result: MetastoreResult<_> = op_fut().await;
        match &op_result {
            Ok(_) => {
                debug!("committing transaction");
                tx.commit().await?;
            }
            Err(error) => {
                rate_limited_error!(limit_per_min = 60, error=%error, "failed to {}, rolling transaction back" , $label);
                tx.rollback().await?;
            }
        }
        op_result
    }};
}

async fn mutate_index_metadata<E, M>(
    tx: &mut Transaction<'_, Postgres>,
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
        SET index_metadata_json = $1
        WHERE index_uid = $2
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
impl MetastoreService for PostgresqlMetastore {
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        self.connection_pool.acquire().await?;
        Ok(())
    }

    fn endpoints(&self) -> Vec<quickwit_common::uri::Uri> {
        vec![self.uri.clone()]
    }

    // Index API:
    // - `create_index`
    // - `update_index`
    // - `index_metadata`
    // - `indexes_metadata`
    // - `list_indexes_metadata`

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
            "INSERT INTO indexes (index_uid, index_id, index_metadata_json) VALUES ($1, $2, $3)",
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
        let pg_index_opt = if let Some(index_uid) = &request.index_uid {
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
        let index_metadata = pg_index_opt
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
        const INDEXES_METADATA_QUERY: &str = include_str!("queries/indexes_metadata.sql");

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
        let pg_indexes: Vec<PgIndex> = sqlx::query_as::<_, PgIndex>(INDEXES_METADATA_QUERY)
            .bind(&index_ids)
            .bind(&index_uids)
            .fetch_all(&self.connection_pool)
            .await?;

        let indexes_metadata: Vec<IndexMetadata> = pg_indexes
            .iter()
            .map(|pg_index| pg_index.index_metadata())
            .collect::<MetastoreResult<_>>()?;

        if pg_indexes.len() + failures.len() < num_subrequests {
            for index_id in index_ids {
                if pg_indexes
                    .iter()
                    .all(|pg_index| pg_index.index_id != index_id)
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
                if pg_indexes
                    .iter()
                    .all(|pg_index| pg_index.index_uid != index_uid)
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
        let pg_indexes = sqlx::query_as::<_, PgIndex>(&sql)
            .fetch_all(&self.connection_pool)
            .await?;
        let indexes_metadata: Vec<IndexMetadata> = pg_indexes
            .into_iter()
            .map(|pg_index| pg_index.index_metadata())
            .collect::<MetastoreResult<_>>()?;
        let response =
            ListIndexesMetadataResponse::try_from_indexes_metadata(indexes_metadata).await?;
        Ok(response)
    }

    #[instrument(skip_all, fields(index_id=%request.index_uid()))]
    async fn delete_index(&self, request: DeleteIndexRequest) -> MetastoreResult<EmptyResponse> {
        let index_uid: IndexUid = request.index_uid().clone();
        let delete_result = sqlx::query("DELETE FROM indexes WHERE index_uid = $1")
            .bind(&index_uid)
            .execute(&self.connection_pool)
            .await?;
        // FIXME: This is not idempotent.
        if delete_result.rows_affected() == 0 {
            return Err(MetastoreError::NotFound(EntityKind::Index {
                index_id: index_uid.index_id,
            }));
        }
        info!(index_id = index_uid.index_id, "deleted index successfully");
        Ok(EmptyResponse {})
    }

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
            tags_list.push(sqlx::types::Json(tags));
            split_ids.push(split_metadata.split_id);
            delete_opstamps.push(split_metadata.delete_opstamp as i64);
            node_ids.push(split_metadata.node_id);
        }
        tracing::Span::current().record("split_ids", format!("{split_ids:?}"));

        // TODO: Remove transaction.
        run_with_tx!(self.connection_pool, tx, "stage splits", {
            let upserted_split_ids: Vec<String> = sqlx::query_scalar(r#"
                INSERT INTO splits
                    (split_id, time_range_start, time_range_end, tags, split_metadata_json, delete_opstamp, maturity_timestamp, split_state, index_uid, node_id)
                SELECT
                    split_id,
                    time_range_start,
                    time_range_end,
                    ARRAY(SELECT json_array_elements_text(tags_json::json)) as tags,
                    split_metadata_json,
                    delete_opstamp,
                    to_timestamp(maturity_timestamp),
                    $9 as split_state,
                    $10 as index_uid,
                    node_id
                FROM
                    UNNEST($1, $2, $3, $4, $5, $6, $7, $8)
                    AS staged_splits (split_id, time_range_start, time_range_end, tags_json, split_metadata_json, delete_opstamp, maturity_timestamp, node_id)
                ON CONFLICT(index_uid, split_id) DO UPDATE
                    SET
                        time_range_start = excluded.time_range_start,
                        time_range_end = excluded.time_range_end,
                        tags = excluded.tags,
                        split_metadata_json = excluded.split_metadata_json,
                        delete_opstamp = excluded.delete_opstamp,
                        maturity_timestamp = excluded.maturity_timestamp,
                        node_id = excluded.node_id,
                        update_timestamp = CURRENT_TIMESTAMP,
                        create_timestamp = CURRENT_TIMESTAMP
                    WHERE splits.split_id = excluded.split_id AND splits.split_state = 'Staged'
                RETURNING split_id;
                "#)
                .bind(&split_ids)
                .bind(time_range_start_list)
                .bind(time_range_end_list)
                .bind(tags_list)
                .bind(splits_metadata_json)
                .bind(delete_opstamps)
                .bind(maturity_timestamps)
                .bind(&node_ids)
                .bind(SplitState::Staged.as_str())
                .bind(&index_uid)
                .fetch_all(tx.as_mut())
                .await
                .map_err(|sqlx_error| convert_sqlx_err(&index_uid.index_id, sqlx_error))?;

            if upserted_split_ids.len() != split_ids.len() {
                let failed_split_ids: Vec<String> = split_ids
                    .into_iter()
                    .filter(|split_id| !upserted_split_ids.contains(split_id))
                    .collect();
                let entity = EntityKind::Splits {
                    split_ids: failed_split_ids,
                };
                let message = "splits are not staged".to_string();
                return Err(MetastoreError::FailedPrecondition { entity, message });
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

            const PUBLISH_SPLITS_QUERY: &str = r#"
            -- Select the splits to update, regardless of their state.
            -- The left join make it possible to identify the splits that do not exist.
            WITH input_splits AS (
                SELECT input_splits.split_id, input_splits.expected_split_state, splits.actual_split_state
                FROM (
                    SELECT split_id, 'Staged' AS expected_split_state
                    FROM UNNEST($3) AS staged_splits(split_id)
                    UNION
                    SELECT split_id, 'Published' AS expected_split_state
                    FROM UNNEST($4) AS published_splits(split_id)
                ) input_splits
                LEFT JOIN (
                    SELECT split_id, split_state AS actual_split_state
                    FROM splits
                    WHERE
                        index_uid = $1
                        AND (split_id = ANY($3) OR split_id = ANY($4))
                    FOR UPDATE
                    ) AS splits
                USING (split_id)
            ),
            -- Update the index metadata with the new checkpoint.
            updated_index_metadata AS (
                UPDATE indexes
                SET
                    index_metadata_json = $2
                WHERE
                    index_uid = $1
                    AND NOT EXISTS (
                        SELECT 1
                        FROM input_splits
                        WHERE
                            actual_split_state != expected_split_state
                        )
            ),
            -- Publish the staged splits and mark the published splits for deletion.
            updated_splits AS (
                UPDATE splits
                SET
                    split_state = CASE split_state
                        WHEN 'Staged' THEN 'Published'
                        ELSE 'MarkedForDeletion'
                    END,
                    update_timestamp = (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
                    publish_timestamp = (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')
                FROM input_splits
                WHERE
                    splits.index_uid = $1
                    AND splits.split_id = input_splits.split_id
                    AND NOT EXISTS (
                        SELECT 1
                        FROM input_splits
                        WHERE
                            actual_split_state != expected_split_state
                    )
            )
            -- Report the outcome of the update query.
            SELECT
                COUNT(1) FILTER (WHERE actual_split_state = 'Staged' AND expected_split_state = 'Staged'),
                COUNT(1) FILTER (WHERE actual_split_state = 'Published' AND expected_split_state = 'Published'),
                COALESCE(ARRAY_AGG(split_id) FILTER (WHERE actual_split_state IS NULL), ARRAY[]::TEXT[]),
                COALESCE(ARRAY_AGG(split_id) FILTER (WHERE actual_split_state != 'Staged' AND expected_split_state = 'Staged'), ARRAY[]::TEXT[]),
                COALESCE(ARRAY_AGG(split_id) FILTER (WHERE actual_split_state != 'Published' AND expected_split_state = 'Published'), ARRAY[]::TEXT[])
                FROM input_splits
        "#;
            let (
                num_published_splits,
                num_marked_splits,
                not_found_split_ids,
                not_staged_split_ids,
                not_marked_split_ids,
            ): (i64, i64, Vec<String>, Vec<String>, Vec<String>) =
                sqlx::query_as(PUBLISH_SPLITS_QUERY)
                    .bind(&index_uid)
                    .bind(index_metadata_json)
                    .bind(staged_split_ids)
                    .bind(replaced_split_ids)
                    .fetch_one(tx.as_mut())
                    .await
                    .map_err(|sqlx_error| convert_sqlx_err(&index_uid.index_id, sqlx_error))?;

            if !not_found_split_ids.is_empty() {
                return Err(MetastoreError::NotFound(EntityKind::Splits {
                    split_ids: not_found_split_ids,
                }));
            }
            if !not_staged_split_ids.is_empty() {
                let entity = EntityKind::Splits {
                    split_ids: not_staged_split_ids,
                };
                let message = "splits are not staged".to_string();
                return Err(MetastoreError::FailedPrecondition { entity, message });
            }
            if !not_marked_split_ids.is_empty() {
                let entity = EntityKind::Splits {
                    split_ids: not_marked_split_ids,
                };
                let message = "splits are not marked for deletion".to_string();
                return Err(MetastoreError::FailedPrecondition { entity, message });
            }
            info!(
                %index_uid,
                "published {num_published_splits} splits and marked {num_marked_splits} for deletion successfully"
            );
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

        let (sql_query, values) = sql_query_builder.build_sqlx(PostgresQueryBuilder);
        let pg_split_stream = SplitStream::new(
            self.connection_pool.clone(),
            sql_query,
            |connection_pool: &TrackedPool<Postgres>, sql_query: &String| {
                sqlx::query_as_with::<_, PgSplit, _>(sql_query, values).fetch(connection_pool)
            },
        );
        let split_stream =
            pg_split_stream
                .chunks(STREAM_SPLITS_CHUNK_SIZE)
                .map(|pg_splits_results| {
                    let mut splits = Vec::with_capacity(pg_splits_results.len());
                    for pg_split_result in pg_splits_results {
                        let pg_split = match pg_split_result {
                            Ok(pg_split) => pg_split,
                            Err(error) => {
                                return Err(MetastoreError::Internal {
                                    message: "failed to fetch splits".to_string(),
                                    cause: error.to_string(),
                                });
                            }
                        };
                        let split: Split = match pg_split.try_into() {
                            Ok(split) => split,
                            Err(error) => {
                                return Err(MetastoreError::Internal {
                                    message: "failed to convert `PgSplit` to `Split`".to_string(),
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

    #[instrument(skip(self))]
    async fn mark_splits_for_deletion(
        &self,
        request: MarkSplitsForDeletionRequest,
    ) -> MetastoreResult<EmptyResponse> {
        let index_uid: IndexUid = request.index_uid().clone();
        let split_ids = request.split_ids;
        const MARK_SPLITS_FOR_DELETION_QUERY: &str = r#"
            -- Select the splits to update, regardless of their state.
            -- The left join make it possible to identify the splits that do not exist.
            WITH input_splits AS (
                SELECT input_splits.split_id, splits.split_state
                FROM UNNEST($2) AS input_splits(split_id)
                LEFT JOIN (
                    SELECT split_id, split_state
                    FROM splits
                    WHERE
                        index_uid = $1
                        AND split_id = ANY($2)
                    FOR UPDATE
                    ) AS splits
                USING (split_id)
            ),
            -- Mark the staged and published splits for deletion.
            marked_splits AS (
                UPDATE splits
                SET
                    split_state = 'MarkedForDeletion',
                    update_timestamp = (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')
                FROM input_splits
                WHERE
                    splits.index_uid = $1
                    AND splits.split_id = input_splits.split_id
                    AND splits.split_state IN ('Staged', 'Published')
            )
            -- Report the outcome of the update query.
            SELECT
                COUNT(split_state),
                COUNT(1) FILTER (WHERE split_state IN ('Staged', 'Published')),
                COALESCE(ARRAY_AGG(split_id) FILTER (WHERE split_state IS NULL), ARRAY[]::TEXT[])
                FROM input_splits
        "#;
        let (num_found_splits, num_marked_splits, not_found_split_ids): (i64, i64, Vec<String>) =
            sqlx::query_as(MARK_SPLITS_FOR_DELETION_QUERY)
                .bind(&index_uid)
                .bind(split_ids.clone())
                .fetch_one(&self.connection_pool)
                .await
                .map_err(|sqlx_error| convert_sqlx_err(&index_uid.index_id, sqlx_error))?;

        if num_found_splits == 0
            && index_opt(&self.connection_pool, &index_uid.index_id, false)
                .await?
                .is_none()
        {
            return Err(MetastoreError::NotFound(EntityKind::Index {
                index_id: index_uid.index_id,
            }));
        }
        info!(
            %index_uid,
            "Marked {} splits for deletion, among which {} were newly marked.",
            split_ids.len() - not_found_split_ids.len(),
            num_marked_splits
        );
        if !not_found_split_ids.is_empty() {
            warn!(
                %index_uid,
                split_ids=?PrettySample::new(&not_found_split_ids, 5),
                "{} splits were not found and could not be marked for deletion.",
                not_found_split_ids.len()
            );
        }
        Ok(EmptyResponse {})
    }

    #[instrument(skip(self))]
    async fn delete_splits(&self, request: DeleteSplitsRequest) -> MetastoreResult<EmptyResponse> {
        let index_uid: IndexUid = request.index_uid().clone();
        let split_ids = request.split_ids;
        const DELETE_SPLITS_QUERY: &str = r#"
            -- Select the splits to delete, regardless of their state.
            -- The left join make it possible to identify the splits that do not exist.
            WITH input_splits AS (
                SELECT input_splits.split_id, splits.split_state
                FROM UNNEST($2) AS input_splits(split_id)
                LEFT JOIN (
                    SELECT split_id, split_state
                    FROM splits
                    WHERE
                        index_uid = $1
                        AND split_id = ANY($2)
                    FOR UPDATE
                    ) AS splits
                USING (split_id)
            ),
            -- Delete the splits if and only if all the splits are marked for deletion.
            deleted_splits AS (
                DELETE FROM splits
                USING input_splits
                WHERE
                    splits.index_uid = $1
                    AND splits.split_id = input_splits.split_id
                    AND NOT EXISTS (
                        SELECT 1
                        FROM input_splits
                        WHERE
                            split_state IN ('Staged', 'Published')
                    )
            )
            -- Report the outcome of the delete query.
            SELECT
                COUNT(split_state),
                COUNT(1) FILTER (WHERE split_state = 'MarkedForDeletion'),
                COALESCE(ARRAY_AGG(split_id) FILTER (WHERE split_state IN ('Staged', 'Published')), ARRAY[]::TEXT[]),
                COALESCE(ARRAY_AGG(split_id) FILTER (WHERE split_state IS NULL), ARRAY[]::TEXT[])
                FROM input_splits
        "#;
        let (num_found_splits, num_deleted_splits, not_deletable_split_ids, not_found_split_ids): (
            i64,
            i64,
            Vec<String>,
            Vec<String>,
        ) = sqlx::query_as(DELETE_SPLITS_QUERY)
            .bind(&index_uid)
            .bind(split_ids)
            .fetch_one(&self.connection_pool)
            .await
            .map_err(|sqlx_error| convert_sqlx_err(&index_uid.index_id, sqlx_error))?;

        if num_found_splits == 0
            && index_opt_for_uid(&self.connection_pool, index_uid.clone(), false)
                .await?
                .is_none()
        {
            return Err(MetastoreError::NotFound(EntityKind::Index {
                index_id: index_uid.index_id,
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
        info!(%index_uid, "deleted {} splits from index", num_deleted_splits);

        if !not_found_split_ids.is_empty() {
            warn!(
                %index_uid,
                split_ids=?PrettySample::new(&not_found_split_ids, 5),
                "{} splits were not found and could not be deleted.",
                not_found_split_ids.len()
            );
        }
        Ok(EmptyResponse {})
    }

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
                        index_uid = $1
                        AND source_id = $2
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

    /// Retrieves the last delete opstamp for a given `index_id`.
    #[instrument(skip(self))]
    async fn last_delete_opstamp(
        &self,
        request: LastDeleteOpstampRequest,
    ) -> MetastoreResult<LastDeleteOpstampResponse> {
        let max_opstamp: i64 = sqlx::query_scalar(
            r#"
            SELECT COALESCE(MAX(opstamp), 0)
            FROM delete_tasks
            WHERE index_uid = $1
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
    #[instrument(skip(self))]
    async fn create_delete_task(&self, delete_query: DeleteQuery) -> MetastoreResult<DeleteTask> {
        let delete_query_json = serde_utils::to_json_str(&delete_query)?;
        let (create_timestamp, opstamp): (sqlx::types::time::PrimitiveDateTime, i64) =
            sqlx::query_as(
                r#"
                INSERT INTO delete_tasks (index_uid, delete_query_json) VALUES ($1, $2)
                RETURNING create_timestamp, opstamp
            "#,
            )
            .bind(delete_query.index_uid().to_string())
            .bind(&delete_query_json)
            .fetch_one(&self.connection_pool)
            .await
            .map_err(|error| convert_sqlx_err(&delete_query.index_uid().index_id, error))?;

        Ok(DeleteTask {
            create_timestamp: create_timestamp.assume_utc().unix_timestamp(),
            opstamp: opstamp as u64,
            delete_query: Some(delete_query),
        })
    }

    /// Update splits delete opstamps.
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
        let update_result = sqlx::query(
            r#"
            UPDATE splits
            SET
                delete_opstamp = $1,
                -- The values we compare with are *before* the modification:
                update_timestamp = CASE
                    WHEN delete_opstamp != $1 THEN (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')
                    ELSE update_timestamp
                END
            WHERE
                index_uid = $2
                AND split_id = ANY($3)
        "#,
        )
        .bind(request.delete_opstamp as i64)
        .bind(&index_uid)
        .bind(split_ids)
        .execute(&self.connection_pool)
        .await?;

        // If no splits were updated, maybe the index does not exist in the first place?
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

    /// Lists the delete tasks with opstamp > `opstamp_start`.
    #[instrument(skip(self))]
    async fn list_delete_tasks(
        &self,
        request: ListDeleteTasksRequest,
    ) -> MetastoreResult<ListDeleteTasksResponse> {
        let index_uid: IndexUid = request.index_uid().clone();
        let pg_delete_tasks: Vec<PgDeleteTask> = sqlx::query_as::<_, PgDeleteTask>(
            r#"
                SELECT * FROM delete_tasks
                WHERE
                    index_uid = $1
                    AND opstamp > $2
                "#,
        )
        .bind(&index_uid)
        .bind(request.opstamp_start as i64)
        .fetch_all(&self.connection_pool)
        .await?;
        let delete_tasks: Vec<DeleteTask> = pg_delete_tasks
            .into_iter()
            .map(|pg_delete_task| pg_delete_task.try_into())
            .collect::<MetastoreResult<_>>()?;
        Ok(ListDeleteTasksResponse { delete_tasks })
    }

    /// Returns `num_splits` published splits with `split.delete_opstamp` < `delete_opstamp`.
    /// Results are ordered by ascending `split.delete_opstamp` and `split.publish_timestamp`
    /// values.
    #[instrument(skip(self))]
    async fn list_stale_splits(
        &self,
        request: ListStaleSplitsRequest,
    ) -> MetastoreResult<ListSplitsResponse> {
        let index_uid: IndexUid = request.index_uid().clone();
        let stale_pg_splits: Vec<PgSplit> = sqlx::query_as::<_, PgSplit>(
            r#"
                SELECT *
                FROM splits
                WHERE
                    index_uid = $1
                    AND delete_opstamp < $2
                    AND split_state = $3
                    AND (maturity_timestamp = to_timestamp(0) OR (CURRENT_TIMESTAMP AT TIME ZONE 'UTC') >= maturity_timestamp)
                ORDER BY delete_opstamp ASC, publish_timestamp ASC
                LIMIT $4
            "#,
        )
        .bind(&index_uid)
        .bind(request.delete_opstamp as i64)
        .bind(SplitState::Published.as_str())
        .bind(request.num_splits as i64)
        .fetch_all(&self.connection_pool)
        .await?;

        let stale_splits: Vec<Split> = stale_pg_splits
            .into_iter()
            .map(|pg_split| pg_split.try_into())
            .collect::<MetastoreResult<_>>()?;
        let response = ListSplitsResponse::try_from_splits(stale_splits)?;
        Ok(response)
    }

    // TODO: Issue a single SQL query.
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
        const ACQUIRE_SHARDS_QUERY: &str = include_str!("queries/shards/acquire.sql");

        if request.shard_ids.is_empty() {
            return Ok(Default::default());
        }
        let pg_shards: Vec<PgShard> = sqlx::query_as(ACQUIRE_SHARDS_QUERY)
            .bind(request.index_uid())
            .bind(&request.source_id)
            .bind(&request.shard_ids)
            .bind(&request.publish_token)
            .fetch_all(&self.connection_pool)
            .await?;
        let acquired_shards = pg_shards
            .into_iter()
            .map(|pg_shard| pg_shard.into())
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
                let shard_state_alias = Alias::new("SHARD_STATE");
                let cast_expr = Func::cast_as(shard_state_str, shard_state_alias);
                sql_subquery_builder.and_where(Expr::col(Shards::ShardState).eq(cast_expr));
            }
            if idx == 0 {
                sql_query_builder = sql_subquery_builder;
            } else {
                sql_query_builder.union(UnionType::All, sql_subquery_builder);
            }
        }
        let (sql_query, values) = sql_query_builder.build_sqlx(PostgresQueryBuilder);

        let pg_shards: Vec<PgShard> = sqlx::query_as_with::<_, PgShard, _>(&sql_query, values)
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

        for pg_shard in pg_shards {
            let shard: Shard = pg_shard.into();
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
        const DELETE_SHARDS_QUERY: &str = include_str!("queries/shards/delete.sql");

        const FIND_NOT_DELETABLE_SHARDS_QUERY: &str =
            include_str!("queries/shards/find_not_deletable.sql");

        if request.shard_ids.is_empty() {
            return Ok(Default::default());
        }
        let query_result = sqlx::query(DELETE_SHARDS_QUERY)
            .bind(request.index_uid())
            .bind(&request.source_id)
            .bind(&request.shard_ids)
            .bind(request.force)
            .execute(&self.connection_pool)
            .await?;

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
        // Unhappy path: some shards were not deleted because they do not exist or are not fully
        // indexed.
        let not_deletable_pg_shards: Vec<PgShard> = sqlx::query_as(FIND_NOT_DELETABLE_SHARDS_QUERY)
            .bind(request.index_uid())
            .bind(&request.source_id)
            .bind(&request.shard_ids)
            .fetch_all(&self.connection_pool)
            .await?;

        if not_deletable_pg_shards.is_empty() {
            let response = DeleteShardsResponse {
                index_uid: request.index_uid,
                source_id: request.source_id,
                successes: request.shard_ids,
                failures: Vec::new(),
            };
            return Ok(response);
        }
        let failures: Vec<ShardId> = not_deletable_pg_shards
            .into_iter()
            .map(|pg_shard| pg_shard.shard_id)
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
        if request.overwrite {
            sqlx::query(UPSERT_INDEX_TEMPLATE_QUERY)
                .bind(&index_template.template_id)
                .bind(positive_patterns)
                .bind(negative_patterns)
                .bind(index_template.priority as i32)
                .bind(&request.index_template_json)
                .execute(&self.connection_pool)
                .await?;

            return Ok(EmptyResponse {});
        }
        let pg_query_result = sqlx::query(INSERT_INDEX_TEMPLATE_QUERY)
            .bind(&index_template.template_id)
            .bind(positive_patterns)
            .bind(negative_patterns)
            .bind(index_template.priority as i32)
            .bind(&request.index_template_json)
            .execute(&self.connection_pool)
            .await?;

        if pg_query_result.rows_affected() == 0 {
            return Err(MetastoreError::AlreadyExists(EntityKind::IndexTemplate {
                template_id: index_template.template_id,
            }));
        }
        Ok(EmptyResponse {})
    }

    async fn get_index_template(
        &self,
        request: GetIndexTemplateRequest,
    ) -> MetastoreResult<GetIndexTemplateResponse> {
        let pg_index_template_json: PgIndexTemplate =
            sqlx::query_as("SELECT * FROM index_templates WHERE template_id = $1")
                .bind(&request.template_id)
                .fetch_optional(&self.connection_pool)
                .await?
                .ok_or({
                    MetastoreError::NotFound(EntityKind::IndexTemplate {
                        template_id: request.template_id,
                    })
                })?;
        let response = GetIndexTemplateResponse {
            index_template_json: pg_index_template_json.index_template_json,
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

        let sql_matches: Vec<(IndexId, IndexTemplateId, String)> =
            sqlx::query_as(FIND_INDEX_TEMPLATE_MATCHES_QUERY)
                .bind(&request.index_ids)
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
        let pg_index_templates_json: Vec<(String,)> = sqlx::query_as(
            "SELECT index_template_json FROM index_templates ORDER BY template_id ASC",
        )
        .fetch_all(&self.connection_pool)
        .await?;
        let index_templates_json: Vec<String> = pg_index_templates_json
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
        sqlx::query("DELETE FROM index_templates WHERE template_id = ANY($1)")
            .bind(&request.template_ids)
            .execute(&self.connection_pool)
            .await?;
        Ok(EmptyResponse {})
    }

    async fn get_cluster_identity(
        &self,
        _: GetClusterIdentityRequest,
    ) -> MetastoreResult<GetClusterIdentityResponse> {
        let (uuid,) = sqlx::query_as(
            r"
                WITH insert AS (
                    INSERT INTO kv (key, value)
                           VALUES ('cluster_identity', $1)
                           ON CONFLICT (key) DO NOTHING
                )
                SELECT value FROM kv where key = 'cluster_identity';
                ",
        )
        .bind(Uuid::new_v4().hyphenated().to_string())
        .fetch_one(&self.connection_pool)
        .await?;
        Ok(GetClusterIdentityResponse { uuid })
    }
}

async fn open_or_fetch_shard<'e>(
    executor: impl Executor<'e, Database = Postgres> + Clone,
    subrequest: &OpenShardSubrequest,
) -> MetastoreResult<Shard> {
    const OPEN_SHARDS_QUERY: &str = include_str!("queries/shards/open.sql");

    let pg_shard_opt: Option<PgShard> = sqlx::query_as(OPEN_SHARDS_QUERY)
        .bind(subrequest.index_uid())
        .bind(&subrequest.source_id)
        .bind(subrequest.shard_id().as_str())
        .bind(&subrequest.leader_id)
        .bind(&subrequest.follower_id)
        .bind(subrequest.doc_mapping_uid)
        .bind(&subrequest.publish_token)
        // Use a timestamp generated by the metastore node to avoid clock drift issues
        .bind(OffsetDateTime::now_utc())
        .fetch_optional(executor.clone())
        .await?;

    if let Some(pg_shard) = pg_shard_opt {
        let shard: Shard = pg_shard.into();
        info!(
            index_uid=%shard.index_uid(),
            source_id=%shard.source_id,
            shard_id=%shard.shard_id(),
            leader_id=%shard.leader_id,
            follower_id=?shard.follower_id,
            "opened shard"
        );
        return Ok(shard);
    }
    const FETCH_SHARD_QUERY: &str = include_str!("queries/shards/fetch.sql");

    let pg_shard_opt: Option<PgShard> = sqlx::query_as(FETCH_SHARD_QUERY)
        .bind(subrequest.index_uid())
        .bind(&subrequest.source_id)
        .bind(subrequest.shard_id().as_str())
        .fetch_optional(executor)
        .await?;

    if let Some(pg_shard) = pg_shard_opt {
        return Ok(pg_shard.into());
    }
    Err(MetastoreError::NotFound(EntityKind::Source {
        index_id: subrequest.index_uid().to_string(),
        source_id: subrequest.source_id.clone(),
    }))
}

impl MetastoreServiceExt for PostgresqlMetastore {}

/// Builds the SQL query that returns indexes matching at least one pattern in
/// `index_id_patterns`, and none of the patterns starting with '-'
///
/// For each pattern, we check whether the pattern is valid and replace `*` by `%`
/// to build a SQL `LIKE` query.
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

/// A postgres metastore factory
#[cfg(test)]
#[async_trait]
impl crate::tests::DefaultForTest for PostgresqlMetastore {
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
        dotenvy::dotenv().ok();
        let uri: Uri = std::env::var("QW_TEST_DATABASE_URL")
            .expect("environment variable `QW_TEST_DATABASE_URL` should be set")
            .parse()
            .expect("environment variable `QW_TEST_DATABASE_URL` should be a valid URI");
        PostgresqlMetastore::new(&PostgresMetastoreConfig::default(), &uri)
            .await
            .expect("failed to initialize PostgreSQL metastore test")
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use quickwit_common::uri::Protocol;
    use quickwit_doc_mapper::tag_pruning::TagFilterAst;
    use quickwit_proto::ingest::Shard;
    use quickwit_proto::metastore::MetastoreService;
    use quickwit_proto::types::{IndexUid, SourceId};
    use sea_query::{Asterisk, PostgresQueryBuilder, Query};
    use time::OffsetDateTime;

    use super::*;
    use crate::metastore::postgres::metastore::build_index_id_patterns_sql_query;
    use crate::metastore::postgres::model::{PgShard, Splits};
    use crate::tests::DefaultForTest;
    use crate::tests::shard::ReadWriteShardsForTest;
    use crate::{ListSplitsQuery, SplitState, metastore_test_suite};

    #[async_trait]
    impl ReadWriteShardsForTest for PostgresqlMetastore {
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
                // explicit destructuring to ensure new fields are properly handled
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
                    .expect("Bad timestamp format");
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
            let pg_shards: Vec<PgShard> = sqlx::query_as(
                r#"
                SELECT *
                FROM shards
                WHERE
                    index_uid = $1
                    AND source_id = $2
                "#,
            )
            .bind(index_uid)
            .bind(source_id)
            .fetch_all(&self.connection_pool)
            .await
            .unwrap();

            pg_shards
                .into_iter()
                .map(|pg_shard| pg_shard.into())
                .collect()
        }
    }

    metastore_test_suite!(crate::PostgresqlMetastore);

    #[tokio::test]
    async fn test_metastore_connectivity_and_endpoints() {
        let metastore = PostgresqlMetastore::default_for_test().await;
        metastore.check_connectivity().await.unwrap();
        assert_eq!(metastore.endpoints()[0].protocol(), Protocol::PostgreSQL);
    }

    #[test]
    fn test_single_sql_query_builder() {
        let mut select_statement = Query::select();

        let sql = select_statement.column(Asterisk).from(Splits::Table);
        let index_uid = IndexUid::new_with_random_ulid("test-index");
        let query =
            ListSplitsQuery::for_index(index_uid.clone()).with_split_state(SplitState::Staged);
        append_query_filters_and_order_by(sql, &query);

        assert_eq!(
            sql.to_string(PostgresQueryBuilder),
            format!(
                r#"SELECT * FROM "splits" WHERE "index_uid" IN ('{index_uid}') AND "split_state" IN ('Staged')"#
            )
        );

        let mut select_statement = Query::select();
        let sql = select_statement.column(Asterisk).from(Splits::Table);

        let query =
            ListSplitsQuery::for_index(index_uid.clone()).with_split_state(SplitState::Published);
        append_query_filters_and_order_by(sql, &query);

        assert_eq!(
            sql.to_string(PostgresQueryBuilder),
            format!(
                r#"SELECT * FROM "splits" WHERE "index_uid" IN ('{index_uid}') AND "split_state" IN ('Published')"#
            )
        );

        let mut select_statement = Query::select();
        let sql = select_statement.column(Asterisk).from(Splits::Table);

        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_split_states([SplitState::Published, SplitState::MarkedForDeletion]);
        append_query_filters_and_order_by(sql, &query);
        assert_eq!(
            sql.to_string(PostgresQueryBuilder),
            format!(
                r#"SELECT * FROM "splits" WHERE "index_uid" IN ('{index_uid}') AND "split_state" IN ('Published', 'MarkedForDeletion')"#
            )
        );

        let mut select_statement = Query::select();
        let sql = select_statement.column(Asterisk).from(Splits::Table);

        let query = ListSplitsQuery::for_index(index_uid.clone()).with_update_timestamp_lt(51);
        append_query_filters_and_order_by(sql, &query);
        assert_eq!(
            sql.to_string(PostgresQueryBuilder),
            format!(
                r#"SELECT * FROM "splits" WHERE "index_uid" IN ('{index_uid}') AND "update_timestamp" < TO_TIMESTAMP(51)"#
            )
        );

        let mut select_statement = Query::select();
        let sql = select_statement.column(Asterisk).from(Splits::Table);

        let query = ListSplitsQuery::for_index(index_uid.clone()).with_create_timestamp_lte(55);
        append_query_filters_and_order_by(sql, &query);
        assert_eq!(
            sql.to_string(PostgresQueryBuilder),
            format!(
                r#"SELECT * FROM "splits" WHERE "index_uid" IN ('{index_uid}') AND "create_timestamp" <= TO_TIMESTAMP(55)"#
            )
        );

        let mut select_statement = Query::select();
        let sql = select_statement.column(Asterisk).from(Splits::Table);

        let maturity_evaluation_datetime = OffsetDateTime::from_unix_timestamp(55).unwrap();
        let query = ListSplitsQuery::for_index(index_uid.clone())
            .retain_mature(maturity_evaluation_datetime);
        append_query_filters_and_order_by(sql, &query);

        assert_eq!(
            sql.to_string(PostgresQueryBuilder),
            format!(
                r#"SELECT * FROM "splits" WHERE "index_uid" IN ('{index_uid}') AND ("maturity_timestamp" = TO_TIMESTAMP(0) OR "maturity_timestamp" <= TO_TIMESTAMP(55))"#
            )
        );

        let mut select_statement = Query::select();
        let sql = select_statement.column(Asterisk).from(Splits::Table);

        let query = ListSplitsQuery::for_index(index_uid.clone())
            .retain_immature(maturity_evaluation_datetime);
        append_query_filters_and_order_by(sql, &query);
        assert_eq!(
            sql.to_string(PostgresQueryBuilder),
            format!(
                r#"SELECT * FROM "splits" WHERE "index_uid" IN ('{index_uid}') AND "maturity_timestamp" > TO_TIMESTAMP(55)"#
            )
        );

        let mut select_statement = Query::select();
        let sql = select_statement.column(Asterisk).from(Splits::Table);

        let query = ListSplitsQuery::for_index(index_uid.clone()).with_delete_opstamp_gte(4);
        append_query_filters_and_order_by(sql, &query);
        assert_eq!(
            sql.to_string(PostgresQueryBuilder),
            format!(
                r#"SELECT * FROM "splits" WHERE "index_uid" IN ('{index_uid}') AND "delete_opstamp" >= 4"#
            )
        );

        let mut select_statement = Query::select();
        let sql = select_statement.column(Asterisk).from(Splits::Table);

        let query = ListSplitsQuery::for_index(index_uid.clone()).with_time_range_start_gt(45);
        append_query_filters_and_order_by(sql, &query);
        assert_eq!(
            sql.to_string(PostgresQueryBuilder),
            format!(
                r#"SELECT * FROM "splits" WHERE "index_uid" IN ('{index_uid}') AND ("time_range_end" > 45 OR "time_range_end" IS NULL)"#
            )
        );

        let mut select_statement = Query::select();
        let sql = select_statement.column(Asterisk).from(Splits::Table);

        let query = ListSplitsQuery::for_index(index_uid.clone()).with_time_range_end_lt(45);
        append_query_filters_and_order_by(sql, &query);
        assert_eq!(
            sql.to_string(PostgresQueryBuilder),
            format!(
                r#"SELECT * FROM "splits" WHERE "index_uid" IN ('{index_uid}') AND ("time_range_start" < 45 OR "time_range_start" IS NULL)"#
            )
        );

        let mut select_statement = Query::select();
        let sql = select_statement.column(Asterisk).from(Splits::Table);

        let query =
            ListSplitsQuery::for_index(index_uid.clone()).with_tags_filter(TagFilterAst::Tag {
                is_present: false,
                tag: "tag-2".to_string(),
            });
        append_query_filters_and_order_by(sql, &query);

        assert_eq!(
            sql.to_string(PostgresQueryBuilder),
            format!(
                r#"SELECT * FROM "splits" WHERE "index_uid" IN ('{index_uid}') AND (NOT ($$tag-2$$ = ANY(tags)))"#
            )
        );

        let mut select_statement = Query::select();
        let sql = select_statement.column(Asterisk).from(Splits::Table);

        let query = ListSplitsQuery::for_index(index_uid.clone()).with_offset(4);
        append_query_filters_and_order_by(sql, &query);

        assert_eq!(
            sql.to_string(PostgresQueryBuilder),
            format!(
                r#"SELECT * FROM "splits" WHERE "index_uid" IN ('{index_uid}') ORDER BY "split_id" ASC OFFSET 4"#
            )
        );

        let mut select_statement = Query::select();
        let sql = select_statement.column(Asterisk).from(Splits::Table);

        let query = ListSplitsQuery::for_index(index_uid.clone()).sort_by_index_uid();
        append_query_filters_and_order_by(sql, &query);

        assert_eq!(
            sql.to_string(PostgresQueryBuilder),
            format!(
                r#"SELECT * FROM "splits" WHERE "index_uid" IN ('{index_uid}') ORDER BY "index_uid" ASC, "split_id" ASC"#
            )
        );

        let mut select_statement = Query::select();
        let sql = select_statement.column(Asterisk).from(Splits::Table);

        let query =
            ListSplitsQuery::for_index(index_uid.clone()).after_split(&crate::SplitMetadata {
                index_uid: index_uid.clone(),
                split_id: "my_split".to_string(),
                ..Default::default()
            });
        append_query_filters_and_order_by(sql, &query);

        assert_eq!(
            sql.to_string(PostgresQueryBuilder),
            format!(
                r#"SELECT * FROM "splits" WHERE "index_uid" IN ('{index_uid}') AND ("index_uid", "split_id") > ('{index_uid}', 'my_split')"#
            )
        );

        let mut select_statement = Query::select();
        let sql = select_statement.column(Asterisk).from(Splits::Table);

        let query = ListSplitsQuery::for_all_indexes().with_split_state(SplitState::Staged);
        append_query_filters_and_order_by(sql, &query);

        assert_eq!(
            sql.to_string(PostgresQueryBuilder),
            r#"SELECT * FROM "splits" WHERE "split_state" IN ('Staged')"#
        );

        let mut select_statement = Query::select();
        let sql = select_statement.column(Asterisk).from(Splits::Table);

        let query = ListSplitsQuery::for_all_indexes().with_max_time_range_end(42);
        append_query_filters_and_order_by(sql, &query);

        assert_eq!(
            sql.to_string(PostgresQueryBuilder),
            r#"SELECT * FROM "splits" WHERE "time_range_end" <= 42"#
        );
    }

    #[test]
    fn test_combination_sql_query_builder() {
        let mut select_statement = Query::select();
        let sql = select_statement.column(Asterisk).from(Splits::Table);

        let index_uid = IndexUid::new_with_random_ulid("test-index");
        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_time_range_start_gt(0)
            .with_time_range_end_lt(40);
        append_query_filters_and_order_by(sql, &query);
        assert_eq!(
            sql.to_string(PostgresQueryBuilder),
            format!(
                r#"SELECT * FROM "splits" WHERE "index_uid" IN ('{index_uid}') AND ("time_range_end" > 0 OR "time_range_end" IS NULL) AND ("time_range_start" < 40 OR "time_range_start" IS NULL)"#
            )
        );

        let mut select_statement = Query::select();
        let sql = select_statement.column(Asterisk).from(Splits::Table);

        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_time_range_start_gt(45)
            .with_delete_opstamp_gt(0);
        append_query_filters_and_order_by(sql, &query);
        assert_eq!(
            sql.to_string(PostgresQueryBuilder),
            format!(
                r#"SELECT * FROM "splits" WHERE "index_uid" IN ('{index_uid}') AND ("time_range_end" > 45 OR "time_range_end" IS NULL) AND "delete_opstamp" > 0"#
            )
        );

        let mut select_statement = Query::select();
        let sql = select_statement.column(Asterisk).from(Splits::Table);

        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_update_timestamp_lt(51)
            .with_create_timestamp_lte(63);
        append_query_filters_and_order_by(sql, &query);
        assert_eq!(
            sql.to_string(PostgresQueryBuilder),
            format!(
                r#"SELECT * FROM "splits" WHERE "index_uid" IN ('{index_uid}') AND "update_timestamp" < TO_TIMESTAMP(51) AND "create_timestamp" <= TO_TIMESTAMP(63)"#
            )
        );

        let mut select_statement = Query::select();
        let sql = select_statement.column(Asterisk).from(Splits::Table);

        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_time_range_start_gt(90)
            .with_tags_filter(TagFilterAst::Tag {
                is_present: true,
                tag: "tag-1".to_string(),
            });
        append_query_filters_and_order_by(sql, &query);
        assert_eq!(
            sql.to_string(PostgresQueryBuilder),
            format!(
                r#"SELECT * FROM "splits" WHERE "index_uid" IN ('{index_uid}') AND ($$tag-1$$ = ANY(tags)) AND ("time_range_end" > 90 OR "time_range_end" IS NULL)"#
            )
        );

        let mut select_statement = Query::select();
        let sql = select_statement.column(Asterisk).from(Splits::Table);

        let index_uid_2 = IndexUid::new_with_random_ulid("test-index-2");
        let query =
            ListSplitsQuery::try_from_index_uids(vec![index_uid.clone(), index_uid_2.clone()])
                .unwrap();
        append_query_filters_and_order_by(sql, &query);
        assert_eq!(
            sql.to_string(PostgresQueryBuilder),
            format!(
                r#"SELECT * FROM "splits" WHERE "index_uid" IN ('{index_uid}', '{index_uid_2}')"#
            )
        );
    }

    #[test]
    fn test_index_id_pattern_like_query() {
        assert_eq!(
            &build_index_id_patterns_sql_query(&["*-index-*-last*".to_string()]).unwrap(),
            "SELECT * FROM indexes WHERE (index_id LIKE '%-index-%-last%')"
        );
        assert_eq!(
            &build_index_id_patterns_sql_query(&[
                "*-index-*-last*".to_string(),
                "another-index".to_string()
            ])
            .unwrap(),
            "SELECT * FROM indexes WHERE (index_id LIKE '%-index-%-last%' OR index_id = \
             'another-index')"
        );
        assert_eq!(
            &build_index_id_patterns_sql_query(&[
                "*-index-*-last**".to_string(),
                "another-index".to_string(),
                "*".to_string()
            ])
            .unwrap(),
            "SELECT * FROM indexes"
        );
        assert_eq!(
            build_index_id_patterns_sql_query(&["*-index-*-&-last**".to_string()])
                .unwrap_err()
                .to_string(),
            "internal error: failed to build list indexes query; cause: `index ID pattern \
             `*-index-*-&-last**` is invalid: patterns must match the following regular \
             expression: `^[a-zA-Z\\*][a-zA-Z0-9-_\\.\\*]{0,254}$``"
        );

        assert_eq!(
            &build_index_id_patterns_sql_query(&["*".to_string(), "-index-name".to_string()])
                .unwrap(),
            "SELECT * FROM indexes WHERE (index_id LIKE '%') AND index_id <> 'index-name'"
        );

        assert_eq!(
            &build_index_id_patterns_sql_query(&[
                "*-index-*-last*".to_string(),
                "another-index".to_string(),
                "-*-index-1-last*".to_string(),
                "-index-2-last".to_string(),
            ])
            .unwrap(),
            "SELECT * FROM indexes WHERE (index_id LIKE '%-index-%-last%' OR index_id = \
             'another-index') AND index_id NOT LIKE '%-index-1-last%' AND index_id <> \
             'index-2-last'"
        );
    }
}
