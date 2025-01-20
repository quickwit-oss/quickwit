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

use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use futures::TryStreamExt;
use itertools::Itertools;
use quickwit_actors::{
    Actor, ActorContext, ActorExitStatus, ActorHandle, ActorState, Handler, Healthz, Mailbox,
    Observation,
};
use quickwit_cluster::Cluster;
use quickwit_common::fs::get_cache_directory_path;
use quickwit_common::io::Limiter;
use quickwit_common::pubsub::EventBroker;
use quickwit_common::{io, temp_dir};
use quickwit_config::{
    build_doc_mapper, IndexConfig, IndexerConfig, SourceConfig, INGEST_API_SOURCE_ID,
};
use quickwit_ingest::{
    DropQueueRequest, GetPartitionId, IngestApiService, IngesterPool, ListQueuesRequest,
    QUEUES_DIR_NAME,
};
use quickwit_metastore::{
    IndexMetadata, IndexMetadataResponseExt, IndexesMetadataResponseExt,
    ListIndexesMetadataResponseExt, ListSplitsQuery, ListSplitsRequestExt, ListSplitsResponseExt,
    SplitMetadata, SplitState,
};
use quickwit_proto::indexing::{
    ApplyIndexingPlanRequest, ApplyIndexingPlanResponse, IndexingError, IndexingPipelineId,
    IndexingTask, MergePipelineId, PipelineMetrics,
};
use quickwit_proto::metastore::{
    IndexMetadataRequest, IndexMetadataSubrequest, IndexesMetadataRequest,
    ListIndexesMetadataRequest, ListSplitsRequest, MetastoreResult, MetastoreService,
    MetastoreServiceClient,
};
use quickwit_proto::types::{IndexId, IndexUid, NodeId, PipelineUid, ShardId};
use quickwit_storage::StorageResolver;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tokio::sync::Semaphore;
use tracing::{debug, error, info, warn};

use super::merge_pipeline::{MergePipeline, MergePipelineParams};
use super::{MergePlanner, MergeSchedulerService};
use crate::actors::merge_pipeline::FinishPendingMergesAndShutdownPipeline;
use crate::models::{DetachIndexingPipeline, DetachMergePipeline, ObservePipeline, SpawnPipeline};
use crate::source::{AssignShards, Assignment};
use crate::split_store::{IndexingSplitCache, SplitStoreQuota};
use crate::{IndexingPipeline, IndexingPipelineParams, IndexingSplitStore, IndexingStatistics};

/// Name of the indexing directory, usually located at `<data_dir_path>/indexing`.
pub const INDEXING_DIR_NAME: &str = "indexing";

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct IndexingServiceCounters {
    pub num_running_pipelines: usize,
    pub num_successful_pipelines: usize,
    pub num_failed_pipelines: usize,
    pub num_running_merge_pipelines: usize,
    pub num_deleted_queues: usize,
    pub num_delete_queue_failures: usize,
}

struct MergePipelineHandle {
    mailbox: Mailbox<MergePlanner>,
    handle: ActorHandle<MergePipeline>,
}

struct PipelineHandle {
    mailbox: Mailbox<IndexingPipeline>,
    handle: ActorHandle<IndexingPipeline>,
    indexing_pipeline_id: IndexingPipelineId,
}

/// The indexing service is (single) actor service running on indexer and in charge
/// of executing the indexing plans received from the control plane.
///
/// Concretely this means receiving new plans, comparing the current situation
/// with the target situation, and spawning/shutting down the  indexing pipelines that
/// are respectively missing or extranumerous.
pub struct IndexingService {
    node_id: NodeId,
    indexing_root_directory: PathBuf,
    queue_dir_path: PathBuf,
    cluster: Cluster,
    metastore: MetastoreServiceClient,
    ingest_api_service_opt: Option<Mailbox<IngestApiService>>,
    merge_scheduler_service: Mailbox<MergeSchedulerService>,
    ingester_pool: IngesterPool,
    storage_resolver: StorageResolver,
    indexing_pipelines: HashMap<PipelineUid, PipelineHandle>,
    counters: IndexingServiceCounters,
    local_split_store: Arc<IndexingSplitCache>,
    max_concurrent_split_uploads: usize,
    merge_pipeline_handles: HashMap<MergePipelineId, MergePipelineHandle>,
    cooperative_indexing_permits: Option<Arc<Semaphore>>,
    merge_io_throughput_limiter_opt: Option<Limiter>,
    event_broker: EventBroker,
}

impl Debug for IndexingService {
    fn fmt(&self, formatter: &mut Formatter) -> std::fmt::Result {
        formatter
            .debug_struct("IndexingService")
            .field("cluster_id", &self.cluster.cluster_id())
            .field("self_node_id", &self.node_id)
            .field("indexing_root_directory", &self.indexing_root_directory)
            .finish()
    }
}

impl IndexingService {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        node_id: NodeId,
        data_dir_path: PathBuf,
        indexer_config: IndexerConfig,
        num_blocking_threads: usize,
        cluster: Cluster,
        metastore: MetastoreServiceClient,
        ingest_api_service_opt: Option<Mailbox<IngestApiService>>,
        merge_scheduler_service: Mailbox<MergeSchedulerService>,
        ingester_pool: IngesterPool,
        storage_resolver: StorageResolver,
        event_broker: EventBroker,
    ) -> anyhow::Result<IndexingService> {
        let split_store_space_quota = SplitStoreQuota::new(
            indexer_config.split_store_max_num_splits,
            indexer_config.split_store_max_num_bytes,
        );
        let merge_io_throughput_limiter_opt =
            indexer_config.max_merge_write_throughput.map(io::limiter);
        let split_cache_dir_path = get_cache_directory_path(&data_dir_path);
        let local_split_store =
            IndexingSplitCache::open(split_cache_dir_path, split_store_space_quota).await?;
        let indexing_root_directory =
            temp_dir::create_or_purge_directory(&data_dir_path.join(INDEXING_DIR_NAME)).await?;
        let queue_dir_path = data_dir_path.join(QUEUES_DIR_NAME);
        let cooperative_indexing_permits = if indexer_config.enable_cooperative_indexing {
            Some(Arc::new(Semaphore::new(num_blocking_threads)))
        } else {
            None
        };
        Ok(IndexingService {
            node_id,
            indexing_root_directory,
            queue_dir_path,
            cluster,
            metastore,
            ingest_api_service_opt,
            merge_scheduler_service,
            ingester_pool,
            storage_resolver,
            local_split_store: Arc::new(local_split_store),
            indexing_pipelines: Default::default(),
            counters: Default::default(),
            max_concurrent_split_uploads: indexer_config.max_concurrent_split_uploads,
            merge_pipeline_handles: HashMap::new(),
            merge_io_throughput_limiter_opt,
            cooperative_indexing_permits,
            event_broker,
        })
    }

    async fn detach_indexing_pipeline(
        &mut self,
        pipeline_uid: &PipelineUid,
    ) -> Result<ActorHandle<IndexingPipeline>, IndexingError> {
        let pipeline_handle = self
            .indexing_pipelines
            .remove(pipeline_uid)
            .ok_or_else(|| {
                let message = format!("could not find indexing pipeline `{pipeline_uid}`");
                IndexingError::Internal(message)
            })?;
        self.counters.num_running_pipelines -= 1;
        Ok(pipeline_handle.handle)
    }

    async fn detach_merge_pipeline(
        &mut self,
        pipeline_id: &MergePipelineId,
    ) -> Result<ActorHandle<MergePipeline>, IndexingError> {
        let pipeline_handle = self
            .merge_pipeline_handles
            .remove(pipeline_id)
            .ok_or_else(|| {
                let message = format!("could not find merge pipeline `{pipeline_id}`");
                IndexingError::Internal(message)
            })?;
        self.counters.num_running_merge_pipelines -= 1;
        Ok(pipeline_handle.handle)
    }

    async fn observe_pipeline(
        &mut self,
        pipeline_uid: &PipelineUid,
    ) -> Result<Observation<IndexingStatistics>, IndexingError> {
        let pipeline_handle = &self
            .indexing_pipelines
            .get(pipeline_uid)
            .ok_or_else(|| {
                let message = format!("could not find indexing pipeline `{pipeline_uid}`");
                IndexingError::Internal(message)
            })?
            .handle;
        let observation = pipeline_handle.observe().await;
        Ok(observation)
    }

    async fn spawn_pipeline(
        &mut self,
        ctx: &ActorContext<Self>,
        index_id: IndexId,
        source_config: SourceConfig,
        pipeline_uid: PipelineUid,
    ) -> Result<IndexingPipelineId, IndexingError> {
        let index_metadata = self.index_metadata(ctx, &index_id).await?;
        let pipeline_id = IndexingPipelineId {
            index_uid: index_metadata.index_uid.clone(),
            source_id: source_config.source_id.clone(),
            node_id: self.node_id.clone(),
            pipeline_uid,
        };
        let index_config = index_metadata.into_index_config();
        self.spawn_pipeline_inner(ctx, pipeline_id.clone(), index_config, source_config, None)
            .await?;
        Ok(pipeline_id)
    }

    async fn spawn_pipeline_inner(
        &mut self,
        ctx: &ActorContext<Self>,
        indexing_pipeline_id: IndexingPipelineId,
        index_config: IndexConfig,
        source_config: SourceConfig,
        immature_splits_opt: Option<Vec<SplitMetadata>>,
    ) -> Result<(), IndexingError> {
        if self
            .indexing_pipelines
            .contains_key(&indexing_pipeline_id.pipeline_uid)
        {
            let message = format!("pipeline `{indexing_pipeline_id}` already exists");
            return Err(IndexingError::Internal(message));
        }
        let pipeline_uid_str = indexing_pipeline_id.pipeline_uid.to_string();
        let indexing_directory = temp_dir::Builder::default()
            .join(&indexing_pipeline_id.index_uid.index_id)
            .join(&indexing_pipeline_id.index_uid.incarnation_id.to_string())
            .join(&indexing_pipeline_id.source_id)
            .join(&pipeline_uid_str)
            .tempdir_in(&self.indexing_root_directory)
            .map_err(|error| {
                let message = format!("failed to create indexing directory: {error}");
                IndexingError::Internal(message)
            })?;
        let storage = self
            .storage_resolver
            .resolve(&index_config.index_uri)
            .await
            .map_err(|error| {
                let message = format!("failed to spawn indexing pipeline: {error}");
                IndexingError::Internal(message)
            })?;
        let merge_policy =
            crate::merge_policy::merge_policy_from_settings(&index_config.indexing_settings);
        let retention_policy = index_config.retention_policy_opt.clone();
        let split_store = IndexingSplitStore::new(storage.clone(), self.local_split_store.clone());

        let doc_mapper = build_doc_mapper(&index_config.doc_mapping, &index_config.search_settings)
            .map_err(|error| IndexingError::Internal(error.to_string()))?;

        let merge_pipeline_id = indexing_pipeline_id.merge_pipeline_id();
        let merge_pipeline_params = MergePipelineParams {
            pipeline_id: merge_pipeline_id.clone(),
            doc_mapper: doc_mapper.clone(),
            indexing_directory: indexing_directory.clone(),
            metastore: self.metastore.clone(),
            split_store: split_store.clone(),
            merge_scheduler_service: self.merge_scheduler_service.clone(),
            merge_policy: merge_policy.clone(),
            retention_policy: retention_policy.clone(),
            merge_io_throughput_limiter_opt: self.merge_io_throughput_limiter_opt.clone(),
            max_concurrent_split_uploads: self.max_concurrent_split_uploads,
            event_broker: self.event_broker.clone(),
        };
        let merge_planner_mailbox =
            self.get_or_create_merge_pipeline(merge_pipeline_params, immature_splits_opt, ctx)?;
        // The concurrent uploads budget is split in 2: 1/2 for the indexing pipeline, 1/2 for the
        // merge pipeline.
        let max_concurrent_split_uploads_index = (self.max_concurrent_split_uploads / 2).max(1);
        let max_concurrent_split_uploads_merge =
            (self.max_concurrent_split_uploads - max_concurrent_split_uploads_index).max(1);

        let params_fingerprint = index_config.indexing_params_fingerprint();
        let pipeline_params = IndexingPipelineParams {
            pipeline_id: indexing_pipeline_id.clone(),
            metastore: self.metastore.clone(),
            storage,

            // Indexing-related parameters
            doc_mapper,
            indexing_directory,
            indexing_settings: index_config.indexing_settings.clone(),
            split_store,
            max_concurrent_split_uploads_index,
            cooperative_indexing_permits: self.cooperative_indexing_permits.clone(),

            // Merge-related parameters
            merge_policy,
            retention_policy,
            max_concurrent_split_uploads_merge,
            merge_planner_mailbox,

            // Source-related parameters
            source_config,
            ingester_pool: self.ingester_pool.clone(),
            queues_dir_path: self.queue_dir_path.clone(),
            source_storage_resolver: self.storage_resolver.clone(),
            params_fingerprint,

            event_broker: self.event_broker.clone(),
        };
        let pipeline = IndexingPipeline::new(pipeline_params);
        let (pipeline_mailbox, pipeline_handle) = ctx.spawn_actor().spawn(pipeline);
        let pipeline_handle = PipelineHandle {
            mailbox: pipeline_mailbox,
            handle: pipeline_handle,
            indexing_pipeline_id: indexing_pipeline_id.clone(),
        };
        self.indexing_pipelines
            .insert(indexing_pipeline_id.pipeline_uid, pipeline_handle);
        self.counters.num_running_pipelines += 1;
        Ok(())
    }

    async fn index_metadata(
        &mut self,
        ctx: &ActorContext<Self>,
        index_id: &str,
    ) -> Result<IndexMetadata, IndexingError> {
        let _protected_zone_guard = ctx.protect_zone();
        let index_metadata_response = self
            .metastore
            .index_metadata(IndexMetadataRequest::for_index_id(index_id.to_string()))
            .await?;
        let index_metadata = index_metadata_response.deserialize_index_metadata()?;
        Ok(index_metadata)
    }

    async fn indexes_metadata(
        &mut self,
        ctx: &ActorContext<Self>,
        indexing_pipeline_ids: &[IndexingPipelineId],
    ) -> Result<Vec<IndexMetadata>, IndexingError> {
        let index_metadata_subrequests: Vec<IndexMetadataSubrequest> = indexing_pipeline_ids
            .iter()
            // Remove duplicate subrequests
            .unique_by(|pipeline_id| &pipeline_id.index_uid)
            .map(|pipeline_id| IndexMetadataSubrequest {
                index_id: None,
                index_uid: Some(pipeline_id.index_uid.clone()),
            })
            .collect();
        let indexes_metadata_request = IndexesMetadataRequest {
            subrequests: index_metadata_subrequests,
        };
        let _protected_zone_guard = ctx.protect_zone();

        let indexes_metadata_response = self
            .metastore
            .indexes_metadata(indexes_metadata_request)
            .await?;
        let indexes_metadata = indexes_metadata_response
            .deserialize_indexes_metadata()
            .await?;
        Ok(indexes_metadata)
    }

    /// Fetches the immature splits candidates for merge for all the indexing pipelines for which a
    /// merge pipeline is not running.
    async fn fetch_immature_splits_for_new_merge_pipelines(
        &mut self,
        indexing_pipeline_ids: &[IndexingPipelineId],
        ctx: &ActorContext<Self>,
    ) -> MetastoreResult<HashMap<MergePipelineId, Vec<SplitMetadata>>> {
        let mut index_uids = Vec::new();

        for indexing_pipeline_id in indexing_pipeline_ids {
            let merge_pipeline_id = indexing_pipeline_id.merge_pipeline_id();

            if !self.merge_pipeline_handles.contains_key(&merge_pipeline_id) {
                index_uids.push(merge_pipeline_id.index_uid);
            }
        }
        if index_uids.is_empty() {
            return Ok(Default::default());
        }
        index_uids.sort_unstable();
        index_uids.dedup();

        let list_splits_query = ListSplitsQuery::try_from_index_uids(index_uids)
            .expect("`index_uids` should not be empty")
            .with_node_id(self.node_id.clone())
            .with_split_state(SplitState::Published)
            .retain_immature(OffsetDateTime::now_utc());
        let list_splits_request =
            ListSplitsRequest::try_from_list_splits_query(&list_splits_query)?;

        let mut immature_splits_stream = ctx
            .protect_future(self.metastore.list_splits(list_splits_request))
            .await?;

        let mut per_merge_pipeline_immature_splits: HashMap<MergePipelineId, Vec<SplitMetadata>> =
            indexing_pipeline_ids
                .iter()
                .map(|indexing_pipeline_id| (indexing_pipeline_id.merge_pipeline_id(), Vec::new()))
                .collect();

        let mut num_immature_splits = 0usize;

        while let Some(list_splits_response) = immature_splits_stream.try_next().await? {
            for split_metadata in list_splits_response.deserialize_splits_metadata().await? {
                num_immature_splits += 1;

                let merge_pipeline_id = MergePipelineId {
                    node_id: self.node_id.clone(),
                    index_uid: split_metadata.index_uid.clone(),
                    source_id: split_metadata.source_id.clone(),
                };
                per_merge_pipeline_immature_splits
                    .entry(merge_pipeline_id)
                    .or_default()
                    .push(split_metadata);
            }
        }
        info!("fetched {num_immature_splits} splits candidates for merge");
        Ok(per_merge_pipeline_immature_splits)
    }

    async fn handle_supervise(&mut self) -> Result<(), ActorExitStatus> {
        self.indexing_pipelines
            .retain(|pipeline_uid, pipeline_handle| {
                match pipeline_handle.handle.state() {
                    ActorState::Paused | ActorState::Running => true,
                    ActorState::Success => {
                        info!(
                            pipeline_uid=%pipeline_uid,
                            "indexing pipeline exited successfully"
                        );
                        self.counters.num_successful_pipelines += 1;
                        self.counters.num_running_pipelines -= 1;
                        false
                    }
                    ActorState::Failure => {
                        // This should never happen: Indexing Pipelines are not supposed to fail,
                        // and are themselves in charge of supervising the pipeline actors.
                        error!(
                            pipeline_uid=%pipeline_uid,
                            "indexing pipeline exited with failure: this should never happen, please report"
                        );
                        self.counters.num_failed_pipelines += 1;
                        self.counters.num_running_pipelines -= 1;
                        false
                    }
                }
            });
        let merge_pipelines_to_retain: HashSet<MergePipelineId> = self
            .indexing_pipelines
            .values()
            .map(|pipeline_handle| pipeline_handle.indexing_pipeline_id.merge_pipeline_id())
            .collect();

        let merge_pipelines_to_shutdown: Vec<MergePipelineId> = self
            .merge_pipeline_handles
            .keys()
            .filter(|running_merge_pipeline_id| {
                !merge_pipelines_to_retain.contains(running_merge_pipeline_id)
            })
            .cloned()
            .collect();

        for merge_pipeline_to_shutdown in merge_pipelines_to_shutdown {
            if let Some((_, merge_pipeline_handle)) = self
                .merge_pipeline_handles
                .remove_entry(&merge_pipeline_to_shutdown)
            {
                // We gracefully shutdown the merge pipeline, so we can complete the in-flight
                // merges.
                info!(
                    index_uid=%merge_pipeline_to_shutdown.index_uid,
                    source_id=%merge_pipeline_to_shutdown.source_id,
                    "shutting down orphan merge pipeline"
                );
                // The queue capacity of the merge pipeline is unbounded, so `.send_message(...)`
                // should not block.
                // We avoid using `.quit()` here because it waits for the actor to exit.
                merge_pipeline_handle
                    .handle
                    .mailbox()
                    .send_message(FinishPendingMergesAndShutdownPipeline)
                    .await
                    .expect("merge pipeline mailbox should not be full");
            }
        }
        // Finally, we remove the completed or failed merge pipelines.
        self.merge_pipeline_handles
            .retain(|_, merge_pipeline_handle| merge_pipeline_handle.handle.state().is_running());
        self.counters.num_running_merge_pipelines = self.merge_pipeline_handles.len();
        self.update_chitchat_running_plan().await;

        let pipeline_metrics: HashMap<&IndexingPipelineId, PipelineMetrics> = self
            .indexing_pipelines
            .values()
            .filter_map(|pipeline_handle| {
                let indexing_statistics = pipeline_handle.handle.last_observation();
                let pipeline_metrics = indexing_statistics.pipeline_metrics_opt?;
                Some((&pipeline_handle.indexing_pipeline_id, pipeline_metrics))
            })
            .collect();
        self.cluster
            .update_self_node_pipeline_metrics(&pipeline_metrics)
            .await;
        Ok(())
    }

    fn get_or_create_merge_pipeline(
        &mut self,
        merge_pipeline_params: MergePipelineParams,
        immature_splits_opt: Option<Vec<SplitMetadata>>,
        ctx: &ActorContext<Self>,
    ) -> Result<Mailbox<MergePlanner>, IndexingError> {
        if let Some(merge_pipeline_handle) = self
            .merge_pipeline_handles
            .get(&merge_pipeline_params.pipeline_id)
        {
            return Ok(merge_pipeline_handle.mailbox.clone());
        }
        let merge_pipeline_id = merge_pipeline_params.pipeline_id.clone();
        let merge_pipeline =
            MergePipeline::new(merge_pipeline_params, immature_splits_opt, ctx.spawn_ctx());
        let merge_planner_mailbox = merge_pipeline.merge_planner_mailbox().clone();
        let (_pipeline_mailbox, pipeline_handle) = ctx.spawn_actor().spawn(merge_pipeline);
        let merge_pipeline_handle = MergePipelineHandle {
            mailbox: merge_planner_mailbox.clone(),
            handle: pipeline_handle,
        };
        self.merge_pipeline_handles
            .insert(merge_pipeline_id, merge_pipeline_handle);
        self.counters.num_running_merge_pipelines += 1;
        Ok(merge_planner_mailbox)
    }

    /// For all Ingest V2 pipelines, assigns the set of shards they should be working on.
    /// This is done regardless of whether there has been a change in their shard list
    /// or not.
    ///
    /// If a pipeline actor has failed, this function just logs an error.
    async fn assign_shards_to_pipelines(&mut self, tasks: &[IndexingTask]) {
        for task in tasks {
            if task.shard_ids.is_empty() {
                continue;
            }
            let pipeline_uid = task.pipeline_uid();
            let Some(pipeline_handle) = self.indexing_pipelines.get(&pipeline_uid) else {
                continue;
            };
            let assignment = Assignment {
                shard_ids: task.shard_ids.iter().cloned().collect(),
            };
            let message = AssignShards(assignment);

            if let Err(error) = pipeline_handle.mailbox.send_message(message).await {
                error!(%error, "failed to assign shards to indexing pipeline");
            }
        }
    }

    /// Applies the indexing plan by:
    /// - Stopping the running pipelines not present in the provided plan.
    /// - Starting the pipelines that are not running.
    async fn apply_indexing_plan(
        &mut self,
        tasks: &[IndexingTask],
        ctx: &ActorContext<Self>,
    ) -> Result<(), IndexingError> {
        let pipeline_diff = self.compute_pipeline_diff(tasks);

        if !pipeline_diff.pipelines_to_shutdown.is_empty() {
            self.shutdown_pipelines(&pipeline_diff.pipelines_to_shutdown)
                .await;
        }
        let mut spawn_pipeline_failures: Vec<IndexingPipelineId> = Vec::new();

        if !pipeline_diff.pipelines_to_spawn.is_empty() {
            spawn_pipeline_failures = self
                .spawn_pipelines(&pipeline_diff.pipelines_to_spawn, ctx)
                .await?;
        }
        self.assign_shards_to_pipelines(tasks).await;
        self.update_chitchat_running_plan().await;

        if !spawn_pipeline_failures.is_empty() {
            let message = format!(
                "failed to spawn indexing pipelines: {:?}",
                spawn_pipeline_failures
            );
            return Err(IndexingError::Internal(message));
        }
        Ok(())
    }

    /// Identifies the pipelines to spawn and shutdown by comparing the scheduled plan with the
    /// current running plan.
    fn compute_pipeline_diff(&self, tasks: &[IndexingTask]) -> IndexingPipelineDiff {
        let mut pipelines_to_spawn: Vec<IndexingPipelineId> = Vec::new();
        let mut scheduled_pipeline_uids: HashSet<PipelineUid> = HashSet::with_capacity(tasks.len());

        for task in tasks {
            let pipeline_uid = task.pipeline_uid();

            if !self.indexing_pipelines.contains_key(&pipeline_uid) {
                let pipeline_id = IndexingPipelineId {
                    node_id: self.node_id.clone(),
                    index_uid: task.index_uid().clone(),
                    source_id: task.source_id.clone(),
                    pipeline_uid,
                };
                pipelines_to_spawn.push(pipeline_id);
            }
            scheduled_pipeline_uids.insert(pipeline_uid);
        }
        let pipelines_to_shutdown: Vec<PipelineUid> = self
            .indexing_pipelines
            .keys()
            .filter(|pipeline_uid| !scheduled_pipeline_uids.contains(pipeline_uid))
            .copied()
            .collect();

        IndexingPipelineDiff {
            pipelines_to_shutdown,
            pipelines_to_spawn,
        }
    }

    /// Spawns the pipelines with supplied ids and returns a list of failed pipelines.
    async fn spawn_pipelines(
        &mut self,
        pipelines_to_spawn: &[IndexingPipelineId],
        ctx: &ActorContext<Self>,
    ) -> Result<Vec<IndexingPipelineId>, IndexingError> {
        let indexes_metadata = self.indexes_metadata(ctx, pipelines_to_spawn).await?;

        let per_index_uid_indexes_metadata: HashMap<IndexUid, IndexMetadata> = indexes_metadata
            .into_iter()
            .map(|index_metadata| (index_metadata.index_uid.clone(), index_metadata))
            .collect();

        let mut per_merge_pipeline_immature_splits: HashMap<MergePipelineId, Vec<SplitMetadata>> =
            self.fetch_immature_splits_for_new_merge_pipelines(pipelines_to_spawn, ctx)
                .await?;

        let mut spawn_pipeline_failures: Vec<IndexingPipelineId> = Vec::new();

        for pipeline_to_spawn in pipelines_to_spawn {
            if let Some(index_metadata) =
                per_index_uid_indexes_metadata.get(&pipeline_to_spawn.index_uid)
            {
                if let Some(source_config) =
                    index_metadata.sources.get(&pipeline_to_spawn.source_id)
                {
                    let merge_pipeline_id = pipeline_to_spawn.merge_pipeline_id();
                    let immature_splits_opt =
                        per_merge_pipeline_immature_splits.remove(&merge_pipeline_id);

                    if let Err(error) = self
                        .spawn_pipeline_inner(
                            ctx,
                            pipeline_to_spawn.clone(),
                            index_metadata.index_config.clone(),
                            source_config.clone(),
                            immature_splits_opt,
                        )
                        .await
                    {
                        error!(pipeline_id=?pipeline_to_spawn, %error, "failed to spawn pipeline");
                        spawn_pipeline_failures.push(pipeline_to_spawn.clone());
                    }
                } else {
                    error!(pipeline_id=?pipeline_to_spawn, "failed to spawn pipeline: source not found");
                    spawn_pipeline_failures.push(pipeline_to_spawn.clone());
                }
            } else {
                error!(
                    "failed to spawn pipeline: index `{}` no longer exists",
                    pipeline_to_spawn.index_uid
                );
                spawn_pipeline_failures.push(pipeline_to_spawn.clone());
            }
        }
        Ok(spawn_pipeline_failures)
    }

    /// Shuts down the pipelines with supplied ids and performs necessary cleanup.
    async fn shutdown_pipelines(&mut self, pipelines_to_shutdown: &[PipelineUid]) {
        let should_gc_ingest_api_queues = pipelines_to_shutdown
            .iter()
            .flat_map(|pipeline_uid| self.indexing_pipelines.get(pipeline_uid))
            .any(|pipeline_handle| {
                pipeline_handle.indexing_pipeline_id.source_id == INGEST_API_SOURCE_ID
            });

        for pipeline_to_shutdown in pipelines_to_shutdown {
            match self.detach_indexing_pipeline(pipeline_to_shutdown).await {
                Ok(pipeline_handle) => {
                    // Killing the pipeline ensures that all the pipeline actors will stop.
                    pipeline_handle.kill().await;
                }
                Err(error) => {
                    // Just log the detach error, it can only come from a missing pipeline in the
                    // `indexing_pipeline_handles`.
                    error!(
                        pipeline_uid=%pipeline_to_shutdown,
                        ?error,
                        "failed to detach indexing pipeline",
                    );
                }
            }
        }
        // If at least one ingest source has been removed, the related index has possibly been
        // deleted. Thus we run a garbage collect to remove queues of potentially deleted
        // indexes.
        if should_gc_ingest_api_queues {
            if let Err(error) = self.run_ingest_api_queues_gc().await {
                warn!(
                    %error,
                    "failed to garbage collect ingest API queues",
                );
            }
        }
    }

    /// Broadcasts the current running plan via chitchat.
    async fn update_chitchat_running_plan(&self) {
        let mut indexing_tasks: Vec<IndexingTask> = self
            .indexing_pipelines
            .values()
            .map(|pipeline_handle| {
                let assignment = pipeline_handle.handle.last_observation();
                let shard_ids: Vec<ShardId> = assignment.shard_ids.iter().cloned().collect();
                IndexingTask {
                    index_uid: Some(pipeline_handle.indexing_pipeline_id.index_uid.clone()),
                    source_id: pipeline_handle.indexing_pipeline_id.source_id.clone(),
                    pipeline_uid: Some(pipeline_handle.indexing_pipeline_id.pipeline_uid),
                    shard_ids,
                    params_fingerprint: assignment.params_fingerprint,
                }
            })
            .collect();

        // TODO: Does anybody why we sort the indexing tasks by pipeline_uid here?
        indexing_tasks.sort_unstable_by_key(|task| task.pipeline_uid);

        self.cluster
            .update_self_node_indexing_tasks(&indexing_tasks)
            .await;
    }

    /// Garbage collects ingest API queues of deleted indexes.
    async fn run_ingest_api_queues_gc(&mut self) -> anyhow::Result<()> {
        let Some(ingest_api_service) = &self.ingest_api_service_opt else {
            return Ok(());
        };
        let queues: HashSet<String> = ingest_api_service
            .ask_for_res(ListQueuesRequest {})
            .await
            .context("failed to list queues")?
            .queues
            .into_iter()
            .collect();
        debug!(queues=?queues, "list ingest API queues");

        if queues.is_empty() {
            return Ok(());
        }
        let indexes_metadata = self
            .metastore
            .list_indexes_metadata(ListIndexesMetadataRequest::all())
            .await?
            .deserialize_indexes_metadata()
            .await?;
        let index_ids: HashSet<String> = indexes_metadata
            .into_iter()
            .map(|index_metadata| index_metadata.index_id().to_string())
            .collect();
        debug!(index_ids=?index_ids, "list indexes");

        let partition_id = ingest_api_service.ask(GetPartitionId).await?;
        let queue_ids_to_delete = queues.difference(&index_ids);

        for queue_id in queue_ids_to_delete {
            let delete_queue_res = ingest_api_service
                .ask_for_res(DropQueueRequest {
                    queue_id: queue_id.to_string(),
                })
                .await;
            if let Err(delete_queue_error) = delete_queue_res {
                error!(
                    index_id = %queue_id,
                    partition_id,
                    error = %delete_queue_error,
                    "failed to delete queue"
                );
                self.counters.num_delete_queue_failures += 1;
            } else {
                info!(
                    index_id = %queue_id,
                    partition_id,
                    "deleted queue successfully"
                );
                self.counters.num_deleted_queues += 1;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<ObservePipeline> for IndexingService {
    type Reply = Result<Observation<IndexingStatistics>, IndexingError>;

    async fn handle(
        &mut self,
        msg: ObservePipeline,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let pipeline_uid = msg.pipeline_id.pipeline_uid;
        let observation = self.observe_pipeline(&pipeline_uid).await;
        Ok(observation)
    }
}

#[async_trait]
impl Handler<DetachIndexingPipeline> for IndexingService {
    type Reply = Result<ActorHandle<IndexingPipeline>, IndexingError>;

    async fn handle(
        &mut self,
        msg: DetachIndexingPipeline,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let pipeline_uid = msg.pipeline_id.pipeline_uid;
        let detach_pipeline_result = self.detach_indexing_pipeline(&pipeline_uid).await;
        Ok(detach_pipeline_result)
    }
}

#[async_trait]
impl Handler<DetachMergePipeline> for IndexingService {
    type Reply = Result<ActorHandle<MergePipeline>, IndexingError>;

    async fn handle(
        &mut self,
        msg: DetachMergePipeline,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(self.detach_merge_pipeline(&msg.pipeline_id).await)
    }
}

#[derive(Debug)]
struct SuperviseLoop;

#[async_trait]
impl Handler<SuperviseLoop> for IndexingService {
    type Reply = ();

    async fn handle(
        &mut self,
        _message: SuperviseLoop,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        self.handle_supervise().await?;
        ctx.schedule_self_msg(*quickwit_actors::HEARTBEAT, SuperviseLoop);
        Ok(())
    }
}

#[async_trait]
impl Actor for IndexingService {
    type ObservableState = IndexingServiceCounters;

    fn observable_state(&self) -> Self::ObservableState {
        self.counters.clone()
    }

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        self.run_ingest_api_queues_gc().await?;
        self.handle(SuperviseLoop, ctx).await
    }
}

#[async_trait]
impl Handler<SpawnPipeline> for IndexingService {
    type Reply = Result<IndexingPipelineId, IndexingError>;
    async fn handle(
        &mut self,
        message: SpawnPipeline,
        ctx: &ActorContext<Self>,
    ) -> Result<Result<IndexingPipelineId, IndexingError>, ActorExitStatus> {
        Ok(self
            .spawn_pipeline(
                ctx,
                message.index_id,
                message.source_config,
                message.pipeline_uid,
            )
            .await)
    }
}

#[async_trait]
impl Handler<ApplyIndexingPlanRequest> for IndexingService {
    type Reply = Result<ApplyIndexingPlanResponse, IndexingError>;

    async fn handle(
        &mut self,
        plan_request: ApplyIndexingPlanRequest,
        ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(self
            .apply_indexing_plan(&plan_request.indexing_tasks, ctx)
            .await
            .map(|_| ApplyIndexingPlanResponse {}))
    }
}

#[async_trait]
impl Handler<Healthz> for IndexingService {
    type Reply = bool;

    async fn handle(
        &mut self,
        _msg: Healthz,
        _ctx: &ActorContext<Self>,
    ) -> Result<bool, ActorExitStatus> {
        // In the future, check metrics such as available disk space.
        Ok(true)
    }
}

#[derive(Debug)]
struct IndexingPipelineDiff {
    pipelines_to_shutdown: Vec<PipelineUid>,
    pipelines_to_spawn: Vec<IndexingPipelineId>,
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::path::Path;
    use std::time::Duration;

    use quickwit_actors::{Health, ObservationType, Supervisable, Universe, HEARTBEAT};
    use quickwit_cluster::{create_cluster_for_test, ChannelTransport};
    use quickwit_common::rand::append_random_suffix;
    use quickwit_common::ServiceStream;
    use quickwit_config::{
        IngestApiConfig, KafkaSourceParams, SourceConfig, SourceInputFormat, SourceParams,
        VecSourceParams,
    };
    use quickwit_ingest::{init_ingest_api, CreateQueueIfNotExistsRequest};
    use quickwit_metastore::{
        metastore_for_test, AddSourceRequestExt, CreateIndexRequestExt,
        ListIndexesMetadataResponseExt, Split,
    };
    use quickwit_proto::indexing::IndexingTask;
    use quickwit_proto::metastore::{
        AddSourceRequest, CreateIndexRequest, DeleteIndexRequest, IndexMetadataResponse,
        IndexesMetadataResponse, ListIndexesMetadataResponse, ListSplitsResponse,
        MockMetastoreService,
    };

    use super::*;

    async fn spawn_indexing_service_for_test(
        data_dir_path: &Path,
        universe: &Universe,
        metastore: MetastoreServiceClient,
        cluster: Cluster,
    ) -> (Mailbox<IndexingService>, ActorHandle<IndexingService>) {
        let indexer_config = IndexerConfig::for_test().unwrap();
        let num_blocking_threads = 1;
        let storage_resolver = StorageResolver::unconfigured();
        let queues_dir_path = data_dir_path.join(QUEUES_DIR_NAME);
        let ingest_api_service =
            init_ingest_api(universe, &queues_dir_path, &IngestApiConfig::default())
                .await
                .unwrap();
        let merge_scheduler_mailbox: Mailbox<MergeSchedulerService> = universe.get_or_spawn_one();
        let indexing_server = IndexingService::new(
            NodeId::from("test-node"),
            data_dir_path.to_path_buf(),
            indexer_config,
            num_blocking_threads,
            cluster,
            metastore,
            Some(ingest_api_service),
            merge_scheduler_mailbox,
            IngesterPool::default(),
            storage_resolver.clone(),
            EventBroker::default(),
        )
        .await
        .unwrap();
        universe.spawn_builder().spawn(indexing_server)
    }

    #[tokio::test]
    async fn test_indexing_service_spawn_observe_detach() {
        quickwit_common::setup_logging_for_tests();
        let transport = ChannelTransport::default();
        let cluster = create_cluster_for_test(Vec::new(), &["indexer"], &transport, true)
            .await
            .unwrap();
        let metastore = metastore_for_test();

        let index_id = append_random_suffix("test-indexing-service");
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_config = IndexConfig::for_test(&index_id, &index_uri);

        let create_index_request =
            CreateIndexRequest::try_from_index_config(&index_config).unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid()
            .clone();
        let create_source_request = AddSourceRequest::try_from_source_config(
            index_uid.clone(),
            &SourceConfig::ingest_api_default(),
        )
        .unwrap();
        metastore.add_source(create_source_request).await.unwrap();

        let universe = Universe::with_accelerated_time();
        let temp_dir = tempfile::tempdir().unwrap();
        let (indexing_service, indexing_service_handle) =
            spawn_indexing_service_for_test(temp_dir.path(), &universe, metastore, cluster).await;
        let observation = indexing_service_handle.observe().await;
        assert_eq!(observation.num_running_pipelines, 0);
        assert_eq!(observation.num_failed_pipelines, 0);
        assert_eq!(observation.num_successful_pipelines, 0);

        // Test `spawn_pipeline`.
        let source_config_0 = SourceConfig {
            source_id: "test-indexing-service--source-0".to_string(),
            num_pipelines: NonZeroUsize::MIN,
            enabled: true,
            source_params: SourceParams::void(),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        };
        let spawn_pipeline_msg = SpawnPipeline {
            index_id: index_id.clone(),
            pipeline_uid: PipelineUid::for_test(1111u128),
            source_config: source_config_0.clone(),
        };
        let pipeline_id: IndexingPipelineId = indexing_service
            .ask_for_res(spawn_pipeline_msg.clone())
            .await
            .unwrap();
        indexing_service
            .ask_for_res(spawn_pipeline_msg)
            .await
            .unwrap_err();
        assert_eq!(pipeline_id.index_uid.index_id, index_id);
        assert_eq!(pipeline_id.source_id, source_config_0.source_id);
        assert_eq!(pipeline_id.node_id, "test-node");
        assert_eq!(pipeline_id.pipeline_uid, PipelineUid::for_test(1111u128));
        assert_eq!(
            indexing_service_handle
                .observe()
                .await
                .num_running_pipelines,
            1
        );

        // Test `observe_pipeline`.
        let observation = indexing_service
            .ask_for_res(ObservePipeline {
                pipeline_id: pipeline_id.clone(),
            })
            .await
            .unwrap();
        assert_eq!(observation.obs_type, ObservationType::Alive);
        assert_eq!(observation.generation, 1);
        assert_eq!(observation.num_spawn_attempts, 1);

        // Test detach.
        let pipeline_handle = indexing_service
            .ask_for_res(DetachIndexingPipeline {
                pipeline_id: pipeline_id.clone(),
            })
            .await
            .unwrap();
        pipeline_handle.kill().await;
        let _merge_pipeline = indexing_service
            .ask_for_res(DetachMergePipeline {
                pipeline_id: pipeline_id.merge_pipeline_id(),
            })
            .await
            .unwrap();
        let observation = indexing_service_handle.process_pending_and_observe().await;
        assert_eq!(observation.num_running_pipelines, 0);
        assert_eq!(observation.num_running_merge_pipelines, 0);
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_indexing_service_supervise_pipelines() {
        quickwit_common::setup_logging_for_tests();
        let transport = ChannelTransport::default();
        let cluster = create_cluster_for_test(Vec::new(), &["indexer"], &transport, true)
            .await
            .unwrap();
        let metastore = metastore_for_test();

        let index_id = append_random_suffix("test-indexing-service");
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_config = IndexConfig::for_test(&index_id, &index_uri);

        let source_config = SourceConfig {
            source_id: "test-indexing-service--source".to_string(),
            num_pipelines: NonZeroUsize::MIN,
            enabled: true,
            source_params: SourceParams::Vec(VecSourceParams {
                docs: Vec::new(),
                batch_num_docs: 10,
                partition: "0".to_string(),
            }),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        };
        let create_index_request = CreateIndexRequest::try_from_index_and_source_configs(
            &index_config,
            &[source_config.clone()],
        )
        .unwrap();
        metastore.create_index(create_index_request).await.unwrap();

        let universe = Universe::new();
        let temp_dir = tempfile::tempdir().unwrap();
        let (indexing_service, indexing_server_handle) =
            spawn_indexing_service_for_test(temp_dir.path(), &universe, metastore, cluster).await;

        indexing_service
            .ask_for_res(SpawnPipeline {
                index_id: index_id.clone(),
                source_config,
                pipeline_uid: PipelineUid::default(),
            })
            .await
            .unwrap();
        for _ in 0..2000 {
            let obs = indexing_server_handle.observe().await;
            if obs.num_successful_pipelines == 1 {
                // It may or may not panic
                universe.quit().await;
                return;
            }
            universe.sleep(Duration::from_millis(100)).await;
        }
        panic!("Pipeline not exited successfully.");
    }

    #[tokio::test]
    async fn test_indexing_service_apply_plan() {
        const PARAMS_FINGERPRINT: u64 = 3865067856550546352;

        quickwit_common::setup_logging_for_tests();
        let transport = ChannelTransport::default();
        let cluster = create_cluster_for_test(Vec::new(), &["indexer"], &transport, true)
            .await
            .unwrap();
        let metastore = metastore_for_test();

        let index_id = append_random_suffix("test-indexing-service");
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_config = IndexConfig::for_test(&index_id, &index_uri);

        let create_index_request =
            CreateIndexRequest::try_from_index_config(&index_config).unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid()
            .clone();
        let add_source_request = AddSourceRequest::try_from_source_config(
            index_uid.clone(),
            &SourceConfig::ingest_api_default(),
        )
        .unwrap();
        metastore.add_source(add_source_request).await.unwrap();
        let universe = Universe::new();
        let temp_dir = tempfile::tempdir().unwrap();
        let (indexing_service, indexing_service_handle) = spawn_indexing_service_for_test(
            temp_dir.path(),
            &universe,
            metastore.clone(),
            cluster.clone(),
        )
        .await;

        // Test `apply plan`.
        let source_config_1 = SourceConfig {
            source_id: "test-indexing-service--source-1".to_string(),
            num_pipelines: NonZeroUsize::MIN,
            enabled: true,
            source_params: SourceParams::void(),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        };
        let add_source_request =
            AddSourceRequest::try_from_source_config(index_uid.clone(), &source_config_1).unwrap();
        metastore.add_source(add_source_request).await.unwrap();
        let metadata = metastore
            .index_metadata(IndexMetadataRequest::for_index_id(index_id.clone()))
            .await
            .unwrap()
            .deserialize_index_metadata()
            .unwrap();
        let indexing_tasks = vec![
            IndexingTask {
                index_uid: Some(metadata.index_uid.clone()),
                source_id: "test-indexing-service--source-1".to_string(),
                shard_ids: Vec::new(),
                pipeline_uid: Some(PipelineUid::for_test(0u128)),
                params_fingerprint: PARAMS_FINGERPRINT,
            },
            IndexingTask {
                index_uid: Some(metadata.index_uid.clone()),
                source_id: "test-indexing-service--source-1".to_string(),
                shard_ids: Vec::new(),
                pipeline_uid: Some(PipelineUid::for_test(1u128)),
                params_fingerprint: PARAMS_FINGERPRINT,
            },
        ];
        indexing_service
            .ask_for_res(ApplyIndexingPlanRequest { indexing_tasks })
            .await
            .unwrap();
        assert_eq!(
            indexing_service_handle
                .observe()
                .await
                .num_running_pipelines,
            2
        );

        let kafka_params = KafkaSourceParams {
            topic: "my-topic".to_string(),
            client_log_level: None,
            client_params: serde_json::Value::Null,
            enable_backfill_mode: false,
        };
        let source_config_2 = SourceConfig {
            source_id: "test-indexing-service--source-2".to_string(),
            num_pipelines: NonZeroUsize::new(2).unwrap(),
            enabled: true,
            source_params: SourceParams::Kafka(kafka_params),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        };
        let add_source_request_2 =
            AddSourceRequest::try_from_source_config(index_uid.clone(), &source_config_2).unwrap();
        metastore.add_source(add_source_request_2).await.unwrap();

        let indexing_tasks = vec![
            IndexingTask {
                index_uid: Some(metadata.index_uid.clone()),
                source_id: INGEST_API_SOURCE_ID.to_string(),
                shard_ids: Vec::new(),
                pipeline_uid: Some(PipelineUid::for_test(3u128)),
                params_fingerprint: PARAMS_FINGERPRINT,
            },
            IndexingTask {
                index_uid: Some(metadata.index_uid.clone()),
                source_id: "test-indexing-service--source-1".to_string(),
                shard_ids: Vec::new(),
                pipeline_uid: Some(PipelineUid::for_test(1u128)),
                params_fingerprint: PARAMS_FINGERPRINT,
            },
            IndexingTask {
                index_uid: Some(metadata.index_uid.clone()),
                source_id: "test-indexing-service--source-1".to_string(),
                shard_ids: Vec::new(),
                pipeline_uid: Some(PipelineUid::for_test(2u128)),
                params_fingerprint: PARAMS_FINGERPRINT,
            },
            IndexingTask {
                index_uid: Some(metadata.index_uid.clone()),
                source_id: source_config_2.source_id.clone(),
                shard_ids: Vec::new(),
                pipeline_uid: Some(PipelineUid::for_test(4u128)),
                params_fingerprint: PARAMS_FINGERPRINT,
            },
        ];
        indexing_service
            .ask_for_res(ApplyIndexingPlanRequest {
                indexing_tasks: indexing_tasks.clone(),
            })
            .await
            .unwrap();
        assert_eq!(
            indexing_service_handle
                .observe()
                .await
                .num_running_pipelines,
            4
        );

        cluster
            .wait_for_ready_members(
                |members| {
                    members
                        .iter()
                        .any(|member| member.indexing_tasks.len() == indexing_tasks.len())
                },
                Duration::from_secs(5),
            )
            .await
            .unwrap();

        let self_member = &cluster.ready_members().await[0];

        assert_eq!(
            HashSet::<_>::from_iter(self_member.indexing_tasks.iter()),
            HashSet::from_iter(indexing_tasks.iter())
        );
        let indexing_tasks = vec![
            IndexingTask {
                index_uid: Some(metadata.index_uid.clone()),
                source_id: INGEST_API_SOURCE_ID.to_string(),
                shard_ids: Vec::new(),
                pipeline_uid: Some(PipelineUid::for_test(3u128)),
                params_fingerprint: PARAMS_FINGERPRINT,
            },
            IndexingTask {
                index_uid: Some(metadata.index_uid.clone()),
                source_id: "test-indexing-service--source-1".to_string(),
                shard_ids: Vec::new(),
                pipeline_uid: Some(PipelineUid::for_test(1u128)),
                params_fingerprint: PARAMS_FINGERPRINT,
            },
            IndexingTask {
                index_uid: Some(metadata.index_uid.clone()),
                source_id: source_config_2.source_id.clone(),
                shard_ids: Vec::new(),
                pipeline_uid: Some(PipelineUid::for_test(4u128)),
                params_fingerprint: PARAMS_FINGERPRINT,
            },
        ];
        indexing_service
            .ask_for_res(ApplyIndexingPlanRequest {
                indexing_tasks: indexing_tasks.clone(),
            })
            .await
            .unwrap();
        let indexing_service_obs = indexing_service_handle.observe().await;
        assert_eq!(indexing_service_obs.num_running_pipelines, 3);
        assert_eq!(indexing_service_obs.num_deleted_queues, 0);
        assert_eq!(indexing_service_obs.num_delete_queue_failures, 0);

        indexing_service_handle.process_pending_and_observe().await;

        cluster
            .wait_for_ready_members(
                |members| {
                    members
                        .iter()
                        .any(|member| member.indexing_tasks.len() == indexing_tasks.len())
                },
                Duration::from_secs(5),
            )
            .await
            .unwrap();

        let self_member = &cluster.ready_members().await[0];

        assert_eq!(
            HashSet::<_>::from_iter(self_member.indexing_tasks.iter()),
            HashSet::from_iter(indexing_tasks.iter())
        );

        // Delete index and apply empty plan
        metastore
            .delete_index(DeleteIndexRequest {
                index_uid: Some(index_uid.clone()),
            })
            .await
            .unwrap();
        indexing_service
            .ask_for_res(ApplyIndexingPlanRequest {
                indexing_tasks: Vec::new(),
            })
            .await
            .unwrap();
        let indexing_service_obs = indexing_service_handle.observe().await;
        assert_eq!(indexing_service_obs.num_running_pipelines, 0);
        assert_eq!(indexing_service_obs.num_deleted_queues, 1);
        assert_eq!(indexing_service_obs.num_delete_queue_failures, 0);
        indexing_service_handle.quit().await;
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_indexing_service_shutdown_merge_pipeline_when_no_indexing_pipeline() {
        quickwit_common::setup_logging_for_tests();
        let transport = ChannelTransport::default();
        let cluster = create_cluster_for_test(Vec::new(), &["indexer"], &transport, true)
            .await
            .unwrap();
        let metastore = metastore_for_test();

        let index_id = append_random_suffix("test-indexing-service");
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_config = IndexConfig::for_test(&index_id, &index_uri);

        let source_config = SourceConfig {
            source_id: "test-indexing-service--source".to_string(),
            num_pipelines: NonZeroUsize::MIN,
            enabled: true,
            source_params: SourceParams::void(),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        };
        let create_index_request =
            CreateIndexRequest::try_from_index_config(&index_config).unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid()
            .clone();
        let add_source_request =
            AddSourceRequest::try_from_source_config(index_uid.clone(), &source_config).unwrap();
        metastore.add_source(add_source_request).await.unwrap();

        // Test `IndexingService::new`.
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir_path = temp_dir.path().to_path_buf();
        let indexer_config = IndexerConfig::for_test().unwrap();
        let num_blocking_threads = 1;
        let storage_resolver = StorageResolver::unconfigured();
        let universe = Universe::with_accelerated_time();
        let queues_dir_path = data_dir_path.join(QUEUES_DIR_NAME);
        let ingest_api_service =
            init_ingest_api(&universe, &queues_dir_path, &IngestApiConfig::default())
                .await
                .unwrap();
        let merge_scheduler_service = universe.get_or_spawn_one();
        let indexing_server = IndexingService::new(
            NodeId::from("test-node"),
            data_dir_path,
            indexer_config,
            num_blocking_threads,
            cluster.clone(),
            metastore.clone(),
            Some(ingest_api_service),
            merge_scheduler_service,
            IngesterPool::default(),
            storage_resolver.clone(),
            EventBroker::default(),
        )
        .await
        .unwrap();
        let (indexing_server_mailbox, indexing_server_handle) =
            universe.spawn_builder().spawn(indexing_server);
        let pipeline_id = indexing_server_mailbox
            .ask_for_res(SpawnPipeline {
                index_id: index_id.clone(),
                source_config,
                pipeline_uid: PipelineUid::default(),
            })
            .await
            .unwrap();
        let observation = indexing_server_handle.observe().await;
        assert_eq!(observation.num_running_pipelines, 1);
        assert_eq!(observation.num_failed_pipelines, 0);
        assert_eq!(observation.num_successful_pipelines, 0);
        assert_eq!(observation.num_running_merge_pipelines, 1);

        // Test `shutdown_pipeline`
        let pipeline = indexing_server_mailbox
            .ask_for_res(DetachIndexingPipeline { pipeline_id })
            .await
            .unwrap();
        pipeline.quit().await;

        // Let the service cleanup the merge pipelines.
        universe.sleep(*HEARTBEAT).await;

        let observation = indexing_server_handle.process_pending_and_observe().await;
        assert_eq!(observation.num_running_pipelines, 0);
        assert_eq!(observation.num_running_merge_pipelines, 0);
        universe.sleep(*HEARTBEAT).await;
        // Check that the merge pipeline is also shut down as they are no more indexing pipeilne on
        // the index.
        assert!(universe.get_one::<MergePipeline>().is_none());
        // It may or may not panic
        universe.quit().await;
    }

    #[derive(Debug)]
    struct FreezePipeline;
    #[async_trait]
    impl Handler<FreezePipeline> for IndexingPipeline {
        type Reply = ();
        async fn handle(
            &mut self,
            _: FreezePipeline,
            _ctx: &ActorContext<Self>,
        ) -> Result<Self::Reply, ActorExitStatus> {
            tokio::time::sleep(*HEARTBEAT * 5).await;
            Ok(())
        }
    }

    #[derive(Debug)]
    struct ObservePipelineHealth(IndexingPipelineId);
    #[async_trait]
    impl Handler<ObservePipelineHealth> for IndexingService {
        type Reply = Health;
        async fn handle(
            &mut self,
            message: ObservePipelineHealth,
            _ctx: &ActorContext<Self>,
        ) -> Result<Self::Reply, ActorExitStatus> {
            Ok(self
                .indexing_pipelines
                .get(&message.0.pipeline_uid)
                .unwrap()
                .handle
                .check_health(true))
        }
    }

    #[tokio::test]
    async fn test_indexing_service_does_not_shutdown_pipelines_on_indexing_pipeline_freeze() {
        quickwit_common::setup_logging_for_tests();
        let transport = ChannelTransport::default();
        let cluster = create_cluster_for_test(Vec::new(), &["indexer"], &transport, true)
            .await
            .unwrap();
        let index_id = append_random_suffix("test-indexing-service-indexing-pipeline-timeout");
        let index_uri = format!("ram:///indexes/{index_id}");
        let mut index_metadata = IndexMetadata::for_test(&index_id, &index_uri);
        let source_config = SourceConfig {
            source_id: "test-indexing-service--source".to_string(),
            num_pipelines: NonZeroUsize::MIN,
            enabled: true,
            source_params: SourceParams::void(),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        };
        index_metadata
            .sources
            .insert(source_config.source_id.clone(), source_config.clone());
        let mut mock_metastore = MockMetastoreService::new();
        let index_metadata_clone = index_metadata.clone();
        mock_metastore
            .expect_list_indexes_metadata()
            .return_once(move |_request| {
                Ok(ListIndexesMetadataResponse::for_test(vec![
                    index_metadata_clone,
                ]))
            });
        mock_metastore.expect_index_metadata().returning(move |_| {
            Ok(IndexMetadataResponse::try_from_index_metadata(&index_metadata).unwrap())
        });
        mock_metastore
            .expect_list_splits()
            .returning(|_| Ok(ServiceStream::empty()));
        let universe = Universe::new();
        let temp_dir = tempfile::tempdir().unwrap();
        let (indexing_service, indexing_service_handle) = spawn_indexing_service_for_test(
            temp_dir.path(),
            &universe,
            MetastoreServiceClient::from_mock(mock_metastore),
            cluster,
        )
        .await;
        let _pipeline_id = indexing_service
            .ask_for_res(SpawnPipeline {
                index_id: index_id.clone(),
                source_config,
                pipeline_uid: PipelineUid::default(),
            })
            .await
            .unwrap();
        let observation = indexing_service_handle.observe().await;
        assert_eq!(observation.num_running_pipelines, 1);
        assert_eq!(observation.num_failed_pipelines, 0);
        assert_eq!(observation.num_successful_pipelines, 0);

        let indexing_pipeline = universe.get_one::<IndexingPipeline>().unwrap();

        // Freeze pipeline during 5 heartbeats.
        indexing_pipeline
            .send_message(FreezePipeline)
            .await
            .unwrap();
        universe.sleep(*HEARTBEAT * 5).await;
        // Check that indexing and merge pipelines are still running.
        let observation = indexing_service_handle.observe().await;
        assert_eq!(observation.num_running_pipelines, 1);
        assert_eq!(observation.num_failed_pipelines, 0);
        assert_eq!(observation.num_running_merge_pipelines, 1);
        // Might generate panics
        universe.quit().await;
    }

    #[tokio::test]
    async fn test_indexing_service_ingest_api_gc() {
        let index_id = "test-ingest-api-gc-index".to_string();
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_config = IndexConfig::for_test(&index_id, &index_uri);
        let transport = ChannelTransport::default();
        let cluster = create_cluster_for_test(Vec::new(), &["indexer"], &transport, true)
            .await
            .unwrap();
        let metastore = metastore_for_test();
        let create_index_request =
            CreateIndexRequest::try_from_index_config(&index_config).unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid()
            .clone();

        // Setup ingest api objects
        let universe = Universe::with_accelerated_time();
        let temp_dir = tempfile::tempdir().unwrap();
        let queues_dir_path = temp_dir.path().join(QUEUES_DIR_NAME);
        let ingest_api_service =
            init_ingest_api(&universe, &queues_dir_path, &IngestApiConfig::default())
                .await
                .unwrap();
        let create_queue_req = CreateQueueIfNotExistsRequest {
            queue_id: index_id.clone(),
        };
        ingest_api_service
            .ask_for_res(create_queue_req)
            .await
            .unwrap();

        // Setup `IndexingService`
        let data_dir_path = temp_dir.path().to_path_buf();
        let indexer_config = IndexerConfig::for_test().unwrap();
        let num_blocking_threads = 1;
        let storage_resolver = StorageResolver::unconfigured();
        let merge_scheduler_service: Mailbox<MergeSchedulerService> = universe.get_or_spawn_one();
        let mut indexing_server = IndexingService::new(
            NodeId::from("test-ingest-api-gc-node"),
            data_dir_path,
            indexer_config,
            num_blocking_threads,
            cluster.clone(),
            metastore.clone(),
            Some(ingest_api_service.clone()),
            merge_scheduler_service,
            IngesterPool::default(),
            storage_resolver.clone(),
            EventBroker::default(),
        )
        .await
        .unwrap();

        indexing_server.run_ingest_api_queues_gc().await.unwrap();
        assert_eq!(indexing_server.counters.num_deleted_queues, 0);

        metastore
            .delete_index(DeleteIndexRequest {
                index_uid: Some(index_uid.clone()),
            })
            .await
            .unwrap();

        indexing_server.run_ingest_api_queues_gc().await.unwrap();
        assert_eq!(indexing_server.counters.num_deleted_queues, 1);

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_indexing_service_apply_indexing_plan_batches_metastore_calls() {
        let temp_dir = tempfile::tempdir().unwrap();
        let universe = Universe::new();

        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_index_metadata()
            .withf(|request| request.index_id.as_ref().unwrap() == "test-index-0")
            .return_once(|_request| {
                let index_metadata_0 =
                    IndexMetadata::for_test("test-index-0", "ram:///indexes/test-index-0");
                let response =
                    IndexMetadataResponse::try_from_index_metadata(&index_metadata_0).unwrap();
                Ok(response)
            });
        mock_metastore
            .expect_indexes_metadata()
            .withf(|request| {
                let index_uids: Vec<&IndexUid> = request
                    .subrequests
                    .iter()
                    .flat_map(|subrequest| &subrequest.index_uid)
                    .sorted()
                    .collect();

                index_uids == [&("test-index-1", 0), &("test-index-2", 0)]
            })
            .return_once(|_request| {
                let source_config = SourceConfig::for_test("test-source", SourceParams::void());

                let mut index_metadata_1 =
                    IndexMetadata::for_test("test-index-1", "ram:///indexes/test-index-1");
                index_metadata_1.add_source(source_config.clone()).unwrap();

                let mut index_metadata_2 =
                    IndexMetadata::for_test("test-index-2", "ram:///indexes/test-index-2");
                index_metadata_2.add_source(source_config).unwrap();

                let indexes_metadata = vec![index_metadata_1, index_metadata_2];
                let failures = Vec::new();
                let response = IndexesMetadataResponse::for_test(indexes_metadata, failures);
                Ok(response)
            });
        mock_metastore
            .expect_list_splits()
            .withf(|request| {
                let list_splits_query = request.deserialize_list_splits_query().unwrap();
                list_splits_query.index_uids.unwrap() == [("test-index-0", 0)]
            })
            .return_once(|_request| Ok(ServiceStream::empty()));
        mock_metastore
            .expect_list_splits()
            .withf(|request| {
                let list_splits_query = request.deserialize_list_splits_query().unwrap();
                list_splits_query.index_uids.unwrap() == [("test-index-1", 0), ("test-index-2", 0)]
            })
            .return_once(|_request| {
                let splits = vec![Split {
                    split_metadata: SplitMetadata::for_test("test-split".to_string()),
                    split_state: SplitState::Published,
                    update_timestamp: 0,
                    publish_timestamp: Some(0),
                }];
                let list_splits_response = ListSplitsResponse::try_from_splits(splits).unwrap();
                let response = ServiceStream::from(vec![Ok(list_splits_response)]);
                Ok(response)
            });

        let transport = ChannelTransport::default();
        let cluster = create_cluster_for_test(Vec::new(), &["indexer"], &transport, true)
            .await
            .unwrap();
        let (indexing_service, _indexing_service_handle) = spawn_indexing_service_for_test(
            temp_dir.path(),
            &universe,
            MetastoreServiceClient::from_mock(mock_metastore),
            cluster,
        )
        .await;

        let source_config = SourceConfig::for_test("test-source", SourceParams::void());

        indexing_service
            .ask_for_res(SpawnPipeline {
                index_id: "test-index-0".to_string(),
                source_config,
                pipeline_uid: PipelineUid::for_test(0),
            })
            .await
            .unwrap();

        indexing_service
            .ask_for_res(ApplyIndexingPlanRequest {
                indexing_tasks: vec![
                    IndexingTask {
                        index_uid: Some(IndexUid::for_test("test-index-0", 0)),
                        source_id: "test-source".to_string(),
                        shard_ids: Vec::new(),
                        pipeline_uid: Some(PipelineUid::for_test(0)),
                        params_fingerprint: 0,
                    },
                    IndexingTask {
                        index_uid: Some(IndexUid::for_test("test-index-1", 0)),
                        source_id: "test-source".to_string(),
                        shard_ids: Vec::new(),
                        pipeline_uid: Some(PipelineUid::for_test(1)),
                        params_fingerprint: 0,
                    },
                    IndexingTask {
                        index_uid: Some(IndexUid::for_test("test-index-2", 0)),
                        source_id: "test-source".to_string(),
                        shard_ids: Vec::new(),
                        pipeline_uid: Some(PipelineUid::for_test(2)),
                        params_fingerprint: 0,
                    },
                ],
            })
            .await
            .unwrap();

        universe.assert_quit().await;
    }
}
