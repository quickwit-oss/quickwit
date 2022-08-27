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
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use futures::future::try_join_all;
use itertools::Itertools;
use quickwit_actors::{
    Actor, ActorContext, ActorExitStatus, ActorHandle, Handler, Health, Mailbox, Observation,
    Supervisable,
};
use quickwit_cluster::Cluster;
use quickwit_config::merge_policy_config::MergePolicyConfig;
use quickwit_config::{IndexerConfig, SourceConfig, SourceParams, VecSourceParams};
use quickwit_ingest_api::IngestApiService;
use quickwit_metastore::{IndexMetadata, Metastore, MetastoreError};
use quickwit_proto::indexing_api::{
    ApplyIndexingPlanRequest, ApplyIndexingPlanResponse, IndexingTask,
};
use quickwit_proto::ingest_api::{CreateQueueIfNotExistsRequest, DropQueueRequest};
use quickwit_proto::{ServiceError, ServiceErrorCode};
use quickwit_storage::{Storage, StorageError, StorageResolverError, StorageUriResolver};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::{error, info};

use crate::merge_policy::MergePolicy;
use crate::models::{
    DetachPipeline, IndexingDirectory, IndexingPipelineId, Observe, ObservePipeline,
    ShutdownPipeline, ShutdownPipelines, SpawnMergePipeline, SpawnPipeline, WeakIndexingDirectory,
};
use crate::source::INGEST_API_SOURCE_ID;
use crate::split_store::SplitStoreSpaceQuota;
use crate::{
    IndexingPipeline, IndexingPipelineParams, IndexingSplitStore, IndexingStatistics,
    WeakIndexingSplitStore,
};

/// Name of the indexing directory, usually located at `<data_dir_path>/indexing`.
pub const INDEXING_DIR_NAME: &str = "indexing";

#[derive(Error, Debug)]
pub enum IndexingServiceError {
    #[error("Indexing pipeline `{index_id}` for source `{source_id}` does not exist.")]
    MissingPipeline { index_id: String, source_id: String },
    #[error(
        "Pipeline #{pipeline_ord} for index `{index_id}` and source `{source_id}` already exists."
    )]
    PipelineAlreadyExists {
        index_id: String,
        source_id: String,
        pipeline_ord: usize,
    },
    #[error("Source `{source_id}` for `{index_id}` does not exist.")]
    SourceNotFound { index_id: String, source_id: String },
    #[error("Failed to resolve the storage `{0}`.")]
    StorageResolverError(#[from] StorageResolverError),
    #[error("Storage error `{0}`.")]
    StorageError(#[from] StorageError),
    #[error("Metastore error `{0}`.")]
    MetastoreError(#[from] MetastoreError),
    #[error("Invalid params `{0}`.")]
    InvalidParams(anyhow::Error),
}

impl ServiceError for IndexingServiceError {
    fn status_code(&self) -> ServiceErrorCode {
        match self {
            Self::MissingPipeline { .. } => ServiceErrorCode::NotFound,
            Self::PipelineAlreadyExists { .. } => ServiceErrorCode::BadRequest,
            Self::StorageResolverError(_) | Self::StorageError(_) => ServiceErrorCode::Internal,
            Self::MetastoreError(_) => ServiceErrorCode::Internal,
            Self::InvalidParams(_) => ServiceErrorCode::BadRequest,
            Self::SourceNotFound { .. } => ServiceErrorCode::Internal,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct IndexingServiceState {
    pub num_running_pipelines: usize,
    pub num_successful_pipelines: usize,
    pub num_failed_pipelines: usize,
}

type IndexId = String;
type SourceId = String;

pub struct IndexingService {
    node_id: String,
    data_dir_path: PathBuf,
    cluster: Arc<Cluster>,
    metastore: Arc<dyn Metastore>,
    storage_resolver: StorageUriResolver,
    ingest_api_service_mailbox: Mailbox<IngestApiService>,
    indexing_pipeline_handles: HashMap<IndexingPipelineId, ActorHandle<IndexingPipeline>>,
    state: IndexingServiceState,
    indexing_directories: HashMap<(IndexId, SourceId), WeakIndexingDirectory>,
    split_stores: HashMap<(IndexId, SourceId), WeakIndexingSplitStore>,
    split_store_space_quota: Arc<Mutex<SplitStoreSpaceQuota>>,
    max_concurrent_split_uploads: usize,
}

impl IndexingService {
    pub fn check_health(&self) -> Health {
        // In the future, check metrics such as available disk space.
        Health::Healthy
    }

    pub fn new(
        node_id: String,
        data_dir_path: PathBuf,
        indexer_config: IndexerConfig,
        cluster: Arc<Cluster>,
        metastore: Arc<dyn Metastore>,
        ingest_api_service_mailbox: Mailbox<IngestApiService>,
        storage_resolver: StorageUriResolver,
    ) -> IndexingService {
        Self {
            node_id,
            data_dir_path,
            cluster,
            metastore,
            ingest_api_service_mailbox,
            storage_resolver,
            indexing_pipeline_handles: Default::default(),
            state: Default::default(),
            indexing_directories: HashMap::new(),
            split_stores: HashMap::new(),
            split_store_space_quota: Arc::new(Mutex::new(SplitStoreSpaceQuota::new(
                indexer_config.split_store_max_num_splits,
                indexer_config.split_store_max_num_bytes.get_bytes() as usize,
            ))),
            max_concurrent_split_uploads: indexer_config.max_concurrent_split_uploads,
        }
    }

    async fn detach_pipeline(
        &mut self,
        pipeline_id: &IndexingPipelineId,
    ) -> Result<ActorHandle<IndexingPipeline>, IndexingServiceError> {
        let pipeline_handle = self
            .indexing_pipeline_handles
            .remove(pipeline_id)
            .ok_or_else(|| IndexingServiceError::MissingPipeline {
                index_id: pipeline_id.index_id.clone(),
                source_id: pipeline_id.source_id.clone(),
            })?;
        self.state.num_running_pipelines -= 1;
        Ok(pipeline_handle)
    }

    async fn observe_pipeline(
        &mut self,
        pipeline_id: &IndexingPipelineId,
    ) -> Result<Observation<IndexingStatistics>, IndexingServiceError> {
        let pipeline_handle = self
            .indexing_pipeline_handles
            .get(pipeline_id)
            .ok_or_else(|| IndexingServiceError::MissingPipeline {
                index_id: pipeline_id.index_id.clone(),
                source_id: pipeline_id.source_id.clone(),
            })?;
        let observation = pipeline_handle.observe().await;
        Ok(observation)
    }

    async fn spawn_pipeline(
        &mut self,
        ctx: &ActorContext<Self>,
        index_id: String,
        source_config: SourceConfig,
        pipeline_ord: usize,
    ) -> Result<IndexingPipelineId, IndexingServiceError> {
        let pipeline_id = IndexingPipelineId {
            index_id,
            source_id: source_config.source_id.clone(),
            node_id: self.node_id.clone(),
            pipeline_ord,
        };
        let index_metadata = self.index_metadata(ctx, &pipeline_id.index_id).await?;
        self.spawn_pipeline_inner(ctx, pipeline_id.clone(), index_metadata, source_config)
            .await?;
        Ok(pipeline_id)
    }

    async fn spawn_pipeline_inner(
        &mut self,
        ctx: &ActorContext<Self>,
        pipeline_id: IndexingPipelineId,
        index_metadata: IndexMetadata,
        source_config: SourceConfig,
    ) -> Result<(), IndexingServiceError> {
        if self.indexing_pipeline_handles.contains_key(&pipeline_id) {
            return Err(IndexingServiceError::PipelineAlreadyExists {
                index_id: pipeline_id.index_id,
                source_id: pipeline_id.source_id,
                pipeline_ord: pipeline_id.pipeline_ord,
            });
        }

        if let SourceParams::IngestApi(_) = source_config.source_params {
            // Ensure the queue exists.
            let create_queue_req = CreateQueueIfNotExistsRequest {
                queue_id: index_metadata.index_id.clone(),
            };
            self.ingest_api_service_mailbox
                .ask_for_res(create_queue_req)
                .await
                .map_err(|err| IndexingServiceError::InvalidParams(err.into()))?;
        }

        let indexing_dir_path = self.data_dir_path.join(INDEXING_DIR_NAME);
        let indexing_directory = self
            .get_or_create_indexing_directory(&pipeline_id, indexing_dir_path)
            .await?;
        let storage = self.storage_resolver.resolve(&index_metadata.index_uri)?;
        let merge_policy =
            crate::merge_policy::merge_policy_from_settings(&index_metadata.indexing_settings);
        let split_store = self
            .get_or_create_split_store(
                &pipeline_id,
                indexing_directory.cache_directory(),
                storage.clone(),
                merge_policy,
            )
            .await?;

        let pipeline_params = IndexingPipelineParams::try_new(
            pipeline_id.clone(),
            index_metadata,
            source_config,
            indexing_directory,
            split_store,
            self.metastore.clone(),
            storage,
            self.max_concurrent_split_uploads,
        )
        .map_err(IndexingServiceError::InvalidParams)?;

        let pipeline = IndexingPipeline::new(pipeline_params);
        let (_pipeline_mailbox, pipeline_handle) = ctx.spawn_actor().spawn(pipeline);
        self.indexing_pipeline_handles
            .insert(pipeline_id, pipeline_handle);
        self.state.num_running_pipelines += 1;

        Ok(())
    }

    async fn spawn_merge_pipeline(
        &mut self,
        ctx: &ActorContext<Self>,
        index_id: String,
        merge_enabled: bool,
    ) -> Result<IndexingPipelineId, IndexingServiceError> {
        let pipeline_id = IndexingPipelineId {
            index_id: index_id.clone(),
            source_id: "void-source".to_string(),
            node_id: self.node_id.clone(),
            pipeline_ord: 0,
        };
        let mut index_metadata = self.index_metadata(ctx, &pipeline_id.index_id).await?;
        if !merge_enabled {
            index_metadata.indexing_settings.merge_policy = MergePolicyConfig::Nop
        }
        let source_config = SourceConfig {
            source_id: pipeline_id.source_id.clone(),
            num_pipelines: 1,
            source_params: SourceParams::Vec(VecSourceParams::default()),
        };
        self.spawn_pipeline_inner(ctx, pipeline_id.clone(), index_metadata, source_config)
            .await?;
        Ok(pipeline_id)
    }

    async fn index_metadata(
        &self,
        ctx: &ActorContext<Self>,
        index_id: &str,
    ) -> Result<IndexMetadata, IndexingServiceError> {
        let _protect_guard = ctx.protect_zone();
        let index_metadata = self.metastore.index_metadata(index_id).await?;
        Ok(index_metadata)
    }

    async fn get_or_create_indexing_directory(
        &mut self,
        pipeline_id: &IndexingPipelineId,
        indexing_dir_path: PathBuf,
    ) -> Result<IndexingDirectory, IndexingServiceError> {
        let key = (pipeline_id.index_id.clone(), pipeline_id.source_id.clone());
        if let Some(indexing_directory) = self
            .indexing_directories
            .get(&key)
            .and_then(WeakIndexingDirectory::upgrade)
        {
            return Ok(indexing_directory);
        }
        let indexing_directory_path = indexing_dir_path
            .join(&pipeline_id.index_id)
            .join(&pipeline_id.source_id);
        let indexing_directory = IndexingDirectory::create_in_dir(indexing_directory_path)
            .await
            .map_err(IndexingServiceError::InvalidParams)?;

        self.indexing_directories
            .insert(key, indexing_directory.downgrade());
        Ok(indexing_directory)
    }

    async fn get_or_create_split_store(
        &mut self,
        pipeline_id: &IndexingPipelineId,
        cache_directory: &Path,
        storage: Arc<dyn Storage>,
        merge_policy: Arc<dyn MergePolicy>,
    ) -> Result<IndexingSplitStore, IndexingServiceError> {
        let key = (pipeline_id.index_id.clone(), pipeline_id.source_id.clone());
        if let Some(split_store) = self
            .split_stores
            .get(&key)
            .and_then(WeakIndexingSplitStore::upgrade)
        {
            return Ok(split_store);
        }
        let split_store = IndexingSplitStore::create_with_local_store(
            storage.clone(),
            cache_directory,
            merge_policy,
            self.split_store_space_quota.clone(),
        )
        .await?;

        self.split_stores.insert(key, split_store.downgrade());
        Ok(split_store)
    }

    async fn apply_indexing_plan(
        &mut self,
        ctx: &ActorContext<Self>,
        physical_indexing_plan_request: ApplyIndexingPlanRequest,
    ) -> Result<(), Vec<IndexingServiceError>> {
        let updated_pipeline_ids: HashSet<IndexingPipelineId> = physical_indexing_plan_request
            .indexing_tasks
            .iter()
            .map(|indexing_task| IndexingPipelineId {
                node_id: self.node_id.clone(),
                index_id: indexing_task.index_id.clone(),
                source_id: indexing_task.source_id.clone(),
                pipeline_ord: indexing_task.pipeline_ord as usize,
            })
            .collect();
        let running_pipeline_ids: HashSet<IndexingPipelineId> =
            self.indexing_pipeline_handles.keys().cloned().collect();

        let indexes_metadata_futures = updated_pipeline_ids
            .iter()
            .unique_by(|pipeline_id| pipeline_id.index_id.clone())
            .map(|pipeline_id| self.index_metadata(ctx, &pipeline_id.index_id));
        let indexes_metadata = try_join_all(indexes_metadata_futures)
            .await
            .map_err(|error| vec![error])?;
        let indexes_metadata_by_index_id: HashMap<String, IndexMetadata> = indexes_metadata
            .into_iter()
            .map(|index_metadata| (index_metadata.index_id.clone(), index_metadata))
            .collect();

        let mut pipeline_spawning_errors: Vec<IndexingServiceError> = Vec::new();

        // Add new pipeline ids.
        for new_pipeline_id in updated_pipeline_ids.difference(&running_pipeline_ids) {
            info!(pipeline_id=?new_pipeline_id, "Spawning indexing pipeline.");
            // Unwrap is safe as we have just filled the hashmap with the corresponding index ID.
            let index_metadata = indexes_metadata_by_index_id
                .get(&new_pipeline_id.index_id)
                .expect("`indexes_metadata_by_index_id` must contain all index IDs.");
            if let Some(source_config) = index_metadata.sources.get(&new_pipeline_id.source_id) {
                if let Err(error) = self
                    .spawn_pipeline_inner(
                        ctx,
                        new_pipeline_id.clone(),
                        index_metadata.clone(),
                        source_config.clone(),
                    )
                    .await
                {
                    pipeline_spawning_errors.push(error);
                }
            } else {
                pipeline_spawning_errors.push(IndexingServiceError::SourceNotFound {
                    index_id: new_pipeline_id.index_id.clone(),
                    source_id: new_pipeline_id.source_id.clone(),
                });
            }
        }

        // Remove missing pipeline ids.
        for pipeline_id_to_remove in running_pipeline_ids.difference(&updated_pipeline_ids) {
            // Just log the detach error, it can only come from a missing pipeline in the
            // `indexing_pipeline_handles`.
            if let Err(error) = self.detach_pipeline(pipeline_id_to_remove).await {
                error!(
                    index_id=%pipeline_id_to_remove.index_id,
                    source_id=%pipeline_id_to_remove.source_id,
                    pipeline_ord=%pipeline_id_to_remove.pipeline_ord,
                    "Error when trying to detach the pipeline: {}",
                    error,
                );
            }
            // Delete the queue.
            if pipeline_id_to_remove.source_id == INGEST_API_SOURCE_ID {
                let _ = self
                    .ingest_api_service_mailbox
                    .send_message(DropQueueRequest {
                        queue_id: pipeline_id_to_remove.index_id.clone(),
                    })
                    .await;
            }
        }

        self.update_cluster_running_indexing_tasks().await;

        if !pipeline_spawning_errors.is_empty() {
            return Err(pipeline_spawning_errors);
        }

        Ok(())
    }

    async fn update_cluster_running_indexing_tasks(&self) {
        let running_indexing_tasks = self
            .indexing_pipeline_handles
            .keys()
            .map(|pipeline_id| IndexingTask {
                index_id: pipeline_id.index_id.clone(),
                source_id: pipeline_id.source_id.clone(),
                pipeline_ord: pipeline_id.pipeline_ord as u64,
            })
            .collect_vec();
        if let Err(error) = self
            .cluster
            .set_self_node_running_indexing_tasks(&running_indexing_tasks)
            .await
        {
            error!(
                "Error when updating the cluster state with indexing running tasks: {}",
                error
            );
        }
    }
}

#[async_trait]
impl Handler<ObservePipeline> for IndexingService {
    type Reply = Result<Observation<IndexingStatistics>, IndexingServiceError>;

    async fn handle(
        &mut self,
        msg: ObservePipeline,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let observation = self.observe_pipeline(&msg.pipeline_id).await;
        Ok(observation)
    }
}

#[async_trait]
impl Handler<DetachPipeline> for IndexingService {
    type Reply = Result<ActorHandle<IndexingPipeline>, IndexingServiceError>;

    async fn handle(
        &mut self,
        msg: DetachPipeline,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(self.detach_pipeline(&msg.pipeline_id).await)
    }
}

#[async_trait]
impl Handler<ApplyIndexingPlanRequest> for IndexingService {
    type Reply = ApplyIndexingPlanResponse;

    async fn handle(
        &mut self,
        plan_request: ApplyIndexingPlanRequest,
        ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let errors = if let Err(indexing_service_errors) =
            self.apply_indexing_plan(ctx, plan_request).await
        {
            indexing_service_errors
                .iter()
                .map(|error| error.to_string())
                .collect_vec()
        } else {
            Vec::new()
        };
        Ok(ApplyIndexingPlanResponse { errors })
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
        self.indexing_pipeline_handles
            .retain(
                |pipeline_id, pipeline_handle| match pipeline_handle.health() {
                    Health::Healthy => true,
                    Health::Success => {
                        info!(
                            index_id=%pipeline_id.index_id,
                            source_id=%pipeline_id.source_id,
                            pipeline_ord=%pipeline_id.pipeline_ord,
                            "Indexing pipeline completed."
                        );
                        self.state.num_successful_pipelines += 1;
                        self.state.num_running_pipelines -= 1;
                        false
                    }
                    Health::FailureOrUnhealthy => {
                        error!(
                            index_id=%pipeline_id.index_id,
                            source_id=%pipeline_id.source_id,
                            pipeline_ord=%pipeline_id.pipeline_ord,
                            "Indexing pipeline failed."
                        );
                        self.state.num_failed_pipelines += 1;
                        self.state.num_running_pipelines -= 1;
                        false
                    }
                },
            );
        self.update_cluster_running_indexing_tasks().await;
        ctx.schedule_self_msg(quickwit_actors::HEARTBEAT, SuperviseLoop)
            .await;
        Ok(())
    }
}

#[async_trait]
impl Actor for IndexingService {
    type ObservableState = IndexingServiceState;

    fn observable_state(&self) -> Self::ObservableState {
        self.state.clone()
    }

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        self.handle(SuperviseLoop, ctx).await
    }
}

#[async_trait]
impl Handler<SpawnMergePipeline> for IndexingService {
    type Reply = Result<IndexingPipelineId, IndexingServiceError>;
    async fn handle(
        &mut self,
        message: SpawnMergePipeline,
        ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(self
            .spawn_merge_pipeline(ctx, message.index_id, message.merge_enabled)
            .await)
    }
}

#[async_trait]
impl Handler<SpawnPipeline> for IndexingService {
    type Reply = Result<IndexingPipelineId, IndexingServiceError>;
    async fn handle(
        &mut self,
        message: SpawnPipeline,
        ctx: &ActorContext<Self>,
    ) -> Result<Result<IndexingPipelineId, IndexingServiceError>, ActorExitStatus> {
        Ok(self
            .spawn_pipeline(
                ctx,
                message.index_id,
                message.source_config,
                message.pipeline_ord,
            )
            .await)
    }
}

#[async_trait]
impl Handler<Observe> for IndexingService {
    type Reply = Self::ObservableState;
    async fn handle(
        &mut self,
        _message: Observe,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::ObservableState, ActorExitStatus> {
        Ok(self.observable_state())
    }
}

#[async_trait]
impl Handler<ShutdownPipelines> for IndexingService {
    type Reply = Result<(), IndexingServiceError>;
    async fn handle(
        &mut self,
        message: ShutdownPipelines,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let source_filter_fn = |pipeline_id: &IndexingPipelineId| {
            message
                .source_id
                .as_ref()
                .map(|source_id| pipeline_id.source_id == *source_id)
                .unwrap_or(true)
        };
        let pipelines_to_shutdown: Vec<IndexingPipelineId> = self
            .indexing_pipeline_handles
            .keys()
            .filter(|pipeline_id| {
                pipeline_id.index_id == message.index_id && source_filter_fn(pipeline_id)
            })
            .cloned()
            .collect();
        for pipeline_id in pipelines_to_shutdown {
            if let Some(pipeline_handle) = self.indexing_pipeline_handles.remove(&pipeline_id) {
                pipeline_handle.quit().await;
                self.state.num_running_pipelines -= 1;
            }
        }
        Ok(Ok(()))
    }
}

#[async_trait]
impl Handler<ShutdownPipeline> for IndexingService {
    type Reply = Result<(), IndexingServiceError>;
    async fn handle(
        &mut self,
        message: ShutdownPipeline,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        if let Some(pipeline_handle) = self.indexing_pipeline_handles.remove(&message.pipeline_id) {
            pipeline_handle.quit().await;
            self.state.num_running_pipelines -= 1;
        }
        Ok(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use chitchat::transport::ChannelTransport;
    use quickwit_actors::{ObservationType, Universe};
    use quickwit_cluster::create_cluster_for_test;
    use quickwit_common::rand::append_random_suffix;
    use quickwit_common::uri::Uri;
    use quickwit_config::{SourceConfig, VecSourceParams};
    use quickwit_ingest_api::{init_ingest_api, QUEUES_DIR_NAME};
    use quickwit_metastore::quickwit_metastore_uri_resolver;
    use quickwit_proto::indexing_api::IndexingTask;

    use super::*;

    #[tokio::test]
    async fn test_indexing_service() {
        let transport = ChannelTransport::default();
        let cluster = Arc::new(
            create_cluster_for_test(Vec::new(), &["indexer"], &transport, true)
                .await
                .unwrap(),
        );
        let metastore_uri = Uri::new("ram:///metastore".to_string());
        let metastore = quickwit_metastore_uri_resolver()
            .resolve(&metastore_uri)
            .await
            .unwrap();

        let index_id = append_random_suffix("test-indexing-service");
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_metadata = IndexMetadata::for_test(&index_id, &index_uri);

        metastore.create_index(index_metadata).await.unwrap();

        // Test `IndexingService::new`.
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir_path = temp_dir.path().to_path_buf();
        let indexer_config = IndexerConfig::for_test().unwrap();
        let storage_resolver = StorageUriResolver::for_test();
        let universe = Universe::new();
        let queues_dir_path = data_dir_path.join(QUEUES_DIR_NAME);
        let ingest_api_service_mailbox =
            init_ingest_api(&universe, &queues_dir_path).await.unwrap();
        let indexing_server = IndexingService::new(
            "test-node".to_string(),
            data_dir_path,
            indexer_config,
            cluster.clone(),
            metastore.clone(),
            ingest_api_service_mailbox,
            storage_resolver.clone(),
        );
        let (indexing_server_mailbox, indexing_server_handle) =
            universe.spawn_builder().spawn(indexing_server);
        let observation = indexing_server_handle.observe().await;
        assert_eq!(observation.num_running_pipelines, 0);
        assert_eq!(observation.num_failed_pipelines, 0);
        assert_eq!(observation.num_successful_pipelines, 0);

        // Test `spawn_pipeline`.
        let source_config_0 = SourceConfig {
            source_id: "test-indexing-service--source-0".to_string(),
            num_pipelines: 1,
            source_params: SourceParams::void(),
        };
        let spawn_pipeline_msg = SpawnPipeline {
            index_id: index_id.clone(),
            pipeline_ord: 0,
            source_config: source_config_0.clone(),
        };
        let pipeline_id_0 = indexing_server_mailbox
            .ask_for_res(spawn_pipeline_msg.clone())
            .await
            .unwrap();
        indexing_server_mailbox
            .ask_for_res(spawn_pipeline_msg)
            .await
            .unwrap_err();
        assert_eq!(pipeline_id_0.index_id, index_id);
        assert_eq!(pipeline_id_0.source_id, source_config_0.source_id);
        assert_eq!(pipeline_id_0.node_id, "test-node");
        assert_eq!(pipeline_id_0.pipeline_ord, 0);
        assert_eq!(
            indexing_server_handle.observe().await.num_running_pipelines,
            1
        );

        // Test `observe_pipeline`.
        let observation = indexing_server_mailbox
            .ask_for_res(ObservePipeline {
                pipeline_id: pipeline_id_0.clone(),
            })
            .await
            .unwrap();
        assert_eq!(observation.obs_type, ObservationType::Alive);
        assert_eq!(observation.generation, 1);
        assert_eq!(observation.num_spawn_attempts, 1);

        // Test `detach_pipeline`.
        let pipeline_handle = indexing_server_mailbox
            .ask_for_res(DetachPipeline {
                pipeline_id: pipeline_id_0,
            })
            .await
            .unwrap();
        assert_eq!(
            indexing_server_handle.observe().await.num_running_pipelines,
            0
        );
        let observation = pipeline_handle.observe().await;
        assert_eq!(observation.obs_type, ObservationType::Alive);

        // Test `spawn_pipelines`.
        metastore
            .add_source(&index_id, source_config_0.clone())
            .await
            .unwrap();

        let source_config_1 = SourceConfig {
            source_id: "test-indexing-service--source-1".to_string(),
            num_pipelines: 1,
            source_params: SourceParams::void(),
        };
        metastore
            .add_source(&index_id, source_config_1.clone())
            .await
            .unwrap();
        let indexing_tasks = vec![
            IndexingTask {
                index_id: index_id.to_string(),
                source_id: "test-indexing-service--source-1".to_string(),
                pipeline_ord: 0,
            },
            IndexingTask {
                index_id: index_id.to_string(),
                source_id: "test-indexing-service--source-1".to_string(),
                pipeline_ord: 1,
            },
        ];
        indexing_server_mailbox
            .ask(ApplyIndexingPlanRequest { indexing_tasks })
            .await
            .unwrap();
        assert_eq!(
            indexing_server_handle.observe().await.num_running_pipelines,
            2
        );

        let source_config_2 = SourceConfig {
            source_id: "test-indexing-service--source-2".to_string(),
            num_pipelines: 2,
            source_params: SourceParams::void(),
        };
        metastore
            .add_source(&index_id, source_config_2.clone())
            .await
            .unwrap();

        let indexing_tasks = vec![
            IndexingTask {
                index_id: index_id.to_string(),
                source_id: source_config_0.source_id.clone(),
                pipeline_ord: 0,
            },
            IndexingTask {
                index_id: index_id.to_string(),
                source_id: "test-indexing-service--source-1".to_string(),
                pipeline_ord: 0,
            },
            IndexingTask {
                index_id: index_id.to_string(),
                source_id: "test-indexing-service--source-1".to_string(),
                pipeline_ord: 1,
            },
            IndexingTask {
                index_id: index_id.to_string(),
                source_id: source_config_2.source_id.clone(),
                pipeline_ord: 0,
            },
        ];
        indexing_server_mailbox
            .ask(ApplyIndexingPlanRequest {
                indexing_tasks: indexing_tasks.clone(),
            })
            .await
            .unwrap();
        assert_eq!(
            indexing_server_handle.observe().await.num_running_pipelines,
            4
        );
        let self_member = &cluster.ready_members_from_chitchat_state().await[0];
        assert_eq!(
            HashSet::<_>::from_iter(self_member.running_indexing_tasks.iter()),
            HashSet::from_iter(indexing_tasks.iter())
        );

        let indexing_tasks = vec![
            IndexingTask {
                index_id: index_id.to_string(),
                source_id: source_config_0.source_id.clone(),
                pipeline_ord: 0,
            },
            IndexingTask {
                index_id: index_id.to_string(),
                source_id: "test-indexing-service--source-1".to_string(),
                pipeline_ord: 0,
            },
            IndexingTask {
                index_id: index_id.to_string(),
                source_id: source_config_2.source_id.clone(),
                pipeline_ord: 0,
            },
        ];

        indexing_server_mailbox
            .ask(ApplyIndexingPlanRequest {
                indexing_tasks: indexing_tasks.clone(),
            })
            .await
            .unwrap();
        assert_eq!(
            indexing_server_handle.observe().await.num_running_pipelines,
            3
        );
        let self_member = &cluster.ready_members_from_chitchat_state().await[0];
        assert_eq!(
            HashSet::<_>::from_iter(self_member.running_indexing_tasks.iter()),
            HashSet::from_iter(indexing_tasks.iter())
        );

        // Test `shutdown_pipeline`
        indexing_server_mailbox
            .ask_for_res(ShutdownPipeline {
                pipeline_id: IndexingPipelineId {
                    index_id: index_id.clone(),
                    source_id: source_config_2.source_id.clone(),
                    node_id: "test-node".to_string(),
                    pipeline_ord: 0,
                },
            })
            .await
            .unwrap();
        assert_eq!(
            indexing_server_handle.observe().await.num_running_pipelines,
            2
        );

        // Test `shutdown_pipelines`
        indexing_server_mailbox
            .ask_for_res(ShutdownPipelines {
                index_id: index_id.clone(),
                source_id: Some(source_config_0.source_id.clone()),
            })
            .await
            .unwrap();
        assert_eq!(
            indexing_server_handle.observe().await.num_running_pipelines,
            1
        );
        indexing_server_mailbox
            .ask_for_res(ShutdownPipelines {
                index_id: index_id.clone(),
                source_id: None,
            })
            .await
            .unwrap();
        assert_eq!(
            indexing_server_handle.observe().await.num_running_pipelines,
            0
        );

        // Test `spawn_merge_pipeline`.
        indexing_server_mailbox
            .ask_for_res(SpawnMergePipeline {
                index_id: index_id.clone(),
                merge_enabled: true,
            })
            .await
            .unwrap();
        assert_eq!(
            indexing_server_handle.observe().await.num_running_pipelines,
            1
        );

        // Test `supervise_pipelines`
        let source_config_3 = SourceConfig {
            source_id: "test-indexing-service--source-3".to_string(),
            num_pipelines: 1,
            source_params: SourceParams::Vec(VecSourceParams {
                docs: Vec::new(),
                batch_num_docs: 10,
                partition: "0".to_string(),
            }),
        };
        indexing_server_mailbox
            .ask_for_res(SpawnPipeline {
                index_id: index_id.clone(),
                source_config: source_config_3,
                pipeline_ord: 0,
            })
            .await
            .unwrap();
        for _ in 0..2000 {
            let obs = indexing_server_handle.observe().await;
            if obs.num_successful_pipelines == 2 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("Sleep");
    }
}
