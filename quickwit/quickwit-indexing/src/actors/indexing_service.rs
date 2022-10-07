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

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use quickwit_actors::{
    Actor, ActorContext, ActorExitStatus, ActorHandle, Handler, Health, Observation, Supervisable,
};
use quickwit_common::fs::get_cache_directory_path;
use quickwit_config::merge_policy_config::MergePolicyConfig;
use quickwit_config::{
    IndexerConfig, IngestApiSourceParams, SourceConfig, SourceParams, VecSourceParams,
};
use quickwit_ingest_api::{get_ingest_api_service, QUEUES_DIR_NAME};
use quickwit_metastore::{IndexMetadata, Metastore, MetastoreError};
use quickwit_proto::ingest_api::CreateQueueIfNotExistsRequest;
use quickwit_proto::{ServiceError, ServiceErrorCode};
use quickwit_storage::{Storage, StorageError, StorageResolverError, StorageUriResolver};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{error, info};

use crate::merge_policy::MergePolicy;
use crate::models::{
    DetachPipeline, IndexingDirectory, IndexingPipelineId, Observe, ObservePipeline,
    ShutdownPipeline, ShutdownPipelines, SpawnMergePipeline, SpawnPipeline, SpawnPipelines,
    WeakIndexingDirectory,
};
use crate::source::INGEST_API_SOURCE_ID;
use crate::split_store::{IndexingSplitStore, LocalSplitStore, SplitStoreQuota};
use crate::{IndexingPipeline, IndexingPipelineParams, IndexingStatistics};

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
    metastore: Arc<dyn Metastore>,
    storage_resolver: StorageUriResolver,
    indexing_pipeline_handles: HashMap<IndexingPipelineId, ActorHandle<IndexingPipeline>>,
    state: IndexingServiceState,
    enable_ingest_api: bool,
    indexing_directories: HashMap<(IndexId, SourceId), WeakIndexingDirectory>,
    local_split_store: Arc<LocalSplitStore>,
    max_concurrent_split_uploads: usize,
}

impl IndexingService {
    pub fn check_health(&self) -> Health {
        // In the future, check metrics such as available disk space.
        Health::Healthy
    }

    pub async fn new(
        node_id: String,
        data_dir_path: PathBuf,
        indexer_config: IndexerConfig,
        metastore: Arc<dyn Metastore>,
        storage_resolver: StorageUriResolver,
        enable_ingest_api: bool,
    ) -> anyhow::Result<IndexingService> {
        let split_store_space_quota = SplitStoreQuota::new(
            indexer_config.split_store_max_num_splits,
            indexer_config.split_store_max_num_bytes,
        );
        let split_cache_dir_path = get_cache_directory_path(&data_dir_path);
        let local_split_store =
            LocalSplitStore::open(split_cache_dir_path, split_store_space_quota).await?;
        Ok(Self {
            node_id,
            data_dir_path,
            metastore,
            storage_resolver,
            indexing_pipeline_handles: Default::default(),
            state: Default::default(),
            enable_ingest_api,
            indexing_directories: HashMap::new(),
            max_concurrent_split_uploads: indexer_config.max_concurrent_split_uploads,
            local_split_store: Arc::new(local_split_store),
        })
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

    async fn spawn_pipelines(
        &mut self,
        ctx: &ActorContext<Self>,
        index_id: String,
    ) -> Result<Vec<IndexingPipelineId>, IndexingServiceError> {
        let mut pipeline_ids = Vec::new();

        let index_metadata = self.index_metadata(ctx, &index_id).await?;

        for source_config in index_metadata.sources.values() {
            let pipeline_ords = 0..source_config.num_pipelines().unwrap_or(1);
            for pipeline_ord in pipeline_ords {
                let pipeline_id = IndexingPipelineId {
                    index_id: index_id.clone(),
                    source_id: source_config.source_id.clone(),
                    node_id: self.node_id.clone(),
                    pipeline_ord,
                };
                if self.indexing_pipeline_handles.contains_key(&pipeline_id) {
                    continue;
                }
                self.spawn_pipeline_inner(
                    ctx,
                    pipeline_id.clone(),
                    index_metadata.clone(),
                    source_config.clone(),
                )
                .await?;
                pipeline_ids.push(pipeline_id);
            }
            ctx.record_progress();
        }
        if self.enable_ingest_api {
            let pipeline_id = self
                .spawn_ingest_api_pipeline(ctx, index_id, index_metadata)
                .await?;
            pipeline_ids.push(pipeline_id);
        }
        Ok(pipeline_ids)
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
        let indexing_dir_path = self.data_dir_path.join(INDEXING_DIR_NAME);
        let indexing_directory = self
            .get_or_create_indexing_directory(&pipeline_id, indexing_dir_path)
            .await?;
        let storage = self.storage_resolver.resolve(&index_metadata.index_uri)?;
        let merge_policy =
            crate::merge_policy::merge_policy_from_settings(&index_metadata.indexing_settings);
        let split_store = self.create_split_store(storage.clone(), merge_policy);

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

    async fn spawn_ingest_api_pipeline(
        &mut self,
        ctx: &ActorContext<Self>,
        index_id: String,
        index_metadata: IndexMetadata,
    ) -> Result<IndexingPipelineId, IndexingServiceError> {
        let source_id = INGEST_API_SOURCE_ID.to_string();

        let pipeline_id = IndexingPipelineId {
            index_id: index_id.clone(),
            source_id: source_id.clone(),
            node_id: self.node_id.clone(),
            pipeline_ord: 0,
        };
        if self.indexing_pipeline_handles.contains_key(&pipeline_id) {
            return Ok(pipeline_id);
        }
        let queues_dir_path = self.data_dir_path.join(QUEUES_DIR_NAME);
        let ingest_api_service = get_ingest_api_service(&queues_dir_path)
            .await
            .expect("The ingest API service should have been initialized beforehand.");

        // Ensure the queue exists.
        let create_queue_req = CreateQueueIfNotExistsRequest {
            queue_id: index_id.clone(),
        };
        ingest_api_service
            .ask_for_res(create_queue_req)
            .await
            .map_err(|err| IndexingServiceError::InvalidParams(err.into()))?;

        let source_config = SourceConfig {
            source_id,
            num_pipelines: 1,
            source_params: SourceParams::IngestApi(IngestApiSourceParams {
                index_id,
                batch_num_bytes_limit: None,
                queues_dir_path,
            }),
        };
        self.spawn_pipeline_inner(
            ctx,
            pipeline_id.clone(),
            index_metadata.clone(),
            source_config,
        )
        .await?;
        Ok(pipeline_id)
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

    fn create_split_store(
        &mut self,
        storage: Arc<dyn Storage>,
        merge_policy: Arc<dyn MergePolicy>,
    ) -> IndexingSplitStore {
        IndexingSplitStore::new(
            storage.clone(),
            merge_policy,
            self.local_split_store.clone(),
        )
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
impl Handler<SpawnPipelines> for IndexingService {
    type Reply = Result<Vec<IndexingPipelineId>, IndexingServiceError>;
    async fn handle(
        &mut self,
        message: SpawnPipelines,
        ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(self.spawn_pipelines(ctx, message.index_id).await)
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

    use quickwit_actors::{ObservationType, Universe};
    use quickwit_common::rand::append_random_suffix;
    use quickwit_common::uri::Uri;
    use quickwit_config::{SourceConfig, VecSourceParams};
    use quickwit_ingest_api::init_ingest_api;
    use quickwit_metastore::quickwit_metastore_uri_resolver;

    use super::*;

    #[tokio::test]
    async fn test_indexing_service() {
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
        init_ingest_api(&universe, &queues_dir_path).await.unwrap();
        let enable_ingest_api = true;
        let indexing_server = IndexingService::new(
            "test-node".to_string(),
            data_dir_path,
            indexer_config,
            metastore.clone(),
            storage_resolver.clone(),
            enable_ingest_api,
        )
        .await
        .unwrap();
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

        indexing_server_mailbox
            .ask_for_res(SpawnPipelines {
                index_id: index_id.clone(),
            })
            .await
            .unwrap();
        assert_eq!(
            indexing_server_handle.observe().await.num_running_pipelines,
            3
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

        indexing_server_mailbox
            .ask_for_res(SpawnPipelines {
                index_id: index_id.clone(),
            })
            .await
            .unwrap();
        assert_eq!(
            indexing_server_handle.observe().await.num_running_pipelines,
            5
        );

        // Test `shutdown_pipeline`
        indexing_server_mailbox
            .ask_for_res(ShutdownPipeline {
                pipeline_id: IndexingPipelineId {
                    index_id: index_id.clone(),
                    source_id: source_config_2.source_id.clone(),
                    node_id: "test-node".to_string(),
                    pipeline_ord: 1,
                },
            })
            .await
            .unwrap();
        assert_eq!(
            indexing_server_handle.observe().await.num_running_pipelines,
            4
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
            3
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
