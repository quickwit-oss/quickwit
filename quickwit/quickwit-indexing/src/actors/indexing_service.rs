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
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use quickwit_actors::{
    Actor, ActorContext, ActorExitStatus, ActorHandle, ActorState, Handler, Healthz, Mailbox,
    Observation,
};
use quickwit_common::fs::get_cache_directory_path;
use quickwit_config::{
    build_doc_mapper, IndexConfig, IndexerConfig, SourceConfig, CLI_INGEST_SOURCE_ID,
};
use quickwit_ingest_api::QUEUES_DIR_NAME;
use quickwit_metastore::{IndexMetadata, Metastore, MetastoreError};
use quickwit_proto::{ServiceError, ServiceErrorCode};
use quickwit_storage::{StorageError, StorageResolverError, StorageUriResolver};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{error, info};

use super::merge_pipeline::{MergePipeline, MergePipelineParams};
use super::MergePlanner;
use crate::models::{
    DetachIndexingPipeline, DetachMergePipeline, IndexingPipelineId, Observe, ObservePipeline,
    ScratchDirectory, ShutdownPipeline, ShutdownPipelines, SpawnPipeline, SpawnPipelines,
    WeakScratchDirectory,
};
use crate::split_store::{LocalSplitStore, SplitStoreQuota};
use crate::{IndexingPipeline, IndexingPipelineParams, IndexingSplitStore, IndexingStatistics};

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
pub struct IndexingServiceCounters {
    pub num_running_pipelines: usize,
    pub num_successful_pipelines: usize,
    pub num_failed_pipelines: usize,
    pub num_running_merge_pipelines: usize,
}

type IndexId = String;
type SourceId = String;

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct MergePipelineId {
    index_id: String,
    source_id: String,
}

impl<'a> From<&'a IndexingPipelineId> for MergePipelineId {
    fn from(pipeline_id: &'a IndexingPipelineId) -> Self {
        MergePipelineId {
            index_id: pipeline_id.index_id.clone(),
            source_id: pipeline_id.source_id.clone(),
        }
    }
}

struct MergePipelineHandle {
    mailbox: Mailbox<MergePlanner>,
    handle: ActorHandle<MergePipeline>,
}

pub struct IndexingService {
    node_id: String,
    data_dir_path: PathBuf,
    metastore: Arc<dyn Metastore>,
    storage_resolver: StorageUriResolver,
    indexing_pipeline_handles: HashMap<IndexingPipelineId, ActorHandle<IndexingPipeline>>,
    counters: IndexingServiceCounters,
    indexing_directories: HashMap<(IndexId, SourceId), WeakScratchDirectory>,
    local_split_store: Arc<LocalSplitStore>,
    max_concurrent_split_uploads: usize,
    merge_pipeline_handles: HashMap<MergePipelineId, MergePipelineHandle>,
}

impl IndexingService {
    pub async fn new(
        node_id: String,
        data_dir_path: PathBuf,
        indexer_config: IndexerConfig,
        metastore: Arc<dyn Metastore>,
        storage_resolver: StorageUriResolver,
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
            local_split_store: Arc::new(local_split_store),
            indexing_pipeline_handles: Default::default(),
            counters: Default::default(),
            indexing_directories: HashMap::new(),
            max_concurrent_split_uploads: indexer_config.max_concurrent_split_uploads,
            merge_pipeline_handles: HashMap::new(),
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
        self.counters.num_running_pipelines -= 1;
        Ok(pipeline_handle)
    }

    async fn detach_merge_pipeline(
        &mut self,
        pipeline_id: &MergePipelineId,
    ) -> Result<ActorHandle<MergePipeline>, IndexingServiceError> {
        let pipeline_handle = self
            .merge_pipeline_handles
            .remove(pipeline_id)
            .ok_or_else(|| IndexingServiceError::MissingPipeline {
                index_id: pipeline_id.index_id.clone(),
                source_id: pipeline_id.source_id.clone(),
            })?;
        self.counters.num_running_merge_pipelines -= 1;
        Ok(pipeline_handle.handle)
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
        let index_config = self
            .index_metadata(ctx, &pipeline_id.index_id)
            .await?
            .into_index_config();
        self.spawn_pipeline_inner(ctx, pipeline_id.clone(), index_config, source_config)
            .await?;
        Ok(pipeline_id)
    }

    async fn spawn_pipelines(
        &mut self,
        ctx: &ActorContext<Self>,
        index_id: String,
    ) -> Result<Vec<IndexingPipelineId>, IndexingServiceError> {
        let mut pipeline_ids = Vec::new();
        let IndexMetadata {
            index_config,
            sources,
            ..
        } = self.index_metadata(ctx, &index_id).await?;

        for source_config in sources.values() {
            // Skip disabled source
            if !source_config.enabled {
                continue;
            }

            // Skip cli source
            if source_config.source_id == CLI_INGEST_SOURCE_ID {
                continue;
            }

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
                    index_config.clone(),
                    source_config.clone(),
                )
                .await?;
                pipeline_ids.push(pipeline_id);
            }
            ctx.record_progress();
        }
        Ok(pipeline_ids)
    }

    async fn spawn_pipeline_inner(
        &mut self,
        ctx: &ActorContext<Self>,
        pipeline_id: IndexingPipelineId,
        index_config: IndexConfig,
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
        let storage = self.storage_resolver.resolve(&index_config.index_uri)?;
        let queues_dir_path = self.data_dir_path.join(QUEUES_DIR_NAME);
        let merge_policy =
            crate::merge_policy::merge_policy_from_settings(&index_config.indexing_settings);
        let split_store = IndexingSplitStore::new(
            storage.clone(),
            merge_policy.clone(),
            self.local_split_store.clone(),
        );

        let doc_mapper = build_doc_mapper(&index_config.doc_mapping, &index_config.search_settings)
            .map_err(IndexingServiceError::InvalidParams)?;

        let merge_pipeline_params = MergePipelineParams {
            pipeline_id: pipeline_id.clone(),
            doc_mapper: doc_mapper.clone(),
            indexing_directory: indexing_directory.clone(),
            metastore: self.metastore.clone(),
            split_store: split_store.clone(),
            merge_policy,
            merge_max_io_num_bytes_per_sec: index_config
                .indexing_settings
                .resources
                .max_merge_write_throughput,
            max_concurrent_split_uploads: self.max_concurrent_split_uploads,
        };

        let merge_planner_mailbox = self
            .get_or_create_merge_pipeline(merge_pipeline_params, ctx)
            .await?;

        let max_concurrent_split_uploads_index = (self.max_concurrent_split_uploads / 2).max(1);
        let max_concurrent_split_uploads_merge =
            (self.max_concurrent_split_uploads - max_concurrent_split_uploads_index).max(1);
        let pipeline_params = IndexingPipelineParams {
            pipeline_id: pipeline_id.clone(),
            doc_mapper,
            indexing_settings: index_config.indexing_settings.clone(),
            source_config,
            indexing_directory,
            metastore: self.metastore.clone(),
            storage,
            split_store,
            max_concurrent_split_uploads_index,
            max_concurrent_split_uploads_merge,
            queues_dir_path,
            merge_planner_mailbox,
        };
        let pipeline = IndexingPipeline::new(pipeline_params);
        let (_pipeline_mailbox, pipeline_handle) = ctx.spawn_actor().spawn(pipeline);
        self.indexing_pipeline_handles
            .insert(pipeline_id, pipeline_handle);
        self.counters.num_running_pipelines += 1;
        Ok(())
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

    async fn handle_supervise(&mut self) -> Result<(), ActorExitStatus> {
        self.indexing_pipeline_handles
            .retain(
                |pipeline_id, pipeline_handle| match pipeline_handle.state() {
                    ActorState::Idle | ActorState::Paused | ActorState::Processing => true,
                    ActorState::Success => {
                        info!(
                            index_id=%pipeline_id.index_id,
                            source_id=%pipeline_id.source_id,
                            pipeline_ord=%pipeline_id.pipeline_ord,
                            "Indexing pipeline exited successfully."
                        );
                        self.counters.num_successful_pipelines += 1;
                        self.counters.num_running_pipelines -= 1;
                        false
                    }
                    ActorState::Failure => {
                        // This should never happen: Indexing Pipelines are not supposed to fail,
                        // and are themselves in charge of supervising the pipeline actors.
                        error!(
                            index_id=%pipeline_id.index_id,
                            source_id=%pipeline_id.source_id,
                            pipeline_ord=%pipeline_id.pipeline_ord,
                            "Indexing pipeline exited with failure. This should never happen."
                        );
                        self.counters.num_failed_pipelines += 1;
                        self.counters.num_running_pipelines -= 1;
                        false
                    }
                },
            );
        // Evict and kill merge pipelines that are not needed.
        let needed_merge_pipeline_ids: HashSet<MergePipelineId> = self
            .indexing_pipeline_handles
            .keys()
            .map(MergePipelineId::from)
            .collect();
        let current_merge_pipeline_ids: HashSet<MergePipelineId> =
            self.merge_pipeline_handles.keys().cloned().collect();
        for merge_pipeline_id_to_shut_down in
            current_merge_pipeline_ids.difference(&needed_merge_pipeline_ids)
        {
            if let Some((_, merge_pipeline_handle)) = self
                .merge_pipeline_handles
                .remove_entry(merge_pipeline_id_to_shut_down)
            {
                // We kill the merge pipeline to avoid waiting a merge operation to finish as it can
                // be long.
                info!(
                    index_id=%merge_pipeline_id_to_shut_down.index_id,
                    source_id=%merge_pipeline_id_to_shut_down.source_id,
                    "No more indexing pipeline on this index and source, killing merge pipeline."
                );
                merge_pipeline_handle.handle.kill().await;
            }
        }
        // Finally remove the merge pipeline with an exit status.
        self.merge_pipeline_handles
            .retain(|_, merge_pipeline_mailbox_handle| {
                merge_pipeline_mailbox_handle.handle.state().is_running()
            });
        self.counters.num_running_merge_pipelines = self.merge_pipeline_handles.len();
        Ok(())
    }

    async fn get_or_create_indexing_directory(
        &mut self,
        pipeline_id: &IndexingPipelineId,
        indexing_dir_path: PathBuf,
    ) -> Result<ScratchDirectory, IndexingServiceError> {
        let key = (pipeline_id.index_id.clone(), pipeline_id.source_id.clone());
        if let Some(indexing_directory) = self
            .indexing_directories
            .get(&key)
            .and_then(WeakScratchDirectory::upgrade)
        {
            return Ok(indexing_directory);
        }
        let indexing_directory_path = indexing_dir_path
            .join(&pipeline_id.index_id)
            .join(&pipeline_id.source_id);
        let indexing_directory = ScratchDirectory::create_in_dir(indexing_directory_path)
            .await
            .map_err(IndexingServiceError::InvalidParams)?;

        self.indexing_directories
            .insert(key, indexing_directory.downgrade());
        Ok(indexing_directory)
    }

    async fn get_or_create_merge_pipeline(
        &mut self,
        merge_pipeline_params: MergePipelineParams,
        ctx: &ActorContext<Self>,
    ) -> Result<Mailbox<MergePlanner>, IndexingServiceError> {
        let merge_pipeline_id = MergePipelineId::from(&merge_pipeline_params.pipeline_id);
        if let Some(merge_pipeline_mailbox_handle) =
            self.merge_pipeline_handles.get(&merge_pipeline_id)
        {
            return Ok(merge_pipeline_mailbox_handle.mailbox.clone());
        }
        let merge_pipeline = MergePipeline::new(merge_pipeline_params);
        let merge_planner_mailbox = merge_pipeline.merge_planner_mailbox().clone();
        let (_pipeline_mailbox, pipeline_handle) = ctx.spawn_actor().spawn(merge_pipeline);
        let merge_pipeline_mailbox_handle = MergePipelineHandle {
            mailbox: merge_planner_mailbox.clone(),
            handle: pipeline_handle,
        };
        self.merge_pipeline_handles
            .insert(merge_pipeline_id, merge_pipeline_mailbox_handle);
        self.counters.num_running_merge_pipelines += 1;
        Ok(merge_planner_mailbox)
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
impl Handler<DetachIndexingPipeline> for IndexingService {
    type Reply = Result<ActorHandle<IndexingPipeline>, IndexingServiceError>;

    async fn handle(
        &mut self,
        msg: DetachIndexingPipeline,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(self.detach_pipeline(&msg.pipeline_id).await)
    }
}

#[async_trait]
impl Handler<DetachMergePipeline> for IndexingService {
    type Reply = Result<ActorHandle<MergePipeline>, IndexingServiceError>;

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
        ctx.schedule_self_msg(quickwit_actors::HEARTBEAT, SuperviseLoop)
            .await;
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
        self.handle(SuperviseLoop, ctx).await
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
                self.counters.num_running_pipelines -= 1;
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
            self.counters.num_running_pipelines -= 1;
        }
        Ok(Ok(()))
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use quickwit_actors::{Health, ObservationType, Supervisable, Universe, HEARTBEAT};
    use quickwit_common::rand::append_random_suffix;
    use quickwit_common::uri::Uri;
    use quickwit_config::{SourceConfig, SourceParams, VecSourceParams};
    use quickwit_ingest_api::init_ingest_api;
    use quickwit_metastore::{quickwit_metastore_uri_resolver, MockMetastore};

    use super::*;

    #[tokio::test]
    async fn test_indexing_service() {
        let metastore_uri = Uri::from_well_formed("ram:///metastore");
        let metastore = quickwit_metastore_uri_resolver()
            .resolve(&metastore_uri)
            .await
            .unwrap();

        let index_id = append_random_suffix("test-indexing-service");
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_config = IndexConfig::for_test(&index_id, &index_uri);

        metastore.create_index(index_config).await.unwrap();
        metastore
            .add_source(&index_id, SourceConfig::ingest_api_default())
            .await
            .unwrap();

        // Test `IndexingService::new`.
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir_path = temp_dir.path().to_path_buf();
        let indexer_config = IndexerConfig::for_test().unwrap();
        let storage_resolver = StorageUriResolver::for_test();
        let universe = Universe::new();
        let queues_dir_path = data_dir_path.join(QUEUES_DIR_NAME);
        init_ingest_api(&universe, &queues_dir_path).await.unwrap();
        let indexing_server = IndexingService::new(
            "test-node".to_string(),
            data_dir_path,
            indexer_config,
            metastore.clone(),
            storage_resolver.clone(),
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
            enabled: true,
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
            .ask_for_res(DetachIndexingPipeline {
                pipeline_id: pipeline_id_0.clone(),
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
            enabled: true,
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
            enabled: true,
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
        let observation = indexing_server_handle.process_pending_and_observe().await;
        assert_eq!(observation.num_running_pipelines, 0);

        // Let the service cleanup the merge pipelines.
        universe.simulate_time_shift(HEARTBEAT).await;

        // Test spawning a merge pipeline.
        let pipeline_id = indexing_server_mailbox
            .ask_for_res(SpawnPipeline {
                index_id: index_id.clone(),
                source_config: SourceConfig {
                    source_id: source_config_0.source_id,
                    num_pipelines: 1,
                    enabled: true,
                    source_params: SourceParams::Vec(VecSourceParams::default()),
                },
                pipeline_ord: 0,
            })
            .await
            .unwrap();
        let observation = indexing_server_handle.process_pending_and_observe().await;
        assert_eq!(observation.num_running_pipelines, 1);
        assert_eq!(observation.num_running_merge_pipelines, 1);

        // Test `detach_merge_pipeline`
        let _pipeline = indexing_server_mailbox
            .ask_for_res(DetachMergePipeline {
                pipeline_id: MergePipelineId::from(&pipeline_id),
            })
            .await
            .unwrap();
        let observation = indexing_server_handle.process_pending_and_observe().await;
        assert_eq!(observation.num_running_merge_pipelines, 0);

        // Test `supervise_pipelines`
        let source_config_3 = SourceConfig {
            source_id: "test-indexing-service--source-3".to_string(),
            num_pipelines: 1,
            enabled: true,
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

    #[tokio::test]
    async fn test_indexing_service_shut_down_merge_pipeline_when_no_indexing_pipeline() {
        quickwit_common::setup_logging_for_tests();
        let metastore_uri = Uri::from_well_formed("ram:///metastore");
        let metastore = quickwit_metastore_uri_resolver()
            .resolve(&metastore_uri)
            .await
            .unwrap();

        let index_id = append_random_suffix("test-indexing-service");
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_config = IndexConfig::for_test(&index_id, &index_uri);

        let source_config = SourceConfig {
            source_id: "test-indexing-service--source".to_string(),
            num_pipelines: 1,
            enabled: true,
            source_params: SourceParams::void(),
        };
        metastore.create_index(index_config).await.unwrap();
        metastore
            .add_source(&index_id, source_config.clone())
            .await
            .unwrap();

        // Test `IndexingService::new`.
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir_path = temp_dir.path().to_path_buf();
        let indexer_config = IndexerConfig::for_test().unwrap();
        let storage_resolver = StorageUriResolver::for_test();
        let universe = Universe::new();
        let queues_dir_path = data_dir_path.join(QUEUES_DIR_NAME);
        init_ingest_api(&universe, &queues_dir_path).await.unwrap();
        let indexing_server = IndexingService::new(
            "test-node".to_string(),
            data_dir_path,
            indexer_config,
            metastore.clone(),
            storage_resolver.clone(),
        )
        .await
        .unwrap();
        let (indexing_server_mailbox, indexing_server_handle) =
            universe.spawn_builder().spawn(indexing_server);
        indexing_server_mailbox
            .ask_for_res(SpawnPipelines {
                index_id: index_id.clone(),
            })
            .await
            .unwrap();
        let observation = indexing_server_handle.observe().await;
        assert_eq!(observation.num_running_pipelines, 1);
        assert_eq!(observation.num_failed_pipelines, 0);
        assert_eq!(observation.num_successful_pipelines, 0);
        assert_eq!(observation.num_running_merge_pipelines, 1);

        // Test `shutdown_pipeline`
        indexing_server_mailbox
            .ask_for_res(ShutdownPipelines {
                index_id: index_id.clone(),
                source_id: None,
            })
            .await
            .unwrap();

        // Let the service cleanup the merge pipelines.
        universe.simulate_time_shift(HEARTBEAT).await;

        let observation = indexing_server_handle.process_pending_and_observe().await;
        assert_eq!(observation.num_running_pipelines, 0);
        assert_eq!(observation.num_running_merge_pipelines, 0);
        universe.simulate_time_shift(HEARTBEAT).await;
        // Check that the merge pipeline is also shut down as they are no more indexing pipeilne on
        // the index.
        assert!(universe.get_one::<MergePipeline>().is_none());
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
            tokio::time::sleep(HEARTBEAT * 5).await;
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
                .indexing_pipeline_handles
                .get(&message.0)
                .unwrap()
                .harvest_health())
        }
    }

    #[tokio::test]
    async fn test_indexing_service_does_not_shut_down_pipelines_on_indexing_pipeline_freeze() {
        quickwit_common::setup_logging_for_tests();
        let index_id = append_random_suffix("test-indexing-service-indexing-pipeline-timeout");
        let index_uri = format!("ram:///indexes/{index_id}");
        let mut index_metadata = IndexMetadata::for_test(&index_id, &index_uri);
        let source_config = SourceConfig {
            source_id: "test-indexing-service--source".to_string(),
            num_pipelines: 1,
            enabled: true,
            source_params: SourceParams::void(),
        };
        index_metadata
            .sources
            .insert(source_config.source_id.clone(), source_config);
        let mut metastore = MockMetastore::default();
        metastore
            .expect_index_metadata()
            .returning(move |_| Ok(index_metadata.clone()));
        metastore.expect_list_splits().returning(|_| Ok(Vec::new()));

        // Test `IndexingService::new`.
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir_path = temp_dir.path().to_path_buf();
        let indexer_config = IndexerConfig::for_test().unwrap();
        let storage_resolver = StorageUriResolver::for_test();
        let universe = Universe::new();
        let queues_dir_path = data_dir_path.join(QUEUES_DIR_NAME);
        init_ingest_api(&universe, &queues_dir_path).await.unwrap();
        let indexing_server = IndexingService::new(
            "test-node".to_string(),
            data_dir_path,
            indexer_config,
            Arc::new(metastore),
            storage_resolver.clone(),
        )
        .await
        .unwrap();
        let (indexing_server_mailbox, indexing_server_handle) =
            universe.spawn_builder().spawn(indexing_server);
        let _pipeline_ids = indexing_server_mailbox
            .ask_for_res(SpawnPipelines {
                index_id: index_id.clone(),
            })
            .await
            .unwrap();
        let observation = indexing_server_handle.observe().await;
        assert_eq!(observation.num_running_pipelines, 1);
        assert_eq!(observation.num_failed_pipelines, 0);
        assert_eq!(observation.num_successful_pipelines, 0);

        let indexing_pipeline = universe.get_one::<IndexingPipeline>().unwrap();

        // Freeze pipeline during 5 heartbeats.
        indexing_pipeline
            .send_message(FreezePipeline)
            .await
            .unwrap();
        tokio::time::sleep(HEARTBEAT * 5).await;
        // Check that indexing and merge pipelines are still running.
        let observation = indexing_server_handle.observe().await;
        assert_eq!(observation.num_running_pipelines, 1);
        assert_eq!(observation.num_failed_pipelines, 0);
        assert_eq!(observation.num_running_merge_pipelines, 1);
    }
}
