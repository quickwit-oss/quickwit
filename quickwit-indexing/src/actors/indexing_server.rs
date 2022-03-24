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

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{bail, Context};
use async_trait::async_trait;
use quickwit_actors::{
    Actor, ActorContext, ActorExitStatus, ActorHandle, Handler, Health, Mailbox, Observation,
    Supervisable, Universe,
};
use quickwit_config::{IndexerConfig, SourceConfig, SourceParams, VecSourceParams};
use quickwit_metastore::{IndexMetadata, Metastore};
use quickwit_storage::StorageUriResolver;
use serde::Serialize;
use tracing::{error, info};

use crate::models::{
    DetachPipeline, IndexingPipelineId, ObservePipeline, SpawnMergePipeline, SpawnPipeline,
    SpawnPipelines,
};
use crate::{IndexingPipeline, IndexingPipelineParams, IndexingStatistics};

pub struct IndexingServerClient {
    universe: Universe,
    mailbox: Mailbox<IndexingServer>,
    handle: ActorHandle<IndexingServer>,
}

impl IndexingServerClient {
    /// Spawns an indexing pipeline for a particular source.
    pub async fn spawn_pipeline(
        &self,
        index_id: String,
        source: SourceConfig,
    ) -> anyhow::Result<IndexingPipelineId> {
        let message = SpawnPipeline { index_id, source };
        self.universe.ask(&self.mailbox, message).await?
    }

    /// Spawns an indexing pipeline for each source defined for the index.
    pub async fn spawn_pipelines(
        &self,
        index_id: String,
    ) -> anyhow::Result<Vec<IndexingPipelineId>> {
        let message = SpawnPipelines { index_id };
        self.universe.ask(&self.mailbox, message).await?
    }

    /// Spawns a merge pipeline.
    pub async fn spawn_merge_pipeline(
        &self,
        index_id: String,
        merge_enabled: bool,
        demux_enabled: bool,
    ) -> anyhow::Result<IndexingPipelineId> {
        let message = SpawnMergePipeline {
            index_id,
            merge_enabled,
            demux_enabled,
        };
        self.universe.ask(&self.mailbox, message).await?
    }

    /// Retrieves the indexing statistics of a pipeline.
    pub async fn observe_pipeline(
        &self,
        pipeline_id: &IndexingPipelineId,
    ) -> anyhow::Result<Observation<IndexingStatistics>> {
        let message = ObservePipeline {
            pipeline_id: pipeline_id.clone(),
        };
        self.universe.ask(&self.mailbox, message).await?
    }

    /// Detaches a pipeline from the indexing server. The pipeline is no longer managed by the
    /// server. This is mostly useful for ad-hoc indexing pipelines launched with `quickwit index
    /// ingest ..` and testing.
    pub async fn detach_pipeline(
        &self,
        pipeline_id: &IndexingPipelineId,
    ) -> anyhow::Result<ActorHandle<IndexingPipeline>> {
        let message = DetachPipeline {
            pipeline_id: pipeline_id.clone(),
        };
        self.universe.ask(&self.mailbox, message).await?
    }

    pub async fn observe_server(&self) -> Observation<<IndexingServer as Actor>::ObservableState> {
        self.handle.observe().await
    }

    /// Waits for the indexing server to exit, which may never happen :)
    pub async fn join_server(
        self,
    ) -> (ActorExitStatus, <IndexingServer as Actor>::ObservableState) {
        self.handle.join().await
    }

    /// Waits for the server's state to satisfy the given predicate.
    #[cfg(test)]
    pub async fn wait_for_server<F>(
        &self,
        mut predicate: F,
        timeout_after: std::time::Duration,
    ) -> anyhow::Result<()>
    where
        F: FnMut(&Observation<<IndexingServer as Actor>::ObservableState>) -> bool,
    {
        tokio::time::timeout(timeout_after, async {
            while !predicate(&self.observe_server().await) {}
        })
        .await?;
        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct IndexingServerState {
    num_running_pipelines: usize,
    num_successful_pipelines: usize,
    num_failed_pipelines: usize,
}

pub struct IndexingServer {
    indexing_dir_path: PathBuf,
    split_store_max_num_bytes: usize,
    split_store_max_num_splits: usize,
    metastore: Arc<dyn Metastore>,
    storage_resolver: StorageUriResolver,
    pipeline_handles: HashMap<IndexingPipelineId, ActorHandle<IndexingPipeline>>,
    state: IndexingServerState,
}

impl IndexingServer {
    pub fn check_health(&self) -> Health {
        // In the future, check metrics such as available disk space.
        Health::Healthy
    }

    pub fn spawn(
        data_dir_path: PathBuf,
        indexer_config: IndexerConfig,
        metastore: Arc<dyn Metastore>,
        storage_resolver: StorageUriResolver,
    ) -> IndexingServerClient {
        let universe = Universe::new();
        let server = Self {
            indexing_dir_path: data_dir_path.join("indexing"),
            split_store_max_num_bytes: indexer_config.split_store_max_num_bytes.get_bytes()
                as usize,
            split_store_max_num_splits: indexer_config.split_store_max_num_splits,
            metastore,
            storage_resolver,
            pipeline_handles: Default::default(),
            state: Default::default(),
        };
        let (mailbox, handle) = universe.spawn_actor(server).spawn();
        IndexingServerClient {
            universe,
            mailbox,
            handle,
        }
    }

    async fn detach_pipeline(
        &mut self,
        pipeline_id: &IndexingPipelineId,
    ) -> anyhow::Result<ActorHandle<IndexingPipeline>> {
        let pipeline_handle = self.pipeline_handles.remove(pipeline_id).with_context(|| {
            format!(
                "Indexing pipeline `{}` for source `{}` does not exist.",
                pipeline_id.index_id, pipeline_id.source_id
            )
        })?;
        self.state.num_running_pipelines -= 1;
        Ok(pipeline_handle)
    }

    async fn observe_pipeline(
        &mut self,
        pipeline_id: &IndexingPipelineId,
    ) -> anyhow::Result<Observation<IndexingStatistics>> {
        let pipeline_handle = self.pipeline_handles.get(pipeline_id).with_context(|| {
            format!(
                "Indexing pipeline `{}` for source `{}` does not exist.",
                pipeline_id.index_id, pipeline_id.source_id
            )
        })?;
        let observation = pipeline_handle.observe().await;
        Ok(observation)
    }

    async fn spawn_pipeline(
        &mut self,
        index_id: String,
        source: SourceConfig,
        ctx: &ActorContext<Self>,
    ) -> anyhow::Result<IndexingPipelineId> {
        let pipeline_id = IndexingPipelineId {
            index_id,
            source_id: source.source_id.clone(),
        };
        let index_metadata = self.index_metadata(&pipeline_id.index_id, ctx).await?;
        self.spawn_pipeline_inner(pipeline_id.clone(), index_metadata, source, ctx)
            .await?;
        Ok(pipeline_id)
    }

    async fn spawn_pipelines(
        &mut self,
        index_id: String,
        ctx: &ActorContext<Self>,
    ) -> anyhow::Result<Vec<IndexingPipelineId>> {
        let mut pipeline_ids = Vec::new();

        let index_metadata = self.index_metadata(&index_id, ctx).await?;

        for source in index_metadata.sources.values() {
            let pipeline_id = IndexingPipelineId {
                index_id: index_id.clone(),
                source_id: source.source_id.clone(),
            };
            if self.pipeline_handles.contains_key(&pipeline_id) {
                continue;
            }
            self.spawn_pipeline_inner(
                pipeline_id.clone(),
                index_metadata.clone(),
                source.clone(),
                ctx,
            )
            .await?;
            pipeline_ids.push(pipeline_id);
        }
        Ok(pipeline_ids)
    }

    async fn spawn_pipeline_inner(
        &mut self,
        pipeline_id: IndexingPipelineId,
        index_metadata: IndexMetadata,
        source: SourceConfig,
        ctx: &ActorContext<Self>,
    ) -> anyhow::Result<()> {
        if self.pipeline_handles.contains_key(&pipeline_id) {
            bail!(
                "An indexing pipeline `{}` for source `{}` is already running.",
                pipeline_id.index_id,
                pipeline_id.source_id
            );
        }
        let storage = self.storage_resolver.resolve(&index_metadata.index_uri)?;

        let pipeline_params = IndexingPipelineParams::try_new(
            index_metadata,
            source,
            self.indexing_dir_path.clone(),
            self.split_store_max_num_bytes,
            self.split_store_max_num_splits,
            self.metastore.clone(),
            storage,
        )
        .await?;

        let pipeline = IndexingPipeline::new(pipeline_params);
        let (_pipeline_mailbox, pipeline_handle) = ctx.spawn_actor(pipeline).spawn();
        self.pipeline_handles.insert(pipeline_id, pipeline_handle);
        self.state.num_running_pipelines += 1;
        Ok(())
    }

    async fn spawn_merge_pipeline(
        &mut self,
        index_id: String,
        merge_enabled: bool,
        demux_enabled: bool,
        ctx: &ActorContext<Self>,
    ) -> anyhow::Result<IndexingPipelineId> {
        let pipeline_id = IndexingPipelineId {
            index_id,
            source_id: "void-source".to_string(),
        };
        let mut index_metadata = self.index_metadata(&pipeline_id.index_id, ctx).await?;
        index_metadata.indexing_settings.merge_enabled = merge_enabled;
        index_metadata.indexing_settings.demux_enabled = demux_enabled;

        let source = SourceConfig {
            source_id: pipeline_id.source_id.clone(),
            source_params: SourceParams::Vec(VecSourceParams::default()),
        };
        self.spawn_pipeline_inner(pipeline_id.clone(), index_metadata, source, ctx)
            .await?;
        Ok(pipeline_id)
    }

    async fn index_metadata(
        &self,
        index_id: &str,
        ctx: &ActorContext<Self>,
    ) -> anyhow::Result<IndexMetadata> {
        let _protect_guard = ctx.protect_zone();
        let index_metadata = self.metastore.index_metadata(index_id).await?;
        Ok(index_metadata)
    }
}

#[async_trait]
impl Handler<ObservePipeline> for IndexingServer {
    type Reply = anyhow::Result<Observation<IndexingStatistics>>;

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
impl Handler<DetachPipeline> for IndexingServer {
    type Reply = anyhow::Result<ActorHandle<IndexingPipeline>>;

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
impl Handler<SuperviseLoop> for IndexingServer {
    type Reply = ();

    async fn handle(
        &mut self,
        _message: SuperviseLoop,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        self.pipeline_handles
            .retain(|pipeline_id, pipeline_handle| match pipeline_handle
                    .health()
                {
                    Health::Healthy => true,
                    Health::Success => {
                        info!(index_id = %pipeline_id.index_id, source_id = %pipeline_id.source_id, "Indexing pipeline completed.");
                        self.state.num_successful_pipelines += 1;
                        self.state.num_running_pipelines -= 1;
                        false
                    }
                    Health::FailureOrUnhealthy => {
                        error!(index_id = %pipeline_id.index_id, source_id = %pipeline_id.source_id, "Indexing pipeline failed.");
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
impl Actor for IndexingServer {
    type ObservableState = IndexingServerState;

    fn observable_state(&self) -> Self::ObservableState {
        self.state.clone()
    }

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        self.handle(SuperviseLoop, ctx).await
    }
}

#[async_trait]
impl Handler<SpawnMergePipeline> for IndexingServer {
    type Reply = anyhow::Result<IndexingPipelineId>;
    async fn handle(
        &mut self,
        message: SpawnMergePipeline,
        ctx: &ActorContext<Self>,
    ) -> Result<anyhow::Result<IndexingPipelineId>, ActorExitStatus> {
        Ok(self
            .spawn_merge_pipeline(
                message.index_id,
                message.merge_enabled,
                message.demux_enabled,
                ctx,
            )
            .await)
    }
}

#[async_trait]
impl Handler<SpawnPipeline> for IndexingServer {
    type Reply = anyhow::Result<IndexingPipelineId>;
    async fn handle(
        &mut self,
        message: SpawnPipeline,
        ctx: &ActorContext<Self>,
    ) -> Result<anyhow::Result<IndexingPipelineId>, ActorExitStatus> {
        Ok(self
            .spawn_pipeline(message.index_id, message.source, ctx)
            .await)
    }
}

#[async_trait]
impl Handler<SpawnPipelines> for IndexingServer {
    type Reply = anyhow::Result<Vec<IndexingPipelineId>>;
    async fn handle(
        &mut self,
        message: SpawnPipelines,
        ctx: &ActorContext<Self>,
    ) -> Result<anyhow::Result<Vec<IndexingPipelineId>>, ActorExitStatus> {
        Ok(self.spawn_pipelines(message.index_id, ctx).await)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use quickwit_actors::ObservationType;
    use quickwit_common::rand::append_random_suffix;
    use quickwit_config::VecSourceParams;
    use quickwit_metastore::quickwit_metastore_uri_resolver;

    use super::*;

    const METASTORE_URI: &str = "ram:///qwdata/indexes";

    #[tokio::test]
    async fn test_indexing_server() {
        let index_id = append_random_suffix("test-indexing-server");
        let index_uri = format!("{}/{}", METASTORE_URI, index_id);
        let index_metadata = IndexMetadata::for_test(&index_id, &index_uri);

        let metastore = quickwit_metastore_uri_resolver()
            .resolve(METASTORE_URI)
            .await
            .unwrap();
        metastore.create_index(index_metadata).await.unwrap();

        // Test `IndexingServer::spawn`.
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir_path = temp_dir.path().to_path_buf();
        let indexer_config = IndexerConfig::for_test().unwrap();
        let storage_resolver = StorageUriResolver::for_test();
        let client = IndexingServer::spawn(
            data_dir_path.clone(),
            indexer_config,
            metastore.clone(),
            storage_resolver.clone(),
        );
        let observation = client.observe_server().await;
        assert_eq!(observation.num_running_pipelines, 0);
        assert_eq!(observation.num_failed_pipelines, 0);
        assert_eq!(observation.num_successful_pipelines, 0);

        // Test `spawn_pipeline`.
        let source_1 = SourceConfig {
            source_id: "test-indexing-server--source-1".to_string(),
            source_params: SourceParams::void(),
        };
        let pipeline_id1 = client
            .spawn_pipeline(index_id.clone(), source_1.clone())
            .await
            .unwrap();
        client
            .spawn_pipeline(index_id.clone(), source_1.clone())
            .await
            .unwrap_err();
        assert_eq!(pipeline_id1.index_id, index_id);
        assert_eq!(pipeline_id1.source_id, source_1.source_id);
        assert_eq!(client.observe_server().await.num_running_pipelines, 1);

        // Test `observe_pipeline`.
        let observation = client.observe_pipeline(&pipeline_id1).await.unwrap();
        assert_eq!(observation.obs_type, ObservationType::Alive);

        // Test `detach_pipeline`.
        client.detach_pipeline(&pipeline_id1).await.unwrap();
        assert_eq!(client.observe_server().await.num_running_pipelines, 0);

        // Test `spawn_pipelines`.
        metastore.add_source(&index_id, source_1).await.unwrap();
        client.spawn_pipelines(index_id.clone()).await.unwrap();
        assert_eq!(client.observe_server().await.num_running_pipelines, 1);

        let source_2 = SourceConfig {
            source_id: "test-indexing-server--source-2".to_string(),
            source_params: SourceParams::void(),
        };
        metastore.add_source(&index_id, source_2).await.unwrap();
        client.spawn_pipelines(index_id.clone()).await.unwrap();
        assert_eq!(client.observe_server().await.num_running_pipelines, 2);

        // Test `spawn_merge_pipeline`.
        let merge_pipeline_id = client
            .spawn_merge_pipeline(index_id.clone(), true, false)
            .await
            .unwrap();
        assert_eq!(client.observe_server().await.num_running_pipelines, 3);
        let pipeline_observation = client.observe_pipeline(&merge_pipeline_id).await.unwrap();
        assert_eq!(pipeline_observation.generation, 1);
        assert_eq!(pipeline_observation.num_spawn_attempts, 1);

        // Test `supervise_pipelines`
        let source_3 = SourceConfig {
            source_id: "test-indexing-server--source-3".to_string(),
            source_params: SourceParams::Vec(VecSourceParams {
                items: Vec::new(),
                batch_num_docs: 10,
                partition: "0".to_string(),
            }),
        };
        client
            .spawn_pipeline(index_id.clone(), source_3)
            .await
            .unwrap();
        client
            .wait_for_server(
                |state| state.num_successful_pipelines == 2,
                Duration::from_secs(5),
            )
            .await
            .unwrap();
    }
}
