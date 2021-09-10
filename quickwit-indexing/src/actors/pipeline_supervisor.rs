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

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use quickwit_actors::{
    Actor, ActorContext, ActorExitStatus, ActorHandle, AsyncActor, Health, KillSwitch, Supervisable,
};
use quickwit_metastore::Metastore;
use quickwit_storage::{create_cachable_storage, CacheConfig, StorageUriResolver};
use smallvec::SmallVec;
use tokio::join;
use tracing::{debug, error, info};

use crate::actors::{Indexer, IndexerParams, Packager, Publisher, Uploader};
use crate::models::IndexingStatistics;
use crate::source::{quickwit_supported_sources, SourceActor, SourceConfig};

pub struct IndexingPipelineHandler {
    pub source: ActorHandle<SourceActor>,
    pub indexer: ActorHandle<Indexer>,
    pub packager: ActorHandle<Packager>,
    pub uploader: ActorHandle<Uploader>,
    pub publisher: ActorHandle<Publisher>,
}

#[derive(Debug, Clone, Copy)]
pub enum Msg {
    Supervise,
    Observe,
}

/// TODO have a clear strategy on when we should retry, and when we should not.
pub struct IndexingPipelineSupervisor {
    params: IndexingPipelineParams,
    previous_generations_statistics: IndexingStatistics,
    statistics: IndexingStatistics,
    handlers: Option<IndexingPipelineHandler>,
    // Killswitch used for the actors in the pipeline. This is not the supervisor killswitch.
    kill_switch: KillSwitch,
}

impl Actor for IndexingPipelineSupervisor {
    type Message = Msg;

    type ObservableState = IndexingStatistics;

    fn observable_state(&self) -> Self::ObservableState {
        self.statistics.clone()
    }
}

impl IndexingPipelineSupervisor {
    pub fn new(params: IndexingPipelineParams) -> Self {
        Self {
            params,
            previous_generations_statistics: Default::default(),
            handlers: None,
            kill_switch: KillSwitch::default(),
            statistics: IndexingStatistics::default(),
        }
    }

    async fn process_observe(&mut self, ctx: &ActorContext<Msg>) -> Result<(), ActorExitStatus> {
        if let Some(handlers) = self.handlers.as_ref() {
            let (indexer_counters, uploader_counters, publisher_counters) = join!(
                handlers.indexer.observe(),
                handlers.uploader.observe(),
                handlers.publisher.observe(),
            );
            self.statistics = self
                .previous_generations_statistics
                .clone()
                .add_actor_counters(
                    &*indexer_counters,
                    &*uploader_counters,
                    &*publisher_counters,
                );
        }
        ctx.schedule_self_msg(Duration::from_secs(1), Msg::Observe)
            .await;
        Ok(())
    }

    fn supervisables(&self) -> SmallVec<[&dyn Supervisable; 5]> {
        if let Some(handlers) = self.handlers.as_ref() {
            SmallVec::from_buf([
                &handlers.source,
                &handlers.indexer,
                &handlers.packager,
                &handlers.uploader,
                &handlers.publisher,
            ])
        } else {
            SmallVec::new()
        }
    }

    /// Performs healthcheck on all of the actors in the pipeline,
    /// and consolidates the result.
    fn healthcheck(&self) -> Health {
        let supervisables = self.supervisables();
        let mut healthy_actors: SmallVec<[&str; 5]> = Default::default();
        let mut failure_or_unhealthy_actors: SmallVec<[&str; 5]> = Default::default();
        let mut success_actors: SmallVec<[&str; 5]> = Default::default();
        for &supervisable in &supervisables {
            match supervisable.health() {
                Health::Healthy => {
                    // At least one other actor is running.
                    healthy_actors.push(supervisable.name());
                }
                Health::FailureOrUnhealthy => {
                    failure_or_unhealthy_actors.push(supervisable.name());
                }
                Health::Success => {
                    success_actors.push(supervisable.name());
                }
            }
        }
        if failure_or_unhealthy_actors.is_empty() {
            if success_actors.len() == supervisables.len() {
                // all actors finished successfully.
                info!("indexing pipeline success!");
                Health::Success
            } else {
                // No error at this point, and there are still actors running
                debug!(healthy=?healthy_actors, failure_or_unhealthy_actors=?failure_or_unhealthy_actors, success=?success_actors, "pipeline is judged healthy.");
                Health::Healthy
            }
        } else {
            error!(healthy=?healthy_actors, failure_or_unhealthy_actors=?failure_or_unhealthy_actors, success=?success_actors, "indexing pipeline error.");
            Health::FailureOrUnhealthy
        }
    }

    // TODO this should return an error saying whether we can retry or not.
    async fn spawn_pipeline(&mut self, ctx: &ActorContext<Msg>) -> anyhow::Result<()> {
        info!(index_id=%self.params.index_id, "spawn-indexing-pipeline");
        self.kill_switch = KillSwitch::default();
        let index_metadata = self
            .params
            .metastore
            .index_metadata(&self.params.index_id)
            .await?;
        let index_storage = self
            .params
            .storage_uri_resolver
            .resolve(&index_metadata.index_uri)?;

        // TODO: Make cache path configurable [https://github.com/quickwit-inc/quickwit/issues/520]
        let cache_directory = self.params.indexer_params.scratch_directory.temp_child()?;
        let index_storage = create_cachable_storage(
            index_storage,
            &self.params.storage_uri_resolver,
            cache_directory.path(),
            CacheConfig::default(),
        )?;

        let publisher = Publisher::new(self.params.metastore.clone());
        let (publisher_mailbox, publisher_handler) = ctx
            .spawn_actor(publisher)
            .set_kill_switch(self.kill_switch.clone())
            .spawn_async();
        let uploader = Uploader::new(
            self.params.metastore.clone(),
            index_storage,
            publisher_mailbox,
        );
        let (uploader_mailbox, uploader_handler) = ctx
            .spawn_actor(uploader)
            .set_kill_switch(self.kill_switch.clone())
            .spawn_async();
        info!(actor_name=%uploader_mailbox.actor_instance_id());
        let packager = Packager::new(uploader_mailbox);
        let (packager_mailbox, packager_handler) = ctx
            .spawn_actor(packager)
            .set_kill_switch(self.kill_switch.clone())
            .spawn_sync();
        let indexer = Indexer::try_new(
            self.params.index_id.clone(),
            index_metadata.index_config.clone(),
            self.params.indexer_params.clone(),
            packager_mailbox,
        )?;
        let (indexer_mailbox, indexer_handler) = ctx
            .spawn_actor(indexer)
            .set_kill_switch(self.kill_switch.clone())
            .spawn_sync();
        let source = quickwit_supported_sources()
            .load_source(self.params.source_config.clone(), index_metadata.checkpoint)
            .await?;
        let actor_source = SourceActor {
            source,
            batch_sink: indexer_mailbox,
        };
        let (_source_mailbox, source_handler) = ctx
            .spawn_actor(actor_source)
            .set_kill_switch(self.kill_switch.clone())
            .spawn_async();
        self.handlers = Some(IndexingPipelineHandler {
            source: source_handler,
            indexer: indexer_handler,
            packager: packager_handler,
            uploader: uploader_handler,
            publisher: publisher_handler,
        });
        Ok(())
    }

    async fn process_supervise(&mut self, ctx: &ActorContext<Msg>) -> Result<(), ActorExitStatus> {
        if self.handlers.is_none() {
            // TODO Accept errors in spawning. See #463.
            self.spawn_pipeline(ctx).await?;
            // if let Err(spawn_error) = self.spawn_pipeline(ctx).await {
            //    // only retry n-times.
            //    error!(err=?spawn_error, "Error while spawning");
            //    self.terminate().await;
            // }
        } else {
            match self.healthcheck() {
                Health::Healthy => {}
                Health::FailureOrUnhealthy => {
                    self.terminate().await;
                }
                Health::Success => {
                    return Err(ActorExitStatus::Success);
                }
            }
        }
        ctx.schedule_self_msg_blocking(quickwit_actors::HEARTBEAT, Msg::Supervise);
        Ok(())
    }

    async fn terminate(&mut self) {
        self.kill_switch.kill();
        if let Some(handlers) = self.handlers.take() {
            tokio::join!(
                handlers.source.kill(),
                handlers.indexer.kill(),
                handlers.packager.kill(),
                handlers.uploader.kill(),
                handlers.publisher.kill(),
            );
        }
    }
}

#[async_trait]
impl AsyncActor for IndexingPipelineSupervisor {
    async fn initialize(
        &mut self,
        ctx: &ActorContext<Self::Message>,
    ) -> Result<(), ActorExitStatus> {
        self.process_observe(ctx).await?;
        self.process_supervise(ctx).await?;
        Ok(())
    }

    async fn process_message(
        &mut self,
        message: Self::Message,

        ctx: &ActorContext<Self::Message>,
    ) -> Result<(), ActorExitStatus> {
        match message {
            Msg::Observe => self.process_observe(ctx).await?,
            Msg::Supervise => self.process_supervise(ctx).await?,
        }
        Ok(())
    }
}

pub struct IndexingPipelineParams {
    pub index_id: String,
    pub source_config: SourceConfig,
    pub indexer_params: IndexerParams,
    pub metastore: Arc<dyn Metastore>,
    pub storage_uri_resolver: StorageUriResolver,
}

#[cfg(test)]
mod tests {

    use std::path::PathBuf;
    use std::sync::Arc;

    use quickwit_actors::Universe;
    use quickwit_metastore::{IndexMetadata, MockMetastore, SplitState};
    use quickwit_storage::StorageUriResolver;
    use serde_json::json;

    use super::{IndexingPipelineParams, IndexingPipelineSupervisor};
    use crate::actors::IndexerParams;
    use crate::source::SourceConfig;

    #[tokio::test]
    async fn test_indexing_pipeline() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let mut metastore = MockMetastore::default();
        metastore
            .expect_index_metadata()
            .withf(|index_id| index_id == "test-index")
            .times(1)
            .returning(|_| {
                let index_metadata = IndexMetadata {
                    index_id: "test-index".to_string(),
                    index_uri: "ram://test-index".to_string(),
                    index_config: Arc::new(quickwit_index_config::default_config_for_tests()),
                    checkpoint: Default::default(),
                };
                Ok(index_metadata)
            });
        metastore
            .expect_stage_split()
            .withf(move |index_id, metadata| -> bool {
                (index_id == "test-index") && metadata.split_metadata.split_state == SplitState::New
            })
            .times(1)
            .returning(|_, _| Ok(()));
        metastore
            .expect_publish_splits()
            .withf(move |index_id, splits, checkpoint_delta| -> bool {
                index_id == "test-index"
                    && splits.len() == 1
                    && format!("{:?}", checkpoint_delta)
                        .ends_with(":(00000000000000000000..00000000000000000070])")
            })
            .times(1)
            .returning(|_, _, _| Ok(()));
        let universe = Universe::new();
        let source_config = SourceConfig {
            id: "test-source".to_string(),
            source_type: "file".to_string(),
            params: json!({ "filepath": PathBuf::from("data/test_corpus.json") }),
        };
        let indexer_params = IndexerParams::for_test()?;
        let indexing_pipeline_params = IndexingPipelineParams {
            index_id: "test-index".to_string(),
            source_config,
            indexer_params,
            metastore: Arc::new(metastore),
            storage_uri_resolver: StorageUriResolver::for_test(),
        };
        let indexing_supervisor = IndexingPipelineSupervisor::new(indexing_pipeline_params);
        let (_pipeline_mailbox, pipeline_handler) =
            universe.spawn_actor(indexing_supervisor).spawn_async();
        let (pipeline_termination, pipeline_statistics) = pipeline_handler.join().await;
        assert!(pipeline_termination.is_success());
        assert_eq!(pipeline_statistics.num_published_splits, 1);
        Ok(())
    }
}
