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
use itertools::Itertools;
use quickwit_actors::{
    create_mailbox, Actor, ActorContext, ActorExitStatus, ActorHandle, AsyncActor, Health,
    KillSwitch, QueueCapacity, Supervisable,
};
use quickwit_metastore::{Metastore, SplitState};
use quickwit_storage::StorageUriResolver;
use tokio::join;
use tracing::{debug, error, info, info_span, instrument, Span};

use crate::actors::merge_split_downloader::MergeSplitDownloader;
use crate::actors::{
    GarbageCollector, Indexer, IndexerParams, MergeExecutor, MergePlanner, Packager, Publisher,
    Uploader,
};
use crate::models::IndexingStatistics;
use crate::source::{quickwit_supported_sources, SourceActor, SourceConfig};
use crate::split_store::{IndexingSplitStore, IndexingSplitStoreParams};
use crate::{MergePolicy, StableMultitenantWithTimestampMergePolicy};

pub struct IndexingPipelineHandler {
    /// Indexing pipeline
    pub source: ActorHandle<SourceActor>,
    pub indexer: ActorHandle<Indexer>,
    pub packager: ActorHandle<Packager>,
    pub uploader: ActorHandle<Uploader>,
    pub publisher: ActorHandle<Publisher>,
    pub garbage_collector: ActorHandle<GarbageCollector>,

    /// Merging pipeline subpipeline
    pub merge_planner: ActorHandle<MergePlanner>,
    pub merge_split_downloader: ActorHandle<MergeSplitDownloader>,
    pub merge_executor: ActorHandle<MergeExecutor>,
    pub merge_packager: ActorHandle<Packager>,
    pub merge_uploader: ActorHandle<Uploader>,
    pub merge_publisher: ActorHandle<Publisher>,
}

#[derive(Debug, Clone, Copy)]
pub enum Msg {
    Supervise,
    Observe,
}

/// TODO have a clear strategy on when we should retry, and when we should not.
pub struct IndexingPipelineSupervisor {
    generation: usize,
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

    fn name(&self) -> String {
        "Pipeline".to_string()
    }

    fn span(&self, _ctx: &ActorContext<Self::Message>) -> Span {
        info_span!("")
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
            generation: 0,
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

    fn supervisables(&self) -> Vec<&dyn Supervisable> {
        if let Some(handlers) = self.handlers.as_ref() {
            let supervisables: Vec<&dyn Supervisable> = vec![
                &handlers.source,
                &handlers.indexer,
                &handlers.packager,
                &handlers.uploader,
                &handlers.publisher,
                &handlers.garbage_collector,
                &handlers.merge_planner,
                &handlers.merge_split_downloader,
                &handlers.merge_executor,
                &handlers.merge_packager,
                &handlers.merge_uploader,
                &handlers.merge_publisher,
            ];
            supervisables
        } else {
            Vec::new()
        }
    }

    /// Performs healthcheck on all of the actors in the pipeline,
    /// and consolidates the result.
    fn healthcheck(&self) -> Health {
        let mut healthy_actors: Vec<&str> = Default::default();
        let mut failure_or_unhealthy_actors: Vec<&str> = Default::default();
        let mut success_actors: Vec<&str> = Default::default();
        for supervisable in self.supervisables() {
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

        if !failure_or_unhealthy_actors.is_empty() {
            error!(healthy=?healthy_actors, failure_or_unhealthy_actors=?failure_or_unhealthy_actors, success=?success_actors, "indexing pipeline error.");
            return Health::FailureOrUnhealthy;
        }

        if healthy_actors.is_empty() {
            // all actors finished successfully.
            info!("indexing-pipeline-success");
            return Health::Success;
        }

        // No error at this point, and there are still actors running
        debug!(healthy=?healthy_actors, failure_or_unhealthy_actors=?failure_or_unhealthy_actors, success=?success_actors, "pipeline is judged healthy.");
        Health::Healthy
    }

    // TODO this should return an error saying whether we can retry or not.
    #[instrument(name="", level="info", skip_all, fields(index=%self.params.index_id, gen=self.generation))]
    async fn spawn_pipeline(&mut self, ctx: &ActorContext<Msg>) -> anyhow::Result<()> {
        self.generation += 1;
        self.previous_generations_statistics = self.statistics.clone();
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
        let timestamp_field_name = index_metadata.index_config.timestamp_field_name();
        let demux_field_name = index_metadata.index_config.demux_field_name();
        let stable_multitenant_merge_policy = StableMultitenantWithTimestampMergePolicy {
            demux_field_name: demux_field_name.clone(),
            merge_enabled: self.params.merge_enabled,
            demux_enabled: self.params.demux_enabled,
            ..Default::default()
        };
        let max_merge_docs = stable_multitenant_merge_policy.max_merge_docs;
        let merge_policy: Arc<dyn MergePolicy> = Arc::new(stable_multitenant_merge_policy);
        info!(
            root_dir=%self.params.indexer_params.indexing_directory.path().display(),
            merge_policy=?merge_policy,
            index_uri=?index_metadata.index_uri,
            "spawn-indexing-pipeline",
        );
        let split_store = IndexingSplitStore::create_with_local_store(
            index_storage,
            self.params
                .indexer_params
                .indexing_directory
                .cache_directory
                .as_path(),
            IndexingSplitStoreParams::default(),
            merge_policy.clone(),
        )?;
        let published_splits = self
            .params
            .metastore
            .list_splits(&self.params.index_id, SplitState::Published, None, &[])
            .await?;
        split_store
            .remove_dangling_splits(&published_splits)
            .await?;

        let tags_field = index_metadata
            .index_config
            .tags_field(&index_metadata.index_config.schema());

        let (merge_planner_mailbox, merge_planner_inbox) =
            create_mailbox::<<MergePlanner as Actor>::Message>(
                "MergePlanner".to_string(),
                QueueCapacity::Unbounded,
            );

        // Garbage colletor
        let garbage_collector = GarbageCollector::new(
            self.params.index_id.clone(),
            split_store.clone(),
            self.params.metastore.clone(),
        );
        let (garbage_collector_mailbox, garbage_collector_handler) = ctx
            .spawn_actor(garbage_collector)
            .set_kill_switch(self.kill_switch.clone())
            .spawn_async();

        // Merge publisher
        let merge_publisher = Publisher::new(
            "MergePublisher",
            self.params.metastore.clone(),
            merge_planner_mailbox.clone(),
            garbage_collector_mailbox.clone(),
        );
        let (merge_publisher_mailbox, merge_publisher_handler) = ctx
            .spawn_actor(merge_publisher)
            .set_kill_switch(self.kill_switch.clone())
            .spawn_async();

        // Merge uploader
        let merge_uploader = Uploader::new(
            "MergeUploader",
            self.params.metastore.clone(),
            split_store.clone(),
            merge_publisher_mailbox,
        );
        let (merge_uploader_mailbox, merge_uploader_handler) = ctx
            .spawn_actor(merge_uploader)
            .set_kill_switch(self.kill_switch.clone())
            .spawn_async();

        // Merge Packager
        let merge_packager = Packager::new("MergePackager", tags_field, merge_uploader_mailbox);
        let (merge_packager_mailbox, merge_packager_handler) = ctx
            .spawn_actor(merge_packager)
            .set_kill_switch(self.kill_switch.clone())
            .spawn_sync();

        let merge_executor = MergeExecutor::new(
            self.params.index_id.clone(),
            merge_packager_mailbox,
            timestamp_field_name,
            demux_field_name,
            max_merge_docs,
            max_merge_docs * 2, // < TODO: put these parameters from a config struct.
        );
        let (merge_executor_mailbox, merge_executor_handler) = ctx
            .spawn_actor(merge_executor)
            .set_kill_switch(self.kill_switch.clone())
            .spawn_sync();

        let merge_split_downloader = MergeSplitDownloader {
            scratch_directory: self
                .params
                .indexer_params
                .indexing_directory
                .scratch_directory
                .clone(),
            storage: split_store.clone(),
            merge_executor_mailbox,
        };
        let (merge_split_downloader_mailbox, merge_split_downloader_handler) = ctx
            .spawn_actor(merge_split_downloader)
            .set_kill_switch(self.kill_switch.clone())
            .spawn_async();

        // Merge planner
        let published_split_metadatas = published_splits
            .into_iter()
            .map(|split| split.split_metadata)
            .collect_vec();
        let merge_planner = MergePlanner::new(
            published_split_metadatas,
            merge_policy.clone(),
            merge_split_downloader_mailbox,
        );
        let (merge_planner_mailbox, merge_planner_handler) = ctx
            .spawn_actor(merge_planner)
            .set_kill_switch(self.kill_switch.clone())
            .set_mailboxes(merge_planner_mailbox, merge_planner_inbox)
            .spawn_sync();

        // Publisher
        let publisher = Publisher::new(
            "Publisher",
            self.params.metastore.clone(),
            merge_planner_mailbox,
            garbage_collector_mailbox,
        );
        let (publisher_mailbox, publisher_handler) = ctx
            .spawn_actor(publisher)
            .set_kill_switch(self.kill_switch.clone())
            .spawn_async();

        // Uploader
        let uploader = Uploader::new(
            "Uploader",
            self.params.metastore.clone(),
            split_store.clone(),
            publisher_mailbox,
        );
        let (uploader_mailbox, uploader_handler) = ctx
            .spawn_actor(uploader)
            .set_kill_switch(self.kill_switch.clone())
            .spawn_async();

        // Packager
        let packager = Packager::new("Packager", tags_field, uploader_mailbox);
        let (packager_mailbox, packager_handler) = ctx
            .spawn_actor(packager)
            .set_kill_switch(self.kill_switch.clone())
            .spawn_sync();

        // Indexer
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

        // Source
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
            garbage_collector: garbage_collector_handler,

            merge_planner: merge_planner_handler,
            merge_split_downloader: merge_split_downloader_handler,
            merge_executor: merge_executor_handler,
            merge_packager: merge_packager_handler,
            merge_uploader: merge_uploader_handler,
            merge_publisher: merge_publisher_handler,
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
                handlers.garbage_collector.kill(),
                handlers.merge_planner.kill(),
                handlers.merge_split_downloader.kill(),
                handlers.merge_executor.kill(),
                handlers.merge_packager.kill(),
                handlers.merge_uploader.kill(),
                handlers.merge_publisher.kill(),
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
    pub demux_enabled: bool,
    pub merge_enabled: bool,
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
            .expect_list_splits()
            .times(3)
            .returning(|_, _, _, _| Ok(Vec::new()));
        metastore
            .expect_mark_splits_for_deletion()
            .times(1)
            .returning(|_, _| Ok(()));

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
            source_id: "test-source".to_string(),
            source_type: "file".to_string(),
            params: json!({ "filepath": PathBuf::from("data/test_corpus.json") }),
        };
        let indexer_params = IndexerParams::for_test().await?;
        let indexing_pipeline_params = IndexingPipelineParams {
            index_id: "test-index".to_string(),
            source_config,
            indexer_params,
            metastore: Arc::new(metastore),
            storage_uri_resolver: StorageUriResolver::for_test(),
            merge_enabled: true,
            demux_enabled: false,
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
