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

use anyhow::Context;
use async_trait::async_trait;
use byte_unit::Byte;
use itertools::Itertools;
use quickwit_actors::{
    create_mailbox, Actor, ActorContext, ActorExitStatus, ActorHandle, AsyncActor, Health,
    KillSwitch, QueueCapacity, Supervisable,
};
use quickwit_config::{IndexerConfig, IndexingSettings, SourceConfig};
use quickwit_index_config::IndexConfig as DocMapper;
use quickwit_metastore::checkpoint::Checkpoint;
use quickwit_metastore::{IndexMetadata, Metastore, SplitState};
use quickwit_storage::Storage;
use tokio::join;
use tracing::{debug, error, info, info_span, instrument, Span};

use crate::actors::merge_split_downloader::MergeSplitDownloader;
use crate::actors::{
    GarbageCollector, Indexer, MergeExecutor, MergePlanner, NamedField, Packager, Publisher,
    Uploader,
};
use crate::models::{IndexingDirectory, IndexingStatistics};
use crate::source::{quickwit_supported_sources, SourceActor};
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
pub enum IndexingPipelineMessage {
    Supervise,
    Observe,
}

/// TODO have a clear strategy on when we should retry, and when we should not.
pub struct IndexingPipeline {
    params: IndexingPipelineParams,
    generation: usize,
    previous_generations_statistics: IndexingStatistics,
    statistics: IndexingStatistics,
    handlers: Option<IndexingPipelineHandler>,
    // Killswitch used for the actors in the pipeline. This is not the supervisor killswitch.
    kill_switch: KillSwitch,
}

impl Actor for IndexingPipeline {
    type Message = IndexingPipelineMessage;

    type ObservableState = IndexingStatistics;

    fn observable_state(&self) -> Self::ObservableState {
        self.statistics.clone()
    }

    fn name(&self) -> String {
        "IndexingPipeline".to_string()
    }

    fn span(&self, _ctx: &ActorContext<Self::Message>) -> Span {
        info_span!("")
    }
}

impl IndexingPipeline {
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

    async fn process_observe(
        &mut self,
        ctx: &ActorContext<IndexingPipelineMessage>,
    ) -> Result<(), ActorExitStatus> {
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
        ctx.schedule_self_msg(Duration::from_secs(1), IndexingPipelineMessage::Observe)
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
            error!(index=%self.params.index_id, gen=self.generation, healthy=?healthy_actors, failure_or_unhealthy_actors=?failure_or_unhealthy_actors, success=?success_actors, "indexing pipeline error.");
            return Health::FailureOrUnhealthy;
        }

        if healthy_actors.is_empty() {
            // all actors finished successfully.
            info!(index=%self.params.index_id, gen=self.generation, "indexing-pipeline-success");
            return Health::Success;
        }

        // No error at this point, and there are still actors running
        debug!(index=%self.params.index_id, gen=self.generation, healthy=?healthy_actors, failure_or_unhealthy_actors=?failure_or_unhealthy_actors, success=?success_actors, "pipeline is judged healthy.");
        Health::Healthy
    }

    // TODO this should return an error saying whether we can retry or not.
    #[instrument(name="", level="info", skip_all, fields(index=%self.params.index_id, gen=self.generation))]
    async fn spawn_pipeline(
        &mut self,
        ctx: &ActorContext<IndexingPipelineMessage>,
    ) -> anyhow::Result<()> {
        self.generation += 1;
        self.previous_generations_statistics = self.statistics.clone();
        self.kill_switch = KillSwitch::default();
        let stable_multitenant_merge_policy = StableMultitenantWithTimestampMergePolicy {
            demux_enabled: self.params.indexing_settings.demux_enabled,
            demux_factor: self.params.indexing_settings.merge_policy.demux_factor,
            demux_field_name: self.params.indexing_settings.demux_field.clone(),
            merge_enabled: self.params.indexing_settings.merge_enabled,
            merge_factor: self.params.indexing_settings.merge_policy.merge_factor,
            max_merge_factor: self.params.indexing_settings.merge_policy.max_merge_factor,
            split_max_num_docs: self.params.indexing_settings.split_max_num_docs,
            ..Default::default()
        };
        let merge_policy: Arc<dyn MergePolicy> = Arc::new(stable_multitenant_merge_policy);
        info!(
            root_dir = %self.params.indexing_directory.path().display(),
            merge_policy = ?merge_policy,
            "spawn-indexing-pipeline",
        );
        let split_store = IndexingSplitStore::create_with_local_store(
            self.params.storage.clone(),
            self.params.indexing_directory.cache_directory.as_path(),
            IndexingSplitStoreParams {
                max_num_bytes: self.params.split_store_max_num_bytes.get_bytes() as usize,
                max_num_splits: self.params.split_store_max_num_splits,
            },
            merge_policy.clone(),
        )?;
        let published_splits = self
            .params
            .metastore
            .list_splits(&self.params.index_id, SplitState::Published, None, &[])
            .await?
            .into_iter()
            .map(|split| split.split_metadata)
            .collect::<Vec<_>>();
        split_store
            .remove_dangling_splits(&published_splits)
            .await?;

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
        let index_schema = self.params.doc_mapper.schema();
        let tag_fields = self
            .params
            .doc_mapper
            .tag_field_names()
            .iter()
            .map(|field_name| {
                index_schema
                    .get_field(field_name)
                    .context(format!("Field `{}` must exist in the schema.", field_name))
                    .map(|field| NamedField {
                        name: field_name.clone(),
                        field,
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let merge_packager =
            Packager::new("MergePackager", tag_fields.clone(), merge_uploader_mailbox);
        let (merge_packager_mailbox, merge_packager_handler) = ctx
            .spawn_actor(merge_packager)
            .set_kill_switch(self.kill_switch.clone())
            .spawn_sync();

        let merge_executor = MergeExecutor::new(
            self.params.index_id.clone(),
            merge_packager_mailbox,
            self.params.indexing_settings.timestamp_field.clone(),
            self.params.indexing_settings.demux_field.clone(),
            self.params.indexing_settings.split_max_num_docs as usize,
            self.params.indexing_settings.split_max_num_docs as usize * 2,
        );
        let (merge_executor_mailbox, merge_executor_handler) = ctx
            .spawn_actor(merge_executor)
            .set_kill_switch(self.kill_switch.clone())
            .spawn_sync();

        let merge_split_downloader = MergeSplitDownloader {
            scratch_directory: self.params.indexing_directory.scratch_directory.clone(),
            storage: split_store.clone(),
            merge_executor_mailbox,
        };
        let (merge_split_downloader_mailbox, merge_split_downloader_handler) = ctx
            .spawn_actor(merge_split_downloader)
            .set_kill_switch(self.kill_switch.clone())
            .spawn_async();

        // Merge planner
        let published_split_metadatas = published_splits.into_iter().collect_vec();
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
        let packager = Packager::new("Packager", tag_fields, uploader_mailbox);
        let (packager_mailbox, packager_handler) = ctx
            .spawn_actor(packager)
            .set_kill_switch(self.kill_switch.clone())
            .spawn_sync();

        // Indexer
        let indexer = Indexer::new(
            self.params.index_id.clone(),
            self.params.doc_mapper.clone(),
            self.params.indexing_directory.clone(),
            self.params.indexing_settings.clone(),
            packager_mailbox,
        );
        let (indexer_mailbox, indexer_handler) = ctx
            .spawn_actor(indexer)
            .set_kill_switch(self.kill_switch.clone())
            .spawn_sync();

        // Source
        let source = quickwit_supported_sources()
            .load_source(
                self.params.source_config.clone(),
                self.params.checkpoint.clone(),
            )
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

    async fn process_supervise(
        &mut self,
        ctx: &ActorContext<IndexingPipelineMessage>,
    ) -> Result<(), ActorExitStatus> {
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
        ctx.schedule_self_msg_blocking(
            quickwit_actors::HEARTBEAT,
            IndexingPipelineMessage::Supervise,
        );
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
impl AsyncActor for IndexingPipeline {
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
            IndexingPipelineMessage::Observe => self.process_observe(ctx).await?,
            IndexingPipelineMessage::Supervise => self.process_supervise(ctx).await?,
        }
        Ok(())
    }
}

pub struct IndexingPipelineParams {
    pub index_id: String,
    pub checkpoint: Checkpoint,
    pub doc_mapper: Arc<dyn DocMapper>,
    pub indexing_directory: IndexingDirectory,
    pub indexing_settings: IndexingSettings,
    pub source_config: SourceConfig,
    pub split_store_max_num_bytes: Byte,
    pub split_store_max_num_splits: usize,
    pub metastore: Arc<dyn Metastore>,
    pub storage: Arc<dyn Storage>,
}

impl IndexingPipelineParams {
    pub async fn try_new(
        index_metadata: IndexMetadata,
        indexer_config: IndexerConfig,
        metastore: Arc<dyn Metastore>,
        storage: Arc<dyn Storage>,
    ) -> anyhow::Result<Self> {
        let doc_mapper = index_metadata.build_doc_mapper()?;
        let indexing_directory_path = indexer_config.data_dir_path.join(&index_metadata.index_id);
        let indexing_directory = IndexingDirectory::create_in_dir(indexing_directory_path).await?;
        let source_config = index_metadata.source()?;
        Ok(Self {
            index_id: index_metadata.index_id,
            checkpoint: index_metadata.checkpoint,
            doc_mapper,
            indexing_directory,
            indexing_settings: index_metadata.indexing_settings,
            source_config,
            split_store_max_num_bytes: indexer_config.split_store_max_num_bytes,
            split_store_max_num_splits: indexer_config.split_store_max_num_splits,
            metastore,
            storage,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use quickwit_actors::Universe;
    use quickwit_config::IndexingSettings;
    use quickwit_index_config::default_config_for_tests;
    use quickwit_metastore::checkpoint::Checkpoint;
    use quickwit_metastore::MockMetastore;
    use quickwit_storage::RamStorage;
    use serde_json::json;

    use super::{IndexingPipeline, *};
    use crate::models::IndexingDirectory;

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
            .expect_stage_split()
            .withf(move |index_id, _metadata| -> bool { index_id == "test-index" })
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
        let pipeline_params = IndexingPipelineParams {
            index_id: "test-index".to_string(),
            checkpoint: Checkpoint::default(),
            doc_mapper: Arc::new(default_config_for_tests()),
            indexing_directory: IndexingDirectory::for_test().await?,
            indexing_settings: IndexingSettings::for_test(),
            split_store_max_num_bytes: Byte::from_bytes(50_000_000),
            split_store_max_num_splits: 100,
            source_config,
            metastore: Arc::new(metastore),
            storage: Arc::new(RamStorage::default()),
        };
        let pipeline = IndexingPipeline::new(pipeline_params);
        let (_pipeline_mailbox, pipeline_handler) = universe.spawn_actor(pipeline).spawn_async();
        let (pipeline_exit_status, pipeline_statistics) = pipeline_handler.join().await;
        assert!(pipeline_exit_status.is_success());
        assert_eq!(pipeline_statistics.num_published_splits, 1);
        Ok(())
    }
}
