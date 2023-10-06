// Copyright (C) 2023 Quickwit, Inc.
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

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use quickwit_actors::{
    Actor, ActorContext, ActorExitStatus, ActorHandle, Handler, Health, Mailbox, QueueCapacity,
    Supervisable, HEARTBEAT,
};
use quickwit_common::pubsub::EventBroker;
use quickwit_common::temp_dir::TempDirectory;
use quickwit_common::KillSwitch;
use quickwit_config::{IndexingSettings, SourceConfig};
use quickwit_doc_mapper::DocMapper;
use quickwit_ingest::IngesterPool;
use quickwit_metastore::Metastore;
use quickwit_proto::indexing::IndexingPipelineId;
use quickwit_proto::metastore::MetastoreError;
use quickwit_storage::{Storage, StorageResolver};
use tokio::sync::Semaphore;
use tracing::{debug, error, info, instrument};

use super::MergePlanner;
use crate::actors::doc_processor::DocProcessor;
use crate::actors::index_serializer::IndexSerializer;
use crate::actors::publisher::PublisherType;
use crate::actors::sequencer::Sequencer;
use crate::actors::uploader::UploaderType;
use crate::actors::{Indexer, Packager, Publisher, Uploader};
use crate::merge_policy::MergePolicy;
use crate::models::IndexingStatistics;
use crate::source::{quickwit_supported_sources, AssignShards, SourceActor, SourceRuntimeArgs};
use crate::split_store::IndexingSplitStore;
use crate::SplitsUpdateMailbox;

const SUPERVISE_INTERVAL: Duration = Duration::from_secs(1);

const MAX_RETRY_DELAY: Duration = Duration::from_secs(600); // 10 min.

#[derive(Debug)]
struct SuperviseLoop;

/// Calculates the wait time based on retry count.
// retry_count, wait_time
// 0   1s
// 1   2s
// 2   4s
// 3   8s
// ...
// >=8   5mn
pub(crate) fn wait_duration_before_retry(retry_count: usize) -> Duration {
    // Protect against a `retry_count` that will lead to an overflow.
    let max_power = (retry_count as u32).min(31);
    Duration::from_secs(2u64.pow(max_power)).min(MAX_RETRY_DELAY)
}

/// Spawning an indexing pipeline puts a lot of pressure on the file system, metastore, etc. so
/// we rely on this semaphore to limit the number of indexing pipelines that can be spawned
/// concurrently.
/// See also <https://github.com/quickwit-oss/quickwit/issues/1638>.
static SPAWN_PIPELINE_SEMAPHORE: Semaphore = Semaphore::const_new(10);

struct IndexingPipelineHandles {
    source_mailbox: Mailbox<SourceActor>,
    source_handle: ActorHandle<SourceActor>,
    doc_processor: ActorHandle<DocProcessor>,
    indexer: ActorHandle<Indexer>,
    index_serializer: ActorHandle<IndexSerializer>,
    packager: ActorHandle<Packager>,
    uploader: ActorHandle<Uploader>,
    sequencer: ActorHandle<Sequencer<Publisher>>,
    publisher: ActorHandle<Publisher>,
    next_check_for_progress: Instant,
}

impl IndexingPipelineHandles {
    fn should_check_for_progress(&mut self) -> bool {
        let now = Instant::now();
        let check_for_progress = now > self.next_check_for_progress;
        if check_for_progress {
            self.next_check_for_progress = now + *HEARTBEAT;
        }
        check_for_progress
    }
}

// Messages

#[derive(Clone, Copy, Debug, Default)]
pub struct Spawn {
    retry_count: usize,
}

pub struct IndexingPipeline {
    params: IndexingPipelineParams,
    previous_generations_statistics: IndexingStatistics,
    statistics: IndexingStatistics,
    handles_opt: Option<IndexingPipelineHandles>,
    // Killswitch used for the actors in the pipeline. This is not the supervisor killswitch.
    kill_switch: KillSwitch,
}

#[async_trait]
impl Actor for IndexingPipeline {
    type ObservableState = IndexingStatistics;

    fn observable_state(&self) -> Self::ObservableState {
        self.statistics.clone()
    }

    fn name(&self) -> String {
        "IndexingPipeline".to_string()
    }

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        self.handle(Spawn::default(), ctx).await?;
        self.handle(SuperviseLoop, ctx).await?;
        Ok(())
    }

    async fn finalize(
        &mut self,
        _exit_status: &ActorExitStatus,
        _ctx: &ActorContext<Self>,
    ) -> anyhow::Result<()> {
        // We update the observation to ensure our last "black box" observation
        // is up to date.
        self.perform_observe().await;
        Ok(())
    }
}

impl IndexingPipeline {
    pub fn new(params: IndexingPipelineParams) -> Self {
        Self {
            params,
            previous_generations_statistics: Default::default(),
            handles_opt: None,
            kill_switch: KillSwitch::default(),
            statistics: IndexingStatistics::default(),
        }
    }

    fn supervisables(&self) -> Vec<&dyn Supervisable> {
        if let Some(handles) = &self.handles_opt {
            let supervisables: Vec<&dyn Supervisable> = vec![
                &handles.source_handle,
                &handles.doc_processor,
                &handles.indexer,
                &handles.index_serializer,
                &handles.packager,
                &handles.uploader,
                &handles.sequencer,
                &handles.publisher,
            ];
            supervisables
        } else {
            Vec::new()
        }
    }

    /// Performs healthcheck on all of the actors in the pipeline,
    /// and consolidates the result.
    fn healthcheck(&self, check_for_progress: bool) -> Health {
        let mut healthy_actors: Vec<&str> = Default::default();
        let mut failure_or_unhealthy_actors: Vec<&str> = Default::default();
        let mut success_actors: Vec<&str> = Default::default();
        for supervisable in self.supervisables() {
            match supervisable.check_health(check_for_progress) {
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
            error!(
                pipeline_id=?self.params.pipeline_id,
                generation=self.generation(),
                healthy_actors=?healthy_actors,
                failed_or_unhealthy_actors=?failure_or_unhealthy_actors,
                success_actors=?success_actors,
                "Indexing pipeline failure."
            );
            return Health::FailureOrUnhealthy;
        }
        if healthy_actors.is_empty() {
            // All the actors finished successfully.
            info!(
                pipeline_id=?self.params.pipeline_id,
                generation=self.generation(),
                "Indexing pipeline success."
            );
            return Health::Success;
        }
        // No error at this point and there are still some actors running.
        debug!(
            pipeline_id=?self.params.pipeline_id,
            generation=self.generation(),
            healthy_actors=?healthy_actors,
            failed_or_unhealthy_actors=?failure_or_unhealthy_actors,
            success_actors=?success_actors,
            "Indexing pipeline running."
        );
        Health::Healthy
    }

    fn generation(&self) -> usize {
        self.statistics.generation
    }

    async fn perform_observe(&mut self) {
        let Some(handles) = &self.handles_opt else {
            return;
        };
        handles.doc_processor.refresh_observe();
        handles.indexer.refresh_observe();
        handles.uploader.refresh_observe();
        handles.publisher.refresh_observe();
        self.statistics = self
            .previous_generations_statistics
            .clone()
            .add_actor_counters(
                &handles.doc_processor.last_observation(),
                &handles.indexer.last_observation(),
                &handles.uploader.last_observation(),
                &handles.publisher.last_observation(),
            )
            .set_generation(self.statistics.generation)
            .set_num_spawn_attempts(self.statistics.num_spawn_attempts);
    }

    /// Checks if some actors have terminated.
    async fn perform_health_check(
        &mut self,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        let Some(handles) = self.handles_opt.as_mut() else {
            return Ok(());
        };

        // While we check if the actor has terminated or not, we do not check for progress
        // at every single loop. Instead, we wait for the `HEARTBEAT` duration to have elapsed,
        // since our last check.
        let check_for_progress = handles.should_check_for_progress();
        let health = self.healthcheck(check_for_progress);
        match health {
            Health::Healthy => {}
            Health::FailureOrUnhealthy => {
                self.terminate().await;
                let first_retry_delay = wait_duration_before_retry(0);
                ctx.schedule_self_msg(first_retry_delay, Spawn { retry_count: 0 })
                    .await;
            }
            Health::Success => {
                return Err(ActorExitStatus::Success);
            }
        }
        Ok(())
    }

    // TODO this should return an error saying whether we can retry or not.
    #[instrument(
        name="spawn_pipeline",
        level="info",
        skip_all,
        fields(
            index=%self.params.pipeline_id.index_uid.index_id(),
            gen=self.generation()
        ))]
    async fn spawn_pipeline(&mut self, ctx: &ActorContext<Self>) -> anyhow::Result<()> {
        let _spawn_pipeline_permit = ctx
            .protect_future(SPAWN_PIPELINE_SEMAPHORE.acquire())
            .await
            .expect("The semaphore should not be closed.");
        self.statistics.num_spawn_attempts += 1;
        let index_id = self.params.pipeline_id.index_uid.index_id();
        let source_id = self.params.pipeline_id.source_id.as_str();
        self.kill_switch = ctx.kill_switch().child();
        info!(
            index_id=%index_id,
            source_id=%source_id,
            pipeline_ord=%self.params.pipeline_id.pipeline_ord,
            "spawning indexing pipeline",
        );
        let (source_mailbox, source_inbox) = ctx
            .spawn_ctx()
            .create_mailbox::<SourceActor>("SourceActor", QueueCapacity::Unbounded);

        // Publisher
        let publisher = Publisher::new(
            PublisherType::MainPublisher,
            self.params.metastore.clone(),
            Some(self.params.merge_planner_mailbox.clone()),
            Some(source_mailbox.clone()),
        );
        let (publisher_mailbox, publisher_handle) = ctx
            .spawn_actor()
            .set_kill_switch(self.kill_switch.clone())
            .set_backpressure_micros_counter(
                crate::metrics::INDEXER_METRICS
                    .backpressure_micros
                    .with_label_values([index_id, "publisher"]),
            )
            .spawn(publisher);

        let sequencer = Sequencer::new(publisher_mailbox);
        let (sequencer_mailbox, sequencer_handle) = ctx
            .spawn_actor()
            .set_backpressure_micros_counter(
                crate::metrics::INDEXER_METRICS
                    .backpressure_micros
                    .with_label_values([index_id, "sequencer"]),
            )
            .set_kill_switch(self.kill_switch.clone())
            .spawn(sequencer);

        // Uploader
        let uploader = Uploader::new(
            UploaderType::IndexUploader,
            self.params.metastore.clone(),
            self.params.merge_policy.clone(),
            self.params.split_store.clone(),
            SplitsUpdateMailbox::Sequencer(sequencer_mailbox),
            self.params.max_concurrent_split_uploads_index,
            self.params.event_broker.clone(),
        );
        let (uploader_mailbox, uploader_handle) = ctx
            .spawn_actor()
            .set_backpressure_micros_counter(
                crate::metrics::INDEXER_METRICS
                    .backpressure_micros
                    .with_label_values([index_id, "uploader"]),
            )
            .set_kill_switch(self.kill_switch.clone())
            .spawn(uploader);

        // Packager
        let tag_fields = self.params.doc_mapper.tag_named_fields()?;
        let packager = Packager::new("Packager", tag_fields, uploader_mailbox);
        let (packager_mailbox, packager_handle) = ctx
            .spawn_actor()
            .set_kill_switch(self.kill_switch.clone())
            .spawn(packager);

        // Index Serializer
        let index_serializer = IndexSerializer::new(packager_mailbox);
        let (index_serializer_mailbox, index_serializer_handle) = ctx
            .spawn_actor()
            .set_kill_switch(self.kill_switch.clone())
            .spawn(index_serializer);

        // Indexer
        let indexer = Indexer::new(
            self.params.pipeline_id.clone(),
            self.params.doc_mapper.clone(),
            self.params.metastore.clone(),
            self.params.indexing_directory.clone(),
            self.params.indexing_settings.clone(),
            self.params.cooperative_indexing_permits.clone(),
            index_serializer_mailbox,
        );
        let (indexer_mailbox, indexer_handle) = ctx
            .spawn_actor()
            .set_backpressure_micros_counter(
                crate::metrics::INDEXER_METRICS
                    .backpressure_micros
                    .with_label_values([index_id, "indexer"]),
            )
            .set_kill_switch(self.kill_switch.clone())
            .spawn(indexer);

        let doc_processor = DocProcessor::try_new(
            index_id.to_string(),
            source_id.to_string(),
            self.params.doc_mapper.clone(),
            indexer_mailbox,
            self.params.source_config.transform_config.clone(),
            self.params.source_config.input_format.clone(),
        )?;
        let (doc_processor_mailbox, doc_processor_handle) = ctx
            .spawn_actor()
            .set_backpressure_micros_counter(
                crate::metrics::INDEXER_METRICS
                    .backpressure_micros
                    .with_label_values([index_id, "doc_processor"]),
            )
            .set_kill_switch(self.kill_switch.clone())
            .spawn(doc_processor);

        // Fetch index_metadata to be sure to have the last updated checkpoint.
        let index_metadata = ctx
            .protect_future(self.params.metastore.index_metadata(index_id))
            .await?;
        let source_checkpoint = index_metadata
            .checkpoint
            .source_checkpoint(source_id)
            .cloned()
            .unwrap_or_default(); // TODO Have a stricter check.
        let source = ctx
            .protect_future(quickwit_supported_sources().load_source(
                Arc::new(SourceRuntimeArgs {
                    pipeline_id: self.params.pipeline_id.clone(),
                    source_config: self.params.source_config.clone(),
                    metastore: self.params.metastore.clone(),
                    ingester_pool: self.params.ingester_pool.clone(),
                    queues_dir_path: self.params.queues_dir_path.clone(),
                    storage_resolver: self.params.source_storage_resolver.clone(),
                }),
                source_checkpoint,
            ))
            .await?;
        let actor_source = SourceActor {
            source,
            doc_processor_mailbox,
        };
        let (source_mailbox, source_handle) = ctx
            .spawn_actor()
            .set_mailboxes(source_mailbox, source_inbox)
            .set_kill_switch(self.kill_switch.clone())
            .spawn(actor_source);

        // Increment generation once we are sure there will be no spawning error.
        self.previous_generations_statistics = self.statistics.clone();
        self.statistics.generation += 1;
        self.handles_opt = Some(IndexingPipelineHandles {
            source_mailbox,
            source_handle,
            doc_processor: doc_processor_handle,
            indexer: indexer_handle,
            index_serializer: index_serializer_handle,
            packager: packager_handle,
            uploader: uploader_handle,
            sequencer: sequencer_handle,
            publisher: publisher_handle,
            next_check_for_progress: Instant::now() + *HEARTBEAT,
        });
        Ok(())
    }

    async fn terminate(&mut self) {
        self.kill_switch.kill();
        if let Some(handles) = self.handles_opt.take() {
            tokio::join!(
                handles.source_handle.kill(),
                handles.indexer.kill(),
                handles.packager.kill(),
                handles.uploader.kill(),
                handles.publisher.kill(),
            );
        }
    }
}

#[async_trait]
impl Handler<SuperviseLoop> for IndexingPipeline {
    type Reply = ();
    async fn handle(
        &mut self,
        supervise_loop_token: SuperviseLoop,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        self.perform_observe().await;
        self.perform_health_check(ctx).await?;
        ctx.schedule_self_msg(SUPERVISE_INTERVAL, supervise_loop_token)
            .await;
        Ok(())
    }
}

#[async_trait]
impl Handler<Spawn> for IndexingPipeline {
    type Reply = ();

    async fn handle(
        &mut self,
        spawn: Spawn,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        if self.handles_opt.is_some() {
            return Ok(());
        }
        self.previous_generations_statistics.num_spawn_attempts = 1 + spawn.retry_count;
        if let Err(spawn_error) = self.spawn_pipeline(ctx).await {
            if let Some(MetastoreError::NotFound { .. }) =
                spawn_error.downcast_ref::<MetastoreError>()
            {
                info!(error = ?spawn_error, "Could not spawn pipeline, index might have been deleted.");
                return Err(ActorExitStatus::Success);
            }
            let retry_delay = wait_duration_before_retry(spawn.retry_count + 1);
            error!(error = ?spawn_error, retry_count = spawn.retry_count, retry_delay = ?retry_delay, "Error while spawning indexing pipeline, retrying after some time.");
            ctx.schedule_self_msg(
                retry_delay,
                Spawn {
                    retry_count: spawn.retry_count + 1,
                },
            )
            .await;
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<AssignShards> for IndexingPipeline {
    type Reply = ();

    async fn handle(
        &mut self,
        message: AssignShards,
        _ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        if let Some(handles) = &mut self.handles_opt {
            info!(
                shard_ids=?message.0.shard_ids,
                "assigning shards to indexing pipeline."
            );
            handles.source_mailbox.send_message(message).await?;
        }
        Ok(())
    }
}

pub struct IndexingPipelineParams {
    pub pipeline_id: IndexingPipelineId,
    pub metastore: Arc<dyn Metastore>,
    pub storage: Arc<dyn Storage>,

    // Indexing-related parameters
    pub doc_mapper: Arc<dyn DocMapper>,
    pub indexing_directory: TempDirectory,
    pub indexing_settings: IndexingSettings,
    pub split_store: IndexingSplitStore,
    pub max_concurrent_split_uploads_index: usize,
    pub cooperative_indexing_permits: Option<Arc<Semaphore>>,

    // Merge-related parameters
    pub merge_policy: Arc<dyn MergePolicy>,
    pub merge_planner_mailbox: Mailbox<MergePlanner>,
    pub max_concurrent_split_uploads_merge: usize,

    // Source-related parameters
    pub source_config: SourceConfig,
    pub source_storage_resolver: StorageResolver,
    pub ingester_pool: IngesterPool,
    pub queues_dir_path: PathBuf,
    pub event_broker: EventBroker,
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::path::PathBuf;
    use std::sync::Arc;

    use quickwit_actors::{Command, Universe};
    use quickwit_config::{IndexingSettings, SourceInputFormat, SourceParams, VoidSourceParams};
    use quickwit_doc_mapper::{default_doc_mapper_for_test, DefaultDocMapper};
    use quickwit_metastore::{IndexMetadata, MockMetastore};
    use quickwit_proto::metastore::MetastoreError;
    use quickwit_proto::IndexUid;
    use quickwit_storage::RamStorage;

    use super::{IndexingPipeline, *};
    use crate::actors::merge_pipeline::{MergePipeline, MergePipelineParams};
    use crate::merge_policy::default_merge_policy;

    #[test]
    fn test_wait_duration() {
        assert_eq!(wait_duration_before_retry(0), Duration::from_secs(1));
        assert_eq!(wait_duration_before_retry(1), Duration::from_secs(2));
        assert_eq!(wait_duration_before_retry(2), Duration::from_secs(4));
        assert_eq!(wait_duration_before_retry(3), Duration::from_secs(8));
        assert_eq!(wait_duration_before_retry(9), Duration::from_secs(512));
        assert_eq!(wait_duration_before_retry(10), MAX_RETRY_DELAY);
    }

    async fn test_indexing_pipeline_num_fails_before_success(
        mut num_fails: usize,
    ) -> anyhow::Result<bool> {
        let universe = Universe::new();
        let mut metastore = MockMetastore::default();
        metastore
            .expect_index_metadata()
            .withf(|index_id| index_id == "test-index")
            .returning(move |_| {
                if num_fails == 0 {
                    let index_metadata =
                        IndexMetadata::for_test("test-index", "ram:///indexes/test-index");
                    return Ok(index_metadata);
                }
                num_fails -= 1;
                Err(MetastoreError::Connection {
                    message: "MetastoreError Alarm".to_string(),
                })
            });
        metastore
            .expect_last_delete_opstamp()
            .returning(move |index_uid| {
                assert_eq!("test-index", index_uid.index_id());
                Ok(10)
            });
        metastore
            .expect_mark_splits_for_deletion()
            .returning(|_, _| Ok(()));
        metastore
            .expect_stage_splits()
            .withf(|index_uid, _metadata| -> bool {
                *index_uid == "test-index:11111111111111111111111111"
            })
            .returning(|_, _| Ok(()));
        metastore
            .expect_publish_splits()
            .withf(
                |index_uid,
                 splits,
                 replaced_splits,
                 checkpoint_delta_opt,
                 _publish_token_opt|
                 -> bool {
                    let checkpoint_delta = checkpoint_delta_opt.as_ref().unwrap();
                    *index_uid == "test-index:11111111111111111111111111"
                        && checkpoint_delta.source_id == "test-source"
                        && splits.len() == 1
                        && replaced_splits.is_empty()
                        && format!("{:?}", checkpoint_delta.source_delta)
                            .ends_with(":(00000000000000000000..00000000000000001030])")
                },
            )
            .returning(|_, _, _, _, _| Ok(()));
        let node_id = "test-node";
        let metastore = Arc::new(metastore);
        let pipeline_id = IndexingPipelineId {
            index_uid: "test-index:11111111111111111111111111".to_string().into(),
            source_id: "test-source".to_string(),
            node_id: node_id.to_string(),
            pipeline_ord: 0,
        };
        let source_config = SourceConfig {
            source_id: "test-source".to_string(),
            max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
            desired_num_pipelines: NonZeroUsize::new(1).unwrap(),
            enabled: true,
            source_params: SourceParams::file(PathBuf::from("data/test_corpus.json")),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        };
        let storage = Arc::new(RamStorage::default());
        let split_store = IndexingSplitStore::create_without_local_store_for_test(storage.clone());
        let (merge_planner_mailbox, _) = universe.create_test_mailbox();
        let event_broker = EventBroker::default();
        let pipeline_params = IndexingPipelineParams {
            pipeline_id,
            doc_mapper: Arc::new(default_doc_mapper_for_test()),
            source_config,
            source_storage_resolver: StorageResolver::ram_and_file_for_test(),
            indexing_directory: TempDirectory::for_test(),
            indexing_settings: IndexingSettings::for_test(),
            ingester_pool: IngesterPool::default(),
            metastore: metastore.clone(),
            storage,
            split_store,
            merge_policy: default_merge_policy(),
            queues_dir_path: PathBuf::from("./queues"),
            max_concurrent_split_uploads_index: 4,
            max_concurrent_split_uploads_merge: 5,
            cooperative_indexing_permits: None,
            merge_planner_mailbox,
            event_broker,
        };
        let pipeline = IndexingPipeline::new(pipeline_params);
        let (_pipeline_mailbox, pipeline_handle) = universe.spawn_builder().spawn(pipeline);
        let (pipeline_exit_status, pipeline_statistics) = pipeline_handle.join().await;
        assert_eq!(pipeline_statistics.generation, 1);
        assert_eq!(pipeline_statistics.num_spawn_attempts, 1 + num_fails);
        Ok(pipeline_exit_status.is_success())
    }

    #[tokio::test]
    async fn test_indexing_pipeline_retry_0() -> anyhow::Result<()> {
        test_indexing_pipeline_num_fails_before_success(0).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_indexing_pipeline_retry_1() -> anyhow::Result<()> {
        test_indexing_pipeline_num_fails_before_success(1).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_indexing_pipeline_simple() -> anyhow::Result<()> {
        let mut metastore = MockMetastore::default();
        metastore
            .expect_index_metadata()
            .withf(|index_id| index_id == "test-index")
            .returning(|_| {
                Ok(IndexMetadata::for_test(
                    "test-index",
                    "ram:///indexes/test-index",
                ))
            });
        metastore
            .expect_last_delete_opstamp()
            .returning(move |index_uid| {
                assert_eq!("test-index", index_uid.index_id());
                Ok(10)
            });
        metastore
            .expect_stage_splits()
            .withf(|index_uid, _metadata| *index_uid == "test-index:11111111111111111111111111")
            .returning(|_, _| Ok(()));
        metastore
            .expect_publish_splits()
            .withf(
                |index_uid,
                 splits,
                 replaced_split_ids,
                 checkpoint_delta_opt,
                 _publish_token_opt|
                 -> bool {
                    let checkpoint_delta = checkpoint_delta_opt.as_ref().unwrap();
                    *index_uid == "test-index:11111111111111111111111111"
                        && splits.len() == 1
                        && replaced_split_ids.is_empty()
                        && checkpoint_delta.source_id == "test-source"
                        && format!("{:?}", checkpoint_delta.source_delta)
                            .ends_with(":(00000000000000000000..00000000000000001030])")
                },
            )
            .returning(|_, _, _, _, _| Ok(()));
        let universe = Universe::new();
        let node_id = "test-node";
        let metastore = Arc::new(metastore);
        let pipeline_id = IndexingPipelineId {
            index_uid: "test-index:11111111111111111111111111".to_string().into(),
            source_id: "test-source".to_string(),
            node_id: node_id.to_string(),
            pipeline_ord: 0,
        };
        let source_config = SourceConfig {
            source_id: "test-source".to_string(),
            max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
            desired_num_pipelines: NonZeroUsize::new(1).unwrap(),
            enabled: true,
            source_params: SourceParams::file(PathBuf::from("data/test_corpus.json")),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        };
        let storage = Arc::new(RamStorage::default());
        let split_store = IndexingSplitStore::create_without_local_store_for_test(storage.clone());
        let (merge_planner_mailbox, _) = universe.create_test_mailbox();
        let pipeline_params = IndexingPipelineParams {
            pipeline_id,
            doc_mapper: Arc::new(default_doc_mapper_for_test()),
            source_config,
            source_storage_resolver: StorageResolver::ram_and_file_for_test(),
            indexing_directory: TempDirectory::for_test(),
            indexing_settings: IndexingSettings::for_test(),
            ingester_pool: IngesterPool::default(),
            metastore: metastore.clone(),
            queues_dir_path: PathBuf::from("./queues"),
            storage,
            split_store,
            merge_policy: default_merge_policy(),
            max_concurrent_split_uploads_index: 4,
            max_concurrent_split_uploads_merge: 5,
            cooperative_indexing_permits: None,
            merge_planner_mailbox,
            event_broker: Default::default(),
        };
        let pipeline = IndexingPipeline::new(pipeline_params);
        let (_pipeline_mailbox, pipeline_handler) = universe.spawn_builder().spawn(pipeline);
        let (pipeline_exit_status, pipeline_statistics) = pipeline_handler.join().await;
        assert!(pipeline_exit_status.is_success());
        assert_eq!(pipeline_statistics.generation, 1);
        assert_eq!(pipeline_statistics.num_spawn_attempts, 1);
        assert_eq!(pipeline_statistics.num_published_splits, 1);
        universe.assert_quit().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_merge_pipeline_does_not_stop_on_indexing_pipeline_failure() {
        let mut metastore = MockMetastore::default();
        metastore
            .expect_index_metadata()
            .withf(|index_id| index_id == "test-index")
            .returning(|_| {
                Ok(IndexMetadata::for_test(
                    "test-index",
                    "ram:///indexes/test-index",
                ))
            });
        metastore.expect_list_splits().returning(|_| Ok(Vec::new()));
        let universe = Universe::with_accelerated_time();
        let node_id = "test-node";
        let metastore = Arc::new(metastore);
        let doc_mapper = Arc::new(default_doc_mapper_for_test());
        let pipeline_id = IndexingPipelineId {
            index_uid: IndexUid::new("test-index"),
            source_id: "test-source".to_string(),
            node_id: node_id.to_string(),
            pipeline_ord: 0,
        };
        let source_config = SourceConfig {
            source_id: "test-source".to_string(),
            max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
            desired_num_pipelines: NonZeroUsize::new(1).unwrap(),
            enabled: true,
            source_params: SourceParams::Void(VoidSourceParams),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        };
        let storage = Arc::new(RamStorage::default());
        let split_store = IndexingSplitStore::create_without_local_store_for_test(storage.clone());
        let merge_pipeline_params = MergePipelineParams {
            pipeline_id: pipeline_id.clone(),
            doc_mapper: doc_mapper.clone(),
            indexing_directory: TempDirectory::for_test(),
            metastore: metastore.clone(),
            split_store: split_store.clone(),
            merge_policy: default_merge_policy(),
            max_concurrent_split_uploads: 2,
            merge_max_io_num_bytes_per_sec: None,
            event_broker: Default::default(),
        };
        let merge_pipeline = MergePipeline::new(merge_pipeline_params, universe.spawn_ctx());
        let merge_planner_mailbox = merge_pipeline.merge_planner_mailbox().clone();
        let (_merge_pipeline_mailbox, merge_pipeline_handler) =
            universe.spawn_builder().spawn(merge_pipeline);
        let indexing_pipeline_params = IndexingPipelineParams {
            pipeline_id,
            doc_mapper,
            source_config,
            source_storage_resolver: StorageResolver::ram_and_file_for_test(),
            indexing_directory: TempDirectory::for_test(),
            indexing_settings: IndexingSettings::for_test(),
            ingester_pool: IngesterPool::default(),
            metastore: metastore.clone(),
            queues_dir_path: PathBuf::from("./queues"),
            storage,
            split_store,
            merge_policy: default_merge_policy(),
            max_concurrent_split_uploads_index: 4,
            max_concurrent_split_uploads_merge: 5,
            cooperative_indexing_permits: None,
            merge_planner_mailbox: merge_planner_mailbox.clone(),
            event_broker: Default::default(),
        };
        let indexing_pipeline = IndexingPipeline::new(indexing_pipeline_params);
        let (_indexing_pipeline_mailbox, indexing_pipeline_handler) =
            universe.spawn_builder().spawn(indexing_pipeline);
        let obs = indexing_pipeline_handler
            .process_pending_and_observe()
            .await;
        assert_eq!(obs.generation, 1);
        // Let's shutdown the indexer, this will trigger the the indexing pipeline failure and the
        // restart.
        let indexer = universe.get::<Indexer>().into_iter().next().unwrap();
        let _ = indexer.ask(Command::Quit).await;
        for _ in 0..10 {
            universe.sleep(*quickwit_actors::HEARTBEAT).await;
            // Check indexing pipeline has restarted.
            let obs = indexing_pipeline_handler
                .process_pending_and_observe()
                .await;
            if obs.generation == 2 {
                assert_eq!(merge_pipeline_handler.check_health(true), Health::Healthy);
                universe.quit().await;
                return;
            }
        }
        panic!("Pipeline was apparently not restarted.");
    }

    #[tokio::test]
    async fn test_indexing_pipeline_all_failures_handling() -> anyhow::Result<()> {
        let mut metastore = MockMetastore::default();
        metastore
            .expect_index_metadata()
            .withf(|index_id| index_id == "test-index")
            .returning(|_| {
                Ok(IndexMetadata::for_test(
                    "test-index",
                    "ram:///indexes/test-index",
                ))
            });
        metastore
            .expect_last_delete_opstamp()
            .returning(move |index_uid| {
                assert_eq!("test-index", index_uid.index_id());
                Ok(10)
            });
        metastore
            .expect_stage_splits()
            .never()
            .returning(|_, _| Ok(()));
        metastore
            .expect_publish_splits()
            .withf(
                |index_uid,
                 splits,
                 replaced_split_ids,
                 checkpoint_delta_opt,
                 _publish_token_opt|
                 -> bool {
                    let checkpoint_delta = checkpoint_delta_opt.as_ref().unwrap();
                    *index_uid == "test-index:11111111111111111111111111"
                        && splits.is_empty()
                        && replaced_split_ids.is_empty()
                        && checkpoint_delta.source_id == "test-source"
                        && format!("{:?}", checkpoint_delta.source_delta)
                            .ends_with(":(00000000000000000000..00000000000000001030])")
                },
            )
            .returning(|_, _, _, _, _| Ok(()));
        let universe = Universe::new();
        let node_id = "test-node";
        let metastore = Arc::new(metastore);
        let pipeline_id = IndexingPipelineId {
            index_uid: "test-index:11111111111111111111111111".to_string().into(),
            source_id: "test-source".to_string(),
            node_id: node_id.to_string(),
            pipeline_ord: 0,
        };
        let source_config = SourceConfig {
            source_id: "test-source".to_string(),
            max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
            desired_num_pipelines: NonZeroUsize::new(1).unwrap(),
            enabled: true,
            source_params: SourceParams::file(PathBuf::from("data/test_corpus.json")),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        };
        let storage = Arc::new(RamStorage::default());
        let split_store = IndexingSplitStore::create_without_local_store_for_test(storage.clone());
        let (merge_planner_mailbox, _) = universe.create_test_mailbox();
        // Create a minimal mapper with wrong date format to ensure that all documents will fail
        let broken_mapper = serde_json::from_str::<DefaultDocMapper>(
            r#"
                {
                    "store_source": true,
                    "timestamp_field": "timestamp",
                    "field_mappings": [
                        {
                            "name": "timestamp",
                            "type": "datetime",
                            "input_formats": ["iso8601"],
                            "fast": true
                        }
                    ]
                }"#,
        )
        .unwrap();

        let pipeline_params = IndexingPipelineParams {
            pipeline_id,
            doc_mapper: Arc::new(broken_mapper),
            source_config,
            source_storage_resolver: StorageResolver::ram_and_file_for_test(),
            indexing_directory: TempDirectory::for_test(),
            indexing_settings: IndexingSettings::for_test(),
            ingester_pool: IngesterPool::default(),
            metastore: metastore.clone(),
            queues_dir_path: PathBuf::from("./queues"),
            storage,
            split_store,
            merge_policy: default_merge_policy(),
            max_concurrent_split_uploads_index: 4,
            max_concurrent_split_uploads_merge: 5,
            cooperative_indexing_permits: None,
            merge_planner_mailbox,
            event_broker: Default::default(),
        };
        let pipeline = IndexingPipeline::new(pipeline_params);
        let (_pipeline_mailbox, pipeline_handler) = universe.spawn_builder().spawn(pipeline);
        let (pipeline_exit_status, pipeline_statistics) = pipeline_handler.join().await;
        assert!(pipeline_exit_status.is_success());
        assert_eq!(pipeline_statistics.generation, 1);
        assert_eq!(pipeline_statistics.num_spawn_attempts, 1);
        assert_eq!(pipeline_statistics.num_published_splits, 0);
        assert_eq!(pipeline_statistics.num_empty_splits, 1);
        assert_eq!(
            pipeline_statistics.num_docs,
            pipeline_statistics.num_invalid_docs
        );
        universe.assert_quit().await;
        Ok(())
    }
}
