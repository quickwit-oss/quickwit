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

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use quickwit_actors::{
    Actor, ActorContext, ActorExitStatus, ActorHandle, HEARTBEAT, Handler, Health, Mailbox,
    QueueCapacity, Supervisable,
};
use quickwit_common::KillSwitch;
use quickwit_common::metrics::OwnedGaugeGuard;
use quickwit_common::pubsub::EventBroker;
use quickwit_common::temp_dir::TempDirectory;
use quickwit_config::{IndexingSettings, RetentionPolicy, SourceConfig};
use quickwit_doc_mapper::DocMapper;
use quickwit_ingest::IngesterPool;
use quickwit_proto::indexing::IndexingPipelineId;
use quickwit_proto::metastore::{MetastoreError, MetastoreServiceClient};
use quickwit_proto::types::ShardId;
use quickwit_storage::{Storage, StorageResolver};
use tokio::sync::Semaphore;
use tracing::{debug, error, info, instrument};

use super::MergePlanner;
use crate::SplitsUpdateMailbox;
use crate::actors::doc_processor::DocProcessor;
use crate::actors::index_serializer::IndexSerializer;
use crate::actors::publisher::PublisherType;
use crate::actors::sequencer::Sequencer;
use crate::actors::uploader::UploaderType;
use crate::actors::{Indexer, Packager, Publisher, Uploader};
use crate::merge_policy::MergePolicy;
use crate::models::IndexingStatistics;
use crate::source::{
    AssignShards, Assignment, SourceActor, SourceRuntime, quickwit_supported_sources,
};
use crate::split_store::IndexingSplitStore;

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

    // The set of shard is something that can change dynamically without necessarily
    // requiring a respawn of the pipeline.
    // We keep the list of shards here however, to reassign them after a respawn.
    shard_ids: BTreeSet<ShardId>,
    _indexing_pipelines_gauge_guard: OwnedGaugeGuard,
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
        ctx: &ActorContext<Self>,
    ) -> anyhow::Result<()> {
        // We update the observation to ensure our last "black box" observation
        // is up to date.
        self.perform_observe(ctx);
        Ok(())
    }
}

impl IndexingPipeline {
    pub fn new(params: IndexingPipelineParams) -> Self {
        let indexing_pipelines_gauge = crate::metrics::INDEXER_METRICS
            .indexing_pipelines
            .with_label_values([&params.pipeline_id.index_uid.index_id]);
        let indexing_pipelines_gauge_guard = OwnedGaugeGuard::from_gauge(indexing_pipelines_gauge);
        let params_fingerprint = params.params_fingerprint;
        IndexingPipeline {
            params,
            previous_generations_statistics: Default::default(),
            handles_opt: None,
            kill_switch: KillSwitch::default(),
            statistics: IndexingStatistics {
                params_fingerprint,
                ..Default::default()
            },
            shard_ids: Default::default(),
            _indexing_pipelines_gauge_guard: indexing_pipelines_gauge_guard,
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

    fn perform_observe(&mut self, ctx: &ActorContext<Self>) {
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
        let pipeline_metrics_opt = handles.indexer.last_observation().pipeline_metrics_opt;
        self.statistics.pipeline_metrics_opt = pipeline_metrics_opt;
        self.statistics.params_fingerprint = self.params.params_fingerprint;
        self.statistics.shard_ids.clone_from(&self.shard_ids);
        ctx.observe(self);
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
                ctx.schedule_self_msg(first_retry_delay, Spawn { retry_count: 0 });
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
            index=%self.params.pipeline_id.index_uid.index_id,
            r#gen=self.generation()
        ))]
    async fn spawn_pipeline(&mut self, ctx: &ActorContext<Self>) -> anyhow::Result<()> {
        let _spawn_pipeline_permit = ctx
            .protect_future(SPAWN_PIPELINE_SEMAPHORE.acquire())
            .await
            .expect("semaphore should not be closed");

        self.statistics.num_spawn_attempts += 1;
        self.kill_switch = ctx.kill_switch().child();

        let index_id = &self.params.pipeline_id.index_uid.index_id;
        let source_id = &self.params.pipeline_id.source_id;

        info!(
            index_id,
            source_id,
            pipeline_uid=%self.params.pipeline_id.pipeline_uid,
            root_dir=%self.params.indexing_directory.path().display(),
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
                    .with_label_values(["publisher"]),
            )
            .spawn(publisher);

        let sequencer = Sequencer::new(publisher_mailbox);
        let (sequencer_mailbox, sequencer_handle) = ctx
            .spawn_actor()
            .set_backpressure_micros_counter(
                crate::metrics::INDEXER_METRICS
                    .backpressure_micros
                    .with_label_values(["sequencer"]),
            )
            .set_kill_switch(self.kill_switch.clone())
            .spawn(sequencer);

        // Uploader
        let uploader = Uploader::new(
            UploaderType::IndexUploader,
            self.params.metastore.clone(),
            self.params.merge_policy.clone(),
            self.params.retention_policy.clone(),
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
                    .with_label_values(["uploader"]),
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
                    .with_label_values(["indexer"]),
            )
            .set_kill_switch(self.kill_switch.clone())
            .spawn(indexer);

        let doc_processor = DocProcessor::try_new(
            index_id.to_string(),
            source_id.to_string(),
            self.params.doc_mapper.clone(),
            indexer_mailbox,
            self.params.source_config.transform_config.clone(),
            self.params.source_config.input_format,
        )?;
        let (doc_processor_mailbox, doc_processor_handle) = ctx
            .spawn_actor()
            .set_backpressure_micros_counter(
                crate::metrics::INDEXER_METRICS
                    .backpressure_micros
                    .with_label_values(["doc_processor"]),
            )
            .set_kill_switch(self.kill_switch.clone())
            .spawn(doc_processor);
        let source_runtime = SourceRuntime {
            pipeline_id: self.params.pipeline_id.clone(),
            source_config: self.params.source_config.clone(),
            metastore: self.params.metastore.clone(),
            ingester_pool: self.params.ingester_pool.clone(),
            queues_dir_path: self.params.queues_dir_path.clone(),
            storage_resolver: self.params.source_storage_resolver.clone(),
            event_broker: self.params.event_broker.clone(),
            indexing_setting: self.params.indexing_settings.clone(),
        };
        let source = ctx
            .protect_future(quickwit_supported_sources().load_source(source_runtime))
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
        let assign_shards_message = AssignShards(Assignment {
            shard_ids: self.shard_ids.clone(),
        });
        source_mailbox.send_message(assign_shards_message).await?;

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
        self.perform_observe(ctx);
        self.perform_health_check(ctx).await?;
        ctx.schedule_self_msg(SUPERVISE_INTERVAL, supervise_loop_token);
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
                info!(error = ?spawn_error, "could not spawn pipeline, index might have been deleted");
                return Err(ActorExitStatus::Success);
            }
            let retry_delay = wait_duration_before_retry(spawn.retry_count + 1);
            error!(error = ?spawn_error, retry_count = spawn.retry_count, retry_delay = ?retry_delay, "error while spawning indexing pipeline, retrying after some time");
            ctx.schedule_self_msg(
                retry_delay,
                Spawn {
                    retry_count: spawn.retry_count + 1,
                },
            );
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<AssignShards> for IndexingPipeline {
    type Reply = ();

    async fn handle(
        &mut self,
        assign_shards_message: AssignShards,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        self.shard_ids
            .clone_from(&assign_shards_message.0.shard_ids);
        // If the pipeline is running, we forward the message to its source.
        // If it is not, it will be respawned soon, and the shards will be assigned afterward.
        if let Some(handles) = &mut self.handles_opt {
            info!(
                shard_ids=?assign_shards_message.0.shard_ids,
                "assigning shards to indexing pipeline"
            );
            handles
                .source_mailbox
                .send_message(assign_shards_message)
                .await?;
        }
        // We perform observe to make sure the set of shard ids is up to date.
        self.perform_observe(ctx);
        Ok(())
    }
}

pub struct IndexingPipelineParams {
    pub pipeline_id: IndexingPipelineId,
    pub metastore: MetastoreServiceClient,
    pub storage: Arc<dyn Storage>,

    // Indexing-related parameters
    pub doc_mapper: Arc<DocMapper>,
    pub indexing_directory: TempDirectory,
    pub indexing_settings: IndexingSettings,
    pub split_store: IndexingSplitStore,
    pub max_concurrent_split_uploads_index: usize,
    pub cooperative_indexing_permits: Option<Arc<Semaphore>>,

    // Merge-related parameters
    pub merge_policy: Arc<dyn MergePolicy>,
    pub retention_policy: Option<RetentionPolicy>,
    pub merge_planner_mailbox: Mailbox<MergePlanner>,
    pub max_concurrent_split_uploads_merge: usize,

    // Source-related parameters
    pub source_config: SourceConfig,
    pub source_storage_resolver: StorageResolver,
    pub ingester_pool: IngesterPool,
    pub queues_dir_path: PathBuf,
    pub params_fingerprint: u64,

    pub event_broker: EventBroker,
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::path::PathBuf;
    use std::sync::Arc;

    use quickwit_actors::{Command, Universe};
    use quickwit_common::ServiceStream;
    use quickwit_config::{IndexingSettings, SourceInputFormat, SourceParams};
    use quickwit_doc_mapper::{DocMapper, default_doc_mapper_for_test};
    use quickwit_metastore::checkpoint::IndexCheckpointDelta;
    use quickwit_metastore::{IndexMetadata, IndexMetadataResponseExt, PublishSplitsRequestExt};
    use quickwit_proto::metastore::{
        EmptyResponse, IndexMetadataResponse, LastDeleteOpstampResponse, MetastoreError,
        MockMetastoreService,
    };
    use quickwit_proto::types::{IndexUid, NodeId, PipelineUid};
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
        test_file: &str,
    ) -> anyhow::Result<()> {
        let node_id = NodeId::from("test-node");
        let index_uid = IndexUid::for_test("test-index", 2);
        let pipeline_id = IndexingPipelineId {
            node_id,
            index_uid,
            source_id: "test-source".to_string(),
            pipeline_uid: PipelineUid::for_test(0u128),
        };
        let source_config = SourceConfig {
            source_id: "test-source".to_string(),
            num_pipelines: NonZeroUsize::MIN,
            enabled: true,
            source_params: SourceParams::file_from_str(test_file).unwrap(),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        };
        let source_config_clone = source_config.clone();

        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_index_metadata()
            .withf(|index_metadata_request| {
                index_metadata_request.index_uid.as_ref().unwrap() == &("test-index", 2)
            })
            .returning(move |_| {
                if num_fails == 0 {
                    let mut index_metadata =
                        IndexMetadata::for_test("test-index", "ram:///indexes/test-index");
                    index_metadata
                        .add_source(source_config_clone.clone())
                        .unwrap();
                    let response =
                        IndexMetadataResponse::try_from_index_metadata(&index_metadata).unwrap();
                    return Ok(response);
                }
                num_fails -= 1;
                Err(MetastoreError::Timeout("timeout error".to_string()))
            });
        mock_metastore
            .expect_last_delete_opstamp()
            .returning(move |_last_delete_opstamp_request| Ok(LastDeleteOpstampResponse::new(10)));
        mock_metastore
            .expect_mark_splits_for_deletion()
            .returning(|_| Ok(EmptyResponse {}));
        mock_metastore
            .expect_stage_splits()
            .withf(|stage_splits_request| -> bool {
                stage_splits_request.index_uid() == &("test-index", 2)
            })
            .returning(|_| Ok(EmptyResponse {}));
        mock_metastore
            .expect_publish_splits()
            .withf(|publish_splits_request| -> bool {
                let checkpoint_delta: IndexCheckpointDelta = publish_splits_request
                    .deserialize_index_checkpoint()
                    .unwrap()
                    .unwrap();
                publish_splits_request.index_uid() == &("test-index", 2)
                    && checkpoint_delta.source_id == "test-source"
                    && publish_splits_request.staged_split_ids.len() == 1
                    && publish_splits_request.replaced_split_ids.is_empty()
                    && format!("{:?}", checkpoint_delta.source_delta)
                        .ends_with(":(00000000000000000000..~00000000000000001030])")
            })
            .returning(|_| Ok(EmptyResponse {}));

        let universe = Universe::new();
        let (merge_planner_mailbox, _) = universe.create_test_mailbox();
        let storage = Arc::new(RamStorage::default());
        let split_store = IndexingSplitStore::create_without_local_store_for_test(storage.clone());
        let pipeline_params = IndexingPipelineParams {
            pipeline_id,
            doc_mapper: Arc::new(default_doc_mapper_for_test()),
            source_config,
            source_storage_resolver: StorageResolver::for_test(),
            indexing_directory: TempDirectory::for_test(),
            indexing_settings: IndexingSettings::for_test(),
            ingester_pool: IngesterPool::default(),
            metastore: MetastoreServiceClient::from_mock(mock_metastore),
            storage,
            split_store,
            merge_policy: default_merge_policy(),
            retention_policy: None,
            queues_dir_path: PathBuf::from("./queues"),
            max_concurrent_split_uploads_index: 4,
            max_concurrent_split_uploads_merge: 5,
            cooperative_indexing_permits: None,
            merge_planner_mailbox,
            event_broker: EventBroker::default(),
            params_fingerprint: 42u64,
        };
        let pipeline = IndexingPipeline::new(pipeline_params);
        let (_pipeline_mailbox, pipeline_handle) = universe.spawn_builder().spawn(pipeline);
        let (pipeline_exit_status, pipeline_statistics) = pipeline_handle.join().await;
        assert_eq!(
            pipeline_statistics.generation, 1,
            "generation is {}, expected 1",
            pipeline_statistics.generation
        );
        assert_eq!(
            pipeline_statistics.num_spawn_attempts,
            1 + num_fails,
            "num spawn attempts is {}, expected 1 + {}",
            pipeline_statistics.num_spawn_attempts,
            1 + num_fails
        );
        assert!(pipeline_exit_status.is_success());
        Ok(())
    }

    #[tokio::test]
    async fn test_indexing_pipeline_retry_0() -> anyhow::Result<()> {
        test_indexing_pipeline_num_fails_before_success(0, "data/test_corpus.json").await
    }

    #[tokio::test]
    async fn test_indexing_pipeline_retry_1() -> anyhow::Result<()> {
        test_indexing_pipeline_num_fails_before_success(1, "data/test_corpus.json").await
    }

    #[tokio::test]
    async fn test_indexing_pipeline_retry_0_gz() -> anyhow::Result<()> {
        test_indexing_pipeline_num_fails_before_success(0, "data/test_corpus.json.gz").await
    }

    #[tokio::test]
    async fn test_indexing_pipeline_retry_1_gz() -> anyhow::Result<()> {
        test_indexing_pipeline_num_fails_before_success(1, "data/test_corpus.json.gz").await
    }

    async fn indexing_pipeline_simple(test_file: &str) -> anyhow::Result<()> {
        let node_id = NodeId::from("test-node");
        let index_uid: IndexUid = IndexUid::for_test("test-index", 1);
        let pipeline_id = IndexingPipelineId {
            node_id,
            index_uid: index_uid.clone(),
            source_id: "test-source".to_string(),
            pipeline_uid: PipelineUid::for_test(0u128),
        };
        let source_config = SourceConfig {
            source_id: "test-source".to_string(),
            num_pipelines: NonZeroUsize::MIN,
            enabled: true,
            source_params: SourceParams::file_from_str(test_file).unwrap(),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        };
        let source_config_clone = source_config.clone();

        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_index_metadata()
            .withf(|index_metadata_request| {
                index_metadata_request.index_uid.as_ref().unwrap() == &("test-index", 1)
            })
            .returning(move |_| {
                let mut index_metadata =
                    IndexMetadata::for_test("test-index", "ram:///indexes/test-index");
                index_metadata
                    .add_source(source_config_clone.clone())
                    .unwrap();
                Ok(IndexMetadataResponse::try_from_index_metadata(&index_metadata).unwrap())
            });
        let index_uid_clone = index_uid.clone();
        mock_metastore
            .expect_last_delete_opstamp()
            .withf(move |last_delete_opstamp| last_delete_opstamp.index_uid() == &index_uid_clone)
            .returning(move |_| Ok(LastDeleteOpstampResponse::new(10)));
        let index_uid_clone = index_uid.clone();
        mock_metastore
            .expect_stage_splits()
            .withf(move |stage_splits_request| stage_splits_request.index_uid() == &index_uid_clone)
            .returning(|_| Ok(EmptyResponse {}));
        let index_uid_clone = index_uid.clone();
        mock_metastore
            .expect_publish_splits()
            .withf(move |publish_splits_request| -> bool {
                let checkpoint_delta: IndexCheckpointDelta = publish_splits_request
                    .deserialize_index_checkpoint()
                    .unwrap()
                    .unwrap();
                publish_splits_request.index_uid() == &index_uid_clone
                    && publish_splits_request.staged_split_ids.len() == 1
                    && publish_splits_request.replaced_split_ids.is_empty()
                    && checkpoint_delta.source_id == "test-source"
                    && format!("{:?}", checkpoint_delta.source_delta)
                        .ends_with(":(00000000000000000000..~00000000000000001030])")
            })
            .returning(|_| Ok(EmptyResponse {}));

        let universe = Universe::new();
        let storage = Arc::new(RamStorage::default());
        let split_store = IndexingSplitStore::create_without_local_store_for_test(storage.clone());
        let (merge_planner_mailbox, _) = universe.create_test_mailbox();
        let pipeline_params = IndexingPipelineParams {
            pipeline_id,
            doc_mapper: Arc::new(default_doc_mapper_for_test()),
            source_config,
            source_storage_resolver: StorageResolver::for_test(),
            indexing_directory: TempDirectory::for_test(),
            indexing_settings: IndexingSettings::for_test(),
            ingester_pool: IngesterPool::default(),
            metastore: MetastoreServiceClient::from_mock(mock_metastore),
            queues_dir_path: PathBuf::from("./queues"),
            storage,
            split_store,
            merge_policy: default_merge_policy(),
            retention_policy: None,
            max_concurrent_split_uploads_index: 4,
            max_concurrent_split_uploads_merge: 5,
            cooperative_indexing_permits: None,
            merge_planner_mailbox,
            event_broker: Default::default(),
            params_fingerprint: 42u64,
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
    async fn test_indexing_pipeline_simple() -> anyhow::Result<()> {
        indexing_pipeline_simple("data/test_corpus.json").await
    }

    #[tokio::test]
    async fn test_indexing_pipeline_simple_gz() -> anyhow::Result<()> {
        indexing_pipeline_simple("data/test_corpus.json.gz").await
    }

    #[tokio::test]
    async fn test_merge_pipeline_does_not_stop_on_indexing_pipeline_failure() {
        let node_id = NodeId::from("test-node");
        let pipeline_id = IndexingPipelineId {
            node_id,
            index_uid: IndexUid::new_with_random_ulid("test-index"),
            source_id: "test-source".to_string(),
            pipeline_uid: PipelineUid::for_test(0u128),
        };
        let source_config = SourceConfig {
            source_id: "test-source".to_string(),
            num_pipelines: NonZeroUsize::MIN,
            enabled: true,
            source_params: SourceParams::void(),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        };
        let source_config_clone = source_config.clone();

        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_index_metadata()
            .withf(|index_metadata_request| {
                index_metadata_request.index_uid.as_ref().unwrap() == &("test-index", 2)
            })
            .returning(move |_| {
                let mut index_metadata =
                    IndexMetadata::for_test("test-index", "ram:///indexes/test-index");
                index_metadata
                    .add_source(source_config_clone.clone())
                    .unwrap();
                Ok(IndexMetadataResponse::try_from_index_metadata(&index_metadata).unwrap())
            });
        mock_metastore
            .expect_list_splits()
            .returning(|_| Ok(ServiceStream::empty()));
        let metastore = MetastoreServiceClient::from_mock(mock_metastore);

        let universe = Universe::with_accelerated_time();
        let doc_mapper = Arc::new(default_doc_mapper_for_test());
        let storage = Arc::new(RamStorage::default());
        let split_store = IndexingSplitStore::create_without_local_store_for_test(storage.clone());
        let merge_pipeline_params = MergePipelineParams {
            pipeline_id: pipeline_id.merge_pipeline_id(),
            doc_mapper: doc_mapper.clone(),
            indexing_directory: TempDirectory::for_test(),
            metastore: metastore.clone(),
            split_store: split_store.clone(),
            merge_policy: default_merge_policy(),
            retention_policy: None,
            max_concurrent_split_uploads: 2,
            merge_io_throughput_limiter_opt: None,
            merge_scheduler_service: universe.get_or_spawn_one(),
            event_broker: Default::default(),
        };
        let merge_pipeline = MergePipeline::new(merge_pipeline_params, None, universe.spawn_ctx());
        let merge_planner_mailbox = merge_pipeline.merge_planner_mailbox().clone();
        let (_merge_pipeline_mailbox, merge_pipeline_handler) =
            universe.spawn_builder().spawn(merge_pipeline);
        let indexing_pipeline_params = IndexingPipelineParams {
            pipeline_id,
            doc_mapper,
            source_config,
            source_storage_resolver: StorageResolver::for_test(),
            indexing_directory: TempDirectory::for_test(),
            indexing_settings: IndexingSettings::for_test(),
            ingester_pool: IngesterPool::default(),
            metastore,
            queues_dir_path: PathBuf::from("./queues"),
            storage,
            split_store,
            merge_policy: default_merge_policy(),
            retention_policy: None,
            max_concurrent_split_uploads_index: 4,
            max_concurrent_split_uploads_merge: 5,
            cooperative_indexing_permits: None,
            merge_planner_mailbox: merge_planner_mailbox.clone(),
            event_broker: Default::default(),
            params_fingerprint: 42u64,
        };
        let indexing_pipeline = IndexingPipeline::new(indexing_pipeline_params);
        let (_indexing_pipeline_mailbox, indexing_pipeline_handler) =
            universe.spawn_builder().spawn(indexing_pipeline);
        let obs = indexing_pipeline_handler
            .process_pending_and_observe()
            .await;
        assert_eq!(obs.generation, 1);
        // Let's shutdown the indexer, this will trigger the indexing pipeline failure and the
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

    async fn indexing_pipeline_all_failures_handling(test_file: &str) -> anyhow::Result<()> {
        let node_id = NodeId::from("test-node");
        let index_uid: IndexUid = IndexUid::for_test("test-index", 2);
        let pipeline_id = IndexingPipelineId {
            node_id,
            index_uid: index_uid.clone(),
            source_id: "test-source".to_string(),
            pipeline_uid: PipelineUid::for_test(0u128),
        };
        let source_config = SourceConfig {
            source_id: "test-source".to_string(),
            num_pipelines: NonZeroUsize::MIN,
            enabled: true,
            source_params: SourceParams::file_from_str(test_file).unwrap(),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        };
        let source_config_clone = source_config.clone();

        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_index_metadata()
            .withf(|index_metadata_request| {
                index_metadata_request.index_uid.as_ref().unwrap() == &("test-index", 2)
            })
            .returning(move |_| {
                let mut index_metadata =
                    IndexMetadata::for_test("test-index", "ram:///indexes/test-index");
                index_metadata
                    .add_source(source_config_clone.clone())
                    .unwrap();

                Ok(IndexMetadataResponse::try_from_index_metadata(&index_metadata).unwrap())
            });
        let index_uid_clone = index_uid.clone();
        mock_metastore
            .expect_last_delete_opstamp()
            .withf(move |last_delete_opstamp| last_delete_opstamp.index_uid() == &index_uid_clone)
            .returning(move |_| Ok(LastDeleteOpstampResponse::new(10)));
        mock_metastore
            .expect_stage_splits()
            .never()
            .returning(|_| Ok(EmptyResponse {}));
        let index_uid_clone = index_uid.clone();
        mock_metastore
            .expect_publish_splits()
            .withf(move |publish_splits_request| -> bool {
                let checkpoint_delta: IndexCheckpointDelta = publish_splits_request
                    .deserialize_index_checkpoint()
                    .unwrap()
                    .unwrap();
                publish_splits_request.index_uid() == &index_uid_clone
                    && publish_splits_request.staged_split_ids.is_empty()
                    && publish_splits_request.replaced_split_ids.is_empty()
                    && checkpoint_delta.source_id == "test-source"
                    && format!("{:?}", checkpoint_delta.source_delta)
                        .ends_with(":(00000000000000000000..~00000000000000001030])")
            })
            .returning(|_| Ok(EmptyResponse {}));
        let universe = Universe::new();
        let storage = Arc::new(RamStorage::default());
        let split_store = IndexingSplitStore::create_without_local_store_for_test(storage.clone());
        let (merge_planner_mailbox, _) = universe.create_test_mailbox();
        // Create a minimal mapper with wrong date format to ensure that all documents will fail
        let broken_mapper = serde_json::from_str::<DocMapper>(
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
            source_storage_resolver: StorageResolver::for_test(),
            indexing_directory: TempDirectory::for_test(),
            indexing_settings: IndexingSettings::for_test(),
            ingester_pool: IngesterPool::default(),
            metastore: MetastoreServiceClient::from_mock(mock_metastore),
            queues_dir_path: PathBuf::from("./queues"),
            storage,
            split_store,
            merge_policy: default_merge_policy(),
            retention_policy: None,
            max_concurrent_split_uploads_index: 4,
            max_concurrent_split_uploads_merge: 5,
            cooperative_indexing_permits: None,
            merge_planner_mailbox,
            params_fingerprint: 42u64,
            event_broker: Default::default(),
        };
        let pipeline = IndexingPipeline::new(pipeline_params);
        let (_pipeline_mailbox, pipeline_handler) = universe.spawn_builder().spawn(pipeline);
        let (pipeline_exit_status, pipeline_statistics) = pipeline_handler.join().await;
        assert!(pipeline_exit_status.is_success());
        // flaky. Sometimes generations is 2.
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

    #[tokio::test]
    async fn test_indexing_pipeline_all_failures_handling() -> anyhow::Result<()> {
        indexing_pipeline_all_failures_handling("data/test_corpus.json").await
    }

    #[tokio::test]
    async fn test_indexing_pipeline_all_failures_handling_gz() -> anyhow::Result<()> {
        indexing_pipeline_all_failures_handling("data/test_corpus.json.gz").await
    }
}
