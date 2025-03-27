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

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use quickwit_actors::{
    Actor, ActorContext, ActorExitStatus, ActorHandle, Handler, Health, Inbox, Mailbox,
    SpawnContext, Supervisable, HEARTBEAT,
};
use quickwit_common::io::{IoControls, Limiter};
use quickwit_common::pubsub::EventBroker;
use quickwit_common::temp_dir::TempDirectory;
use quickwit_common::KillSwitch;
use quickwit_config::RetentionPolicy;
use quickwit_doc_mapper::DocMapper;
use quickwit_metastore::{
    ListSplitsQuery, ListSplitsRequestExt, MetastoreServiceStreamSplitsExt, SplitMetadata,
    SplitState,
};
use quickwit_proto::indexing::MergePipelineId;
use quickwit_proto::metastore::{
    ListSplitsRequest, MetastoreError, MetastoreResult, MetastoreService, MetastoreServiceClient,
};
use time::OffsetDateTime;
use tokio::sync::Semaphore;
use tracing::{debug, error, info, instrument};

use super::publisher::DisconnectMergePlanner;
use super::{MergeSchedulerService, RunFinalizeMergePolicyAndQuit};
use crate::actors::indexing_pipeline::wait_duration_before_retry;
use crate::actors::merge_split_downloader::MergeSplitDownloader;
use crate::actors::publisher::PublisherType;
use crate::actors::{MergeExecutor, MergePlanner, Packager, Publisher, Uploader, UploaderType};
use crate::merge_policy::MergePolicy;
use crate::models::MergeStatistics;
use crate::split_store::IndexingSplitStore;

/// Spawning a merge pipeline puts a lot of pressure on the metastore so
/// we rely on this semaphore to limit the number of merge pipelines that can be spawned
/// concurrently.
static SPAWN_PIPELINE_SEMAPHORE: Semaphore = Semaphore::const_new(10);

/// Instructs the merge pipeline that it should stop itself.
/// Merges that have already been scheduled are not aborted.
///
/// In addition, the finalizer merge policy will be executed to schedule a few
/// additional merges.
///
/// After reception the `FinalizeAndClosePipeline`, the merge pipeline loop will
/// be disconnected. In other words, the connection from the merge publisher to
/// the merge planner will be cut, so that the merge pipeline will terminate naturally.
///
/// Supervisation will still exist. However it will not restart the pipeline
/// in case of failure, it will just kill all of the merge pipeline actors. (for
/// instance, if one of the actor is stuck).
#[derive(Debug, Clone, Copy)]
pub struct FinishPendingMergesAndShutdownPipeline;

struct MergePipelineHandles {
    merge_planner: ActorHandle<MergePlanner>,
    merge_split_downloader: ActorHandle<MergeSplitDownloader>,
    merge_executor: ActorHandle<MergeExecutor>,
    merge_packager: ActorHandle<Packager>,
    merge_uploader: ActorHandle<Uploader>,
    merge_publisher: ActorHandle<Publisher>,
    next_check_for_progress: Instant,
}

impl MergePipelineHandles {
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
#[derive(Debug)]
struct SuperviseLoop;

#[derive(Clone, Copy, Debug, Default)]
struct Spawn {
    retry_count: usize,
}

pub struct MergePipeline {
    params: MergePipelineParams,
    merge_planner_mailbox: Mailbox<MergePlanner>,
    merge_planner_inbox: Inbox<MergePlanner>,
    previous_generations_statistics: MergeStatistics,
    statistics: MergeStatistics,
    handles_opt: Option<MergePipelineHandles>,
    kill_switch: KillSwitch,
    /// Immature splits passed to the merge planner the first time the pipeline is spawned.
    initial_immature_splits_opt: Option<Vec<SplitMetadata>>,
    // After it is set to true, we don't respawn pipeline actors if they fail.
    shutdown_initiated: bool,
}

#[async_trait]
impl Actor for MergePipeline {
    type ObservableState = MergeStatistics;

    fn observable_state(&self) -> Self::ObservableState {
        self.statistics.clone()
    }

    fn name(&self) -> String {
        "MergePipeline".to_string()
    }

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        self.handle(Spawn::default(), ctx).await?;
        self.handle(SuperviseLoop, ctx).await?;
        Ok(())
    }
}

impl MergePipeline {
    /// Creates a new merge pipeline. `initial_immature_splits_opt` is typically "seeded" by the
    /// indexing service who fetches the immature splits from the metastore for all the merge
    /// pipelines it is about to spawn. By issuing a single metastore query instead of one per merge
    /// pipeline, we reduce the load on the metastore. If the merge pipeline crashes and is
    /// respawned by the supervisor, the immature splits are fetched directly from the metastore.
    pub fn new(
        params: MergePipelineParams,
        initial_immature_splits_opt: Option<Vec<SplitMetadata>>,
        spawn_ctx: &SpawnContext,
    ) -> Self {
        // TODO improve API. Maybe it could take a spawnbuilder as argument, hence removing the need
        // for a public create_mailbox / MessageCount.
        let (merge_planner_mailbox, merge_planner_inbox) = spawn_ctx
            .create_mailbox::<MergePlanner>("MergePlanner", MergePlanner::queue_capacity());
        Self {
            params,
            previous_generations_statistics: Default::default(),
            handles_opt: None,
            kill_switch: KillSwitch::default(),
            statistics: MergeStatistics::default(),
            merge_planner_inbox,
            merge_planner_mailbox,
            initial_immature_splits_opt,
            shutdown_initiated: false,
        }
    }

    pub fn merge_planner_mailbox(&self) -> &Mailbox<MergePlanner> {
        &self.merge_planner_mailbox
    }

    fn supervisables(&self) -> Vec<&dyn Supervisable> {
        if let Some(handles) = &self.handles_opt {
            let supervisables: Vec<&dyn Supervisable> = vec![
                &handles.merge_planner,
                &handles.merge_split_downloader,
                &handles.merge_executor,
                &handles.merge_packager,
                &handles.merge_uploader,
                &handles.merge_publisher,
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
                index_uid=%self.params.pipeline_id.index_uid,
                source_id=%self.params.pipeline_id.source_id,
                generation=self.generation(),
                healthy_actors=?healthy_actors,
                failed_or_unhealthy_actors=?failure_or_unhealthy_actors,
                success_actors=?success_actors,
                "merge pipeline failed"
            );
            return Health::FailureOrUnhealthy;
        }
        if healthy_actors.is_empty() {
            // All the actors finished successfully.
            info!(
                index_uid=%self.params.pipeline_id.index_uid,
                source_id=%self.params.pipeline_id.source_id,
                generation=self.generation(),
                "merge pipeline completed successfully"
            );
            return Health::Success;
        }
        // No error at this point and there are still some actors running.
        debug!(
            index_uid=%self.params.pipeline_id.index_uid,
            source_id=%self.params.pipeline_id.source_id,
            generation=self.generation(),
            healthy_actors=?healthy_actors,
            failed_or_unhealthy_actors=?failure_or_unhealthy_actors,
            success_actors=?success_actors,
            "merge pipeline is running and healthy"
        );
        Health::Healthy
    }

    fn generation(&self) -> usize {
        self.statistics.generation
    }

    // TODO: Should return an error saying whether we can retry or not.
    #[instrument(name="spawn_merge_pipeline", level="info", skip_all, fields(index_uid=%self.params.pipeline_id.index_uid, generation=self.generation()))]
    async fn spawn_pipeline(&mut self, ctx: &ActorContext<Self>) -> anyhow::Result<()> {
        let _spawn_pipeline_permit = ctx
            .protect_future(SPAWN_PIPELINE_SEMAPHORE.acquire())
            .await
            .expect("semaphore should not be closed");

        self.statistics.num_spawn_attempts += 1;
        self.kill_switch = ctx.kill_switch().child();

        info!(
            index_uid=%self.params.pipeline_id.index_uid,
            source_id=%self.params.pipeline_id.source_id,
            root_dir=%self.params.indexing_directory.path().display(),
            merge_policy=?self.params.merge_policy,
            "spawning merge pipeline",
        );
        let immature_splits = self.fetch_immature_splits(ctx).await?;

        // Merge publisher
        let merge_publisher = Publisher::new(
            PublisherType::MergePublisher,
            self.params.metastore.clone(),
            Some(self.merge_planner_mailbox.clone()),
            None,
        );
        let (merge_publisher_mailbox, merge_publisher_handle) = ctx
            .spawn_actor()
            .set_kill_switch(self.kill_switch.clone())
            .set_backpressure_micros_counter(
                crate::metrics::INDEXER_METRICS
                    .backpressure_micros
                    .with_label_values(["merge_publisher"]),
            )
            .spawn(merge_publisher);

        // Merge uploader
        let merge_uploader = Uploader::new(
            UploaderType::MergeUploader,
            self.params.metastore.clone(),
            self.params.merge_policy.clone(),
            self.params.retention_policy.clone(),
            self.params.split_store.clone(),
            merge_publisher_mailbox.into(),
            self.params.max_concurrent_split_uploads,
            self.params.event_broker.clone(),
        );
        let (merge_uploader_mailbox, merge_uploader_handle) = ctx
            .spawn_actor()
            .set_kill_switch(self.kill_switch.clone())
            .spawn(merge_uploader);

        // Merge Packager
        let tag_fields = self.params.doc_mapper.tag_named_fields()?;
        let merge_packager = Packager::new("MergePackager", tag_fields, merge_uploader_mailbox);
        let (merge_packager_mailbox, merge_packager_handle) = ctx
            .spawn_actor()
            .set_kill_switch(self.kill_switch.clone())
            .spawn(merge_packager);

        let split_downloader_io_controls = IoControls::default()
            .set_throughput_limiter_opt(self.params.merge_io_throughput_limiter_opt.clone())
            .set_component("split_downloader_merge");

        // The merge and split download share the same throughput limiter.
        // This is how cloning the `IoControls` works.
        let merge_executor_io_controls =
            split_downloader_io_controls.clone().set_component("merger");

        let merge_executor = MergeExecutor::new(
            self.params.pipeline_id.clone(),
            self.params.metastore.clone(),
            self.params.doc_mapper.clone(),
            merge_executor_io_controls,
            merge_packager_mailbox,
        );
        let (merge_executor_mailbox, merge_executor_handle) = ctx
            .spawn_actor()
            .set_kill_switch(self.kill_switch.clone())
            .set_backpressure_micros_counter(
                crate::metrics::INDEXER_METRICS
                    .backpressure_micros
                    .with_label_values(["merge_executor"]),
            )
            .spawn(merge_executor);

        let merge_split_downloader = MergeSplitDownloader {
            scratch_directory: self.params.indexing_directory.clone(),
            split_store: self.params.split_store.clone(),
            executor_mailbox: merge_executor_mailbox,
            io_controls: split_downloader_io_controls,
        };
        let (merge_split_downloader_mailbox, merge_split_downloader_handle) = ctx
            .spawn_actor()
            .set_kill_switch(self.kill_switch.clone())
            .set_backpressure_micros_counter(
                crate::metrics::INDEXER_METRICS
                    .backpressure_micros
                    .with_label_values(["merge_split_downloader"]),
            )
            .spawn(merge_split_downloader);

        // Merge planner
        let merge_planner = MergePlanner::new(
            &self.params.pipeline_id,
            immature_splits,
            self.params.merge_policy.clone(),
            merge_split_downloader_mailbox,
            self.params.merge_scheduler_service.clone(),
        )
        .await?;
        let (_, merge_planner_handle) = ctx
            .spawn_actor()
            .set_kill_switch(self.kill_switch.clone())
            .set_mailboxes(
                self.merge_planner_mailbox.clone(),
                self.merge_planner_inbox.clone(),
            )
            .spawn(merge_planner);

        self.previous_generations_statistics = self.statistics.clone();
        self.statistics.generation += 1;
        self.handles_opt = Some(MergePipelineHandles {
            merge_planner: merge_planner_handle,
            merge_split_downloader: merge_split_downloader_handle,
            merge_executor: merge_executor_handle,
            merge_packager: merge_packager_handle,
            merge_uploader: merge_uploader_handle,
            merge_publisher: merge_publisher_handle,
            next_check_for_progress: Instant::now() + *HEARTBEAT,
        });
        Ok(())
    }

    async fn terminate(&mut self) {
        self.kill_switch.kill();
        if let Some(handles) = self.handles_opt.take() {
            tokio::join!(
                handles.merge_planner.kill(),
                handles.merge_split_downloader.kill(),
                handles.merge_executor.kill(),
                handles.merge_packager.kill(),
                handles.merge_uploader.kill(),
                handles.merge_publisher.kill(),
            );
        }
    }

    async fn perform_observe(&mut self) {
        let Some(handles) = &self.handles_opt else {
            return;
        };
        handles.merge_planner.refresh_observe();
        handles.merge_uploader.refresh_observe();
        handles.merge_publisher.refresh_observe();
        let num_ongoing_merges = crate::metrics::INDEXER_METRICS
            .ongoing_merge_operations
            .get();
        self.statistics = self
            .previous_generations_statistics
            .clone()
            .add_actor_counters(
                &handles.merge_uploader.last_observation(),
                &handles.merge_publisher.last_observation(),
            )
            .set_generation(self.statistics.generation)
            .set_num_spawn_attempts(self.statistics.num_spawn_attempts)
            .set_ongoing_merges(usize::try_from(num_ongoing_merges).unwrap_or(0));
    }

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
                ctx.schedule_self_msg(*quickwit_actors::HEARTBEAT, Spawn { retry_count: 0 });
            }
            Health::Success => {
                info!(index_uid=%self.params.pipeline_id.index_uid, "merge pipeline success, shutting down");
                return Err(ActorExitStatus::Success);
            }
        }
        Ok(())
    }

    async fn fetch_immature_splits(
        &mut self,
        ctx: &ActorContext<Self>,
    ) -> MetastoreResult<Vec<quickwit_metastore::SplitMetadata>> {
        // We consume the initial immature splits provided by the indexing service on the first
        // spawn.
        if let Some(immature_splits) = self.initial_immature_splits_opt.take() {
            return Ok(immature_splits);
        }
        // On subsequent spawns, we fetch the immature splits directly from the metastore.
        let index_uid = self.params.pipeline_id.index_uid.clone();
        let node_id = self.params.pipeline_id.node_id.clone();
        let list_splits_query = ListSplitsQuery::for_index(index_uid)
            .with_node_id(node_id)
            .with_split_state(SplitState::Published)
            .retain_immature(OffsetDateTime::now_utc());
        let list_splits_request =
            ListSplitsRequest::try_from_list_splits_query(&list_splits_query)?;
        let immature_splits_stream = ctx
            .protect_future(self.params.metastore.list_splits(list_splits_request))
            .await?;
        let immature_splits = ctx
            .protect_future(immature_splits_stream.collect_splits_metadata())
            .await?;
        info!(
            index_uid=%self.params.pipeline_id.index_uid,
            source_id=%self.params.pipeline_id.source_id,
            "fetched {} splits candidates for merge",
            immature_splits.len()
        );
        Ok(immature_splits)
    }
}

#[async_trait]
impl Handler<SuperviseLoop> for MergePipeline {
    type Reply = ();
    async fn handle(
        &mut self,
        supervise_loop_token: SuperviseLoop,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        self.perform_observe().await;
        self.perform_health_check(ctx).await?;
        ctx.schedule_self_msg(Duration::from_secs(1), supervise_loop_token);
        Ok(())
    }
}

#[async_trait]
impl Handler<FinishPendingMergesAndShutdownPipeline> for MergePipeline {
    type Reply = ();
    async fn handle(
        &mut self,
        _: FinishPendingMergesAndShutdownPipeline,
        _ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        info!(index_uid=%self.params.pipeline_id.index_uid, "shutdown merge pipeline initiated");
        // From now on, we will not respawn the pipeline if it fails.
        self.shutdown_initiated = true;
        if let Some(handles) = &self.handles_opt {
            // This disconnects the merge planner from the merge publisher,
            // breaking the merge planner pipeline loop.
            //
            // As a result, the pipeline will naturally terminate
            // once all of the pending / ongoing merge operations are completed.
            let _ = handles
                .merge_publisher
                .mailbox()
                .send_message(DisconnectMergePlanner)
                .await;

            // We also initiate the merge planner finalization routine.
            // Depending on the merge policy, it may emit a few more merge
            // operations.
            let _ = handles
                .merge_planner
                .mailbox()
                .send_message(RunFinalizeMergePolicyAndQuit)
                .await;
        } else {
            // we won't respawn the pipeline in the future, so there is nothing
            // to do here.
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<Spawn> for MergePipeline {
    type Reply = ();

    async fn handle(
        &mut self,
        spawn: Spawn,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        if self.shutdown_initiated {
            return Ok(());
        }
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
            let retry_delay = wait_duration_before_retry(spawn.retry_count);
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

#[derive(Clone)]
pub struct MergePipelineParams {
    pub pipeline_id: MergePipelineId,
    pub doc_mapper: Arc<DocMapper>,
    pub indexing_directory: TempDirectory,
    pub metastore: MetastoreServiceClient,
    pub merge_scheduler_service: Mailbox<MergeSchedulerService>,
    pub split_store: IndexingSplitStore,
    pub merge_policy: Arc<dyn MergePolicy>,
    pub retention_policy: Option<RetentionPolicy>,
    pub max_concurrent_split_uploads: usize, //< TODO share with the indexing pipeline.
    pub merge_io_throughput_limiter_opt: Option<Limiter>,
    pub event_broker: EventBroker,
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;
    use std::sync::Arc;

    use quickwit_actors::{ActorExitStatus, Universe};
    use quickwit_common::temp_dir::TempDirectory;
    use quickwit_common::ServiceStream;
    use quickwit_doc_mapper::default_doc_mapper_for_test;
    use quickwit_metastore::ListSplitsRequestExt;
    use quickwit_proto::indexing::MergePipelineId;
    use quickwit_proto::metastore::{MetastoreServiceClient, MockMetastoreService};
    use quickwit_proto::types::{IndexUid, NodeId};
    use quickwit_storage::RamStorage;

    use crate::actors::merge_pipeline::{MergePipeline, MergePipelineParams};
    use crate::actors::{MergePlanner, Publisher};
    use crate::merge_policy::default_merge_policy;
    use crate::IndexingSplitStore;

    #[tokio::test]
    async fn test_merge_pipeline_simple() -> anyhow::Result<()> {
        let node_id = NodeId::from("test-node");
        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = "test-source".to_string();
        let pipeline_id = MergePipelineId {
            index_uid: index_uid.clone(),
            source_id,
            node_id,
        };
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_list_splits()
            .times(1)
            .withf(move |list_splits_request| {
                let list_split_query = list_splits_request.deserialize_list_splits_query().unwrap();
                assert_eq!(list_split_query.index_uids, Some(vec![index_uid.clone()]));
                assert_eq!(
                    list_split_query.split_states,
                    vec![quickwit_metastore::SplitState::Published]
                );
                let Bound::Excluded(_) = list_split_query.mature else {
                    panic!("expected `Bound::Excluded`");
                };
                true
            })
            .returning(|_| Ok(ServiceStream::empty()));
        let universe = Universe::with_accelerated_time();
        let storage = Arc::new(RamStorage::default());
        let split_store = IndexingSplitStore::create_without_local_store_for_test(storage.clone());
        let pipeline_params = MergePipelineParams {
            pipeline_id,
            doc_mapper: Arc::new(default_doc_mapper_for_test()),
            indexing_directory: TempDirectory::for_test(),
            metastore: MetastoreServiceClient::from_mock(mock_metastore),
            merge_scheduler_service: universe.get_or_spawn_one(),
            split_store,
            merge_policy: default_merge_policy(),
            retention_policy: None,
            max_concurrent_split_uploads: 2,
            merge_io_throughput_limiter_opt: None,
            event_broker: Default::default(),
        };
        let pipeline = MergePipeline::new(pipeline_params, None, universe.spawn_ctx());
        let _merge_planner_mailbox = pipeline.merge_planner_mailbox().clone();
        let (pipeline_mailbox, pipeline_handle) = universe.spawn_builder().spawn(pipeline);
        pipeline_mailbox
            .ask(super::FinishPendingMergesAndShutdownPipeline)
            .await
            .unwrap();

        let (pipeline_exit_status, pipeline_statistics) = pipeline_handle.join().await;
        assert_eq!(pipeline_statistics.generation, 1);
        assert_eq!(pipeline_statistics.num_spawn_attempts, 1);
        assert_eq!(pipeline_statistics.num_published_splits, 0);
        assert!(matches!(pipeline_exit_status, ActorExitStatus::Success));

        // Checking that the merge pipeline actors have been properly cleaned up.
        assert!(universe.get_one::<MergePlanner>().is_none());
        assert!(universe.get_one::<Publisher>().is_none());
        assert!(universe.get_one::<MergePipeline>().is_none());

        universe.assert_quit().await;
        Ok(())
    }
}
