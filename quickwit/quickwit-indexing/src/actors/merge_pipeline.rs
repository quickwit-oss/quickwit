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

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use byte_unit::Byte;
use quickwit_actors::{
    Actor, ActorContext, ActorExitStatus, ActorHandle, Handler, Health, Inbox, Mailbox,
    SpawnContext, Supervisable, HEARTBEAT,
};
use quickwit_common::io::IoControls;
use quickwit_common::temp_dir::TempDirectory;
use quickwit_common::KillSwitch;
use quickwit_doc_mapper::DocMapper;
use quickwit_metastore::{ListSplitsQuery, Metastore, SplitState};
use quickwit_proto::indexing::IndexingPipelineId;
use quickwit_proto::metastore::MetastoreError;
use time::OffsetDateTime;
use tokio::join;
use tracing::{debug, error, info, instrument};

use crate::actors::indexing_pipeline::wait_duration_before_retry;
use crate::actors::merge_split_downloader::MergeSplitDownloader;
use crate::actors::publisher::PublisherType;
use crate::actors::{MergeExecutor, MergePlanner, Packager, Publisher, Uploader, UploaderType};
use crate::merge_policy::MergePolicy;
use crate::models::MergeStatistics;
use crate::split_store::IndexingSplitStore;

#[derive(Debug)]
struct ObserveLoop;

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
    // TODO improve API. Maybe it could take a spawnbuilder as argument, hence removing the need
    // for a public create_mailbox / MessageCount.
    pub fn new(params: MergePipelineParams, spawn_ctx: &SpawnContext) -> Self {
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
                pipeline_id=?self.params.pipeline_id,
                generation=self.generation(),
                healthy_actors=?healthy_actors,
                failed_or_unhealthy_actors=?failure_or_unhealthy_actors,
                success_actors=?success_actors,
                "Merge pipeline failure."
            );
            return Health::FailureOrUnhealthy;
        }
        if healthy_actors.is_empty() {
            // All the actors finished successfully.
            info!(
                pipeline_id=?self.params.pipeline_id,
                generation=self.generation(),
                "Merge pipeline success."
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
            "Merge pipeline running."
        );
        Health::Healthy
    }

    fn generation(&self) -> usize {
        self.statistics.generation
    }

    // TODO: Should return an error saying whether we can retry or not.
    #[instrument(name="spawn_merge_pipeline", level="info", skip_all, fields(index=%self.params.pipeline_id.index_uid.index_id(), gen=self.generation()))]
    async fn spawn_pipeline(&mut self, ctx: &ActorContext<Self>) -> anyhow::Result<()> {
        self.statistics.num_spawn_attempts += 1;
        self.kill_switch = ctx.kill_switch().child();

        info!(
            index_id=%self.params.pipeline_id.index_uid.index_id(),
            source_id=%self.params.pipeline_id.source_id,
            pipeline_ord=%self.params.pipeline_id.pipeline_ord,
            root_dir=%self.params.indexing_directory.path().display(),
            merge_policy=?self.params.merge_policy,
            "Spawning merge pipeline.",
        );
        let query = ListSplitsQuery::for_index(self.params.pipeline_id.index_uid.clone())
            .with_split_state(SplitState::Published)
            .retain_immature(OffsetDateTime::now_utc());
        let published_splits = ctx
            .protect_future(self.params.metastore.list_splits(query))
            .await?
            .into_iter()
            .map(|split| split.split_metadata)
            .collect::<Vec<_>>();

        // Merge publisher
        let merge_publisher = Publisher::new(
            PublisherType::MergePublisher,
            self.params.metastore.clone(),
            Some(self.merge_planner_mailbox.clone()),
            None,
        );
        let (merge_publisher_mailbox, merge_publisher_handler) = ctx
            .spawn_actor()
            .set_kill_switch(self.kill_switch.clone())
            .spawn(merge_publisher);

        // Merge uploader
        let merge_uploader = Uploader::new(
            UploaderType::MergeUploader,
            self.params.metastore.clone(),
            self.params.merge_policy.clone(),
            self.params.split_store.clone(),
            merge_publisher_mailbox.into(),
            self.params.max_concurrent_split_uploads,
        );
        let (merge_uploader_mailbox, merge_uploader_handler) = ctx
            .spawn_actor()
            .set_kill_switch(self.kill_switch.clone())
            .spawn(merge_uploader);

        // Merge Packager
        let tag_fields = self.params.doc_mapper.tag_named_fields()?;
        let merge_packager = Packager::new("MergePackager", tag_fields, merge_uploader_mailbox);
        let (merge_packager_mailbox, merge_packager_handler) = ctx
            .spawn_actor()
            .set_kill_switch(self.kill_switch.clone())
            .spawn(merge_packager);

        let max_merge_write_throughput: f64 = self
            .params
            .merge_max_io_num_bytes_per_sec
            .as_ref()
            .map(|bytes_per_sec| bytes_per_sec.get_bytes() as f64)
            .unwrap_or(f64::INFINITY);

        let split_downloader_io_controls = IoControls::default()
            .set_throughput_limit(max_merge_write_throughput)
            .set_index_and_component(
                self.params.pipeline_id.index_uid.index_id(),
                "split_downloader_merge",
            );

        // The merge and split download share the same throughput limiter.
        // This is how cloning the `IoControls` works.
        let merge_executor_io_controls = split_downloader_io_controls
            .clone()
            .set_index_and_component(self.params.pipeline_id.index_uid.index_id(), "merger");

        let merge_executor = MergeExecutor::new(
            self.params.pipeline_id.clone(),
            self.params.metastore.clone(),
            self.params.doc_mapper.clone(),
            merge_executor_io_controls,
            merge_packager_mailbox,
        );
        let (merge_executor_mailbox, merge_executor_handler) = ctx
            .spawn_actor()
            .set_kill_switch(self.kill_switch.clone())
            .spawn(merge_executor);

        let merge_split_downloader = MergeSplitDownloader {
            scratch_directory: self.params.indexing_directory.clone(),
            split_store: self.params.split_store.clone(),
            executor_mailbox: merge_executor_mailbox,
            io_controls: split_downloader_io_controls,
        };
        let (merge_split_downloader_mailbox, merge_split_downloader_handler) = ctx
            .spawn_actor()
            .set_kill_switch(self.kill_switch.clone())
            .set_backpressure_micros_counter(
                crate::metrics::INDEXER_METRICS
                    .backpressure_micros
                    .with_label_values([
                        self.params.pipeline_id.index_uid.index_id(),
                        "MergeSplitDownloader",
                    ]),
            )
            .spawn(merge_split_downloader);

        // Merge planner
        let merge_planner = MergePlanner::new(
            self.params.pipeline_id.clone(),
            published_splits,
            self.params.merge_policy.clone(),
            merge_split_downloader_mailbox,
        );
        let (_, merge_planner_handler) = ctx
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
            merge_planner: merge_planner_handler,
            merge_split_downloader: merge_split_downloader_handler,
            merge_executor: merge_executor_handler,
            merge_packager: merge_packager_handler,
            merge_uploader: merge_uploader_handler,
            merge_publisher: merge_publisher_handler,
            next_check_for_progress: Instant::now() + *HEARTBEAT,
        });
        Ok(())
    }

    async fn terminate(&mut self) {
        self.kill_switch.kill();
        if let Some(handlers) = self.handles_opt.take() {
            tokio::join!(
                handlers.merge_planner.kill(),
                handlers.merge_split_downloader.kill(),
                handlers.merge_executor.kill(),
                handlers.merge_packager.kill(),
                handlers.merge_uploader.kill(),
                handlers.merge_publisher.kill(),
            );
        }
    }

    async fn perform_observe(&mut self) {
        let Some(handles) = &self.handles_opt else {
            return;
        };
        let (merge_planner_state, merge_uploader_counters, merge_publisher_counters) = join!(
            handles.merge_planner.observe(),
            handles.merge_uploader.observe(),
            handles.merge_publisher.observe(),
        );
        self.statistics = self
            .previous_generations_statistics
            .clone()
            .add_actor_counters(&merge_uploader_counters, &merge_publisher_counters)
            .set_generation(self.statistics.generation)
            .set_num_spawn_attempts(self.statistics.num_spawn_attempts)
            .set_ongoing_merges(merge_planner_state.ongoing_merge_operations.len());
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
                ctx.schedule_self_msg(*quickwit_actors::HEARTBEAT, Spawn { retry_count: 0 })
                    .await;
            }
            Health::Success => {
                return Err(ActorExitStatus::Success);
            }
        }
        Ok(())
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
        ctx.schedule_self_msg(Duration::from_secs(1), supervise_loop_token)
            .await;
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
            let retry_delay = wait_duration_before_retry(spawn.retry_count);
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

#[derive(Clone)]
pub struct MergePipelineParams {
    pub pipeline_id: IndexingPipelineId,
    pub doc_mapper: Arc<dyn DocMapper>,
    pub indexing_directory: TempDirectory,
    pub metastore: Arc<dyn Metastore>,
    pub split_store: IndexingSplitStore,
    pub merge_policy: Arc<dyn MergePolicy>,
    pub max_concurrent_split_uploads: usize, //< TODO share with the indexing pipeline.
    pub merge_max_io_num_bytes_per_sec: Option<Byte>,
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;
    use std::sync::Arc;

    use quickwit_actors::{ActorExitStatus, Universe};
    use quickwit_common::temp_dir::TempDirectory;
    use quickwit_doc_mapper::default_doc_mapper_for_test;
    use quickwit_metastore::MockMetastore;
    use quickwit_proto::indexing::IndexingPipelineId;
    use quickwit_proto::IndexUid;
    use quickwit_storage::RamStorage;

    use crate::actors::merge_pipeline::{MergePipeline, MergePipelineParams};
    use crate::merge_policy::default_merge_policy;
    use crate::IndexingSplitStore;

    #[tokio::test]
    async fn test_merge_pipeline_simple() -> anyhow::Result<()> {
        let mut metastore = MockMetastore::default();
        let index_uid = IndexUid::new("test-index");
        let pipeline_id = IndexingPipelineId {
            index_uid: index_uid.clone(),
            source_id: "test-source".to_string(),
            node_id: "test-node".to_string(),
            pipeline_ord: 0,
        };
        metastore
            .expect_list_splits()
            .times(1)
            .returning(move |list_split_query| {
                assert_eq!(list_split_query.index_uids, &[index_uid.clone()]);
                assert_eq!(
                    list_split_query.split_states,
                    vec![quickwit_metastore::SplitState::Published]
                );
                let Bound::Excluded(_) = list_split_query.mature else {
                    panic!("Expected excluded bound.");
                };
                Ok(Vec::new())
            });
        let universe = Universe::with_accelerated_time();
        let storage = Arc::new(RamStorage::default());
        let split_store = IndexingSplitStore::create_without_local_store(storage.clone());
        let pipeline_params = MergePipelineParams {
            pipeline_id,
            doc_mapper: Arc::new(default_doc_mapper_for_test()),
            indexing_directory: TempDirectory::for_test(),
            metastore: Arc::new(metastore),
            split_store,
            merge_policy: default_merge_policy(),
            max_concurrent_split_uploads: 2,
            merge_max_io_num_bytes_per_sec: None,
        };
        let pipeline = MergePipeline::new(pipeline_params, universe.spawn_ctx());
        let (_pipeline_mailbox, pipeline_handler) = universe.spawn_builder().spawn(pipeline);
        let (pipeline_exit_status, pipeline_statistics) = pipeline_handler.quit().await;
        assert_eq!(pipeline_statistics.generation, 1);
        assert_eq!(pipeline_statistics.num_spawn_attempts, 1);
        assert_eq!(pipeline_statistics.num_published_splits, 0);
        assert!(matches!(pipeline_exit_status, ActorExitStatus::Quit));
        universe.assert_quit().await;
        Ok(())
    }
}
