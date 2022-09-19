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

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use quickwit_actors::{
    create_mailbox, Actor, ActorContext, ActorExitStatus, ActorHandle, Handler, Health, KillSwitch,
    Mailbox, QueueCapacity, Supervisable,
};
use quickwit_doc_mapper::DocMapper;
use quickwit_metastore::{Metastore, MetastoreError, SplitState};
use tokio::join;
use tracing::{debug, error, info, info_span, instrument, Span};

use crate::actors::indexing_pipeline::wait_duration_before_retry;
use crate::actors::merge_split_downloader::MergeSplitDownloader;
use crate::actors::publisher::PublisherType;
use crate::actors::sequencer::Sequencer;
use crate::actors::{MergeExecutor, MergePlanner, Packager, Publisher, Uploader};
use crate::merge_policy::MergePolicy;
use crate::models::{IndexingDirectory, IndexingPipelineId, MergingStatistics, Observe};
use crate::split_store::IndexingSplitStore;

pub struct MergingPipelineHandle {
    pub merge_planner: ActorHandle<MergePlanner>,
    pub merge_split_downloader: ActorHandle<MergeSplitDownloader>,
    pub merge_executor: ActorHandle<MergeExecutor>,
    pub merge_packager: ActorHandle<Packager>,
    pub merge_uploader: ActorHandle<Uploader>,
    pub merge_sequencer: ActorHandle<Sequencer<Publisher>>,
    pub merge_publisher: ActorHandle<Publisher>,
}

// Messages
#[derive(Clone, Copy, Debug)]
pub struct Supervise;

#[derive(Clone, Copy, Debug, Default)]
pub struct Spawn {
    retry_count: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct GetMergePlannerMailbox;

pub struct MergingPipeline {
    params: MergingPipelineParams,
    previous_generations_statistics: MergingStatistics,
    statistics: MergingStatistics,
    merge_planner_mailbox: Option<Mailbox<MergePlanner>>,
    handles: Option<MergingPipelineHandle>,
    kill_switch: KillSwitch,
}

#[async_trait]
impl Actor for MergingPipeline {
    type ObservableState = MergingStatistics;

    fn observable_state(&self) -> Self::ObservableState {
        self.statistics.clone()
    }

    fn name(&self) -> String {
        "MergingPipeline".to_string()
    }

    fn span(&self, _ctx: &ActorContext<Self>) -> Span {
        info_span!("")
    }

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        self.handle(Spawn::default(), ctx).await?;
        self.handle(Observe, ctx).await?;
        self.handle(Supervise, ctx).await?;
        Ok(())
    }
}

impl MergingPipeline {
    pub fn new(params: MergingPipelineParams) -> Self {
        Self {
            params,
            previous_generations_statistics: Default::default(),
            merge_planner_mailbox: None,
            handles: None,
            kill_switch: KillSwitch::default(),
            statistics: MergingStatistics::default(),
        }
    }

    fn supervisables(&self) -> Vec<&dyn Supervisable> {
        if let Some(handles) = &self.handles {
            let supervisables: Vec<&dyn Supervisable> = vec![
                &handles.merge_planner,
                &handles.merge_split_downloader,
                &handles.merge_executor,
                &handles.merge_packager,
                &handles.merge_uploader,
                &handles.merge_sequencer,
                &handles.merge_publisher,
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

    // TODO: This should return an error saying whether we can retry or not.
    #[instrument(name="", level="info", skip_all, fields(index=%self.params.pipeline_id.index_id, gen=self.generation()))]
    async fn spawn_pipeline(&mut self, ctx: &ActorContext<Self>) -> anyhow::Result<()> {
        self.statistics.num_spawn_attempts += 1;
        self.kill_switch = KillSwitch::default();

        info!(
            index_id=%self.params.pipeline_id.index_id,
            source_id=%self.params.pipeline_id.source_id,
            pipeline_ord=%self.params.pipeline_id.pipeline_ord,
            root_dir=%self.params.indexing_directory.path().display(),
            merge_policy=?self.params.merge_policy,
            "Spawning merging pipeline.",
        );
        let published_splits = self
            .params
            .metastore
            .list_splits(
                &self.params.pipeline_id.index_id,
                SplitState::Published,
                None,
                None,
            )
            .await?
            .into_iter()
            .map(|split| split.split_metadata)
            .collect::<Vec<_>>();
        self.params
            .split_store
            .remove_dangling_splits(&published_splits)
            .await?;

        let (merge_planner_mailbox, merge_planner_inbox) =
            create_mailbox::<MergePlanner>("MergePlanner".to_string(), QueueCapacity::Unbounded);

        // Merge publisher
        let merge_publisher = Publisher::new(
            PublisherType::MergePublisher,
            self.params.metastore.clone(),
            Some(merge_planner_mailbox.clone()),
            None,
        );
        let (merge_publisher_mailbox, merge_publisher_handler) = ctx
            .spawn_actor(merge_publisher)
            .set_kill_switch(self.kill_switch.clone())
            .spawn();

        let merge_sequencer = Sequencer::new(merge_publisher_mailbox);
        let (merge_sequencer_mailbox, merge_sequencer_handler) = ctx
            .spawn_actor(merge_sequencer)
            .set_kill_switch(self.kill_switch.clone())
            .spawn();

        // Merge uploader
        let merge_uploader = Uploader::new(
            "MergeUploader",
            self.params.metastore.clone(),
            self.params.split_store.clone(),
            merge_sequencer_mailbox,
        );
        let (merge_uploader_mailbox, merge_uploader_handler) = ctx
            .spawn_actor(merge_uploader)
            .set_kill_switch(self.kill_switch.clone())
            .spawn();

        // Merge Packager
        let tag_fields = self.params.doc_mapper.tag_named_fields()?;
        let merge_packager = Packager::new("MergePackager", tag_fields, merge_uploader_mailbox);
        let (merge_packager_mailbox, merge_packager_handler) = ctx
            .spawn_actor(merge_packager)
            .set_kill_switch(self.kill_switch.clone())
            .spawn();

        let merge_executor = MergeExecutor::new(
            self.params.pipeline_id.clone(),
            self.params.metastore.clone(),
            merge_packager_mailbox,
        );
        let (merge_executor_mailbox, merge_executor_handler) = ctx
            .spawn_actor(merge_executor)
            .set_kill_switch(self.kill_switch.clone())
            .spawn();

        let merge_split_downloader = MergeSplitDownloader {
            scratch_directory: self.params.indexing_directory.scratch_directory.clone(),
            storage: self.params.split_store.clone(),
            executor_mailbox: merge_executor_mailbox,
        };
        let (merge_split_downloader_mailbox, merge_split_downloader_handler) = ctx
            .spawn_actor(merge_split_downloader)
            .set_kill_switch(self.kill_switch.clone())
            .spawn();

        // Merge planner
        let merge_planner = MergePlanner::new(
            self.params.pipeline_id.clone(),
            published_splits,
            self.params.merge_policy.clone(),
            merge_split_downloader_mailbox,
        );
        let (merge_planner_mailbox, merge_planner_handler) = ctx
            .spawn_actor(merge_planner)
            .set_kill_switch(self.kill_switch.clone())
            .set_mailboxes(merge_planner_mailbox, merge_planner_inbox)
            .spawn();

        self.merge_planner_mailbox = Some(merge_planner_mailbox);
        self.previous_generations_statistics = self.statistics.clone();
        self.statistics.generation += 1;
        self.handles = Some(MergingPipelineHandle {
            merge_planner: merge_planner_handler,
            merge_split_downloader: merge_split_downloader_handler,
            merge_executor: merge_executor_handler,
            merge_packager: merge_packager_handler,
            merge_uploader: merge_uploader_handler,
            merge_sequencer: merge_sequencer_handler,
            merge_publisher: merge_publisher_handler,
        });
        Ok(())
    }

    async fn terminate(&mut self) {
        self.kill_switch.kill();
        if let Some(handlers) = self.handles.take() {
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
}

#[async_trait]
impl Handler<Observe> for MergingPipeline {
    type Reply = ();
    async fn handle(
        &mut self,
        _: Observe,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        if let Some(handles) = &self.handles {
            let (merge_uploader_counters, merge_publisher_counters) = join!(
                handles.merge_uploader.observe(),
                handles.merge_publisher.observe(),
            );
            self.statistics = self
                .previous_generations_statistics
                .clone()
                .add_actor_counters(&*merge_uploader_counters, &*merge_publisher_counters)
                .set_generation(self.statistics.generation)
                .set_num_spawn_attempts(self.statistics.num_spawn_attempts);
        }
        ctx.schedule_self_msg(Duration::from_secs(1), Observe).await;
        Ok(())
    }
}

#[async_trait]
impl Handler<Supervise> for MergingPipeline {
    type Reply = ();

    async fn handle(
        &mut self,
        _: Supervise,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        if self.handles.is_some() {
            match self.healthcheck() {
                Health::Healthy => {}
                Health::FailureOrUnhealthy => {
                    self.terminate().await;
                    ctx.schedule_self_msg(quickwit_actors::HEARTBEAT, Spawn { retry_count: 0 })
                        .await;
                }
                Health::Success => {
                    return Err(ActorExitStatus::Success);
                }
            }
        }
        ctx.schedule_self_msg(quickwit_actors::HEARTBEAT, Supervise)
            .await;
        Ok(())
    }
}

#[async_trait]
impl Handler<Spawn> for MergingPipeline {
    type Reply = ();

    async fn handle(
        &mut self,
        spawn: Spawn,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        if self.handles.is_some() {
            return Ok(());
        }
        self.previous_generations_statistics.num_spawn_attempts = 1 + spawn.retry_count;
        if let Err(spawn_error) = self.spawn_pipeline(ctx).await {
            if let Some(MetastoreError::IndexDoesNotExist { .. }) =
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

#[async_trait]
impl Handler<GetMergePlannerMailbox> for MergingPipeline {
    type Reply = Option<Mailbox<MergePlanner>>;

    async fn handle(
        &mut self,
        _: GetMergePlannerMailbox,
        _: &ActorContext<Self>,
    ) -> Result<Option<Mailbox<MergePlanner>>, ActorExitStatus> {
        Ok(self.merge_planner_mailbox.clone())
    }
}

pub struct MergingPipelineParams {
    pub pipeline_id: IndexingPipelineId,
    pub doc_mapper: Arc<dyn DocMapper>,
    pub indexing_directory: Arc<IndexingDirectory>,
    pub metastore: Arc<dyn Metastore>,
    pub split_store: Arc<IndexingSplitStore>,
    pub merge_policy: Arc<dyn MergePolicy>,
}

impl MergingPipelineParams {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pipeline_id: IndexingPipelineId,
        doc_mapper: Arc<dyn DocMapper>,
        indexing_directory: Arc<IndexingDirectory>,
        metastore: Arc<dyn Metastore>,
        split_store: Arc<IndexingSplitStore>,
        merge_policy: Arc<dyn MergePolicy>,
    ) -> Self {
        Self {
            pipeline_id,
            doc_mapper,
            indexing_directory,
            metastore,
            split_store,
            merge_policy,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use quickwit_actors::{ActorExitStatus, Universe};
    use quickwit_doc_mapper::default_doc_mapper_for_test;
    use quickwit_metastore::MockMetastore;
    use quickwit_storage::RamStorage;

    use crate::actors::merging_pipeline::{MergingPipeline, MergingPipelineParams};
    use crate::merge_policy::StableMultitenantWithTimestampMergePolicy;
    use crate::models::{IndexingDirectory, IndexingPipelineId};
    use crate::IndexingSplitStore;

    #[tokio::test]
    async fn test_merging_pipeline_simple() -> anyhow::Result<()> {
        let mut metastore = MockMetastore::default();
        metastore
            .expect_list_splits()
            .times(1)
            .returning(|_, _, _, _| Ok(Vec::new()));
        let universe = Universe::new();
        let pipeline_id = IndexingPipelineId {
            index_id: "test-index".to_string(),
            source_id: "test-source".to_string(),
            node_id: "test-node".to_string(),
            pipeline_ord: 0,
        };
        let storage = Arc::new(RamStorage::default());
        let split_store = Arc::new(IndexingSplitStore::create_with_no_local_store(
            storage.clone(),
        ));
        let pipeline_params = MergingPipelineParams {
            pipeline_id,
            doc_mapper: Arc::new(default_doc_mapper_for_test()),
            indexing_directory: Arc::new(IndexingDirectory::for_test().await?),
            metastore: Arc::new(metastore),
            split_store,
            merge_policy: Arc::new(StableMultitenantWithTimestampMergePolicy::default()),
        };
        let pipeline = MergingPipeline::new(pipeline_params);
        let (_pipeline_mailbox, pipeline_handler) = universe.spawn_actor(pipeline).spawn();
        tokio::time::sleep(Duration::from_secs(2)).await;
        let (pipeline_exit_status, pipeline_statistics) = pipeline_handler.quit().await;
        assert_eq!(pipeline_statistics.generation, 1);
        assert_eq!(pipeline_statistics.num_spawn_attempts, 1);
        assert_eq!(pipeline_statistics.num_published_splits, 0);
        assert!(matches!(pipeline_exit_status, ActorExitStatus::Quit));
        Ok(())
    }
}
