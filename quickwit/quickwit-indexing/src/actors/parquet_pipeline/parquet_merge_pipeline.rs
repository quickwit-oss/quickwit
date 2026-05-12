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

//! Parquet merge pipeline supervisor.
//!
//! Spawns and supervises the merge actor chain:
//!
//! ```text
//! ParquetMergePlanner → MergeSchedulerService → ParquetMergeSplitDownloader
//!       → ParquetMergeExecutor → ParquetUploader → Sequencer → Publisher
//!                                                                   │
//!                                                   (feedback to Planner)
//! ```
//!
//! Follows the same pattern as the Tantivy [`MergePipeline`] but without
//! `doc_mapper` or Packager (Parquet merges are schema-agnostic and produce
//! ready-to-upload files).

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use quickwit_actors::{
    Actor, ActorContext, ActorExitStatus, ActorHandle, HEARTBEAT, Handler, Health, Inbox, Mailbox,
    QueueCapacity, SpawnContext, Supervisable,
};
use quickwit_common::KillSwitch;
use quickwit_common::pubsub::EventBroker;
use quickwit_common::temp_dir::TempDirectory;
use quickwit_dst::events::merge_pipeline::{MergePipelineEvent, record_merge_pipeline_event};
use quickwit_metastore::{ListParquetSplitsRequestExt, ListParquetSplitsResponseExt};
use quickwit_parquet_engine::merge::policy::ParquetMergePolicy;
use quickwit_parquet_engine::split::ParquetSplitMetadata;
use quickwit_proto::metastore::{MetastoreService, MetastoreServiceClient};
use quickwit_proto::types::IndexUid;
use quickwit_storage::Storage;
use tokio::sync::Semaphore;
use tracing::{debug, error, info, instrument};

use super::parquet_merge_executor::ParquetMergeExecutor;
use super::parquet_merge_planner::{ParquetMergePlanner, RunFinalizeMergePolicyAndQuit};
use super::parquet_merge_split_downloader::ParquetMergeSplitDownloader;
use super::{METRICS_PUBLISHER_NAME, ParquetUploader};
use crate::actors::pipeline_shared::wait_duration_before_retry;
use crate::actors::publisher::DisconnectMergePlanner;
use crate::actors::{MergeSchedulerService, Publisher, Sequencer, UploaderType};
use crate::models::MergeStatistics;

/// Limits concurrent Parquet merge pipeline spawns to avoid overwhelming the
/// metastore. This is a separate semaphore from the Tantivy merge pipeline's.
static SPAWN_PIPELINE_SEMAPHORE: Semaphore = Semaphore::const_new(10);

pub const SUPERVISE_LOOP_INTERVAL: Duration = Duration::from_secs(1);

/// Holds actor handles for health-checking and lifecycle management.
/// When `None` in the parent pipeline, no actors are running (pre-spawn or
/// post-terminate). The supervisor checks these on each `SuperviseLoop` tick.
struct ParquetMergePipelineHandles {
    merge_planner: ActorHandle<ParquetMergePlanner>,
    merge_split_downloader: ActorHandle<ParquetMergeSplitDownloader>,
    merge_executor: ActorHandle<ParquetMergeExecutor>,
    merge_uploader: ActorHandle<ParquetUploader>,
    merge_publisher: ActorHandle<Publisher>,
    next_check_for_progress: Instant,
}

impl ParquetMergePipelineHandles {
    /// Rate-limits progress checks to once per HEARTBEAT interval.
    /// Without this, every supervision tick would check progress, which
    /// is wasteful — actors only need to demonstrate liveness periodically.
    fn should_check_for_progress(&mut self) -> bool {
        let now = Instant::now();
        let check_for_progress = now > self.next_check_for_progress;
        if check_for_progress {
            self.next_check_for_progress = now + *HEARTBEAT;
        }
        check_for_progress
    }
}

#[derive(Debug)]
struct SuperviseLoop;

#[derive(Clone, Copy, Debug, Default)]
struct Spawn {
    retry_count: usize,
}

/// Parquet merge pipeline supervisor.
///
/// Spawns and supervises the merge actor chain. On actor failure, the entire
/// pipeline is killed and respawned after a backoff delay. On graceful
/// shutdown, in-flight merges drain to completion.
pub struct ParquetMergePipeline {
    params: ParquetMergePipelineParams,
    /// The planner's mailbox and inbox are created once and recycled across
    /// pipeline restarts. This lets the publisher's feedback loop survive a
    /// respawn — messages sent to the old planner's mailbox are delivered to
    /// the new planner instance.
    merge_planner_mailbox: Mailbox<ParquetMergePlanner>,
    merge_planner_inbox: Inbox<ParquetMergePlanner>,
    previous_generations_statistics: MergeStatistics,
    statistics: MergeStatistics,
    handles_opt: Option<ParquetMergePipelineHandles>,
    /// Child kill switch — killing this kills all actors in the pipeline
    /// without affecting the supervisor itself.
    kill_switch: KillSwitch,
    /// Immature splits passed to the planner on first spawn. On subsequent
    /// spawns (after crash/respawn), the planner starts empty and picks up
    /// new splits from the feedback loop.
    initial_immature_splits_opt: Option<Vec<ParquetSplitMetadata>>,
    shutdown_initiated: bool,
}

#[async_trait]
impl Actor for ParquetMergePipeline {
    type ObservableState = MergeStatistics;

    fn observable_state(&self) -> Self::ObservableState {
        self.statistics.clone()
    }

    fn name(&self) -> String {
        "ParquetMergePipeline".to_string()
    }

    /// Kicks off the pipeline by sending Spawn (which spawns all actors)
    /// followed by SuperviseLoop (which starts periodic health checks).
    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        self.handle(Spawn::default(), ctx).await?;
        self.handle(SuperviseLoop, ctx).await?;
        Ok(())
    }
}

impl ParquetMergePipeline {
    pub fn new(
        params: ParquetMergePipelineParams,
        initial_immature_splits_opt: Option<Vec<ParquetSplitMetadata>>,
        spawn_ctx: &SpawnContext,
    ) -> Self {
        let (merge_planner_mailbox, merge_planner_inbox) = spawn_ctx
            .create_mailbox::<ParquetMergePlanner>(
                "ParquetMergePlanner",
                QueueCapacity::Bounded(1),
            );
        Self {
            params,
            previous_generations_statistics: MergeStatistics::default(),
            statistics: MergeStatistics::default(),
            handles_opt: None,
            kill_switch: KillSwitch::default(),
            merge_planner_inbox,
            merge_planner_mailbox,
            initial_immature_splits_opt,
            shutdown_initiated: false,
        }
    }

    pub fn merge_planner_mailbox(&self) -> &Mailbox<ParquetMergePlanner> {
        &self.merge_planner_mailbox
    }

    fn supervisables(&self) -> Vec<&dyn Supervisable> {
        let Some(handles) = &self.handles_opt else {
            return Vec::new();
        };
        vec![
            &handles.merge_planner,
            &handles.merge_split_downloader,
            &handles.merge_executor,
            &handles.merge_uploader,
            &handles.merge_publisher,
        ]
    }

    /// Consolidates health from all supervised actors into a single verdict.
    /// Any single actor failure makes the whole pipeline unhealthy (triggers
    /// terminate + respawn). All actors exiting with Success means the pipeline
    /// completed (e.g., after shutdown drain).
    fn healthcheck(&self, check_for_progress: bool) -> Health {
        let mut healthy_actors: Vec<&str> = Vec::new();
        let mut failure_or_unhealthy_actors: Vec<&str> = Vec::new();
        let mut success_actors: Vec<&str> = Vec::new();

        for supervisable in self.supervisables() {
            match supervisable.check_health(check_for_progress) {
                Health::Healthy => {
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
                generation = self.generation(),
                healthy_actors = ?healthy_actors,
                failed_or_unhealthy_actors = ?failure_or_unhealthy_actors,
                success_actors = ?success_actors,
                "parquet merge pipeline failed"
            );
            return Health::FailureOrUnhealthy;
        }
        if healthy_actors.is_empty() {
            info!(
                generation = self.generation(),
                "parquet merge pipeline completed successfully"
            );
            return Health::Success;
        }
        debug!(
            generation = self.generation(),
            healthy_actors = ?healthy_actors,
            success_actors = ?success_actors,
            "parquet merge pipeline is running and healthy"
        );
        Health::Healthy
    }

    fn generation(&self) -> usize {
        self.statistics.generation
    }

    #[instrument(name="spawn_parquet_merge_pipeline", level="info", skip_all, fields(generation=self.generation()))]
    async fn spawn_pipeline(&mut self, ctx: &ActorContext<Self>) -> anyhow::Result<()> {
        let _spawn_permit = ctx
            .protect_future(SPAWN_PIPELINE_SEMAPHORE.acquire())
            .await
            .expect("semaphore should not be closed");

        self.statistics.num_spawn_attempts += 1;
        self.kill_switch = ctx.kill_switch().child();

        info!(
            generation = self.generation(),
            root_dir = %self.params.indexing_directory.path().display(),
            "spawning parquet merge pipeline"
        );

        let immature_splits = self.fetch_immature_splits(ctx).await?;

        // Trace event: a fresh process invocation has just re-seeded the
        // planner. Includes the immature split IDs as the planner will see
        // them (before the policy filters mature/expired splits).
        record_merge_pipeline_event(&MergePipelineEvent::Restart {
            index_uid: self.params.index_uid.to_string(),
            re_seeded_immature_split_ids: immature_splits
                .iter()
                .map(|s| s.split_id.as_str().to_string())
                .collect(),
        });

        // Spawn actors bottom-up: each actor's constructor needs a mailbox
        // for the actor below it in the chain, so we start from the publisher
        // (bottom) and work up to the planner (top).

        // 1. Merge publisher — publishes merged splits to the metastore and feeds back
        //    ParquetNewSplits to the planner for further merging.
        let merge_publisher = Publisher::new(
            METRICS_PUBLISHER_NAME,
            QueueCapacity::Unbounded,
            self.params.metastore.clone(),
            None, // No Tantivy planner
            None, // No source
        )
        .set_parquet_merge_planner_mailbox(self.merge_planner_mailbox.clone());

        let (merge_publisher_mailbox, merge_publisher_handle) = ctx
            .spawn_actor()
            .set_kill_switch(self.kill_switch.clone())
            .spawn(merge_publisher);

        // 2. Sequencer — ensures merged splits are published in the order they were uploaded, even
        //    if uploads complete out of order.
        let sequencer = Sequencer::new(merge_publisher_mailbox.clone());
        let (sequencer_mailbox, _sequencer_handle) = ctx
            .spawn_actor()
            .set_kill_switch(self.kill_switch.clone())
            .spawn(sequencer);

        // 3. Merge uploader
        let merge_uploader = ParquetUploader::new(
            UploaderType::MergeUploader,
            self.params.metastore.clone(),
            self.params.storage.clone(),
            sequencer_mailbox,
            self.params.max_concurrent_split_uploads,
        );
        let (merge_uploader_mailbox, merge_uploader_handle) = ctx
            .spawn_actor()
            .set_kill_switch(self.kill_switch.clone())
            .spawn(merge_uploader);

        // 4. Merge executor
        let merge_executor =
            ParquetMergeExecutor::new(merge_uploader_mailbox, self.params.writer_config.clone());
        let (merge_executor_mailbox, merge_executor_handle) = ctx
            .spawn_actor()
            .set_kill_switch(self.kill_switch.clone())
            .spawn(merge_executor);

        // 5. Merge split downloader
        let merge_split_downloader = ParquetMergeSplitDownloader::new(
            self.params.indexing_directory.clone(),
            self.params.storage.clone(),
            merge_executor_mailbox,
        );
        let (merge_split_downloader_mailbox, merge_split_downloader_handle) = ctx
            .spawn_actor()
            .set_kill_switch(self.kill_switch.clone())
            .spawn(merge_split_downloader);

        // 6. Merge planner — uses recycled mailbox/inbox so the publisher's feedback loop (which
        //    holds a clone of the planner mailbox) survives pipeline restarts without needing to be
        //    re-wired.
        let merge_planner = ParquetMergePlanner::new(
            immature_splits,
            self.params.merge_policy.clone(),
            merge_split_downloader_mailbox,
            self.params.merge_scheduler_service.clone(),
        );
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
        self.handles_opt = Some(ParquetMergePipelineHandles {
            merge_planner: merge_planner_handle,
            merge_split_downloader: merge_split_downloader_handle,
            merge_executor: merge_executor_handle,
            merge_uploader: merge_uploader_handle,
            merge_publisher: merge_publisher_handle,
            next_check_for_progress: Instant::now() + *HEARTBEAT,
        });
        Ok(())
    }

    /// Kills all actors in the pipeline immediately. Used when the health check
    /// detects a failure — we tear everything down and schedule a respawn.
    async fn terminate(&mut self) {
        self.kill_switch.kill();
        if let Some(handles) = self.handles_opt.take() {
            tokio::join!(
                handles.merge_planner.kill(),
                handles.merge_split_downloader.kill(),
                handles.merge_executor.kill(),
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
        let check_for_progress = handles.should_check_for_progress();
        let health = self.healthcheck(check_for_progress);
        match health {
            Health::Healthy => {}
            Health::FailureOrUnhealthy => {
                self.terminate().await;
                ctx.schedule_self_msg(*HEARTBEAT, Spawn { retry_count: 0 });
            }
            Health::Success => {
                info!("parquet merge pipeline success, shutting down");
                return Err(ActorExitStatus::Success);
            }
        }
        Ok(())
    }

    /// Fetch published Parquet splits from the metastore for merge planning.
    ///
    /// On first spawn, uses the initial splits provided by the IndexingService
    /// (avoids per-pipeline metastore queries when many pipelines start).
    /// On subsequent spawns (after crash/respawn), queries the metastore
    /// directly to recover splits that were in-flight during the crash.
    ///
    /// The planner's `record_splits_if_necessary` filters out mature splits,
    /// so we don't need to filter here.
    async fn fetch_immature_splits(
        &mut self,
        ctx: &ActorContext<Self>,
    ) -> anyhow::Result<Vec<ParquetSplitMetadata>> {
        // On first spawn, use the initial splits provided by the IndexingService.
        if let Some(immature_splits) = self.initial_immature_splits_opt.take() {
            return Ok(immature_splits);
        }
        // On subsequent spawns, query the metastore for published splits.
        // Dispatch to the correct RPC based on whether this is a metrics or
        // sketches index — they use separate Postgres tables.
        let index_uid = self.params.index_uid.clone();
        let query = quickwit_metastore::ListParquetSplitsQuery::for_index(index_uid.clone());
        let is_sketch = quickwit_common::is_sketches_index(&index_uid.index_id);
        let records = if is_sketch {
            let list_request = quickwit_proto::metastore::ListSketchSplitsRequest::try_from_query(
                index_uid.clone(),
                &query,
            )?;
            let response = ctx
                .protect_future(self.params.metastore.list_sketch_splits(list_request))
                .await?;
            response.deserialize_splits()?
        } else {
            let list_request = quickwit_proto::metastore::ListMetricsSplitsRequest::try_from_query(
                index_uid.clone(),
                &query,
            )?;
            let response = ctx
                .protect_future(self.params.metastore.list_metrics_splits(list_request))
                .await?;
            response.deserialize_splits()?
        };
        let splits: Vec<ParquetSplitMetadata> = records.into_iter().map(|r| r.metadata).collect();
        info!(
            num_splits = splits.len(),
            "fetched published parquet splits for merge planning on respawn"
        );
        Ok(splits)
    }
}

#[async_trait]
impl Handler<SuperviseLoop> for ParquetMergePipeline {
    type Reply = ();

    async fn handle(
        &mut self,
        supervise_loop_token: SuperviseLoop,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        self.perform_observe().await;
        self.perform_health_check(ctx).await?;
        ctx.schedule_self_msg(SUPERVISE_LOOP_INTERVAL, supervise_loop_token);
        Ok(())
    }
}

/// Instructs the merge pipeline to finish pending merges and shut down.
///
/// Reuses the same message type as the Tantivy merge pipeline for
/// consistency in the IndexingService shutdown path.
pub use crate::actors::FinishPendingMergesAndShutdownPipeline;

#[async_trait]
impl Handler<FinishPendingMergesAndShutdownPipeline> for ParquetMergePipeline {
    type Reply = ();

    async fn handle(
        &mut self,
        _: FinishPendingMergesAndShutdownPipeline,
        _ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        info!("shutdown parquet merge pipeline initiated");
        // Prevent respawn on failure from this point forward.
        self.shutdown_initiated = true;
        if let Some(handles) = &self.handles_opt {
            // Two-phase graceful shutdown:
            //
            // Phase 1: Break the feedback loop so completed merges don't
            // trigger new merge planning. Without this, the pipeline would
            // never drain — each completed merge feeds back new splits that
            // trigger more merges.
            let _ = handles
                .merge_publisher
                .mailbox()
                .send_message(DisconnectMergePlanner)
                .await;
            record_merge_pipeline_event(&MergePipelineEvent::DisconnectMergePlanner {
                index_uid: self.params.index_uid.to_string(),
            });

            // Phase 2: Run the finalize policy (merges cold-window stragglers
            // with a lower merge factor), then the planner exits. Downstream
            // actors drain naturally as their inboxes empty.
            let _ = handles
                .merge_planner
                .mailbox()
                .send_message(RunFinalizeMergePolicyAndQuit)
                .await;
            record_merge_pipeline_event(&MergePipelineEvent::RunFinalizeAndQuit {
                index_uid: self.params.index_uid.to_string(),
                // The exact count is computed inside the planner; we record
                // 0 here as a sentinel and the planner will emit a more
                // precise count on completion if needed. For now this event
                // marks "finalize requested".
                finalize_merges_emitted: 0,
            });
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<Spawn> for ParquetMergePipeline {
    type Reply = ();

    async fn handle(
        &mut self,
        spawn: Spawn,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        // Don't respawn after graceful shutdown was requested.
        if self.shutdown_initiated {
            return Ok(());
        }
        // Don't spawn if actors are already running (duplicate Spawn message).
        if self.handles_opt.is_some() {
            return Ok(());
        }
        if let Err(spawn_error) = self.spawn_pipeline(ctx).await {
            let retry_delay = wait_duration_before_retry(spawn.retry_count);
            error!(
                error = ?spawn_error,
                retry_count = spawn.retry_count,
                retry_delay = ?retry_delay,
                "error while spawning parquet merge pipeline, retrying"
            );
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

/// Parameters for spawning a `ParquetMergePipeline`.
///
/// Constructed by the IndexingService from `IndexConfig` + node-level settings.
/// All actors in the pipeline share these resources via `Arc`/`Clone`.
#[derive(Clone)]
pub struct ParquetMergePipelineParams {
    /// Index UID for metastore queries when re-seeding on respawn.
    pub index_uid: IndexUid,
    /// Root temp directory for scratch files (downloads, merge output).
    pub indexing_directory: TempDirectory,
    /// Metastore client for staging/publishing merged splits and for
    /// re-seeding the planner with immature splits on respawn.
    pub metastore: MetastoreServiceClient,
    /// Object storage for downloading input splits and uploading merge output.
    pub storage: Arc<dyn Storage>,
    /// Determines which splits to merge and when. Read from the index's
    /// `parquet_merge_policy` config section.
    pub merge_policy: Arc<dyn ParquetMergePolicy>,
    /// Node-wide merge scheduler — shared with Tantivy merge pipelines for
    /// global concurrency control via a single semaphore.
    pub merge_scheduler_service: Mailbox<MergeSchedulerService>,
    pub max_concurrent_split_uploads: usize,
    pub event_broker: EventBroker,
    /// Parquet writer config for merge output (compression, page size, etc.).
    /// Should match the ingest pipeline's writer config so merged files have
    /// consistent compression.
    pub writer_config: quickwit_parquet_engine::storage::ParquetWriterConfig,
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use quickwit_actors::{ActorExitStatus, Universe};
    use quickwit_common::temp_dir::TempDirectory;
    use quickwit_parquet_engine::merge::policy::{
        ConstWriteAmplificationParquetMergePolicy, ParquetMergePolicyConfig,
    };
    use quickwit_parquet_engine::split::{ParquetSplitId, ParquetSplitMetadata, TimeRange};
    use quickwit_proto::metastore::{MetastoreServiceClient, MockMetastoreService};

    use super::*;

    fn make_pipeline_params(universe: &Universe) -> ParquetMergePipelineParams {
        let mut mock_metastore = MockMetastoreService::new();
        // Allow list_metrics_splits for respawn seeding (returns empty).
        mock_metastore.expect_list_metrics_splits().returning(|_| {
            Ok(quickwit_proto::metastore::ListMetricsSplitsResponse {
                splits_serialized_json: Vec::new(),
            })
        });
        let storage = Arc::new(quickwit_storage::RamStorage::default());
        let merge_policy = Arc::new(ConstWriteAmplificationParquetMergePolicy::new(
            ParquetMergePolicyConfig {
                merge_factor: 2,
                max_merge_factor: 2,
                max_merge_ops: 5,
                target_split_size_bytes: 256 * 1024 * 1024,
                maturation_period: Duration::from_secs(3600),
                max_finalize_merge_operations: 3,
            },
        ));
        ParquetMergePipelineParams {
            index_uid: quickwit_proto::types::IndexUid::for_test("test-merge-index", 0),
            indexing_directory: TempDirectory::for_test(),
            metastore: MetastoreServiceClient::from_mock(mock_metastore),
            storage,
            merge_policy,
            merge_scheduler_service: universe.get_or_spawn_one(),
            max_concurrent_split_uploads: 4,
            event_broker: EventBroker::default(),
            writer_config: quickwit_parquet_engine::storage::ParquetWriterConfig::default(),
        }
    }

    fn make_split(split_id: &str) -> ParquetSplitMetadata {
        ParquetSplitMetadata::metrics_builder()
            .split_id(ParquetSplitId::new(split_id))
            .index_uid("test-index:00000000000000000000000001")
            .partition_id(0)
            .time_range(TimeRange::new(1000, 2000))
            .num_rows(100)
            .size_bytes(1_000_000)
            .sort_fields("metric_name|host|timestamp_secs/V2")
            .window_start_secs(0)
            .window_duration_secs(3600)
            .build()
    }

    #[tokio::test]
    async fn test_pipeline_spawns_and_supervises() {
        let universe = Universe::with_accelerated_time();
        let params = make_pipeline_params(&universe);

        let pipeline = ParquetMergePipeline::new(params, None, universe.spawn_ctx());
        let (_pipeline_mailbox, pipeline_handle) = universe.spawn_builder().spawn(pipeline);

        // Give the pipeline time to initialize and spawn actors.
        universe.sleep(Duration::from_secs(2)).await;

        let observation = pipeline_handle.process_pending_and_observe().await;
        assert_eq!(
            observation.obs_type,
            quickwit_actors::ObservationType::Alive
        );

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_pipeline_shutdown_drain() {
        let universe = Universe::with_accelerated_time();
        let params = make_pipeline_params(&universe);

        let pipeline = ParquetMergePipeline::new(params, None, universe.spawn_ctx());
        let (pipeline_mailbox, pipeline_handle) = universe.spawn_builder().spawn(pipeline);

        // Let it initialize.
        universe.sleep(Duration::from_secs(2)).await;

        // Initiate shutdown.
        pipeline_mailbox
            .send_message(FinishPendingMergesAndShutdownPipeline)
            .await
            .unwrap();

        // The pipeline should eventually exit with Success.
        let (exit_status, _) = pipeline_handle.join().await;
        assert!(
            matches!(exit_status, ActorExitStatus::Success),
            "expected Success exit, got {:?}",
            exit_status
        );

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_pipeline_accepts_initial_splits() {
        let universe = Universe::with_accelerated_time();
        let params = make_pipeline_params(&universe);

        let initial_splits = Some(vec![make_split("s0"), make_split("s1")]);
        let pipeline = ParquetMergePipeline::new(params, initial_splits, universe.spawn_ctx());
        let planner_mailbox = pipeline.merge_planner_mailbox().clone();
        let (pipeline_mailbox, pipeline_handle) = universe.spawn_builder().spawn(pipeline);

        // Let it initialize.
        universe.sleep(Duration::from_secs(2)).await;

        // The planner mailbox should be accessible.
        assert!(!planner_mailbox.is_disconnected());

        // Gracefully shut down before asserting quit.
        pipeline_mailbox
            .send_message(FinishPendingMergesAndShutdownPipeline)
            .await
            .unwrap();
        let (exit_status, _) = pipeline_handle.join().await;
        assert!(matches!(exit_status, ActorExitStatus::Success));

        universe.assert_quit().await;
    }
}
