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

//! MetricsPipeline actor — supervises the Parquet/DataFusion indexing pipeline.
//!
//! This is the metrics counterpart of `IndexingPipeline`. It spawns and
//! supervises the chain:
//!
//! ```text
//! Source → ParquetDocProcessor → ParquetIndexer → ParquetPackager → ParquetUploader → Publisher
//! ```

use std::collections::BTreeSet;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::Ordering;
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
use quickwit_config::{IndexingSettings, SourceConfig};
use quickwit_ingest::IngesterPool;
use quickwit_proto::indexing::IndexingPipelineId;
use quickwit_proto::metastore::{MetastoreError, MetastoreServiceClient};
use quickwit_proto::types::ShardId;
use quickwit_storage::{Storage, StorageResolver};
use tracing::{debug, error, info, instrument};

use super::{ParquetDocProcessor, ParquetIndexer, ParquetPackager, ParquetUploader};
use crate::actors::pipeline_shared::{
    SPAWN_PIPELINE_SEMAPHORE, SUPERVISE_INTERVAL, Spawn, SuperviseLoop, wait_duration_before_retry,
};
use crate::actors::sequencer::Sequencer;
use crate::actors::{Publisher, UploaderType};
use crate::models::IndexingStatistics;
use crate::source::{
    AssignShards, Assignment, SourceActor, SourceRuntime, quickwit_supported_sources,
};

struct MetricsPipelineHandles {
    source_mailbox: Mailbox<SourceActor>,
    source_handle: ActorHandle<SourceActor>,
    doc_processor: ActorHandle<ParquetDocProcessor>,
    indexer: ActorHandle<ParquetIndexer>,
    packager: ActorHandle<ParquetPackager>,
    uploader: ActorHandle<ParquetUploader>,
    sequencer: ActorHandle<Sequencer<Publisher>>,
    publisher: ActorHandle<Publisher>,
    next_check_for_progress: Instant,
}

impl MetricsPipelineHandles {
    fn should_check_for_progress(&mut self) -> bool {
        let now = Instant::now();
        let check_for_progress = now > self.next_check_for_progress;
        if check_for_progress {
            self.next_check_for_progress = now + *HEARTBEAT;
        }
        check_for_progress
    }
}

pub struct MetricsPipelineParams {
    pub pipeline_id: IndexingPipelineId,
    pub metastore: MetastoreServiceClient,
    pub storage: Arc<dyn Storage>,
    pub indexing_directory: TempDirectory,
    pub indexing_settings: IndexingSettings,
    pub max_concurrent_split_uploads: usize,
    pub source_config: SourceConfig,
    pub source_storage_resolver: StorageResolver,
    pub ingester_pool: IngesterPool,
    pub queues_dir_path: std::path::PathBuf,
    pub params_fingerprint: u64,
    pub event_broker: EventBroker,
    pub use_sketch_processors: bool,
    /// Routing expression for partitioning incoming data.
    pub partition_key: quickwit_doc_mapper::RoutingExpr,
    /// Maximum number of index partitions allowed in a workbench.
    pub max_num_partitions: NonZeroU32,
    /// Parquet merge planner mailbox for the publisher feedback loop.
    /// When set, the publisher sends ParquetNewSplits to the planner
    /// after publishing ingest splits so they can be considered for merging.
    /// `None` when no merge pipeline is running for this index.
    pub parquet_merge_planner_mailbox_opt:
        Option<quickwit_actors::Mailbox<super::ParquetMergePlanner>>,
}

pub struct MetricsPipeline {
    params: MetricsPipelineParams,
    previous_generations_statistics: IndexingStatistics,
    statistics: IndexingStatistics,
    handles_opt: Option<MetricsPipelineHandles>,
    kill_switch: KillSwitch,
    shard_ids: BTreeSet<ShardId>,
    _indexing_pipelines_gauge_guard: OwnedGaugeGuard,
}

#[async_trait]
impl Actor for MetricsPipeline {
    type ObservableState = IndexingStatistics;

    fn observable_state(&self) -> Self::ObservableState {
        self.statistics.clone()
    }

    fn name(&self) -> String {
        "MetricsPipeline".to_string()
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
        self.perform_observe(ctx);
        Ok(())
    }
}

impl MetricsPipeline {
    pub fn new(params: MetricsPipelineParams) -> Self {
        let indexing_pipelines_gauge = crate::metrics::INDEXER_METRICS
            .indexing_pipelines
            .with_label_values([&params.pipeline_id.index_uid.index_id]);
        let indexing_pipelines_gauge_guard = OwnedGaugeGuard::from_gauge(indexing_pipelines_gauge);
        let params_fingerprint = params.params_fingerprint;
        MetricsPipeline {
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
        match &self.handles_opt {
            Some(handles) => {
                vec![
                    &handles.source_handle,
                    &handles.doc_processor,
                    &handles.indexer,
                    &handles.packager,
                    &handles.uploader,
                    &handles.sequencer,
                    &handles.publisher,
                ]
            }
            None => Vec::new(),
        }
    }

    fn healthcheck(&self, check_for_progress: bool) -> Health {
        let mut healthy_actors: Vec<&str> = Default::default();
        let mut failure_or_unhealthy_actors: Vec<&str> = Default::default();
        let mut success_actors: Vec<&str> = Default::default();
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
                pipeline_id=?self.params.pipeline_id,
                generation=self.generation(),
                healthy_actors=?healthy_actors,
                failed_or_unhealthy_actors=?failure_or_unhealthy_actors,
                success_actors=?success_actors,
                "metrics pipeline failure"
            );
            return Health::FailureOrUnhealthy;
        }
        if healthy_actors.is_empty() {
            info!(
                pipeline_id=?self.params.pipeline_id,
                generation=self.generation(),
                "metrics pipeline success"
            );
            return Health::Success;
        }
        debug!(
            pipeline_id=?self.params.pipeline_id,
            generation=self.generation(),
            healthy_actors=?healthy_actors,
            failed_or_unhealthy_actors=?failure_or_unhealthy_actors,
            success_actors=?success_actors,
            "metrics pipeline running"
        );
        Health::Healthy
    }

    fn generation(&self) -> usize {
        self.statistics.generation
    }

    fn perform_observe(&mut self, ctx: &ActorContext<Self>) {
        if let Some(handles) = &self.handles_opt {
            handles.doc_processor.refresh_observe();
            handles.indexer.refresh_observe();
            handles.uploader.refresh_observe();
            handles.publisher.refresh_observe();

            let doc_counters = handles.doc_processor.last_observation();
            let indexer_counters = handles.indexer.last_observation();
            let uploader_counters = handles.uploader.last_observation();
            let publisher_counters = handles.publisher.last_observation();

            let mut stats = self.previous_generations_statistics.clone();
            stats.num_docs += doc_counters.valid_rows;
            stats.num_invalid_docs += doc_counters.num_errors();
            stats.total_bytes_processed += doc_counters.bytes_total;
            stats.num_local_splits += indexer_counters.batches_flushed;
            stats.num_staged_splits += uploader_counters.num_staged_splits.load(Ordering::Relaxed);
            stats.num_uploaded_splits += uploader_counters
                .num_uploaded_splits
                .load(Ordering::Relaxed);
            stats.num_published_splits += publisher_counters.num_published_splits;
            stats.num_empty_splits += publisher_counters.num_empty_splits;
            stats.generation = self.statistics.generation;
            stats.num_spawn_attempts = self.statistics.num_spawn_attempts;
            self.statistics = stats;
        }
        self.statistics.params_fingerprint = self.params.params_fingerprint;
        self.statistics.shard_ids.clone_from(&self.shard_ids);
        ctx.observe(self);
    }

    async fn perform_health_check(
        &mut self,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        let check_for_progress = match &mut self.handles_opt {
            Some(handles) => handles.should_check_for_progress(),
            None => return Ok(()),
        };
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

    #[instrument(
        name="spawn_metrics_pipeline",
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
            "spawning parquet indexing pipeline for metrics",
        );

        let (source_mailbox, source_inbox) = ctx
            .spawn_ctx()
            .create_mailbox::<SourceActor>("SourceActor", QueueCapacity::Unbounded);

        // Publisher — optionally wired to the Parquet merge planner for
        // merge feedback. When set, the publisher sends ParquetNewSplits
        // after publishing ingest splits so they can be considered for merging.
        let mut publisher = Publisher::new(
            super::METRICS_PUBLISHER_NAME,
            QueueCapacity::Bounded(1),
            self.params.metastore.clone(),
            None,
            Some(source_mailbox.clone()),
        );
        if let Some(planner_mailbox) = &self.params.parquet_merge_planner_mailbox_opt {
            publisher = publisher.set_parquet_merge_planner_mailbox(planner_mailbox.clone());
        }
        let (publisher_mailbox, publisher_handle) = ctx
            .spawn_actor()
            .set_kill_switch(self.kill_switch.clone())
            .spawn(publisher);

        // Sequencer
        let sequencer = Sequencer::new(publisher_mailbox);
        let (sequencer_mailbox, sequencer_handle) = ctx
            .spawn_actor()
            .set_kill_switch(self.kill_switch.clone())
            .spawn(sequencer);

        // ParquetUploader
        let uploader = ParquetUploader::new(
            UploaderType::IndexUploader,
            self.params.metastore.clone(),
            self.params.storage.clone(),
            sequencer_mailbox,
            self.params.max_concurrent_split_uploads,
        );
        let (uploader_mailbox, uploader_handle) = ctx
            .spawn_actor()
            .set_kill_switch(self.kill_switch.clone())
            .spawn(uploader);

        // ParquetPackager — read sort schema and window duration from index config.
        let writer_config = quickwit_parquet_engine::storage::ParquetWriterConfig::default();
        let parquet_indexing_config = self.params.indexing_settings.parquet_indexing();
        let mut table_config = quickwit_parquet_engine::table_config::TableConfig::default();
        if let Some(ref sort_fields) = parquet_indexing_config.sort_fields {
            table_config.sort_fields = Some(sort_fields.clone());
        }
        table_config.window_duration_secs = parquet_indexing_config.window_duration_secs;
        let split_kind = if self.params.use_sketch_processors {
            quickwit_parquet_engine::split::ParquetSplitKind::Sketches
        } else {
            quickwit_parquet_engine::split::ParquetSplitKind::Metrics
        };
        let split_writer = quickwit_parquet_engine::storage::ParquetSplitWriter::new(
            split_kind,
            writer_config,
            self.params.indexing_directory.path(),
            &table_config,
        )?;
        let packager = ParquetPackager::new(split_writer, uploader_mailbox);
        let (packager_mailbox, packager_handle) = ctx
            .spawn_actor()
            .set_kill_switch(self.kill_switch.clone())
            .spawn(packager);

        // ParquetIndexer
        let commit_timeout =
            Duration::from_secs(self.params.indexing_settings.commit_timeout_secs as u64);
        let indexer = ParquetIndexer::new_with_partition_key_and_max_num_partitions(
            self.params.pipeline_id.index_uid.clone(),
            source_id.to_string(),
            None,
            packager_mailbox,
            self.params.partition_key.clone(),
            self.params.max_num_partitions,
            Some(commit_timeout),
        );
        let (indexer_mailbox, indexer_handle) = ctx
            .spawn_actor()
            .set_kill_switch(self.kill_switch.clone())
            .spawn(indexer);

        // ParquetDocProcessor
        let processor = if self.params.use_sketch_processors {
            super::parquet_doc_processor::IngestProcessor::Sketches(
                quickwit_parquet_engine::ingest::SketchParquetIngestProcessor::new(),
            )
        } else {
            super::parquet_doc_processor::IngestProcessor::Metrics(
                quickwit_parquet_engine::ingest::ParquetIngestProcessor,
            )
        };
        let doc_processor = ParquetDocProcessor::new(
            processor,
            index_id.to_string(),
            source_id.to_string(),
            indexer_mailbox,
        );
        let (doc_processor_mailbox, doc_processor_handle) = ctx
            .spawn_actor()
            .set_kill_switch(self.kill_switch.clone())
            .spawn(doc_processor);

        // Source
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
        let actor_source = SourceActor::new(source, doc_processor_mailbox);
        let (source_mailbox, source_handle) = ctx
            .spawn_actor()
            .set_mailboxes(source_mailbox, source_inbox)
            .set_kill_switch(self.kill_switch.clone())
            .spawn(actor_source);
        let assign_shards_message = AssignShards(Assignment {
            shard_ids: self.shard_ids.clone(),
        });
        source_mailbox.send_message(assign_shards_message).await?;

        self.previous_generations_statistics = self.statistics.clone();
        self.statistics.generation += 1;
        self.handles_opt = Some(MetricsPipelineHandles {
            source_mailbox,
            source_handle,
            doc_processor: doc_processor_handle,
            indexer: indexer_handle,
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
impl Handler<SuperviseLoop> for MetricsPipeline {
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
impl Handler<Spawn> for MetricsPipeline {
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
                info!(error = ?spawn_error, "could not spawn metrics pipeline, index might have been deleted");
                return Err(ActorExitStatus::Success);
            }
            let retry_delay = wait_duration_before_retry(spawn.retry_count + 1);
            error!(error = ?spawn_error, retry_count = spawn.retry_count, retry_delay = ?retry_delay, "error while spawning metrics pipeline, retrying after some time");
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
impl Handler<AssignShards> for MetricsPipeline {
    type Reply = ();

    async fn handle(
        &mut self,
        assign_shards_message: AssignShards,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        self.shard_ids
            .clone_from(&assign_shards_message.0.shard_ids);
        if let Some(handles) = &self.handles_opt {
            info!(
                shard_ids=?assign_shards_message.0.shard_ids,
                "assigning shards to metrics pipeline"
            );
            handles
                .source_mailbox
                .send_message(assign_shards_message)
                .await?;
        }
        self.perform_observe(ctx);
        Ok(())
    }
}
