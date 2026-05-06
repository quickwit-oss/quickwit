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

use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use quickwit_actors::{
    Actor, ActorContext, ActorExitStatus, ActorHandle, Handler, Health, Mailbox, QueueCapacity,
    SpawnContext, Supervisable,
};
use quickwit_common::KillSwitch;
use quickwit_common::temp_dir::TempDirectory;
use quickwit_common::thread_pool::run_cpu_intensive;
use quickwit_indexing::actors::{
    ParquetSplitBatch, ParquetUploader, Publisher, Sequencer, UploaderType,
};
use quickwit_indexing::models::PublishLock;
use quickwit_parquet_engine::merge::metadata_aggregation::merge_parquet_split_metadata;
use quickwit_parquet_engine::merge::policy::ParquetMergeOperation;
use quickwit_parquet_engine::merge::{MergeConfig, MergeOutputFile, merge_sorted_parquet_files};
use quickwit_parquet_engine::storage::ParquetWriterConfig;
use quickwit_proto::compaction::CompactionTaskKind;
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_proto::types::{IndexUid, SourceId, SplitId};
use quickwit_storage::Storage;
use tracing::{debug, error, info};

use crate::compaction_pipeline::{PipelineStatus, PipelineStatusUpdate};

const PARQUET_COMPACTION_PUBLISHER_NAME: &str = "ParquetCompactionPublisher";

struct ParquetCompactionExecutor {
    scratch_directory: TempDirectory,
    storage: Arc<dyn Storage>,
    uploader_mailbox: Mailbox<ParquetUploader>,
    writer_config: ParquetWriterConfig,
}

impl ParquetCompactionExecutor {
    fn new(
        scratch_directory: TempDirectory,
        storage: Arc<dyn Storage>,
        uploader_mailbox: Mailbox<ParquetUploader>,
        writer_config: ParquetWriterConfig,
    ) -> Self {
        Self {
            scratch_directory,
            storage,
            uploader_mailbox,
            writer_config,
        }
    }
}

#[async_trait]
impl Actor for ParquetCompactionExecutor {
    type ObservableState = ();

    fn observable_state(&self) {}

    fn name(&self) -> String {
        "ParquetCompactionExecutor".to_string()
    }

    fn queue_capacity(&self) -> QueueCapacity {
        QueueCapacity::Bounded(1)
    }
}

#[async_trait]
impl Handler<ParquetMergeOperation> for ParquetCompactionExecutor {
    type Reply = ();

    async fn handle(
        &mut self,
        operation: ParquetMergeOperation,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        let merge_split_id = operation.merge_split_id.to_string();
        let num_inputs = operation.splits.len();
        info!(
            merge_split_id = %merge_split_id,
            num_inputs,
            total_bytes = operation.total_size_bytes(),
            "executing parquet compaction task"
        );

        let download_dir = self
            .scratch_directory
            .named_temp_child("parquet-merge-download-")
            .context("failed to create parquet merge download directory")
            .map_err(|error| ActorExitStatus::from(anyhow::anyhow!(error)))?;
        let mut downloaded_paths = Vec::with_capacity(num_inputs);
        for split in &operation.splits {
            if ctx.kill_switch().is_dead() {
                return Err(ActorExitStatus::Killed);
            }

            let parquet_filename = split.parquet_filename();
            let local_path = download_dir.path().join(&parquet_filename);
            let _protect_guard = ctx.protect_zone();
            self.storage
                .copy_to_file(Path::new(&parquet_filename), &local_path)
                .await
                .with_context(|| {
                    format!(
                        "failed to download parquet split {} from {}",
                        split.split_id, parquet_filename
                    )
                })
                .map_err(|error| ActorExitStatus::from(anyhow::anyhow!(error)))?;
            downloaded_paths.push(local_path);
            ctx.record_progress();
        }

        let output_dir = self.scratch_directory.path().join("merged_output");
        std::fs::create_dir_all(&output_dir)
            .context("failed to create parquet merge output directory")
            .map_err(|error| ActorExitStatus::from(anyhow::anyhow!(error)))?;

        let input_paths = downloaded_paths.clone();
        let output_dir_clone = output_dir.clone();
        let writer_config = self.writer_config.clone();
        let outputs: Vec<MergeOutputFile> = run_cpu_intensive(move || {
            let merge_config = MergeConfig {
                num_outputs: 1,
                writer_config,
            };
            merge_sorted_parquet_files(&input_paths, &output_dir_clone, &merge_config)
        })
        .await
        .context("parquet merge task panicked")
        .map_err(|error| ActorExitStatus::from(anyhow::anyhow!(error)))?
        .context("parquet merge task failed")
        .map_err(|error| ActorExitStatus::from(anyhow::anyhow!(error)))?;

        let input_splits = &operation.splits;
        let index_uid: IndexUid = input_splits[0]
            .index_uid
            .parse()
            .context("invalid index_uid in parquet merge input")
            .map_err(|error| ActorExitStatus::from(anyhow::anyhow!(error)))?;
        let replaced_split_ids: Vec<String> = input_splits
            .iter()
            .map(|split| split.split_id.as_str().to_string())
            .collect();

        let mut merged_splits = Vec::with_capacity(outputs.len());
        for output in &outputs {
            let mut metadata = merge_parquet_split_metadata(input_splits, output)
                .context("failed to build parquet merge output metadata")
                .map_err(|error| ActorExitStatus::from(anyhow::anyhow!(error)))?;
            metadata.split_id = operation.merge_split_id.clone();
            metadata.parquet_file = format!("{}.parquet", metadata.split_id);

            let expected_path = output_dir.join(&metadata.parquet_file);
            if output.path != expected_path {
                std::fs::rename(&output.path, &expected_path)
                    .with_context(|| {
                        format!(
                            "failed to rename parquet merge output {} to {}",
                            output.path.display(),
                            expected_path.display()
                        )
                    })
                    .map_err(|error| ActorExitStatus::from(anyhow::anyhow!(error)))?;
            }

            info!(
                split_id = %metadata.split_id,
                num_rows = metadata.num_rows,
                size_bytes = metadata.size_bytes,
                "parquet compaction produced output split"
            );
            merged_splits.push(metadata);
        }

        if merged_splits.is_empty() {
            debug!(
                merge_split_id = %merge_split_id,
                num_replaced = replaced_split_ids.len(),
                "parquet compaction produced no output splits"
            );
        }

        let batch = ParquetSplitBatch {
            index_uid,
            splits: merged_splits,
            output_dir,
            checkpoint_delta_opt: None,
            publish_lock: PublishLock::default(),
            publish_token_opt: None,
            replaced_split_ids,
            _scratch_directory_opt: Some(self.scratch_directory.clone()),
            _merge_task_opt: None,
        };
        ctx.send_message(&self.uploader_mailbox, batch).await?;
        drop(download_dir);
        Ok(())
    }
}

struct ParquetCompactionPipelineHandles {
    executor: ActorHandle<ParquetCompactionExecutor>,
    uploader: ActorHandle<ParquetUploader>,
    sequencer: ActorHandle<Sequencer<Publisher>>,
    publisher: ActorHandle<Publisher>,
}

pub(crate) struct ParquetCompactionPipeline {
    task_id: String,
    merge_operation: ParquetMergeOperation,
    index_uid: IndexUid,
    status: PipelineStatus,
    kill_switch: KillSwitch,
    scratch_directory: TempDirectory,
    storage: Arc<dyn Storage>,
    metastore: MetastoreServiceClient,
    max_concurrent_split_uploads: usize,
    writer_config: ParquetWriterConfig,
    handles: Option<ParquetCompactionPipelineHandles>,
}

impl ParquetCompactionPipeline {
    pub fn new(
        task_id: String,
        scratch_directory: TempDirectory,
        merge_operation: ParquetMergeOperation,
        index_uid: IndexUid,
        storage: Arc<dyn Storage>,
        metastore: MetastoreServiceClient,
        max_concurrent_split_uploads: usize,
        writer_config: ParquetWriterConfig,
    ) -> Self {
        Self {
            task_id,
            merge_operation,
            index_uid,
            status: PipelineStatus::InProgress,
            kill_switch: KillSwitch::default(),
            scratch_directory,
            storage,
            metastore,
            max_concurrent_split_uploads,
            writer_config,
            handles: None,
        }
    }

    pub fn status(&self) -> &PipelineStatus {
        &self.status
    }

    pub fn pipeline_status_update(&mut self) -> PipelineStatusUpdate {
        self.update_status();
        self.build_status_update()
    }

    fn supervisables(&self) -> Vec<&dyn Supervisable> {
        let Some(handles) = &self.handles else {
            return Vec::new();
        };
        vec![
            &handles.executor,
            &handles.uploader,
            &handles.sequencer,
            &handles.publisher,
        ]
    }

    fn update_status(&mut self) {
        if matches!(
            self.status,
            PipelineStatus::Completed | PipelineStatus::Failed { .. }
        ) {
            return;
        }
        if self.handles.is_none() {
            return;
        }

        let mut has_healthy = false;
        let mut failure_actor_names = Vec::new();
        for supervisable in self.supervisables() {
            match supervisable.check_health(true) {
                Health::Healthy => has_healthy = true,
                Health::FailureOrUnhealthy => {
                    failure_actor_names.push(supervisable.name().to_string());
                }
                Health::Success => {}
            }
        }

        if !failure_actor_names.is_empty() {
            let error_msg = format!("failed actors: {:?}", failure_actor_names);
            error!(task_id=%self.task_id, "{error_msg}");
            self.status = PipelineStatus::Failed { error: error_msg };
            return;
        }
        if !has_healthy {
            debug!(task_id=%self.task_id, "all parquet compaction actors completed");
            self.status = PipelineStatus::Completed;
        }
    }

    fn build_status_update(&self) -> PipelineStatusUpdate {
        PipelineStatusUpdate {
            task_id: self.task_id.clone(),
            task_kind: CompactionTaskKind::Parquet,
            index_uid: self.index_uid.clone(),
            source_id: SourceId::default(),
            split_ids: self
                .merge_operation
                .splits_as_slice()
                .iter()
                .map(|split| split.split_id.as_str().to_string())
                .collect::<Vec<SplitId>>(),
            merged_split_id: self.merge_operation.merge_split_id.to_string(),
            status: self.status.clone(),
        }
    }

    pub fn spawn_pipeline(&mut self, spawn_ctx: &SpawnContext) -> anyhow::Result<()> {
        info!(
            task_id = %self.task_id,
            index_uid = %self.index_uid,
            "spawning parquet compaction pipeline"
        );

        let publisher = Publisher::new(
            PARQUET_COMPACTION_PUBLISHER_NAME,
            QueueCapacity::Unbounded,
            self.metastore.clone(),
            None,
            None,
        );
        let (publisher_mailbox, publisher_handle) = spawn_ctx
            .spawn_builder()
            .set_kill_switch(self.kill_switch.child())
            .spawn(publisher);

        let sequencer = Sequencer::new(publisher_mailbox);
        let (sequencer_mailbox, sequencer_handle) = spawn_ctx
            .spawn_builder()
            .set_kill_switch(self.kill_switch.child())
            .spawn(sequencer);

        let uploader = ParquetUploader::new(
            UploaderType::MergeUploader,
            self.metastore.clone(),
            self.storage.clone(),
            sequencer_mailbox,
            self.max_concurrent_split_uploads,
        );
        let (uploader_mailbox, uploader_handle) = spawn_ctx
            .spawn_builder()
            .set_kill_switch(self.kill_switch.child())
            .spawn(uploader);

        let executor = ParquetCompactionExecutor::new(
            self.scratch_directory.clone(),
            self.storage.clone(),
            uploader_mailbox,
            self.writer_config.clone(),
        );
        let (executor_mailbox, executor_handle) = spawn_ctx
            .spawn_builder()
            .set_kill_switch(self.kill_switch.child())
            .spawn(executor);

        executor_mailbox
            .try_send_message(self.merge_operation.clone())
            .map_err(|error| {
                anyhow::anyhow!("failed to send parquet merge operation to executor: {error:?}")
            })?;

        self.handles = Some(ParquetCompactionPipelineHandles {
            executor: executor_handle,
            uploader: uploader_handle,
            sequencer: sequencer_handle,
            publisher: publisher_handle,
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::time::SystemTime;

    use quickwit_parquet_engine::split::{
        ParquetSplitId, ParquetSplitKind, ParquetSplitMetadata, TimeRange,
    };
    use quickwit_proto::metastore::{MetastoreServiceClient, MockMetastoreService};
    use quickwit_storage::RamStorage;

    use super::*;

    fn test_split(split_id: &str) -> ParquetSplitMetadata {
        ParquetSplitMetadata {
            kind: ParquetSplitKind::Metrics,
            split_id: ParquetSplitId::new(split_id),
            index_uid: "datadog-metrics:00000000000000000000000000".to_string(),
            partition_id: 0,
            time_range: TimeRange::new(1000, 2000),
            num_rows: 100,
            size_bytes: 1_000_000,
            metric_names: HashSet::new(),
            low_cardinality_tags: Default::default(),
            high_cardinality_tag_keys: Default::default(),
            created_at: SystemTime::now(),
            parquet_file: format!("{split_id}.parquet"),
            window: Some(0..3600),
            sort_fields: "metric_name|host|timestamp_secs/V2".to_string(),
            num_merge_ops: 0,
            row_keys_proto: None,
            zonemap_regexes: Default::default(),
        }
    }

    #[test]
    fn test_status_update_unspawned_pipeline() {
        let index_uid = IndexUid::for_test("datadog-metrics", 0);
        let operation = ParquetMergeOperation::new(vec![test_split("s1"), test_split("s2")]);
        let mut pipeline = ParquetCompactionPipeline::new(
            "task-1".to_string(),
            TempDirectory::for_test(),
            operation,
            index_uid.clone(),
            Arc::new(RamStorage::default()),
            MetastoreServiceClient::from_mock(MockMetastoreService::new()),
            2,
            ParquetWriterConfig::default(),
        );

        let update = pipeline.pipeline_status_update();

        assert_eq!(update.task_kind, CompactionTaskKind::Parquet);
        assert_eq!(update.index_uid, index_uid);
        assert_eq!(update.split_ids, vec!["s1".to_string(), "s2".to_string()]);
        assert_eq!(update.status, PipelineStatus::InProgress);
    }
}
