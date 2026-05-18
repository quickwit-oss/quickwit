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

//! Parquet merge executor actor.
//!
//! Calls the Phase 1 merge engine (`merge_sorted_parquet_files`) via
//! `run_cpu_intensive()`, builds output split metadata using
//! `merge_parquet_split_metadata()`, and sends the result to the uploader.

use anyhow::Context;
use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox, QueueCapacity};
use quickwit_common::thread_pool::run_cpu_intensive;
use quickwit_dst::check_invariant;
use quickwit_dst::invariants::InvariantId;
use quickwit_dst::invariants::merge_policy::{
    all_same_compaction_scope, all_same_merge_level, has_minimum_splits,
};
use quickwit_parquet_engine::merge::metadata_aggregation::merge_parquet_split_metadata;
use quickwit_parquet_engine::merge::{MergeConfig, MergeOutputFile, merge_sorted_parquet_files};
use quickwit_parquet_engine::storage::ParquetWriterConfig;
use quickwit_proto::types::IndexUid;
use tracing::{info, instrument, warn};

use super::ParquetUploader;
use super::parquet_indexer::ParquetSplitBatch;
use super::parquet_merge_messages::{ParquetMergeScratch, ParquetMergeTask};
use crate::models::PublishLock;

/// Executes Parquet merge operations using the Phase 1 k-way merge engine.
///
/// Receives `ParquetMergeScratch` from the downloader, runs the merge as a
/// CPU-intensive task, builds output metadata, and sends the result to the
/// uploader for staging and upload.
///
/// No separate Packager step is needed — the merge engine produces
/// ready-to-upload Parquet files with complete metadata.
pub struct ParquetMergeExecutor {
    uploader_mailbox: Mailbox<ParquetUploader>,
    writer_config: ParquetWriterConfig,
}

impl ParquetMergeExecutor {
    pub fn new(
        uploader_mailbox: Mailbox<ParquetUploader>,
        writer_config: ParquetWriterConfig,
    ) -> Self {
        Self {
            uploader_mailbox,
            writer_config,
        }
    }
}

#[async_trait]
impl Actor for ParquetMergeExecutor {
    type ObservableState = ();

    fn observable_state(&self) {}

    fn name(&self) -> String {
        "ParquetMergeExecutor".to_string()
    }

    fn queue_capacity(&self) -> QueueCapacity {
        QueueCapacity::Bounded(1)
    }
}

#[async_trait]
impl Handler<ParquetMergeScratch> for ParquetMergeExecutor {
    type Reply = ();

    #[instrument(name = "parquet_merge_executor", skip_all, fields(
        merge_split_id = %scratch.merge_operation.merge_split_id,
        num_inputs = scratch.merge_operation.splits.len(),
    ))]
    async fn handle(
        &mut self,
        scratch: ParquetMergeScratch,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        let merge_split_id = scratch.merge_operation.merge_split_id.to_string();
        let num_inputs = scratch.merge_operation.splits.len();

        info!(
            merge_split_id = %merge_split_id,
            num_inputs,
            total_bytes = scratch.merge_operation.total_size_bytes(),
            "executing parquet merge"
        );

        // Separate output subdirectory so the merge engine's temp files
        // don't collide with the downloaded inputs in scratch_directory.
        let output_dir = scratch.scratch_directory.path().join("merged_output");
        std::fs::create_dir_all(&output_dir)
            .context("failed to create merge output directory")
            .map_err(|e| ActorExitStatus::from(anyhow::anyhow!(e)))?;

        // Run the CPU-intensive merge on the dedicated thread pool.
        let input_paths = scratch.downloaded_parquet_files.clone();
        let output_dir_clone = output_dir.clone();
        let writer_config = self.writer_config.clone();
        let merge_result = run_cpu_intensive(move || {
            let config = MergeConfig {
                num_outputs: 1,
                writer_config,
            };
            merge_sorted_parquet_files(&input_paths, &output_dir_clone, &config)
        })
        .await;

        let outputs: Vec<MergeOutputFile> = match merge_result {
            Ok(Ok(outputs)) => outputs,
            Ok(Err(merge_err)) => {
                warn!(
                    error = %merge_err,
                    merge_split_id = %merge_split_id,
                    "parquet merge failed — input splits will not be retried until \
                     the pipeline restarts with metastore re-seeding"
                );
                // The input splits were drained from the planner by operations().
                // They remain published in the metastore and will be re-seeded
                // into the planner when the pipeline respawns (via
                // fetch_immature_splits on the ParquetMergePipeline supervisor).
                return Ok(());
            }
            Err(panicked) => {
                warn!(
                    error = %panicked,
                    merge_split_id = %merge_split_id,
                    "parquet merge panicked — input splits will not be retried until \
                     the pipeline restarts with metastore re-seeding"
                );
                return Ok(());
            }
        };

        let input_splits = &scratch.merge_operation.splits;
        let index_uid: IndexUid = input_splits[0]
            .index_uid
            .parse()
            .context("invalid index_uid in merge input")
            .map_err(|e| ActorExitStatus::from(anyhow::anyhow!(e)))?;

        let replaced_split_ids: Vec<String> = input_splits
            .iter()
            .map(|s| s.split_id.as_str().to_string())
            .collect();

        // Verify pre-merge invariants on the inputs the planner gave us.
        // These predicates are the same ones evaluated by the Stateright
        // model and the TLA+ spec — see `quickwit-dst/src/invariants/`.
        check_invariant!(
            InvariantId::MP2,
            has_minimum_splits(input_splits.len()),
            ": merge with {} splits, need >= 2",
            input_splits.len()
        );
        let merge_levels: Vec<u32> = input_splits.iter().map(|s| s.num_merge_ops).collect();
        check_invariant!(
            InvariantId::MP1,
            all_same_merge_level(&merge_levels),
            ": merge mixes levels {:?}",
            merge_levels
        );
        let sort_fields_refs: Vec<&str> = input_splits
            .iter()
            .map(|s| s.sort_fields.as_str())
            .collect();
        let scope_windows: Vec<(i64, i64)> = input_splits
            .iter()
            .map(|s| {
                s.window
                    .as_ref()
                    .map(|r| (r.start, r.end - r.start))
                    .unwrap_or((0, 0))
            })
            .collect();
        check_invariant!(
            InvariantId::MP3,
            all_same_compaction_scope(&sort_fields_refs, &scope_windows)
        );

        let total_input_rows: u64 = input_splits.iter().map(|s| s.num_rows).sum();

        // Empty output is valid (all input rows were empty). Still publish
        // the replacement so the empty input splits get marked for deletion
        // in the metastore — otherwise they stay Published forever since the
        // planner already drained them from its working set.
        if outputs.is_empty() {
            // MP-4: empty output is only correct when there were no input
            // rows to merge. Any other case means rows disappeared.
            check_invariant!(
                InvariantId::MP4,
                total_input_rows == 0,
                ": empty merge output but {} input rows",
                total_input_rows
            );
            info!(
                merge_split_id = %merge_split_id,
                num_replaced = replaced_split_ids.len(),
                "merge produced no output — publishing replacement to clean up empty inputs"
            );
            let batch = ParquetSplitBatch {
                index_uid,
                splits: Vec::new(),
                output_dir,
                checkpoint_delta_opt: None,
                publish_lock: PublishLock::default(),
                publish_token_opt: None,
                replaced_split_ids,
                _scratch_directory_opt: Some(scratch.scratch_directory),
                _merge_task_opt: Some(ParquetMergeTask {
                    merge_operation: scratch.merge_operation,
                    merge_permit: scratch.merge_permit,
                }),
            };
            ctx.send_message(&self.uploader_mailbox, batch).await?;
            return Ok(());
        }

        let mut merged_splits = Vec::with_capacity(outputs.len());
        for output in &outputs {
            let mut metadata = merge_parquet_split_metadata(input_splits, output)
                .context("failed to build merge output metadata")
                .map_err(|e| ActorExitStatus::from(anyhow::anyhow!(e)))?;

            // Use the split ID that was assigned when the merge operation was
            // planned, rather than the one generated inside
            // merge_parquet_split_metadata(). This keeps the ID consistent
            // across scheduling, tracing, and the final published split.
            metadata.split_id = scratch.merge_operation.merge_split_id.clone();
            metadata.parquet_file = metadata.split_id.to_string() + ".parquet";

            // Rename the output file to match the split ID.
            // The uploader expects files at `output_dir/{split_id}.parquet`.
            let expected_path = output_dir.join(&metadata.parquet_file);
            if output.path != expected_path {
                std::fs::rename(&output.path, &expected_path)
                    .with_context(|| {
                        format!(
                            "failed to rename merge output {} to {}",
                            output.path.display(),
                            expected_path.display()
                        )
                    })
                    .map_err(|e| ActorExitStatus::from(anyhow::anyhow!(e)))?;
            }

            info!(
                split_id = %metadata.split_id,
                num_rows = metadata.num_rows,
                size_bytes = metadata.size_bytes,
                "merge produced output split"
            );

            merged_splits.push(metadata);
        }

        // MP-4 (RowsConserved): sum of output rows equals sum of input rows.
        // This is the strong "no data loss, no duplication" check on the
        // merge engine's output. Validates the merge engine independently
        // of the planner.
        let total_output_rows: u64 = merged_splits.iter().map(|s| s.num_rows).sum();
        check_invariant!(
            InvariantId::MP4,
            total_input_rows == total_output_rows,
            ": input rows {} != output rows {}",
            total_input_rows,
            total_output_rows
        );

        // Send to uploader. Merges have no checkpoint delta, no publish lock,
        // and no publish token — they're just reorganizing existing data.
        // The scratch directory is passed along to keep it alive until the
        // uploader finishes reading the merged files.
        let batch = ParquetSplitBatch {
            index_uid,
            splits: merged_splits,
            output_dir,
            checkpoint_delta_opt: None,
            publish_lock: PublishLock::default(),
            publish_token_opt: None,
            replaced_split_ids,
            _scratch_directory_opt: Some(scratch.scratch_directory),
            _merge_task_opt: Some(ParquetMergeTask {
                merge_operation: scratch.merge_operation,
                merge_permit: scratch.merge_permit,
            }),
        };

        ctx.send_message(&self.uploader_mailbox, batch).await?;

        // The merge task is now carried by the batch — it will be held
        // through the uploader and released when the publisher drops the
        // ParquetSplitsUpdate message.
        info!(
            merge_split_id = %merge_split_id,
            "parquet merge complete, sent to uploader"
        );
        Ok(())
    }
}
