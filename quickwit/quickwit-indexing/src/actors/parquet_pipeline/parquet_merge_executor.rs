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
//! Receives a `ParquetMergeScratch` from the downloader. Two engines are available:
//!
//! - **Streaming engine** (`execute_merge_operation`): column-major, page-bounded body cache. Used
//!   unconditionally for promotion merges (the in-memory path can't handle mixed prefix lengths).
//!   Optionally used for regular merges when the node-level
//!   `IndexerConfig::parquet_merge_use_streaming_engine` flag is true.
//! - **In-memory engine** (`merge_sorted_parquet_files`): buffers all inputs in memory and runs
//!   inside `run_cpu_intensive`. Kept as the runtime fallback so production can flip back via YAML
//!   config if the streaming engine hits a bug. To be removed once the streaming path has soaked.
//!
//! Routing in `handle()`:
//!
//! - `target_prefix_len_override.is_some()` → streaming engine. Promotion is the whole point of
//!   `target_prefix_len_override`, and the in-memory path's `extract_and_validate_input_metadata`
//!   would bail on the mixed `rg_partition_prefix_len` before any output is produced.
//! - Else `use_streaming_engine == true` → streaming engine (the new default once soaked).
//! - Else → in-memory engine (the runtime fallback).
//!
//! `mixed_prefix_ok` is passed to `merge_parquet_split_metadata` only for promotion merges so
//! the post-merge aggregator's strict input-side equality check stays on for ordinary
//! same-prefix merges.

use std::io;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use bytes::Bytes;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox, QueueCapacity};
use quickwit_common::thread_pool::run_cpu_intensive;
use quickwit_dst::check_invariant;
use quickwit_dst::invariants::InvariantId;
use quickwit_dst::invariants::merge_policy::{
    all_same_compaction_scope, all_same_merge_level, has_minimum_splits,
};
use quickwit_parquet_engine::merge::metadata_aggregation::merge_parquet_split_metadata;
use quickwit_parquet_engine::merge::{
    MergeConfig, MergeOutputFile, execute_merge_operation, merge_sorted_parquet_files,
};
use quickwit_parquet_engine::storage::{ParquetWriterConfig, RemoteByteSource};
use quickwit_proto::types::IndexUid;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt};
use tracing::{info, instrument, warn};

use super::ParquetUploader;
use super::parquet_indexer::ParquetSplitBatch;
use super::parquet_merge_messages::{ParquetMergeScratch, ParquetMergeTask};
use crate::models::PublishLock;

/// `RemoteByteSource` adapter over a single local file. Used by the
/// promotion-merge path to feed downloaded scratch-directory files
/// into `execute_merge_operation` (which composes them with
/// `LegacyInputAdapter` as needed).
///
/// Each instance is bound to one absolute path at construction time
/// and ignores the `path` argument from the trait methods — the
/// trait surface assumes a remote backend keyed by path, but the
/// downloader has already resolved each split to a concrete local
/// file before the executor runs.
struct LocalFileByteSource {
    path: PathBuf,
}

impl LocalFileByteSource {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

#[async_trait]
impl RemoteByteSource for LocalFileByteSource {
    async fn file_size(&self, _path: &Path) -> io::Result<u64> {
        tokio::fs::metadata(&self.path).await.map(|m| m.len())
    }

    async fn get_slice(&self, _path: &Path, range: Range<u64>) -> io::Result<Bytes> {
        let mut file = tokio::fs::File::open(&self.path).await?;
        file.seek(io::SeekFrom::Start(range.start)).await?;
        let len = (range.end - range.start) as usize;
        let mut buf = vec![0u8; len];
        file.read_exact(&mut buf).await?;
        Ok(Bytes::from(buf))
    }

    async fn get_slice_stream(
        &self,
        _path: &Path,
        range: Range<u64>,
    ) -> io::Result<Box<dyn AsyncRead + Send + Unpin>> {
        let mut file = tokio::fs::File::open(&self.path).await?;
        file.seek(io::SeekFrom::Start(range.start)).await?;
        let len = range.end - range.start;
        Ok(Box::new(file.take(len)))
    }
}

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
    /// When true, regular merges run through the streaming engine. When
    /// false, they run through the in-memory `merge_sorted_parquet_files`
    /// fallback. Promotion merges always use the streaming engine
    /// regardless of this flag.
    use_streaming_engine: bool,
    /// Target output split size, sourced from the merge policy. Drives
    /// `num_outputs`: the executor asks the merge engine for
    /// `ceil(total_input_bytes / target_split_size_bytes)` outputs so a
    /// merge that ingests more than one target's worth of data spreads
    /// across multiple output files. The engine clamps the request to
    /// the number of `sorted_series` boundaries actually available, so
    /// the result is an upper bound rather than an exact count.
    /// Operations whose inputs already fit in one target naturally get
    /// `num_outputs = 1`.
    target_split_size_bytes: u64,
}

impl ParquetMergeExecutor {
    pub fn new(
        uploader_mailbox: Mailbox<ParquetUploader>,
        writer_config: ParquetWriterConfig,
        use_streaming_engine: bool,
        target_split_size_bytes: u64,
    ) -> Self {
        Self {
            uploader_mailbox,
            writer_config,
            use_streaming_engine,
            target_split_size_bytes,
        }
    }

    /// Compute the requested `num_outputs` for a merge. Returns at
    /// least 1 (the merge always produces at least one output unless
    /// every input is empty). Guards against the
    /// `target_split_size_bytes = 0` misconfiguration — falling back to
    /// 1 rather than dividing by zero.
    fn compute_num_outputs(&self, total_input_bytes: u64) -> usize {
        if self.target_split_size_bytes == 0 {
            return 1;
        }
        // Ceiling division without overflow: `(a + b - 1) / b` would
        // overflow at u64::MAX; use the explicit add-one-after-divide
        // form when `a` is non-zero.
        let quot = total_input_bytes / self.target_split_size_bytes;
        let extra = if total_input_bytes.is_multiple_of(self.target_split_size_bytes) {
            0
        } else {
            1
        };
        ((quot + extra) as usize).max(1)
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

        // Promotion merges (`target_prefix_len_override.is_some()`) must
        // use the streaming engine; the in-memory path's
        // `extract_and_validate_input_metadata` would bail on mixed
        // `rg_partition_prefix_len` before producing any output. Regular
        // merges follow the operator-controlled `use_streaming_engine`
        // flag: true means the streaming engine, false means the
        // in-memory fallback. Keeping the in-memory branch lets
        // production flip back at runtime if the streaming engine hits a
        // bug; once the streaming path has soaked, the in-memory branch
        // and `merge_sorted_parquet_files` itself can be removed.
        let total_input_bytes = scratch.merge_operation.total_size_bytes();
        let num_outputs = self.compute_num_outputs(total_input_bytes);
        info!(
            merge_split_id = %merge_split_id,
            total_input_bytes,
            target_split_size_bytes = self.target_split_size_bytes,
            num_outputs,
            "computed num_outputs from total input bytes / target split size"
        );

        let is_promotion = scratch.merge_operation.target_prefix_len_override.is_some();
        let merge_result: Result<Result<Vec<MergeOutputFile>, _>, _> = if is_promotion
            || self.use_streaming_engine
        {
            let sources: Vec<Arc<dyn RemoteByteSource>> = scratch
                .downloaded_parquet_files
                .iter()
                .map(|path| Arc::new(LocalFileByteSource::new(path.clone())) as _)
                .collect();
            let config = MergeConfig {
                num_outputs,
                writer_config: self.writer_config.clone(),
            };
            Ok(
                execute_merge_operation(&scratch.merge_operation, sources, &output_dir, &config)
                    .await,
            )
        } else {
            // Fallback: in-memory engine under `run_cpu_intensive`.
            // Kept as the runtime rollback target while the streaming
            // engine soaks in production.
            let input_paths = scratch.downloaded_parquet_files.clone();
            let output_dir_clone = output_dir.clone();
            let writer_config = self.writer_config.clone();
            run_cpu_intensive(move || {
                let config = MergeConfig {
                    num_outputs,
                    writer_config,
                };
                merge_sorted_parquet_files(&input_paths, &output_dir_clone, &config)
            })
            .await
        };

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

        // `mixed_prefix_ok` matches the operation's promotion mode:
        // promote-legacy operations bundle inputs from different
        // `rg_partition_prefix_len` buckets (the adapter normalizes
        // them at read time), so the input-side equality check in
        // `merge_parquet_split_metadata` would spuriously fail. Regular
        // merges keep the strict check.
        let mixed_prefix_ok = scratch.merge_operation.target_prefix_len_override.is_some();

        let mut merged_splits = Vec::with_capacity(outputs.len());
        // First output keeps the planner-assigned `merge_split_id` so
        // existing observability paths (logs, metrics, traces keyed on
        // this ID) continue to see the planned ID at execute time. For
        // n>1 the subsequent outputs get fresh IDs generated by
        // `merge_parquet_split_metadata`. Assigning the same ID to
        // multiple outputs would collide on the rename below and
        // overwrite earlier files.
        for (output_idx, output) in outputs.iter().enumerate() {
            let mut metadata = merge_parquet_split_metadata(input_splits, output, mixed_prefix_ok)
                .context("failed to build merge output metadata")
                .map_err(|e| ActorExitStatus::from(anyhow::anyhow!(e)))?;

            if output_idx == 0 {
                metadata.split_id = scratch.merge_operation.merge_split_id.clone();
            }
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
