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

//! ParquetIndexer actor that accumulates RecordBatches for the metrics pipeline.
//!
//! This actor replaces the Tantivy-based Indexer for metrics workloads, accumulating
//! Arrow RecordBatches and forwarding concatenated batches to ParquetPackager for
//! Parquet encoding and file writing.

use std::collections::HashMap;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::time::Duration;

use arrow::array::UInt32Array;
use arrow::compute::take_record_batch;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox, QueueCapacity};
use quickwit_common::runtimes::RuntimeType;
use quickwit_doc_mapper::{ArrowRowContext, RoutingExpr};
use quickwit_metastore::checkpoint::{IndexCheckpointDelta, SourceCheckpointDelta};
use quickwit_parquet_engine::index::{ParquetBatchAccumulator, ParquetIndexingConfig};
use quickwit_parquet_engine::split::ParquetSplitMetadata;
use quickwit_proto::types::{IndexUid, PublishToken, SourceId};
use serde::Serialize;
use tokio::runtime::Handle;
use tracing::{debug, info, info_span, warn};
use ulid::Ulid;

use super::ProcessedParquetBatch;
use super::parquet_merge_messages::ParquetMergeTask;
use super::parquet_packager::{ParquetBatchForPackager, ParquetPackager, PartitionedRecordBatch};
use crate::actors::indexer::OTHER_PARTITION_ID;
use crate::models::{NewPublishLock, NewPublishToken, PublishLock};

/// Default commit timeout for ParquetIndexer (60 seconds).
// TODO: read from index config commit_timeout_secs.
const DEFAULT_COMMIT_TIMEOUT: Duration = Duration::from_secs(60);

/// Message to trigger a commit after timeout.
#[derive(Debug)]
struct CommitTimeout {
    workbench_id: Ulid,
}

/// Identifies the accumulator selected for a routed row.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum AccumulatorKey {
    Regular(u64),
    Other,
}

impl AccumulatorKey {
    fn partition_id(self) -> u64 {
        match self {
            Self::Regular(partition_id) => partition_id,
            Self::Other => OTHER_PARTITION_ID,
        }
    }
}

/// Counters for ParquetIndexer observability.
#[derive(Debug, Default, Serialize, Clone)]
pub struct ParquetIndexerCounters {
    /// Number of batches received.
    pub batches_received: u64,
    /// Number of rows indexed.
    pub rows_indexed: u64,
    /// Number of batches flushed to packager.
    pub batches_flushed: u64,
    /// Number of errors encountered.
    pub errors: u64,
}

impl ParquetIndexerCounters {
    /// Record a batch received.
    pub fn record_batch(&mut self, num_rows: usize) {
        self.batches_received += 1u64;
        self.rows_indexed += num_rows as u64;
    }

    /// Record a flush (concatenated batch sent to packager).
    pub fn record_flush(&mut self) {
        self.batches_flushed += 1;
    }

    /// Record an error.
    pub fn record_error(&mut self) {
        self.errors += 1;
    }
}

/// Message containing produced ParquetSplits for downstream processing.
///
/// This is sent when the accumulator produces splits (either via threshold or force commit).
pub struct ParquetSplitBatch {
    /// Index unique identifier for the splits in this batch.
    pub index_uid: IndexUid,
    /// The splits produced.
    pub splits: Vec<ParquetSplitMetadata>,
    /// Directory containing the Parquet files referenced by splits.
    /// The uploader uses this to locate and upload the actual file content.
    pub output_dir: PathBuf,
    /// Checkpoint delta covering all data in these splits.
    /// `None` for merge operations (data was already checkpointed at ingest).
    pub checkpoint_delta_opt: Option<IndexCheckpointDelta>,
    /// Publish lock for coordinating with sources.
    pub publish_lock: PublishLock,
    /// Optional publish token.
    pub publish_token_opt: Option<PublishToken>,
    /// Split IDs being replaced by this batch (non-empty for merges).
    /// Empty for the ingest path.
    pub replaced_split_ids: Vec<String>,
    /// Holds the temp directory alive until the uploader finishes reading.
    /// `None` for the ingest path (packager manages its own temp dir).
    /// `Some` for the merge path (executor's scratch directory).
    pub _scratch_directory_opt: Option<quickwit_common::temp_dir::TempDirectory>,
    /// Merge task — carried through to the publisher so the planner inventory
    /// guard and semaphore permit stay alive until publish completes.
    /// `None` for the ingest path. `Some` for the merge path.
    pub _merge_task_opt: Option<ParquetMergeTask>,
}

impl std::fmt::Debug for ParquetSplitBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParquetSplitBatch")
            .field("index_uid", &self.index_uid)
            .field("num_splits", &self.splits.len())
            .field("output_dir", &self.output_dir)
            .field("replaced_split_ids", &self.replaced_split_ids)
            .field("has_merge_task", &self._merge_task_opt.is_some())
            .finish()
    }
}

/// ParquetIndexer actor that accumulates RecordBatches and forwards them to ParquetPackager.
///
/// This actor:
/// - Receives ProcessedParquetBatch messages from ParquetDocProcessor
/// - Accumulates batches using ParquetBatchAccumulator (CPU-bound buffering + concat)
/// - Forwards concatenated RecordBatches to ParquetPackager when thresholds are exceeded or on
///   force commit
/// - Tracks checkpoint progress for reliable delivery
///
/// Unlike the Tantivy-based Indexer, this actor works entirely with Arrow data.
/// IO-bound work (Parquet encoding, compression, file writing) is delegated to
/// the ParquetPackager actor.
pub struct ParquetIndexer {
    /// Index unique identifier.
    index_uid: IndexUid,
    /// Source identifier.
    source_id: SourceId,
    /// Per-partition batch accumulators.
    accumulators: HashMap<u64, ParquetBatchAccumulator>,
    /// Accumulator for rows whose partition would exceed max_num_partitions.
    other_accumulator_opt: Option<ParquetBatchAccumulator>,
    /// Routing expression for computing the true partition of each row.
    partition_key: RoutingExpr,
    /// Maximum number of regular partition accumulators allowed in a workbench.
    max_num_partitions: NonZeroU32,
    /// Config for creating new accumulators.
    accumulator_config: ParquetIndexingConfig,
    /// Accumulated checkpoint delta.
    checkpoint_delta: SourceCheckpointDelta,
    /// Publish lock for coordinating with sources.
    publish_lock: PublishLock,
    /// Optional publish token.
    publish_token_opt: Option<PublishToken>,
    /// Observability counters.
    counters: ParquetIndexerCounters,
    /// Current workbench ID for tracing.
    workbench_id: Ulid,
    /// Mailbox for sending concatenated batches to packager.
    packager_mailbox: Mailbox<ParquetPackager>,
    /// Commit timeout duration.
    commit_timeout: Duration,
    /// Whether a commit timeout has been scheduled for the current workbench.
    commit_timeout_scheduled: bool,
}

impl ParquetIndexer {
    /// Create a new ParquetIndexer.
    ///
    /// # Arguments
    /// * `index_uid` - The index unique identifier
    /// * `source_id` - The source identifier
    /// * `config` - Optional configuration for accumulation thresholds
    /// * `packager_mailbox` - Mailbox for sending concatenated batches to ParquetPackager
    /// * `commit_timeout` - Optional commit timeout (defaults to 60 seconds)
    pub fn new(
        index_uid: IndexUid,
        source_id: SourceId,
        config: Option<ParquetIndexingConfig>,
        packager_mailbox: Mailbox<ParquetPackager>,
        commit_timeout: Option<Duration>,
    ) -> Self {
        Self::new_with_partition_key_and_max_num_partitions(
            index_uid,
            source_id,
            config,
            packager_mailbox,
            RoutingExpr::default(),
            quickwit_doc_mapper::DocMapping::default_max_num_partitions(),
            commit_timeout,
        )
    }

    /// Create a new ParquetIndexer with an explicit max partition cap.
    pub fn new_with_max_num_partitions(
        index_uid: IndexUid,
        source_id: SourceId,
        config: Option<ParquetIndexingConfig>,
        packager_mailbox: Mailbox<ParquetPackager>,
        max_num_partitions: NonZeroU32,
        commit_timeout: Option<Duration>,
    ) -> Self {
        Self::new_with_partition_key_and_max_num_partitions(
            index_uid,
            source_id,
            config,
            packager_mailbox,
            RoutingExpr::default(),
            max_num_partitions,
            commit_timeout,
        )
    }

    /// Create a new ParquetIndexer with an explicit routing expression and max partition cap.
    pub fn new_with_partition_key_and_max_num_partitions(
        index_uid: IndexUid,
        source_id: SourceId,
        config: Option<ParquetIndexingConfig>,
        packager_mailbox: Mailbox<ParquetPackager>,
        partition_key: RoutingExpr,
        max_num_partitions: NonZeroU32,
        commit_timeout: Option<Duration>,
    ) -> Self {
        let accumulator_config = config.unwrap_or_default();
        let counters = ParquetIndexerCounters::default();
        let commit_timeout = commit_timeout.unwrap_or(DEFAULT_COMMIT_TIMEOUT);

        info!(
            index_uid = %index_uid,
            source_id = %source_id,
            commit_timeout_secs = commit_timeout.as_secs(),
            partition_key = %partition_key,
            max_num_partitions = max_num_partitions.get(),
            "ParquetIndexer created"
        );

        Self {
            index_uid,
            source_id,
            accumulators: HashMap::new(),
            other_accumulator_opt: None,
            partition_key,
            max_num_partitions,
            accumulator_config,
            checkpoint_delta: SourceCheckpointDelta::default(),
            publish_lock: PublishLock::default(),
            publish_token_opt: None,
            counters,
            workbench_id: Ulid::new(),
            packager_mailbox,
            commit_timeout,
            commit_timeout_scheduled: false,
        }
    }

    /// Process a batch and potentially flush the current workbench.
    ///
    /// The metrics pipeline now buffers rows separately per `partition_id`, but it still
    /// commits them as a single workbench so checkpoint and timeout semantics stay coherent.
    ///
    /// Any commit trigger flushes all partition accumulators together and returns one batch
    /// per partition.
    fn process_batch(
        &mut self,
        batch: ProcessedParquetBatch,
        ctx: &ActorContext<Self>,
    ) -> Result<Vec<(u64, RecordBatch)>, ActorExitStatus> {
        let _span = info_span!(
            target: "quickwit-indexing",
            "metrics-indexer",
            index_id = %self.index_uid,
            source_id = %self.source_id,
            workbench_id = %self.workbench_id,
        )
        .entered();

        // Record batch stats
        let num_rows = batch.num_rows();
        self.counters.record_batch(num_rows);

        debug!(
            num_rows = num_rows,
            force_commit = batch.force_commit,
            total_batches = self.counters.batches_received,
            total_rows = self.counters.rows_indexed,
            "received batch for accumulation"
        );

        // Extend checkpoint
        self.checkpoint_delta
            .extend(batch.checkpoint_delta)
            .map_err(|e| {
                warn!(error = %e, "Checkpoint delta conflict");
                ActorExitStatus::Failure(anyhow::anyhow!("{}", e).into())
            })?;

        let force_commit = batch.force_commit;
        let mut threshold_flushed_partitions = Vec::new();

        for record_batch in batch.batches {
            let routed_batches = self.route_record_batch(record_batch);

            for (accumulator_key, routed_batch) in routed_batches {
                let partition_id = accumulator_key.partition_id();
                let accumulator = self.accumulator_mut(accumulator_key);

                // Add the batch to the partition accumulator. If this partition crosses its
                // threshold, the returned batch becomes one member of a workbench-wide flush.
                match accumulator.add_batch(routed_batch) {
                    Ok(Some(flushed_batch)) => {
                        debug!(
                            partition_id,
                            num_rows = flushed_batch.num_rows(),
                            "threshold exceeded, marking workbench for flush"
                        );
                        threshold_flushed_partitions.push((partition_id, flushed_batch));
                    }
                    Ok(None) => {}
                    Err(error) => {
                        warn!(error = %error, "failed to add batch to accumulator");
                        self.counters.record_error();
                        return Err(ActorExitStatus::Failure(
                            anyhow::anyhow!("{}", error).into(),
                        ));
                    }
                }
            }
        }

        ctx.record_progress();

        // A force commit always flushes the entire workbench so the checkpoint can advance.
        if force_commit {
            debug!("force commit requested, flushing all partitions");
            return self.flush_workbench(threshold_flushed_partitions);
        }

        // Threshold pressure on any partition commits the whole workbench. This matches the
        // logs pipeline model and avoids advancing checkpoints while other partitions remain
        // buffered in memory.
        if !threshold_flushed_partitions.is_empty() {
            debug!("threshold exceeded, flushing workbench");
            return self.flush_workbench(threshold_flushed_partitions);
        }

        Ok(Vec::new())
    }

    /// Split one processed Arrow batch into accumulator-local batches.
    fn route_record_batch(&mut self, batch: RecordBatch) -> Vec<(AccumulatorKey, RecordBatch)> {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Vec::new();
        }

        if self.partition_key.is_empty() {
            let accumulator_key = self.get_or_create_accumulator_key(0);
            return vec![(accumulator_key, batch)];
        }

        let mut accumulator_rows: HashMap<AccumulatorKey, Vec<u32>> = HashMap::new();
        for row_idx in 0..num_rows {
            let ctx = ArrowRowContext::new(&batch, row_idx);
            let partition_id = self.partition_key.eval_hash(&ctx);
            let accumulator_key = self.get_or_create_accumulator_key(partition_id);
            accumulator_rows
                .entry(accumulator_key)
                .or_default()
                .push(row_idx as u32);
        }

        if accumulator_rows.len() == 1 {
            let accumulator_key = accumulator_rows.into_keys().next().unwrap();
            return vec![(accumulator_key, batch)];
        }

        let mut routed_batches = Vec::with_capacity(accumulator_rows.len());
        for (accumulator_key, row_indices) in accumulator_rows {
            let row_indices = UInt32Array::from(row_indices);
            let routed_batch = take_record_batch(&batch, &row_indices)
                .expect("take_record_batch should not fail on valid row indices");
            routed_batches.push((accumulator_key, routed_batch));
        }
        routed_batches
    }

    /// Get or create the accumulator key for a true partition.
    fn get_or_create_accumulator_key(&mut self, partition_id: u64) -> AccumulatorKey {
        if self.accumulators.contains_key(&partition_id) {
            return AccumulatorKey::Regular(partition_id);
        }

        if self.accumulators.len() < self.max_num_partitions.get() as usize {
            self.accumulators
                .entry(partition_id)
                .or_insert_with(|| ParquetBatchAccumulator::new(self.accumulator_config.clone()));
            return AccumulatorKey::Regular(partition_id);
        }

        if self.other_accumulator_opt.is_none() {
            warn!(
                incoming_partition_id = partition_id,
                max_num_partitions = self.max_num_partitions.get(),
                other_partition_id = OTHER_PARTITION_ID,
                "exceeding max_num_partitions, routing metrics rows to OTHER partition"
            );
            self.other_accumulator_opt = Some(ParquetBatchAccumulator::new(
                self.accumulator_config.clone(),
            ));
        }

        AccumulatorKey::Other
    }

    /// Returns the accumulator for a previously selected key.
    fn accumulator_mut(&mut self, accumulator_key: AccumulatorKey) -> &mut ParquetBatchAccumulator {
        match accumulator_key {
            AccumulatorKey::Regular(partition_id) => self
                .accumulators
                .get_mut(&partition_id)
                .expect("regular accumulator should have been created while routing"),
            AccumulatorKey::Other => self
                .other_accumulator_opt
                .as_mut()
                .expect("OTHER accumulator should have been created while routing"),
        }
    }

    /// Flush the entire workbench and reset timeout state.
    fn flush_workbench(
        &mut self,
        already_flushed: Vec<(u64, RecordBatch)>,
    ) -> Result<Vec<(u64, RecordBatch)>, ActorExitStatus> {
        let mut results = already_flushed;

        for (partition_id, mut accumulator) in std::mem::take(&mut self.accumulators) {
            match accumulator.flush() {
                Ok(Some(batch)) => results.push((partition_id, batch)),
                Ok(None) => {}
                Err(error) => {
                    warn!(
                        error = %error,
                        partition_id,
                        "failed to flush partition accumulator"
                    );
                    self.counters.record_error();
                    return Err(ActorExitStatus::Failure(
                        anyhow::anyhow!("{}", error).into(),
                    ));
                }
            }
        }

        if let Some(mut other_accumulator) = self.other_accumulator_opt.take() {
            match other_accumulator.flush() {
                Ok(Some(batch)) => results.push((OTHER_PARTITION_ID, batch)),
                Ok(None) => {}
                Err(error) => {
                    warn!(
                        error = %error,
                        partition_id = OTHER_PARTITION_ID,
                        "failed to flush OTHER partition accumulator"
                    );
                    self.counters.record_error();
                    return Err(ActorExitStatus::Failure(
                        anyhow::anyhow!("{}", error).into(),
                    ));
                }
            }
        }

        results.sort_by_key(|(partition_id, _batch)| *partition_id);
        self.reset_workbench();
        Ok(results)
    }

    /// Reset workbench-scoped timeout state after a full commit boundary.
    fn reset_workbench(&mut self) {
        self.workbench_id = Ulid::new();
        self.commit_timeout_scheduled = false;
    }

    /// Record the split batches emitted to the packager.
    fn record_flushed_batches(&mut self, flushed_partitions: &[(u64, RecordBatch)]) {
        for (partition_id, batch) in flushed_partitions {
            self.counters.record_flush();
            info!(
                partition_id,
                num_rows = batch.num_rows(),
                "flushed batch to packager"
            );
        }
    }

    /// Take the current checkpoint delta and reset it.
    fn take_checkpoint_delta(&mut self) -> SourceCheckpointDelta {
        std::mem::take(&mut self.checkpoint_delta)
    }

    /// Create an IndexCheckpointDelta from the current source delta.
    fn make_index_checkpoint_delta(&mut self) -> IndexCheckpointDelta {
        IndexCheckpointDelta {
            source_id: self.source_id.clone(),
            source_delta: self.take_checkpoint_delta(),
        }
    }

    /// Send one flushed workbench downstream as a single packager message.
    async fn send_workbench_to_packager(
        &mut self,
        flushed_partitions: Vec<(u64, RecordBatch)>,
        ctx: &ActorContext<Self>,
        error_context: &str,
    ) -> Result<(), ActorExitStatus> {
        if flushed_partitions.is_empty() && self.checkpoint_delta.is_empty() {
            return Ok(());
        }

        if !flushed_partitions.is_empty() {
            self.record_flushed_batches(&flushed_partitions);
        }

        let batch_for_packager = ParquetBatchForPackager {
            batches: flushed_partitions
                .into_iter()
                .map(|(partition_id, batch)| PartitionedRecordBatch {
                    batch,
                    partition_id,
                })
                .collect(),
            index_uid: self.index_uid.clone(),
            checkpoint_delta: self.make_index_checkpoint_delta(),
            publish_lock: self.publish_lock.clone(),
            publish_token_opt: self.publish_token_opt.clone(),
        };

        if batch_for_packager.batches.is_empty()
            && batch_for_packager.checkpoint_delta.source_delta.is_empty()
        {
            return Ok(());
        }

        ctx.send_message(&self.packager_mailbox, batch_for_packager)
            .await
            .map_err(|e| {
                ActorExitStatus::Failure(anyhow::anyhow!("{error_context}: {e}").into())
            })?;
        Ok(())
    }
}

#[async_trait]
impl Actor for ParquetIndexer {
    type ObservableState = ParquetIndexerCounters;

    fn observable_state(&self) -> Self::ObservableState {
        self.counters.clone()
    }

    fn queue_capacity(&self) -> QueueCapacity {
        QueueCapacity::Bounded(10)
    }

    fn name(&self) -> String {
        "ParquetIndexer".to_string()
    }

    fn runtime_handle(&self) -> Handle {
        RuntimeType::Blocking.get_runtime_handle()
    }

    #[inline]
    fn yield_after_each_message(&self) -> bool {
        false
    }

    async fn finalize(
        &mut self,
        exit_status: &ActorExitStatus,
        ctx: &ActorContext<Self>,
    ) -> anyhow::Result<()> {
        match exit_status {
            ActorExitStatus::DownstreamClosed
            | ActorExitStatus::Killed
            | ActorExitStatus::Failure(_)
            | ActorExitStatus::Panicked => return Ok(()),
            ActorExitStatus::Quit | ActorExitStatus::Success => {
                let flushed_partitions = self.flush_workbench(Vec::new()).unwrap_or_default();
                let _ = self
                    .send_workbench_to_packager(
                        flushed_partitions,
                        ctx,
                        "failed to send final ParquetBatchForPackager to packager",
                    )
                    .await;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<ProcessedParquetBatch> for ParquetIndexer {
    type Reply = ();

    async fn handle(
        &mut self,
        batch: ProcessedParquetBatch,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        if self.publish_lock.is_dead() {
            return Ok(());
        }

        // Schedule commit timeout on first batch for this workbench
        if !self.commit_timeout_scheduled {
            let commit_timeout_message = CommitTimeout {
                workbench_id: self.workbench_id,
            };
            ctx.schedule_self_msg(self.commit_timeout, commit_timeout_message);
            self.commit_timeout_scheduled = true;
            debug!(
                workbench_id = %self.workbench_id,
                commit_timeout_secs = self.commit_timeout.as_secs(),
                "scheduled commit timeout"
            );
        }

        let force_commit = batch.force_commit;
        let flushed_partitions = self.process_batch(batch, ctx)?;

        if force_commit || !flushed_partitions.is_empty() {
            self.send_workbench_to_packager(
                flushed_partitions,
                ctx,
                "failed to send ParquetBatchForPackager to packager",
            )
            .await?;
        }

        Ok(())
    }
}

#[async_trait]
impl Handler<NewPublishLock> for ParquetIndexer {
    type Reply = ();

    async fn handle(
        &mut self,
        message: NewPublishLock,
        _ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        let NewPublishLock(publish_lock) = message;

        // Discard pending data — it was accumulated under the old publish lock
        // and must not be published. This matches the standard Indexer behavior
        // which discards its workbench on publish lock change.
        self.accumulators.clear();
        self.other_accumulator_opt = None;

        // Reset state for new lock
        self.publish_lock = publish_lock;
        self.checkpoint_delta = SourceCheckpointDelta::default();
        self.workbench_id = Ulid::new();
        self.commit_timeout_scheduled = false;

        Ok(())
    }
}

#[async_trait]
impl Handler<NewPublishToken> for ParquetIndexer {
    type Reply = ();

    async fn handle(
        &mut self,
        message: NewPublishToken,
        _ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        let NewPublishToken(publish_token) = message;
        self.publish_token_opt = Some(publish_token);
        Ok(())
    }
}

#[async_trait]
impl Handler<CommitTimeout> for ParquetIndexer {
    type Reply = ();

    async fn handle(
        &mut self,
        commit_timeout: CommitTimeout,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        // Ignore stale timeout messages from previous workbenches
        if self.workbench_id != commit_timeout.workbench_id {
            return Ok(());
        }

        debug!(
            workbench_id = %self.workbench_id,
            "commit timeout triggered, flushing all partitions"
        );

        let flushed_partitions = self.flush_workbench(Vec::new())?;
        self.send_workbench_to_packager(
            flushed_partitions,
            ctx,
            "failed to send ParquetBatchForPackager to packager on commit timeout",
        )
        .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap as StdHashMap;
    use std::num::NonZeroU32;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use arrow::array::{ArrayRef, Float64Array, Int64Array, UInt8Array, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use quickwit_actors::{ActorHandle, Universe};
    use quickwit_common::test_utils::wait_until_predicate;
    use quickwit_parquet_engine::storage::{ParquetSplitWriter, ParquetWriterConfig};
    use quickwit_parquet_engine::test_helpers::{
        create_dict_array, create_nullable_dict_array, create_test_batch,
    };
    use quickwit_proto::metastore::{EmptyResponse, MockMetastoreService};
    use quickwit_storage::RamStorage;

    use super::*;
    use crate::actors::parquet_pipeline::{ParquetPackager, ParquetUploader};
    use crate::actors::{Publisher, UploaderType};

    fn create_test_batch_with_service_values(service_values: &[&str]) -> RecordBatch {
        let num_rows = service_values.len();
        let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("metric_name", dict_type.clone(), false),
            Field::new("metric_type", DataType::UInt8, false),
            Field::new("timestamp_secs", DataType::UInt64, false),
            Field::new("value", DataType::Float64, false),
            Field::new("timeseries_id", DataType::Int64, false),
            Field::new("service", dict_type, true),
        ]));

        let metric_names: Vec<&str> = vec!["cpu.usage"; num_rows];
        let metric_name = create_dict_array(&metric_names);
        let metric_type: ArrayRef = Arc::new(UInt8Array::from(vec![0u8; num_rows]));
        let timestamps: Vec<u64> = (0..num_rows).map(|idx| 100 + idx as u64).collect();
        let timestamp_secs: ArrayRef = Arc::new(UInt64Array::from(timestamps));
        let values: Vec<f64> = (0..num_rows).map(|idx| 42.0 + idx as f64).collect();
        let value: ArrayRef = Arc::new(Float64Array::from(values));
        let timeseries_ids: Vec<i64> = (0..num_rows).map(|idx| 1000 + idx as i64).collect();
        let timeseries_id: ArrayRef = Arc::new(Int64Array::from(timeseries_ids));
        let service_values: Vec<Option<&str>> =
            service_values.iter().map(|value| Some(*value)).collect();
        let service = create_nullable_dict_array(&service_values);

        RecordBatch::try_new(
            schema,
            vec![
                metric_name,
                metric_type,
                timestamp_secs,
                value,
                timeseries_id,
                service,
            ],
        )
        .unwrap()
    }

    fn partition_id_for_service(partition_key: &RoutingExpr, service_value: &str) -> u64 {
        let batch = create_test_batch_with_service_values(&[service_value]);
        partition_key.eval_hash(&ArrowRowContext::new(&batch, 0))
    }

    /// Create a test ParquetUploader and return its mailbox.
    fn create_test_uploader(
        universe: &Universe,
    ) -> (Mailbox<ParquetUploader>, ActorHandle<ParquetUploader>) {
        let mock_metastore = MockMetastoreService::new();
        let ram_storage = Arc::new(RamStorage::default());
        let (publisher_mailbox, _publisher_inbox) = universe.create_test_mailbox::<Publisher>();
        let sequencer_mailbox = super::super::spawn_sequencer_for_test(universe, publisher_mailbox);

        let uploader = ParquetUploader::new(
            UploaderType::IndexUploader,
            quickwit_proto::metastore::MetastoreServiceClient::from_mock(mock_metastore),
            ram_storage,
            sequencer_mailbox,
            4,
        );
        universe.spawn_builder().spawn(uploader)
    }

    /// Create a test ParquetUploader with a mock that expects staging calls.
    fn create_test_uploader_with_staging_expectation(
        universe: &Universe,
        expected_index_id: &'static str,
    ) -> (Mailbox<ParquetUploader>, ActorHandle<ParquetUploader>) {
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_stage_metrics_splits()
            .withf(move |request| request.index_uid().index_id == expected_index_id)
            .times(..)
            .returning(|_| Ok(EmptyResponse {}));

        let ram_storage = Arc::new(RamStorage::default());
        let (publisher_mailbox, _publisher_inbox) = universe.create_test_mailbox::<Publisher>();
        let sequencer_mailbox = super::super::spawn_sequencer_for_test(universe, publisher_mailbox);

        let uploader = ParquetUploader::new(
            UploaderType::IndexUploader,
            quickwit_proto::metastore::MetastoreServiceClient::from_mock(mock_metastore),
            ram_storage,
            sequencer_mailbox,
            4,
        );
        universe.spawn_builder().spawn(uploader)
    }

    /// Create a test ParquetPackager wired to an uploader.
    fn create_test_packager(
        universe: &Universe,
        temp_dir: &std::path::Path,
        uploader_mailbox: Mailbox<ParquetUploader>,
    ) -> (Mailbox<ParquetPackager>, ActorHandle<ParquetPackager>) {
        let writer_config = ParquetWriterConfig::default();
        let table_config = quickwit_parquet_engine::table_config::TableConfig::default();
        let split_writer = ParquetSplitWriter::new(
            quickwit_parquet_engine::split::ParquetSplitKind::Metrics,
            writer_config,
            temp_dir,
            &table_config,
        )
        .unwrap();

        let packager = ParquetPackager::new(split_writer, uploader_mailbox);
        universe.spawn_builder().spawn(packager)
    }

    /// Wait for the uploader to have staged the expected number of splits.
    async fn wait_for_staged_splits(
        uploader_handle: &ActorHandle<ParquetUploader>,
        expected_splits: u64,
    ) -> anyhow::Result<()> {
        wait_until_predicate(
            || async {
                uploader_handle.process_pending_and_observe().await;
                let counters = uploader_handle.last_observation();
                counters.num_staged_splits.load(Ordering::Relaxed) >= expected_splits
            },
            Duration::from_secs(15),
            Duration::from_millis(50),
        )
        .await
        .map_err(|_| anyhow::anyhow!("Timeout waiting for {} staged splits", expected_splits))
    }

    #[tokio::test]
    async fn test_metrics_indexer_receives_batch() {
        let universe = Universe::with_accelerated_time();
        let temp_dir = tempfile::tempdir().unwrap();

        let (uploader_mailbox, _uploader_handle) = create_test_uploader(&universe);
        let (packager_mailbox, _packager_handle) =
            create_test_packager(&universe, temp_dir.path(), uploader_mailbox);

        let indexer = ParquetIndexer::new(
            IndexUid::for_test("test-index", 0),
            "test-source".to_string(),
            None,
            packager_mailbox,
            None,
        );

        let (indexer_mailbox, indexer_handle) = universe.spawn_builder().spawn(indexer);

        // Send a batch
        let batch = create_test_batch(10);
        let processed =
            ProcessedParquetBatch::new(batch, SourceCheckpointDelta::from_range(0..10), false);

        indexer_mailbox.send_message(processed).await.unwrap();

        let counters = indexer_handle.process_pending_and_observe().await.state;

        assert_eq!(counters.batches_received, 1);
        assert_eq!(counters.rows_indexed, 10);

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_metrics_indexer_force_commit() {
        let universe = Universe::with_accelerated_time();
        let temp_dir = tempfile::tempdir().unwrap();

        let (uploader_mailbox, uploader_handle) =
            create_test_uploader_with_staging_expectation(&universe, "test-index");
        let (packager_mailbox, packager_handle) =
            create_test_packager(&universe, temp_dir.path(), uploader_mailbox);

        let indexer = ParquetIndexer::new(
            IndexUid::for_test("test-index", 0),
            "test-source".to_string(),
            None,
            packager_mailbox,
            None,
        );

        let (indexer_mailbox, indexer_handle) = universe.spawn_builder().spawn(indexer);

        // Send batch with force_commit
        let batch = create_test_batch(5);
        let processed = ProcessedParquetBatch::new(
            batch,
            SourceCheckpointDelta::from_range(0..5),
            true, // force_commit
        );

        indexer_mailbox.send_message(processed).await.unwrap();

        let counters = indexer_handle.process_pending_and_observe().await.state;

        // Should have flushed a batch due to force_commit
        assert_eq!(counters.batches_received, 1);
        assert_eq!(counters.batches_flushed, 1);

        // Verify packager produced a split
        let packager_counters = packager_handle.process_pending_and_observe().await.state;
        assert_eq!(packager_counters.splits_produced.load(Ordering::Relaxed), 1);
        assert!(packager_counters.bytes_written.load(Ordering::Relaxed) > 0);

        // Verify uploader received the split
        uploader_handle.process_pending_and_observe().await;

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_metrics_indexer_respects_publish_lock() {
        let universe = Universe::with_accelerated_time();
        let temp_dir = tempfile::tempdir().unwrap();

        let (uploader_mailbox, _uploader_handle) = create_test_uploader(&universe);
        let (packager_mailbox, _packager_handle) =
            create_test_packager(&universe, temp_dir.path(), uploader_mailbox);

        let indexer = ParquetIndexer::new(
            IndexUid::for_test("test-index", 0),
            "test-source".to_string(),
            None,
            packager_mailbox,
            None,
        );

        let (indexer_mailbox, indexer_handle) = universe.spawn_builder().spawn(indexer);

        // Set up and kill publish lock
        let publish_lock = PublishLock::default();
        indexer_mailbox
            .send_message(NewPublishLock(publish_lock.clone()))
            .await
            .unwrap();
        indexer_handle.process_pending_and_observe().await;
        publish_lock.kill().await;

        // Send batch after lock is dead
        let batch = create_test_batch(10);
        let processed =
            ProcessedParquetBatch::new(batch, SourceCheckpointDelta::from_range(0..10), false);

        indexer_mailbox.send_message(processed).await.unwrap();

        let counters = indexer_handle.process_pending_and_observe().await.state;

        // Should not process anything when publish lock is dead
        assert_eq!(counters.batches_received, 0);

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_metrics_indexer_threshold_flush() {
        let universe = Universe::with_accelerated_time();
        let temp_dir = tempfile::tempdir().unwrap();

        // Configure low threshold for testing
        let config = ParquetIndexingConfig::default().with_max_rows(50);

        let (uploader_mailbox, uploader_handle) =
            create_test_uploader_with_staging_expectation(&universe, "test-index");
        let (packager_mailbox, packager_handle) =
            create_test_packager(&universe, temp_dir.path(), uploader_mailbox);

        let indexer = ParquetIndexer::new(
            IndexUid::for_test("test-index", 0),
            "test-source".to_string(),
            Some(config),
            packager_mailbox,
            None,
        );

        let (indexer_mailbox, indexer_handle) = universe.spawn_builder().spawn(indexer);

        // Send batch that exceeds threshold
        let batch = create_test_batch(100);
        let processed =
            ProcessedParquetBatch::new(batch, SourceCheckpointDelta::from_range(0..100), false);

        indexer_mailbox.send_message(processed).await.unwrap();

        let counters = indexer_handle.process_pending_and_observe().await.state;

        // Should have flushed due to exceeding threshold
        assert_eq!(counters.batches_received, 1);
        assert_eq!(counters.rows_indexed, 100);
        assert!(counters.batches_flushed >= 1);

        // Verify packager produced split(s)
        let packager_counters = packager_handle.process_pending_and_observe().await.state;
        assert!(packager_counters.splits_produced.load(Ordering::Relaxed) >= 1);

        // Verify uploader received the splits
        uploader_handle.process_pending_and_observe().await;

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_metrics_indexer_threshold_flush_commits_all_partitions_in_workbench() {
        let universe = Universe::with_accelerated_time();
        let temp_dir = tempfile::tempdir().unwrap();

        let config = ParquetIndexingConfig::default().with_max_rows(50);

        let (uploader_mailbox, _uploader_handle) =
            create_test_uploader_with_staging_expectation(&universe, "test-index");
        let (packager_mailbox, packager_handle) =
            create_test_packager(&universe, temp_dir.path(), uploader_mailbox);

        let partition_key = RoutingExpr::new("service").unwrap();
        let indexer = ParquetIndexer::new_with_partition_key_and_max_num_partitions(
            IndexUid::for_test("test-index", 0),
            "test-source".to_string(),
            Some(config),
            packager_mailbox,
            partition_key,
            quickwit_doc_mapper::DocMapping::default_max_num_partitions(),
            None,
        );

        let (indexer_mailbox, indexer_handle) = universe.spawn_builder().spawn(indexer);

        // First partition stays buffered in the workbench.
        let partition_one_values = vec!["a"; 10];
        let partition_one_batch = create_test_batch_with_service_values(&partition_one_values);
        indexer_mailbox
            .send_message(ProcessedParquetBatch::new(
                partition_one_batch,
                SourceCheckpointDelta::from_range(0..10),
                false,
            ))
            .await
            .unwrap();
        let counters = indexer_handle.process_pending_and_observe().await.state;
        assert_eq!(counters.batches_flushed, 0);

        // Second partition crosses the threshold and should commit the full workbench,
        // including the buffered rows from partition 1.
        let partition_two_values = vec!["b"; 100];
        let partition_two_batch = create_test_batch_with_service_values(&partition_two_values);
        indexer_mailbox
            .send_message(ProcessedParquetBatch::new(
                partition_two_batch,
                SourceCheckpointDelta::from_range(10..110),
                false,
            ))
            .await
            .unwrap();

        let counters = indexer_handle.process_pending_and_observe().await.state;
        assert_eq!(counters.batches_received, 2);
        assert_eq!(counters.batches_flushed, 2);

        let packager_counters = packager_handle.process_pending_and_observe().await.state;
        assert_eq!(packager_counters.splits_produced.load(Ordering::Relaxed), 2);

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_metrics_indexer_threshold_flush_waits_for_full_partitioned_batch() {
        let universe = Universe::with_accelerated_time();
        let (packager_mailbox, packager_inbox) = universe.create_test_mailbox::<ParquetPackager>();

        let config = ParquetIndexingConfig::default().with_max_rows(50);
        let partition_key = RoutingExpr::new("service").unwrap();
        let mut expected_partition_ids = vec![
            partition_id_for_service(&partition_key, "a"),
            partition_id_for_service(&partition_key, "b"),
        ];
        expected_partition_ids.sort();
        let indexer = ParquetIndexer::new_with_partition_key_and_max_num_partitions(
            IndexUid::for_test("test-index", 0),
            "test-source".to_string(),
            Some(config),
            packager_mailbox,
            partition_key,
            quickwit_doc_mapper::DocMapping::default_max_num_partitions(),
            None,
        );

        let (indexer_mailbox, indexer_handle) = universe.spawn_builder().spawn(indexer);

        indexer_mailbox
            .send_message(ProcessedParquetBatch::new_batches(
                vec![
                    create_test_batch_with_service_values(&["a"; 100]),
                    create_test_batch_with_service_values(&["b"; 10]),
                ],
                SourceCheckpointDelta::from_range(0..110),
                false,
            ))
            .await
            .unwrap();

        let counters = indexer_handle.process_pending_and_observe().await.state;
        assert_eq!(counters.batches_received, 1);
        assert_eq!(counters.rows_indexed, 110);
        assert_eq!(counters.batches_flushed, 2);

        let packager_batches: Vec<ParquetBatchForPackager> = packager_inbox.drain_for_test_typed();
        assert_eq!(packager_batches.len(), 1);
        assert_eq!(
            packager_batches[0]
                .batches
                .iter()
                .map(|batch| batch.partition_id)
                .collect::<Vec<_>>(),
            expected_partition_ids
        );
        assert_eq!(
            packager_batches[0].checkpoint_delta.source_delta,
            SourceCheckpointDelta::from_range(0..110)
        );

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_metrics_indexer_flush_on_shutdown() {
        let universe = Universe::with_accelerated_time();
        let temp_dir = tempfile::tempdir().unwrap();

        let (uploader_mailbox, _uploader_handle) =
            create_test_uploader_with_staging_expectation(&universe, "test-index");
        let (packager_mailbox, packager_handle) =
            create_test_packager(&universe, temp_dir.path(), uploader_mailbox);

        let indexer = ParquetIndexer::new(
            IndexUid::for_test("test-index", 0),
            "test-source".to_string(),
            None,
            packager_mailbox,
            None,
        );

        let (indexer_mailbox, indexer_handle) = universe.spawn_builder().spawn(indexer);

        // Send batch without force_commit
        let batch = create_test_batch(10);
        let processed =
            ProcessedParquetBatch::new(batch, SourceCheckpointDelta::from_range(0..10), false);

        indexer_mailbox.send_message(processed).await.unwrap();
        indexer_handle.process_pending_and_observe().await;

        // Gracefully shut down - should flush remaining data
        universe
            .send_exit_with_success(&indexer_mailbox)
            .await
            .unwrap();
        let (exit_status, counters) = indexer_handle.join().await;

        assert!(exit_status.is_success());
        // Should have flushed on shutdown
        assert_eq!(counters.batches_flushed, 1);

        // Verify packager produced a split
        let packager_counters = packager_handle.process_pending_and_observe().await.state;
        assert_eq!(packager_counters.splits_produced.load(Ordering::Relaxed), 1);

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_metrics_indexer_force_commit_flushes_all_partitions_in_workbench() {
        let universe = Universe::with_accelerated_time();
        let temp_dir = tempfile::tempdir().unwrap();

        let (uploader_mailbox, _uploader_handle) =
            create_test_uploader_with_staging_expectation(&universe, "test-index");
        let (packager_mailbox, packager_handle) =
            create_test_packager(&universe, temp_dir.path(), uploader_mailbox);

        let partition_key = RoutingExpr::new("service").unwrap();
        let indexer = ParquetIndexer::new_with_partition_key_and_max_num_partitions(
            IndexUid::for_test("test-index", 0),
            "test-source".to_string(),
            None,
            packager_mailbox,
            partition_key,
            quickwit_doc_mapper::DocMapping::default_max_num_partitions(),
            None,
        );

        let (indexer_mailbox, indexer_handle) = universe.spawn_builder().spawn(indexer);

        indexer_mailbox
            .send_message(ProcessedParquetBatch::new(
                create_test_batch_with_service_values(&["a"; 10]),
                SourceCheckpointDelta::from_range(0..10),
                false,
            ))
            .await
            .unwrap();
        indexer_handle.process_pending_and_observe().await;

        indexer_mailbox
            .send_message(ProcessedParquetBatch::new(
                create_test_batch_with_service_values(&["b"; 20]),
                SourceCheckpointDelta::from_range(10..30),
                true,
            ))
            .await
            .unwrap();

        let counters = indexer_handle.process_pending_and_observe().await.state;
        assert_eq!(counters.batches_received, 2);
        assert_eq!(counters.batches_flushed, 2);

        let packager_counters = packager_handle.process_pending_and_observe().await.state;
        assert_eq!(packager_counters.splits_produced.load(Ordering::Relaxed), 2);

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_metrics_indexer_partitioning() {
        let universe = Universe::with_accelerated_time();
        let (packager_mailbox, packager_inbox) = universe.create_test_mailbox::<ParquetPackager>();

        let partition_key = RoutingExpr::new("service").unwrap();
        let mut expected_partition_ids = vec![
            partition_id_for_service(&partition_key, "a"),
            partition_id_for_service(&partition_key, "b"),
        ];
        expected_partition_ids.sort();
        let indexer = ParquetIndexer::new_with_partition_key_and_max_num_partitions(
            IndexUid::for_test("test-index", 0),
            "test-source".to_string(),
            None,
            packager_mailbox,
            partition_key,
            quickwit_doc_mapper::DocMapping::default_max_num_partitions(),
            None,
        );

        let (indexer_mailbox, indexer_handle) = universe.spawn_builder().spawn(indexer);

        indexer_mailbox
            .send_message(ProcessedParquetBatch::new(
                create_test_batch_with_service_values(&["a"; 10]),
                SourceCheckpointDelta::from_range(0..10),
                false,
            ))
            .await
            .unwrap();
        indexer_handle.process_pending_and_observe().await;

        indexer_mailbox
            .send_message(ProcessedParquetBatch::new(
                create_test_batch_with_service_values(&["b"; 20]),
                SourceCheckpointDelta::from_range(10..30),
                false,
            ))
            .await
            .unwrap();
        indexer_handle.process_pending_and_observe().await;

        universe
            .send_exit_with_success(&indexer_mailbox)
            .await
            .unwrap();
        let (exit_status, counters) = indexer_handle.join().await;

        assert!(matches!(exit_status, ActorExitStatus::Success));
        assert_eq!(counters.batches_received, 2);
        assert_eq!(counters.rows_indexed, 30);
        assert_eq!(counters.batches_flushed, 2);

        let packager_batches: Vec<ParquetBatchForPackager> = packager_inbox.drain_for_test_typed();
        assert_eq!(packager_batches.len(), 1);
        assert_eq!(packager_batches[0].batches.len(), 2);
        assert_eq!(
            packager_batches[0]
                .batches
                .iter()
                .map(|batch| batch.partition_id)
                .collect::<Vec<_>>(),
            expected_partition_ids
        );
        assert_eq!(
            packager_batches[0].checkpoint_delta.source_delta,
            SourceCheckpointDelta::from_range(0..30)
        );

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_metrics_indexer_exceeding_max_num_partitions_routes_to_other() {
        let universe = Universe::with_accelerated_time();
        let (packager_mailbox, packager_inbox) = universe.create_test_mailbox::<ParquetPackager>();

        let partition_key = RoutingExpr::new("service").unwrap();
        let partition_a = partition_id_for_service(&partition_key, "a");
        let partition_b = partition_id_for_service(&partition_key, "b");
        let indexer = ParquetIndexer::new_with_partition_key_and_max_num_partitions(
            IndexUid::for_test("test-index", 0),
            "test-source".to_string(),
            None,
            packager_mailbox,
            partition_key,
            NonZeroU32::new(2).unwrap(),
            None,
        );

        let (indexer_mailbox, indexer_handle) = universe.spawn_builder().spawn(indexer);

        for (idx, service_value) in ["a", "b", "c"].into_iter().enumerate() {
            let offset = idx as u64;
            indexer_mailbox
                .send_message(ProcessedParquetBatch::new(
                    create_test_batch_with_service_values(&[service_value; 10]),
                    SourceCheckpointDelta::from_range(offset..offset + 1),
                    idx == 2,
                ))
                .await
                .unwrap();
            indexer_handle.process_pending_and_observe().await;
        }

        let counters = indexer_handle.process_pending_and_observe().await.state;
        assert_eq!(counters.batches_received, 3);
        assert_eq!(counters.rows_indexed, 30);
        assert_eq!(counters.batches_flushed, 3);

        let packager_batches: Vec<ParquetBatchForPackager> = packager_inbox.drain_for_test_typed();
        assert_eq!(packager_batches.len(), 1);
        let partition_ids = packager_batches[0]
            .batches
            .iter()
            .map(|batch| batch.partition_id)
            .collect::<Vec<_>>();
        assert_eq!(partition_ids.len(), 3);
        assert!(partition_ids.contains(&partition_a));
        assert!(partition_ids.contains(&partition_b));
        assert!(partition_ids.contains(&OTHER_PARTITION_ID));
        assert_eq!(
            packager_batches[0].checkpoint_delta.source_delta,
            SourceCheckpointDelta::from_range(0..3)
        );

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_metrics_indexer_overflow_keeps_rows_for_already_open_partition() {
        let universe = Universe::with_accelerated_time();
        let (packager_mailbox, packager_inbox) = universe.create_test_mailbox::<ParquetPackager>();

        let partition_key = RoutingExpr::new("service").unwrap();
        let partition_a = partition_id_for_service(&partition_key, "a");
        let partition_b = partition_id_for_service(&partition_key, "b");
        let indexer = ParquetIndexer::new_with_partition_key_and_max_num_partitions(
            IndexUid::for_test("test-index", 0),
            "test-source".to_string(),
            None,
            packager_mailbox,
            partition_key,
            NonZeroU32::new(2).unwrap(),
            None,
        );

        let (indexer_mailbox, indexer_handle) = universe.spawn_builder().spawn(indexer);

        indexer_mailbox
            .send_message(ProcessedParquetBatch::new(
                create_test_batch_with_service_values(&["a", "b"]),
                SourceCheckpointDelta::from_range(0..2),
                false,
            ))
            .await
            .unwrap();
        let counters = indexer_handle.process_pending_and_observe().await.state;
        assert_eq!(counters.batches_flushed, 0);

        indexer_mailbox
            .send_message(ProcessedParquetBatch::new(
                create_test_batch_with_service_values(&["c", "d", "a"]),
                SourceCheckpointDelta::from_range(2..5),
                true,
            ))
            .await
            .unwrap();

        let counters = indexer_handle.process_pending_and_observe().await.state;
        assert_eq!(counters.batches_received, 2);
        assert_eq!(counters.rows_indexed, 5);
        assert_eq!(counters.batches_flushed, 3);

        let packager_batches: Vec<ParquetBatchForPackager> = packager_inbox.drain_for_test_typed();
        assert_eq!(packager_batches.len(), 1);
        let row_counts: StdHashMap<u64, usize> = packager_batches[0]
            .batches
            .iter()
            .map(|batch| (batch.partition_id, batch.batch.num_rows()))
            .collect();
        assert_eq!(row_counts.get(&partition_a), Some(&2));
        assert_eq!(row_counts.get(&partition_b), Some(&1));
        assert_eq!(row_counts.get(&OTHER_PARTITION_ID), Some(&2));
        assert_eq!(
            packager_batches[0].checkpoint_delta.source_delta,
            SourceCheckpointDelta::from_range(0..5)
        );

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_metrics_indexer_emits_one_packager_message_per_workbench_flush() {
        let universe = Universe::with_accelerated_time();
        let (packager_mailbox, packager_inbox) = universe.create_test_mailbox::<ParquetPackager>();

        let partition_key = RoutingExpr::new("service").unwrap();
        let mut expected_partition_ids = vec![
            partition_id_for_service(&partition_key, "a"),
            partition_id_for_service(&partition_key, "b"),
        ];
        expected_partition_ids.sort();
        let indexer = ParquetIndexer::new_with_partition_key_and_max_num_partitions(
            IndexUid::for_test("test-index", 0),
            "test-source".to_string(),
            None,
            packager_mailbox,
            partition_key,
            quickwit_doc_mapper::DocMapping::default_max_num_partitions(),
            None,
        );

        let (indexer_mailbox, indexer_handle) = universe.spawn_builder().spawn(indexer);

        indexer_mailbox
            .send_message(ProcessedParquetBatch::new(
                create_test_batch_with_service_values(&["a"; 10]),
                SourceCheckpointDelta::from_range(0..10),
                false,
            ))
            .await
            .unwrap();
        indexer_handle.process_pending_and_observe().await;

        indexer_mailbox
            .send_message(ProcessedParquetBatch::new(
                create_test_batch_with_service_values(&["b"; 20]),
                SourceCheckpointDelta::from_range(10..30),
                true,
            ))
            .await
            .unwrap();

        let counters = indexer_handle.process_pending_and_observe().await.state;
        assert_eq!(counters.batches_received, 2);
        assert_eq!(counters.batches_flushed, 2);

        let packager_batches: Vec<ParquetBatchForPackager> = packager_inbox.drain_for_test_typed();
        assert_eq!(packager_batches.len(), 1);
        assert_eq!(packager_batches[0].batches.len(), 2);
        assert_eq!(
            packager_batches[0]
                .batches
                .iter()
                .map(|batch| batch.partition_id)
                .collect::<Vec<_>>(),
            expected_partition_ids
        );
        assert_eq!(
            packager_batches[0].checkpoint_delta.source_delta,
            SourceCheckpointDelta::from_range(0..30)
        );

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_metrics_indexer_commit_timeout() {
        let universe = Universe::new();
        let temp_dir = tempfile::tempdir().unwrap();

        let (uploader_mailbox, uploader_handle) =
            create_test_uploader_with_staging_expectation(&universe, "test-index");
        let (packager_mailbox, packager_handle) =
            create_test_packager(&universe, temp_dir.path(), uploader_mailbox);

        // Use a short commit timeout for testing
        let commit_timeout = Duration::from_millis(50);
        let indexer = ParquetIndexer::new(
            IndexUid::for_test("test-index", 0),
            "test-source".to_string(),
            None,
            packager_mailbox,
            Some(commit_timeout),
        );

        let (indexer_mailbox, indexer_handle) = universe.spawn_builder().spawn(indexer);

        // Send batch without force_commit
        let batch = create_test_batch(10);
        let processed =
            ProcessedParquetBatch::new(batch, SourceCheckpointDelta::from_range(0..10), false);

        indexer_mailbox.send_message(processed).await.unwrap();
        indexer_handle.process_pending_and_observe().await;

        // No flush yet (waiting for threshold or timeout)
        let counters = indexer_handle.observe().await;
        assert_eq!(counters.batches_flushed, 0);

        // Wait for commit timeout to trigger
        tokio::time::sleep(commit_timeout + Duration::from_millis(50)).await;
        indexer_handle.process_pending_and_observe().await;

        // Should have flushed due to commit timeout
        let counters = indexer_handle.observe().await;
        assert_eq!(counters.batches_flushed, 1);

        // Verify packager produced a split
        let packager_counters = packager_handle.process_pending_and_observe().await.state;
        assert_eq!(packager_counters.splits_produced.load(Ordering::Relaxed), 1);

        // Verify uploader received the split
        uploader_handle.process_pending_and_observe().await;

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_metrics_indexer_forwards_to_packager_and_uploader() {
        quickwit_common::setup_logging_for_tests();

        let universe = Universe::with_accelerated_time();
        let temp_dir = tempfile::tempdir().unwrap();

        // Create uploader with staging expectation
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_stage_metrics_splits()
            .returning(|_| Ok(EmptyResponse {}));

        let ram_storage = Arc::new(RamStorage::default());
        let (publisher_mailbox, _publisher_inbox) = universe.create_test_mailbox::<Publisher>();
        let sequencer_mailbox =
            super::super::spawn_sequencer_for_test(&universe, publisher_mailbox);
        let uploader = ParquetUploader::new(
            UploaderType::IndexUploader,
            quickwit_proto::metastore::MetastoreServiceClient::from_mock(mock_metastore),
            ram_storage,
            sequencer_mailbox,
            4,
        );
        let (uploader_mailbox, uploader_handle) = universe.spawn_builder().spawn(uploader);

        let (packager_mailbox, packager_handle) =
            create_test_packager(&universe, temp_dir.path(), uploader_mailbox);

        let indexer = ParquetIndexer::new(
            IndexUid::for_test("test-index", 0),
            "test-source".to_string(),
            None,
            packager_mailbox,
            None,
        );

        let (indexer_mailbox, indexer_handle) = universe.spawn_builder().spawn(indexer);

        // Send batch with force_commit to trigger flush
        let batch = create_test_batch(20);
        let processed = ProcessedParquetBatch::new(
            batch,
            SourceCheckpointDelta::from_range(0..20),
            true, // force_commit
        );

        indexer_mailbox.send_message(processed).await.unwrap();

        // Process indexer messages
        let indexer_counters = indexer_handle.process_pending_and_observe().await.state;

        // Verify indexer flushed a batch
        assert_eq!(indexer_counters.batches_received, 1);
        assert_eq!(indexer_counters.batches_flushed, 1);

        // Verify packager produced a split
        let packager_counters = packager_handle.process_pending_and_observe().await.state;
        assert_eq!(packager_counters.splits_produced.load(Ordering::Relaxed), 1);

        // Wait for the uploader to stage the split
        wait_for_staged_splits(&uploader_handle, 1)
            .await
            .expect("Uploader should have staged 1 split");

        universe.assert_quit().await;
    }
}
