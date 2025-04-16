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

use std::collections::hash_map::Entry;
use std::num::NonZeroU32;
use std::ops::RangeInclusive;
use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use bytesize::ByteSize;
use fail::fail_point;
use fnv::FnvHashMap;
use itertools::Itertools;
use quickwit_actors::{
    Actor, ActorContext, ActorExitStatus, Command, Handler, Mailbox, QueueCapacity,
};
use quickwit_common::io::IoControls;
use quickwit_common::metrics::GaugeGuard;
use quickwit_common::runtimes::RuntimeType;
use quickwit_common::temp_dir::TempDirectory;
use quickwit_config::IndexingSettings;
use quickwit_doc_mapper::DocMapper;
use quickwit_metastore::checkpoint::{IndexCheckpointDelta, SourceCheckpointDelta};
use quickwit_proto::indexing::{IndexingPipelineId, PipelineMetrics};
use quickwit_proto::metastore::{
    LastDeleteOpstampRequest, MetastoreService, MetastoreServiceClient,
};
use quickwit_proto::types::{DocMappingUid, PublishToken};
use quickwit_query::get_quickwit_fastfield_normalizer_manager;
use serde::Serialize;
use tantivy::schema::Schema;
use tantivy::store::{Compressor, ZstdCompressor};
use tantivy::tokenizer::TokenizerManager;
use tantivy::{DateTime, IndexBuilder, IndexSettings};
use tokio::runtime::Handle;
use tokio::sync::Semaphore;
use tracing::{Span, info, info_span, warn};
use ulid::Ulid;

use crate::actors::IndexSerializer;
use crate::actors::cooperative_indexing::{CooperativeIndexingCycle, CooperativeIndexingPeriod};
use crate::models::{
    CommitTrigger, EmptySplit, IndexedSplitBatchBuilder, IndexedSplitBuilder, NewPublishLock,
    NewPublishToken, ProcessedDoc, ProcessedDocBatch, PublishLock,
};

// Random partition ID used to gather partitions exceeding the maximum number of partitions.
const OTHER_PARTITION_ID: u64 = 3264326757911759461u64;

#[derive(Debug)]
struct CommitTimeout {
    workbench_id: Ulid,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct IndexerCounters {
    /// Number of splits that were emitted by the indexer.
    pub num_splits_emitted: u64,

    /// Number of split batches that were emitted by the indexer.
    pub num_split_batches_emitted: u64,

    /// Number of (valid) documents in the current workbench.
    /// This value is used to trigger commit and for observation.
    pub num_docs_in_workbench: u64,

    /// Number of ProcessDocBatch received by the indexer to
    /// build this split.
    pub num_doc_batches_in_workbench: u64,

    /// Metrics describing the load and indexing performance of the
    /// pipeline. This is only updated for cooperative indexers.
    pub pipeline_metrics_opt: Option<PipelineMetrics>,
}

struct IndexerState {
    pipeline_id: IndexingPipelineId,
    metastore: MetastoreServiceClient,
    indexing_directory: TempDirectory,
    indexing_settings: IndexingSettings,
    publish_lock: PublishLock,
    publish_token_opt: Option<PublishToken>,
    schema: Schema,
    doc_mapping_uid: DocMappingUid,
    tokenizer_manager: TokenizerManager,
    max_num_partitions: NonZeroU32,
    index_settings: IndexSettings,
    cooperative_indexing_opt: Option<CooperativeIndexingCycle>,
}

impl IndexerState {
    fn create_indexed_split_builder(
        &self,
        partition_id: u64,
        last_delete_opstamp: u64,
        ctx: &ActorContext<Indexer>,
    ) -> anyhow::Result<IndexedSplitBuilder> {
        let index_builder = IndexBuilder::new()
            .settings(self.index_settings.clone())
            .schema(self.schema.clone())
            .tokenizers(self.tokenizer_manager.clone())
            .fast_field_tokenizers(
                get_quickwit_fastfield_normalizer_manager()
                    .tantivy_manager()
                    .clone(),
            );

        let io_controls = IoControls::default()
            .set_progress(ctx.progress().clone())
            .set_kill_switch(ctx.kill_switch().clone())
            .set_component("indexer");

        let indexed_split = IndexedSplitBuilder::new_in_dir(
            self.pipeline_id.clone(),
            partition_id,
            last_delete_opstamp,
            self.doc_mapping_uid,
            self.indexing_directory.clone(),
            index_builder,
            io_controls,
        )?;
        info!(
            split_id=%indexed_split.split_id(),
            partition_id=%partition_id,
            "new-split"
        );
        Ok(indexed_split)
    }

    fn get_or_create_indexed_split<'a>(
        &self,
        partition_id: u64,
        last_delete_opstamp: u64,
        splits: &'a mut FnvHashMap<u64, IndexedSplitBuilder>,
        other_split_opt: &'a mut Option<IndexedSplitBuilder>,
        counter: &'a mut IndexerCounters,
        ctx: &ActorContext<Indexer>,
    ) -> anyhow::Result<(&'a mut IndexedSplitBuilder, bool)> {
        let num_splits = splits.len();
        match splits.entry(partition_id) {
            Entry::Occupied(indexed_split) => Ok((indexed_split.into_mut(), false)),
            Entry::Vacant(vacant_entry) => {
                if num_splits as u32 >= self.max_num_partitions.get() {
                    // In order to avoid exceeding max_num_partitions, we map the document to the
                    // `OTHER` special partition.
                    if other_split_opt.is_none() {
                        warn!(
                            num_docs_in_workbench = counter.num_docs_in_workbench,
                            max_num_partition = self.max_num_partitions.get(),
                            "Exceeding max_num_partition"
                        );
                        let new_other_split = self.create_indexed_split_builder(
                            OTHER_PARTITION_ID,
                            last_delete_opstamp,
                            ctx,
                        )?;
                        *other_split_opt = Some(new_other_split);
                    }
                    Ok((other_split_opt.as_mut().unwrap(), true))
                } else {
                    let indexed_split =
                        self.create_indexed_split_builder(partition_id, last_delete_opstamp, ctx)?;
                    Ok((vacant_entry.insert(indexed_split), true))
                }
            }
        }
    }

    async fn create_workbench(
        &self,
        ctx: &ActorContext<Indexer>,
    ) -> anyhow::Result<IndexingWorkbench> {
        let workbench_id = Ulid::new();
        let batch_parent_span = info_span!(target: "quickwit-indexing", "index-doc-batches",
            index_id=%self.pipeline_id.index_uid.index_id,
            source_id=%self.pipeline_id.source_id,
            pipeline_uid=%self.pipeline_id.pipeline_uid,
            workbench_id=%workbench_id,
        );
        let indexing_span = info_span!(parent: batch_parent_span.id(), "indexer");
        let cooperative_indexing_period =
            if let Some(cooperative_indexing) = &self.cooperative_indexing_opt {
                Some(
                    ctx.protect_future(cooperative_indexing.cooperative_indexing_period())
                        .await,
                )
            } else {
                None
            };

        let last_delete_opstamp_request = LastDeleteOpstampRequest {
            index_uid: Some(self.pipeline_id.index_uid.clone()),
        };
        let last_delete_opstamp_response = ctx
            .protect_future(
                self.metastore
                    .clone()
                    .last_delete_opstamp(last_delete_opstamp_request),
            )
            .await?;
        let last_delete_opstamp = last_delete_opstamp_response.last_delete_opstamp;

        let checkpoint_delta = IndexCheckpointDelta {
            source_id: self.pipeline_id.source_id.clone(),
            source_delta: SourceCheckpointDelta::default(),
        };
        let publish_lock = self.publish_lock.clone();
        let publish_token_opt = self.publish_token_opt.clone();

        let mut split_builders_guard =
            GaugeGuard::from_gauge(&crate::metrics::INDEXER_METRICS.split_builders);
        split_builders_guard.add(1);

        let workbench = IndexingWorkbench {
            workbench_id,
            batch_parent_span,
            _indexing_span: indexing_span,
            indexed_splits: FnvHashMap::with_capacity_and_hasher(250, Default::default()),
            other_indexed_split_opt: None,
            checkpoint_delta,
            publish_lock,
            publish_token_opt,
            last_delete_opstamp,
            memory_usage: GaugeGuard::from_gauge(
                &quickwit_common::metrics::MEMORY_METRICS
                    .in_flight
                    .index_writer,
            ),
            cooperative_indexing_period,
            split_builders_guard,
        };
        Ok(workbench)
    }

    /// Returns the current_indexed_split. If this is the first message, then
    /// the indexed_split does not exist yet.
    ///
    /// This function will then create it, and can hence return an Error.
    async fn get_or_create_workbench<'a>(
        &'a self,
        indexing_workbench_opt: &'a mut Option<IndexingWorkbench>,
        ctx: &'a ActorContext<Indexer>,
    ) -> anyhow::Result<&'a mut IndexingWorkbench> {
        if indexing_workbench_opt.is_none() {
            let indexing_workbench = self.create_workbench(ctx).await?;
            let commit_timeout_message = CommitTimeout {
                workbench_id: indexing_workbench.workbench_id,
            };
            ctx.schedule_self_msg(
                self.indexing_settings.commit_timeout(),
                commit_timeout_message,
            );
            *indexing_workbench_opt = Some(indexing_workbench);
        }
        let current_indexing_workbench = indexing_workbench_opt.as_mut().context(
            "no index writer available. this should never happen! please, report on https://github.com/quickwit-oss/quickwit/issues"
        )?;
        Ok(current_indexing_workbench)
    }

    async fn index_batch(
        &self,
        batch: ProcessedDocBatch,
        indexing_workbench_opt: &mut Option<IndexingWorkbench>,
        counters: &mut IndexerCounters,
        ctx: &ActorContext<Indexer>,
    ) -> Result<(), ActorExitStatus> {
        let IndexingWorkbench {
            checkpoint_delta,
            indexed_splits,
            other_indexed_split_opt,
            publish_lock,
            last_delete_opstamp,
            memory_usage,
            ..
        } = self
            .get_or_create_workbench(indexing_workbench_opt, ctx)
            .await?;
        if publish_lock.is_dead() {
            // Release indexing permit early.
            indexing_workbench_opt.take();
            return Ok(());
        }
        checkpoint_delta
            .source_delta
            .extend(batch.checkpoint_delta)
            .context("batch delta does not follow indexer checkpoint")?;
        let mut memory_usage_delta: i64 = 0;
        counters.num_doc_batches_in_workbench += 1;
        for doc in batch.docs {
            let ProcessedDoc {
                doc,
                timestamp_opt,
                partition,
                num_bytes,
            } = doc;
            counters.num_docs_in_workbench += 1;
            let (indexed_split, split_created) = self.get_or_create_indexed_split(
                partition,
                *last_delete_opstamp,
                indexed_splits,
                other_indexed_split_opt,
                counters,
                ctx,
            )?;
            let mem_usage_before = indexed_split.index_writer.mem_usage() as u64;
            if split_created {
                // The split was just created. We need to account for the initial index writer's
                // memory usage.
                memory_usage_delta += mem_usage_before as i64;
            }
            indexed_split.split_attrs.uncompressed_docs_size_in_bytes += num_bytes as u64;
            indexed_split.split_attrs.num_docs += 1;
            if let Some(timestamp) = timestamp_opt {
                record_timestamp(timestamp, &mut indexed_split.split_attrs.time_range);
            }
            let _protect_guard = ctx.protect_zone();
            indexed_split
                .index_writer
                .add_document(doc)
                .context("failed to add document")?;
            let mem_usage_after = indexed_split.index_writer.mem_usage() as u64;
            memory_usage_delta += mem_usage_after as i64 - mem_usage_before as i64;
            ctx.record_progress();
        }
        memory_usage.add(memory_usage_delta);
        Ok(())
    }
}

/// A workbench hosts the set of `IndexedSplit` that are being built.
struct IndexingWorkbench {
    workbench_id: Ulid,
    // This span is meant to be passed through the pipeline.
    batch_parent_span: Span,
    // Span for the in-memory indexing (done in the Indexer actor).
    _indexing_span: Span,

    indexed_splits: FnvHashMap<u64, IndexedSplitBuilder>,
    other_indexed_split_opt: Option<IndexedSplitBuilder>,

    checkpoint_delta: IndexCheckpointDelta,
    publish_lock: PublishLock,
    publish_token_opt: Option<PublishToken>,
    // On workbench creation, we fetch from the metastore the last delete task opstamp.
    // We use this value to set the `delete_opstamp` of the workbench splits.
    last_delete_opstamp: u64,
    // Number of bytes declared as used by tantivy.
    memory_usage: GaugeGuard<'static>,
    split_builders_guard: GaugeGuard<'static>,
    cooperative_indexing_period: Option<CooperativeIndexingPeriod>,
}

pub struct Indexer {
    indexer_state: IndexerState,
    index_serializer_mailbox: Mailbox<IndexSerializer>,
    indexing_workbench_opt: Option<IndexingWorkbench>,
    counters: IndexerCounters,
}

#[async_trait]
impl Actor for Indexer {
    type ObservableState = IndexerCounters;

    fn observable_state(&self) -> Self::ObservableState {
        self.counters.clone()
    }

    fn queue_capacity(&self) -> QueueCapacity {
        QueueCapacity::Bounded(10)
    }

    fn name(&self) -> String {
        "Indexer".to_string()
    }

    fn runtime_handle(&self) -> Handle {
        RuntimeType::Blocking.get_runtime_handle()
    }

    #[inline]
    fn yield_after_each_message(&self) -> bool {
        false
    }

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        if let Some(cooperative_indexing_cycle) = &self.indexer_state.cooperative_indexing_opt {
            let initial_sleep_duration = cooperative_indexing_cycle.initial_sleep_duration();
            ctx.pause();
            ctx.schedule_self_msg(initial_sleep_duration, Command::Resume);
        }
        Ok(())
    }

    async fn on_drained_messages(
        &mut self,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        let Some(indexing_workbench) = &mut self.indexing_workbench_opt else {
            return Ok(());
        };

        let Some(cooperative_indexing_period) =
            indexing_workbench.cooperative_indexing_period.take()
        else {
            return Ok(());
        };

        let uncompressed_num_bytes = indexing_workbench
            .indexed_splits
            .values()
            .map(|split| split.split_attrs.uncompressed_docs_size_in_bytes)
            .sum::<u64>();

        // This also drops the indexing permit.
        let (sleep_duration, pipeline_metrics) =
            cooperative_indexing_period.end_of_work(uncompressed_num_bytes);

        self.counters.pipeline_metrics_opt = Some(pipeline_metrics);

        self.send_to_serializer(CommitTrigger::Drained, ctx).await?;

        if !sleep_duration.is_zero() {
            ctx.pause();
            ctx.schedule_self_msg(sleep_duration, Command::Resume);
        }

        Ok(())
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
                let _ = self
                    .send_to_serializer(CommitTrigger::NoMoreDocs, ctx)
                    .await;
            }
        }
        Ok(())
    }
}

fn record_timestamp(timestamp: DateTime, time_range: &mut Option<RangeInclusive<DateTime>>) {
    let new_timestamp_range = match time_range {
        Some(range) => timestamp.min(*range.start())..=timestamp.max(*range.end()),
        None => timestamp..=timestamp,
    };
    *time_range = Some(new_timestamp_range);
}

#[async_trait]
impl Handler<CommitTimeout> for Indexer {
    type Reply = ();

    async fn handle(
        &mut self,
        commit_timeout: CommitTimeout,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        if let Some(indexing_workbench) = &self.indexing_workbench_opt {
            // If this is a timeout for a different workbench, we must ignore it.
            if indexing_workbench.workbench_id != commit_timeout.workbench_id {
                return Ok(());
            }
        }
        self.send_to_serializer(CommitTrigger::Timeout, ctx).await?;
        Ok(())
    }
}

#[async_trait]
impl Handler<ProcessedDocBatch> for Indexer {
    type Reply = ();

    async fn handle(
        &mut self,
        doc_batch: ProcessedDocBatch,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        self.index_batch(doc_batch, ctx).await
    }
}

#[async_trait]
impl Handler<NewPublishLock> for Indexer {
    type Reply = ();

    async fn handle(
        &mut self,
        message: NewPublishLock,
        _ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        let NewPublishLock(publish_lock) = message;
        self.indexing_workbench_opt = None;
        self.indexer_state.publish_lock = publish_lock;
        Ok(())
    }
}

#[async_trait]
impl Handler<NewPublishToken> for Indexer {
    type Reply = ();

    async fn handle(
        &mut self,
        message: NewPublishToken,
        _ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        let NewPublishToken(publish_token) = message;
        self.indexer_state.publish_token_opt = Some(publish_token);
        Ok(())
    }
}

impl Indexer {
    pub fn new(
        pipeline_id: IndexingPipelineId,
        doc_mapper: Arc<DocMapper>,
        metastore: MetastoreServiceClient,
        indexing_directory: TempDirectory,
        indexing_settings: IndexingSettings,
        cooperative_indexing_permits_opt: Option<Arc<Semaphore>>,
        index_serializer_mailbox: Mailbox<IndexSerializer>,
    ) -> Self {
        let schema = doc_mapper.schema();
        let tokenizer_manager = doc_mapper.tokenizer_manager().clone();
        let docstore_compression = Compressor::Zstd(ZstdCompressor {
            compression_level: Some(indexing_settings.docstore_compression_level),
        });
        let index_settings = IndexSettings {
            docstore_blocksize: indexing_settings.docstore_blocksize,
            docstore_compression,
            docstore_compress_dedicated_thread: true,
        };
        let cooperative_indexing_opt: Option<CooperativeIndexingCycle> =
            cooperative_indexing_permits_opt.map(|cooperative_indexing_permits| {
                CooperativeIndexingCycle::new(
                    &pipeline_id,
                    indexing_settings.commit_timeout(),
                    cooperative_indexing_permits,
                )
            });
        Self {
            indexer_state: IndexerState {
                pipeline_id,
                metastore: metastore.clone(),
                indexing_directory,
                indexing_settings,
                publish_lock: PublishLock::default(),
                publish_token_opt: None,
                schema,
                doc_mapping_uid: doc_mapper.doc_mapping_uid(),
                tokenizer_manager: tokenizer_manager.tantivy_manager().clone(),
                index_settings,
                max_num_partitions: doc_mapper.max_num_partitions(),
                cooperative_indexing_opt,
            },
            index_serializer_mailbox,
            indexing_workbench_opt: None,
            counters: IndexerCounters::default(),
        }
    }

    fn memory_usage(&self) -> ByteSize {
        if let Some(workbench) = &self.indexing_workbench_opt {
            ByteSize(workbench.memory_usage.get() as u64)
        } else {
            ByteSize(0u64)
        }
    }

    async fn index_batch(
        &mut self,
        batch: ProcessedDocBatch,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        fail_point!("indexer:batch:before");
        let force_commit = batch.force_commit;
        self.indexer_state
            .index_batch(
                batch,
                &mut self.indexing_workbench_opt,
                &mut self.counters,
                ctx,
            )
            .await?;
        let memory_usage = self.memory_usage();
        if memory_usage >= self.indexer_state.indexing_settings.resources.heap_size {
            self.send_to_serializer(CommitTrigger::MemoryLimit, ctx)
                .await?;
        }
        if self.counters.num_docs_in_workbench
            >= self.indexer_state.indexing_settings.split_num_docs_target as u64
        {
            self.send_to_serializer(CommitTrigger::NumDocsLimit, ctx)
                .await?;
        }
        if force_commit {
            self.send_to_serializer(CommitTrigger::ForceCommit, ctx)
                .await?;
        }
        fail_point!("indexer:batch:after");
        Ok(())
    }

    /// Extract the indexed split and send it to the IndexSerializer.
    async fn send_to_serializer(
        &mut self,
        commit_trigger: CommitTrigger,
        ctx: &ActorContext<Self>,
    ) -> anyhow::Result<()> {
        let Some(IndexingWorkbench {
            indexed_splits,
            other_indexed_split_opt,
            checkpoint_delta,
            publish_lock,
            publish_token_opt,
            batch_parent_span,
            memory_usage,
            split_builders_guard,
            ..
        }) = self.indexing_workbench_opt.take()
        else {
            return Ok(());
        };

        let mut splits: Vec<IndexedSplitBuilder> = indexed_splits.into_values().collect();

        if let Some(other_split) = other_indexed_split_opt {
            splits.push(other_split)
        }

        // Avoid producing empty split, but still update the checkpoint if it is not empty to avoid
        // reprocessing the same faulty documents.
        if splits.is_empty() {
            if !checkpoint_delta.is_empty() {
                ctx.send_message(
                    &self.index_serializer_mailbox,
                    EmptySplit {
                        index_uid: self.indexer_state.pipeline_id.index_uid.clone(),
                        checkpoint_delta,
                        publish_lock,
                        publish_token_opt,
                        batch_parent_span,
                    },
                )
                .await?;
            }
            return Ok(());
        }
        let num_splits = splits.len() as u64;
        let split_ids = splits.iter().map(|split| split.split_id()).join(",");
        info!(
            index=%self.indexer_state.pipeline_id.index_uid,
            source=self.indexer_state.pipeline_id.source_id.as_str(),
            pipeline_uid=%self.indexer_state.pipeline_id.pipeline_uid,
            commit_trigger=?commit_trigger,
            num_batches=%self.counters.num_doc_batches_in_workbench,
            split_ids=%split_ids,
            num_docs=self.counters.num_docs_in_workbench, "send-to-index-serializer");
        ctx.send_message(
            &self.index_serializer_mailbox,
            IndexedSplitBatchBuilder {
                splits,
                checkpoint_delta_opt: Some(checkpoint_delta),
                publish_lock,
                publish_token_opt,
                commit_trigger,
                batch_parent_span,
                memory_usage,
                _split_builders_guard: split_builders_guard,
            },
        )
        .await?;
        self.counters.num_docs_in_workbench = 0;
        self.counters.num_doc_batches_in_workbench = 0;
        self.counters.num_splits_emitted += num_splits;
        self.counters.num_split_batches_emitted += 1;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Write;
    use std::sync::Arc;
    use std::time::Duration;

    use quickwit_actors::Universe;
    use quickwit_doc_mapper::{DocMapper, default_doc_mapper_for_test};
    use quickwit_metastore::checkpoint::SourceCheckpointDelta;
    use quickwit_proto::metastore::{
        EmptyResponse, LastDeleteOpstampResponse, MockMetastoreService,
    };
    use quickwit_proto::types::{IndexUid, NodeId, PipelineUid};
    use tantivy::{DateTime, doc};

    use super::*;
    use crate::actors::indexer::{IndexerCounters, record_timestamp};

    #[test]
    fn test_record_timestamp() {
        let mut time_range = None;
        record_timestamp(DateTime::from_timestamp_secs(1628664679), &mut time_range);
        assert_eq!(
            time_range,
            Some(
                DateTime::from_timestamp_secs(1628664679)
                    ..=DateTime::from_timestamp_secs(1628664679)
            )
        );
        record_timestamp(DateTime::from_timestamp_secs(1628664112), &mut time_range);
        assert_eq!(
            time_range,
            Some(
                DateTime::from_timestamp_secs(1628664112)
                    ..=DateTime::from_timestamp_secs(1628664679)
            )
        );
        record_timestamp(DateTime::from_timestamp_secs(1628665112), &mut time_range);
        assert_eq!(
            time_range,
            Some(
                DateTime::from_timestamp_secs(1628664112)
                    ..=DateTime::from_timestamp_secs(1628665112)
            )
        )
    }

    #[tokio::test]
    async fn test_indexer_triggers_commit_on_target_num_docs() -> anyhow::Result<()> {
        let index_uid = IndexUid::new_with_random_ulid("test-index");
        let pipeline_id = IndexingPipelineId {
            index_uid: index_uid.clone(),
            source_id: "test-source".to_string(),
            node_id: NodeId::from("test-node"),
            pipeline_uid: PipelineUid::default(),
        };
        let doc_mapper = Arc::new(default_doc_mapper_for_test());
        let last_delete_opstamp = 10;
        let schema = doc_mapper.schema();
        let body_field = schema.get_field("body").unwrap();
        let timestamp_field = schema.get_field("timestamp").unwrap();
        let indexing_directory = TempDirectory::for_test();
        let mut indexing_settings = IndexingSettings::for_test();
        indexing_settings.split_num_docs_target = 3;
        let universe = Universe::with_accelerated_time();
        let (index_serializer_mailbox, index_serializer_inbox) = universe.create_test_mailbox();
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore.expect_publish_splits().never();
        mock_metastore
            .expect_last_delete_opstamp()
            .times(2)
            .returning(move |delete_opstamp_request| {
                assert_eq!(delete_opstamp_request.index_uid(), &index_uid);
                Ok(LastDeleteOpstampResponse::new(last_delete_opstamp))
            });
        mock_metastore.expect_publish_splits().never();
        let indexer = Indexer::new(
            pipeline_id,
            doc_mapper,
            MetastoreServiceClient::from_mock(mock_metastore),
            indexing_directory,
            indexing_settings,
            None,
            index_serializer_mailbox,
        );
        let (indexer_mailbox, indexer_handle) = universe.spawn_builder().spawn(indexer);
        indexer_mailbox
            .send_message(ProcessedDocBatch::new(
                vec![
                    ProcessedDoc {
                        doc: doc!(
                            body_field=>"this is a test document",
                            timestamp_field=>DateTime::from_timestamp_secs(1_662_529_435)
                        ),
                        timestamp_opt: Some(DateTime::from_timestamp_secs(1_662_529_435)),
                        partition: 1,
                        num_bytes: 30,
                    },
                    ProcessedDoc {
                        doc: doc!(
                            body_field=>"this is a test document 2",
                            timestamp_field=>DateTime::from_timestamp_secs(1_662_529_435)
                        ),
                        timestamp_opt: Some(DateTime::from_timestamp_secs(1_662_529_435)),
                        partition: 1,
                        num_bytes: 30,
                    },
                ],
                SourceCheckpointDelta::from_range(4..6),
                false,
            ))
            .await?;
        indexer_mailbox
            .send_message(ProcessedDocBatch::new(
                vec![
                    ProcessedDoc {
                        doc: doc!(
                            body_field=>"this is a test document 3",
                            timestamp_field=>DateTime::from_timestamp_secs(1_662_529_435i64)
                        ),
                        timestamp_opt: Some(DateTime::from_timestamp_secs(1_662_529_435i64)),
                        partition: 1,
                        num_bytes: 30,
                    },
                    ProcessedDoc {
                        doc: doc!(
                            body_field=>"this is a test document 4",
                            timestamp_field=>DateTime::from_timestamp_secs(1_662_529_435)
                        ),
                        timestamp_opt: Some(DateTime::from_timestamp_secs(1_662_529_435)),
                        partition: 1,
                        num_bytes: 30,
                    },
                ],
                SourceCheckpointDelta::from_range(6..8),
                false,
            ))
            .await?;
        indexer_mailbox
            .send_message(ProcessedDocBatch::new(
                vec![ProcessedDoc {
                    doc: doc!(
                        body_field=>"this is a test document 5",
                        timestamp_field=>DateTime::from_timestamp_secs(1_662_529_435)
                    ),
                    timestamp_opt: Some(DateTime::from_timestamp_secs(1_662_529_435)),
                    partition: 1,
                    num_bytes: 30,
                }],
                SourceCheckpointDelta::from_range(8..9),
                false,
            ))
            .await?;
        let indexer_counters = indexer_handle.process_pending_and_observe().await.state;
        assert_eq!(
            indexer_counters,
            IndexerCounters {
                num_splits_emitted: 1,
                num_split_batches_emitted: 1,
                num_docs_in_workbench: 1, //< the num docs in split counter has been reset.
                num_doc_batches_in_workbench: 1, //< the num docs in split counter has been reset.
                pipeline_metrics_opt: None,
            }
        );
        let messages: Vec<IndexedSplitBatchBuilder> = index_serializer_inbox.drain_for_test_typed();
        assert_eq!(messages.len(), 1);
        let batch = messages.into_iter().next().unwrap();
        assert_eq!(batch.commit_trigger, CommitTrigger::NumDocsLimit);
        assert_eq!(batch.splits[0].split_attrs.num_docs, 4);
        for split in batch.splits.iter() {
            assert_eq!(split.split_attrs.delete_opstamp, last_delete_opstamp);
        }
        let index_checkpoint = batch.checkpoint_delta_opt.unwrap();
        assert_eq!(index_checkpoint.source_id, "test-source");
        assert_eq!(
            index_checkpoint.source_delta,
            SourceCheckpointDelta::from_range(4..8)
        );
        batch.splits.into_iter().next().unwrap().finalize()?;
        universe.assert_quit().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_indexer_triggers_commit_on_memory_limit() -> anyhow::Result<()> {
        let universe = Universe::new();
        let index_uid = IndexUid::new_with_random_ulid("test-index");
        let pipeline_id = IndexingPipelineId {
            index_uid: index_uid.clone(),
            source_id: "test-source".to_string(),
            node_id: NodeId::from("test-node"),
            pipeline_uid: PipelineUid::default(),
        };
        let doc_mapper = Arc::new(default_doc_mapper_for_test());
        let last_delete_opstamp = 10;
        let schema = doc_mapper.schema();
        let body_field = schema.get_field("body").unwrap();
        let indexing_directory = TempDirectory::for_test();
        let mut indexing_settings = IndexingSettings::for_test();
        indexing_settings.resources.heap_size = ByteSize::mb(16);
        let (index_serializer_mailbox, index_serializer_inbox) = universe.create_test_mailbox();
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore.expect_publish_splits().never();
        mock_metastore
            .expect_last_delete_opstamp()
            .times(1..=2)
            .returning(move |last_delete_opstamp_request| {
                assert_eq!(last_delete_opstamp_request.index_uid(), &index_uid);
                Ok(LastDeleteOpstampResponse::new(last_delete_opstamp))
            });
        mock_metastore.expect_publish_splits().never();
        let indexer = Indexer::new(
            pipeline_id,
            doc_mapper,
            MetastoreServiceClient::from_mock(mock_metastore),
            indexing_directory,
            indexing_settings,
            None,
            index_serializer_mailbox,
        );
        let (indexer_mailbox, _indexer_handle) = universe.spawn_builder().spawn(indexer);

        let make_doc = |i: u64| {
            let mut body = String::new();
            for val in 100 * i..100 * (i + 1) {
                write!(&mut body, "{val} ").unwrap();
            }
            let num_bytes = body.len() * 2;
            ProcessedDoc {
                doc: doc!(body_field=>body),
                timestamp_opt: None,
                partition: 0,
                num_bytes,
            }
        };
        for i in 0..10_000 {
            indexer_mailbox
                .send_message(ProcessedDocBatch::new(
                    vec![make_doc(i)],
                    SourceCheckpointDelta::from_range(i..i + 1),
                    false,
                ))
                .await?;
            let output_messages: Vec<IndexedSplitBatchBuilder> =
                index_serializer_inbox.drain_for_test_typed();
            if !output_messages.is_empty() {
                assert_eq!(output_messages.len(), 1);
                assert_eq!(
                    output_messages[0].commit_trigger,
                    CommitTrigger::MemoryLimit
                );
                // The following assert is not a strict one. It should help detect large
                // regression in memory usage.
                assert!((500..3_000).contains(&i));
                break;
            }
        }
        universe.assert_quit().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_indexer_triggers_commit_on_timeout() -> anyhow::Result<()> {
        let universe = Universe::new();
        let pipeline_id = IndexingPipelineId {
            index_uid: IndexUid::new_with_random_ulid("test-index"),
            source_id: "test-source".to_string(),
            node_id: NodeId::from("test-node"),
            pipeline_uid: PipelineUid::default(),
        };
        let doc_mapper = Arc::new(default_doc_mapper_for_test());
        let last_delete_opstamp = 10;
        let schema = doc_mapper.schema();
        let body_field = schema.get_field("body").unwrap();
        let timestamp_field = schema.get_field("timestamp").unwrap();
        let indexing_directory = TempDirectory::for_test();
        let mut indexing_settings = IndexingSettings::for_test();
        indexing_settings.commit_timeout_secs = 1;
        let (index_serializer_mailbox, index_serializer_inbox) = universe.create_test_mailbox();
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore.expect_publish_splits().never();
        mock_metastore.expect_last_delete_opstamp().returning(
            move |_last_delete_opstamp_request| {
                Ok(LastDeleteOpstampResponse::new(last_delete_opstamp))
            },
        );
        mock_metastore.expect_publish_splits().never();
        let indexer = Indexer::new(
            pipeline_id,
            doc_mapper,
            MetastoreServiceClient::from_mock(mock_metastore),
            indexing_directory,
            indexing_settings,
            None,
            index_serializer_mailbox,
        );
        let (indexer_mailbox, indexer_handle) = universe.spawn_builder().spawn(indexer);
        tokio::task::spawn({
            let indexer_mailbox = indexer_mailbox.clone();
            async move {
                let mut position = 0;
                while indexer_mailbox
                    .send_message(ProcessedDocBatch::new(
                        vec![ProcessedDoc {
                            doc: doc!(
                                body_field=>"this is a test document",
                                timestamp_field=>DateTime::from_timestamp_secs(1_662_529_435)
                            ),
                            timestamp_opt: Some(DateTime::from_timestamp_secs(1_662_529_435)),
                            partition: 1,
                            num_bytes: 30,
                        }],
                        SourceCheckpointDelta::from_range(position..position + 1),
                        false,
                    ))
                    .await
                    .is_ok()
                {
                    position += 1;
                }
            }
        });
        universe.sleep(Duration::from_secs(3)).await;

        let indexer_counters = indexer_handle.process_pending_and_observe().await.state;
        assert!(indexer_counters.num_splits_emitted > 0);
        assert!(indexer_counters.num_split_batches_emitted > 0);

        let indexed_serializer_messages: Vec<IndexedSplitBatchBuilder> =
            index_serializer_inbox.drain_for_test_typed();
        assert!(!indexed_serializer_messages.is_empty());
        assert_eq!(
            indexed_serializer_messages[0].commit_trigger,
            CommitTrigger::Timeout
        );
        assert!(
            indexed_serializer_messages[0].splits[0]
                .split_attrs
                .num_docs
                > 0
        );
        universe.assert_quit().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_indexer_triggers_commit_on_drained_mailbox() -> anyhow::Result<()> {
        let universe = Universe::new();
        let pipeline_id = IndexingPipelineId {
            index_uid: IndexUid::new_with_random_ulid("test-index"),
            source_id: "test-source".to_string(),
            node_id: NodeId::from("test-node"),
            pipeline_uid: PipelineUid::default(),
        };
        let doc_mapper = Arc::new(default_doc_mapper_for_test());
        let last_delete_opstamp = 10;
        let schema = doc_mapper.schema();
        let body_field = schema.get_field("body").unwrap();
        let timestamp_field = schema.get_field("timestamp").unwrap();
        let indexing_directory = TempDirectory::for_test();
        let indexing_settings = IndexingSettings::for_test();
        let (index_serializer_mailbox, index_serializer_inbox) = universe.create_test_mailbox();
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore.expect_publish_splits().never();
        mock_metastore.expect_last_delete_opstamp().returning(
            move |_last_delete_opstamp_request| {
                Ok(LastDeleteOpstampResponse::new(last_delete_opstamp))
            },
        );
        let indexer = Indexer::new(
            pipeline_id,
            doc_mapper,
            MetastoreServiceClient::from_mock(mock_metastore),
            indexing_directory,
            indexing_settings,
            Some(Arc::new(Semaphore::new(1))),
            index_serializer_mailbox,
        );
        let (indexer_mailbox, indexer_handle) = universe.spawn_builder().spawn(indexer);
        indexer_mailbox
            .send_message(ProcessedDocBatch::new(
                vec![ProcessedDoc {
                    doc: doc!(
                        body_field=>"this is a test document 5",
                        timestamp_field=>DateTime::from_timestamp_secs(1_662_529_435)
                    ),
                    timestamp_opt: Some(DateTime::from_timestamp_secs(1_662_529_435)),
                    partition: 1,
                    num_bytes: 30,
                }],
                SourceCheckpointDelta::from_range(8..9),
                false,
            ))
            .await
            .unwrap();
        let mut indexer_counters: IndexerCounters = Default::default();
        for _ in 0..100 {
            // When a lot of unit tests are running concurrently we have a race condition here.
            // It is very difficult to assess when drain will actually be called.
            //
            // Therefore we check that it happens "eventually".
            universe.sleep(Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
            indexer_counters = indexer_handle.observe().await.state;
            indexer_counters.pipeline_metrics_opt = None;
            // drain was called at least once.
            if indexer_counters.num_splits_emitted > 0 {
                break;
            }
        }

        assert_eq!(
            &indexer_counters,
            &IndexerCounters {
                num_splits_emitted: 1,
                num_split_batches_emitted: 1,
                num_docs_in_workbench: 0,
                num_doc_batches_in_workbench: 0,
                pipeline_metrics_opt: None,
            }
        );
        let indexed_split_batches: Vec<IndexedSplitBatchBuilder> =
            index_serializer_inbox.drain_for_test_typed();
        assert_eq!(indexed_split_batches.len(), 1);
        assert_eq!(
            indexed_split_batches[0].commit_trigger,
            CommitTrigger::Drained
        );
        assert_eq!(indexed_split_batches[0].splits[0].split_attrs.num_docs, 1);
        universe.assert_quit().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_indexer_triggers_commit_on_quit() -> anyhow::Result<()> {
        let universe = Universe::with_accelerated_time();
        let pipeline_id = IndexingPipelineId {
            index_uid: IndexUid::new_with_random_ulid("test-index"),
            source_id: "test-source".to_string(),
            node_id: NodeId::from("test-node"),
            pipeline_uid: PipelineUid::default(),
        };
        let doc_mapper = Arc::new(default_doc_mapper_for_test());
        let schema = doc_mapper.schema();
        let body_field = schema.get_field("body").unwrap();
        let timestamp_field = schema.get_field("timestamp").unwrap();
        let indexing_directory = TempDirectory::for_test();
        let indexing_settings = IndexingSettings::for_test();
        let (index_serializer_mailbox, index_serializer_inbox) = universe.create_test_mailbox();
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore.expect_publish_splits().never();
        mock_metastore
            .expect_last_delete_opstamp()
            .once()
            .returning(move |_last_delete_opstamp_request| Ok(LastDeleteOpstampResponse::new(10)));
        mock_metastore.expect_publish_splits().never();
        let indexer = Indexer::new(
            pipeline_id,
            doc_mapper,
            MetastoreServiceClient::from_mock(mock_metastore),
            indexing_directory,
            indexing_settings,
            None,
            index_serializer_mailbox,
        );
        let (indexer_mailbox, indexer_handle) = universe.spawn_builder().spawn(indexer);
        indexer_mailbox
            .send_message(ProcessedDocBatch::new(
                vec![ProcessedDoc {
                    doc: doc!(
                        body_field=>"this is a test document 5",
                        timestamp_field=> DateTime::from_timestamp_secs(1_662_529_435)
                    ),
                    timestamp_opt: Some(DateTime::from_timestamp_secs(1_662_529_435)),
                    partition: 1,
                    num_bytes: 30,
                }],
                SourceCheckpointDelta::from_range(8..9),
                false,
            ))
            .await
            .unwrap();
        universe.send_exit_with_success(&indexer_mailbox).await?;
        let (exit_status, indexer_counters) = indexer_handle.join().await;
        assert!(exit_status.is_success());
        assert_eq!(
            indexer_counters,
            IndexerCounters {
                num_splits_emitted: 1,
                num_split_batches_emitted: 1,
                num_docs_in_workbench: 0,
                num_doc_batches_in_workbench: 0,
                pipeline_metrics_opt: None,
            }
        );
        let output_messages: Vec<IndexedSplitBatchBuilder> =
            index_serializer_inbox.drain_for_test_typed();
        assert_eq!(output_messages.len(), 1);
        assert_eq!(output_messages[0].commit_trigger, CommitTrigger::NoMoreDocs);
        assert_eq!(output_messages[0].splits[0].split_attrs.num_docs, 1);
        universe.assert_quit().await;
        Ok(())
    }

    const DOCMAPPER_WITH_PARTITION_JSON: &str = r#"{
        "tag_fields": ["tenant"],
        "partition_key": "tenant",
        "field_mappings": [
            { "name": "tenant", "type": "text", "tokenizer": "raw", "indexed": true },
            { "name": "body", "type": "text" }
        ]
    }"#;

    #[tokio::test]
    async fn test_indexer_partitioning() -> anyhow::Result<()> {
        let universe = Universe::with_accelerated_time();
        let pipeline_id = IndexingPipelineId {
            index_uid: IndexUid::new_with_random_ulid("test-index"),
            source_id: "test-source".to_string(),
            node_id: NodeId::from("test-node"),
            pipeline_uid: PipelineUid::default(),
        };
        let doc_mapper: Arc<DocMapper> =
            Arc::new(serde_json::from_str::<DocMapper>(DOCMAPPER_WITH_PARTITION_JSON).unwrap());
        let schema = doc_mapper.schema();
        let tenant_field = schema.get_field("tenant").unwrap();
        let body_field = schema.get_field("body").unwrap();

        let indexing_directory = TempDirectory::for_test();
        let mut indexing_settings = IndexingSettings::for_test();
        indexing_settings.resources.heap_size = ByteSize::mb(100);
        let (index_serializer_mailbox, index_serializer_inbox) = universe.create_test_mailbox();
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore.expect_publish_splits().never();
        mock_metastore
            .expect_last_delete_opstamp()
            .once()
            .returning(move |_last_delete_opstamp_request| Ok(LastDeleteOpstampResponse::new(10)));
        mock_metastore.expect_publish_splits().never();
        let indexer = Indexer::new(
            pipeline_id,
            doc_mapper,
            MetastoreServiceClient::from_mock(mock_metastore),
            indexing_directory,
            indexing_settings,
            None,
            index_serializer_mailbox,
        );
        let (indexer_mailbox, indexer_handle) = universe.spawn_builder().spawn(indexer);
        indexer_mailbox
            .send_message(ProcessedDocBatch::new(
                vec![
                    ProcessedDoc {
                        doc: doc!(
                            body_field=>"doc 2",
                            tenant_field=>"tenant_1",
                        ),
                        timestamp_opt: None,
                        partition: 1,
                        num_bytes: 30,
                    },
                    ProcessedDoc {
                        doc: doc!(
                            body_field=>"doc 2",
                            tenant_field=>"tenant_2",
                        ),
                        timestamp_opt: None,
                        partition: 3,
                        num_bytes: 30,
                    },
                ],
                SourceCheckpointDelta::from_range(8..9),
                false,
            ))
            .await?;

        let indexer_counters = indexer_handle.process_pending_and_observe().await.state;
        assert_eq!(
            indexer_counters,
            IndexerCounters {
                num_docs_in_workbench: 2,
                num_doc_batches_in_workbench: 1,
                num_splits_emitted: 0,
                num_split_batches_emitted: 0,
                pipeline_metrics_opt: None,
            }
        );
        universe.send_exit_with_success(&indexer_mailbox).await?;
        let (exit_status, indexer_counters) = indexer_handle.join().await;
        assert!(matches!(exit_status, ActorExitStatus::Success));
        assert_eq!(
            indexer_counters,
            IndexerCounters {
                num_docs_in_workbench: 0,
                num_doc_batches_in_workbench: 0,
                num_splits_emitted: 2,
                num_split_batches_emitted: 1,
                pipeline_metrics_opt: None,
            }
        );
        let split_batches: Vec<IndexedSplitBatchBuilder> =
            index_serializer_inbox.drain_for_test_typed();
        assert_eq!(split_batches.len(), 1);
        assert_eq!(split_batches[0].splits.len(), 2);
        universe.assert_quit().await;
        Ok(())
    }

    const DOCMAPPER_SIMPLE_JSON: &str = r#"{
        "field_mappings": [{"name": "body", "type": "text"}],
        "max_num_partitions": 10
    }"#;

    #[tokio::test]
    async fn test_indexer_exceeding_max_num_partitions() {
        let universe = Universe::with_accelerated_time();
        let pipeline_id = IndexingPipelineId {
            index_uid: IndexUid::new_with_random_ulid("test-index"),
            source_id: "test-source".to_string(),
            node_id: NodeId::from("test-node"),
            pipeline_uid: PipelineUid::default(),
        };
        let doc_mapper: Arc<DocMapper> =
            Arc::new(serde_json::from_str::<DocMapper>(DOCMAPPER_SIMPLE_JSON).unwrap());
        let body_field = doc_mapper.schema().get_field("body").unwrap();
        let indexing_directory = TempDirectory::for_test();
        let mut indexing_settings = IndexingSettings::for_test();
        indexing_settings.resources.heap_size = ByteSize::gb(5);
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_last_delete_opstamp()
            .times(1)
            .returning(move |_last_delete_opstamp_request| Ok(LastDeleteOpstampResponse::new(10)));
        mock_metastore.expect_publish_splits().never();
        let (index_serializer_mailbox, index_serializer_inbox) = universe.create_test_mailbox();
        let indexer = Indexer::new(
            pipeline_id,
            doc_mapper,
            MetastoreServiceClient::from_mock(mock_metastore),
            indexing_directory,
            indexing_settings,
            None,
            index_serializer_mailbox,
        );
        let (indexer_mailbox, indexer_handle) = universe.spawn_builder().spawn(indexer);

        for partition in 0..100 {
            indexer_mailbox
                .send_message(ProcessedDocBatch::new(
                    vec![ProcessedDoc {
                        doc: doc!(body_field=>"doc {i}"),
                        timestamp_opt: None,
                        partition,
                        num_bytes: 30,
                    }],
                    SourceCheckpointDelta::from_range(partition..partition + 1),
                    false,
                ))
                .await
                .unwrap();
        }
        universe
            .send_exit_with_success(&indexer_mailbox)
            .await
            .unwrap();

        let (exit_status, _indexer_counters) = indexer_handle.join().await;
        assert!(matches!(exit_status, ActorExitStatus::Success));

        let index_serializer_msgs: Vec<IndexedSplitBatchBuilder> =
            index_serializer_inbox.drain_for_test_typed();
        assert_eq!(index_serializer_msgs.len(), 1);
        let msg = index_serializer_msgs.into_iter().next().unwrap();
        assert_eq!(msg.splits.len(), 11);
        for split in msg.splits {
            if split.split_attrs.partition_id == OTHER_PARTITION_ID {
                assert_eq!(split.split_attrs.num_docs, 90);
            } else {
                assert_eq!(split.split_attrs.num_docs, 1);
            }
        }
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_indexer_propagates_publish_lock() {
        let universe = Universe::with_accelerated_time();
        let pipeline_id = IndexingPipelineId {
            index_uid: IndexUid::new_with_random_ulid("test-index"),
            source_id: "test-source".to_string(),
            node_id: NodeId::from("test-node"),
            pipeline_uid: PipelineUid::default(),
        };
        let doc_mapper: Arc<DocMapper> =
            Arc::new(serde_json::from_str::<DocMapper>(DOCMAPPER_SIMPLE_JSON).unwrap());
        let body_field = doc_mapper.schema().get_field("body").unwrap();
        let indexing_directory = TempDirectory::for_test();
        let mut indexing_settings = IndexingSettings::for_test();
        indexing_settings.split_num_docs_target = 1;
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_last_delete_opstamp()
            .times(2)
            .returning(move |_last_delete_opstamp_request| Ok(LastDeleteOpstampResponse::new(10)));
        mock_metastore.expect_publish_splits().never();
        let (index_serializer_mailbox, index_serializer_inbox) = universe.create_test_mailbox();
        let indexer = Indexer::new(
            pipeline_id,
            doc_mapper,
            MetastoreServiceClient::from_mock(mock_metastore),
            indexing_directory,
            indexing_settings,
            None,
            index_serializer_mailbox,
        );
        let (indexer_mailbox, indexer_handle) = universe.spawn_builder().spawn(indexer);

        let first_lock = PublishLock::default();
        let second_lock = PublishLock::default();

        for lock in [&first_lock, &second_lock] {
            indexer_mailbox
                .send_message(NewPublishLock(lock.clone()))
                .await
                .unwrap();
            indexer_mailbox
                .send_message(ProcessedDocBatch::new(
                    vec![ProcessedDoc {
                        doc: doc!(body_field=>"doc 1"),
                        timestamp_opt: None,
                        partition: 0,
                        num_bytes: 30,
                    }],
                    SourceCheckpointDelta::from_range(0..1),
                    false,
                ))
                .await
                .unwrap();
        }
        universe
            .send_exit_with_success(&indexer_mailbox)
            .await
            .unwrap();
        let (exit_status, _indexer_counters) = indexer_handle.join().await;
        assert!(matches!(exit_status, ActorExitStatus::Success));

        let index_serializer_messages: Vec<IndexedSplitBatchBuilder> =
            index_serializer_inbox.drain_for_test_typed();
        assert_eq!(index_serializer_messages.len(), 2);
        assert_eq!(index_serializer_messages[0].splits.len(), 1);
        assert_eq!(index_serializer_messages[0].publish_lock, first_lock);
        assert_eq!(index_serializer_messages[1].splits.len(), 1);
        assert_eq!(index_serializer_messages[1].publish_lock, second_lock);
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_indexer_ignores_messages_when_publish_lock_is_dead() {
        let universe = Universe::with_accelerated_time();
        let pipeline_id = IndexingPipelineId {
            index_uid: IndexUid::new_with_random_ulid("test-index"),
            source_id: "test-source".to_string(),
            node_id: NodeId::from("test-node"),
            pipeline_uid: PipelineUid::default(),
        };
        let doc_mapper: Arc<DocMapper> =
            Arc::new(serde_json::from_str::<DocMapper>(DOCMAPPER_SIMPLE_JSON).unwrap());
        let body_field = doc_mapper.schema().get_field("body").unwrap();
        let indexing_directory = TempDirectory::for_test();
        let mut indexing_settings = IndexingSettings::for_test();
        indexing_settings.split_num_docs_target = 1;
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_last_delete_opstamp()
            .times(1)
            .returning(move |_last_delete_opstamp_request| Ok(LastDeleteOpstampResponse::new(10)));
        mock_metastore.expect_publish_splits().never();
        let (index_serializer_mailbox, index_serializer_inbox) = universe.create_test_mailbox();
        let indexer = Indexer::new(
            pipeline_id,
            doc_mapper,
            MetastoreServiceClient::from_mock(mock_metastore),
            indexing_directory,
            indexing_settings,
            None,
            index_serializer_mailbox,
        );
        let (indexer_mailbox, indexer_handle) = universe.spawn_builder().spawn(indexer);

        let publish_lock = PublishLock::default();
        indexer_mailbox
            .send_message(NewPublishLock(publish_lock.clone()))
            .await
            .unwrap();
        indexer_handle.process_pending_and_observe().await;
        publish_lock.kill().await;
        indexer_mailbox
            .send_message(ProcessedDocBatch::new(
                vec![ProcessedDoc {
                    doc: doc!(body_field=>"doc 1"),
                    timestamp_opt: None,
                    partition: 0,
                    num_bytes: 30,
                }],
                SourceCheckpointDelta::from_range(0..1),
                false,
            ))
            .await
            .unwrap();
        universe
            .send_exit_with_success(&indexer_mailbox)
            .await
            .unwrap();
        let (exit_status, _indexer_counters) = indexer_handle.join().await;
        assert!(matches!(exit_status, ActorExitStatus::Success));

        let index_serializer_messages = index_serializer_inbox.drain_for_test();
        assert!(index_serializer_messages.is_empty());
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_indexer_honors_batch_commit_request() {
        let universe = Universe::with_accelerated_time();
        let pipeline_id = IndexingPipelineId {
            index_uid: IndexUid::new_with_random_ulid("test-index"),
            source_id: "test-source".to_string(),
            node_id: NodeId::from("test-node"),
            pipeline_uid: PipelineUid::default(),
        };
        let doc_mapper: Arc<DocMapper> =
            Arc::new(serde_json::from_str::<DocMapper>(DOCMAPPER_SIMPLE_JSON).unwrap());
        let body_field = doc_mapper.schema().get_field("body").unwrap();
        let indexing_directory = TempDirectory::for_test();
        let indexing_settings = IndexingSettings::for_test();
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_last_delete_opstamp()
            .times(1)
            .returning(move |_last_delete_opstamp_request| Ok(LastDeleteOpstampResponse::new(10)));
        mock_metastore.expect_publish_splits().never();
        let (index_serializer_mailbox, index_serializer_inbox) = universe.create_test_mailbox();
        let indexer = Indexer::new(
            pipeline_id,
            doc_mapper,
            MetastoreServiceClient::from_mock(mock_metastore),
            indexing_directory,
            indexing_settings,
            None,
            index_serializer_mailbox,
        );
        let (indexer_mailbox, indexer_handle) = universe.spawn_builder().spawn(indexer);
        indexer_mailbox
            .send_message(ProcessedDocBatch::new(
                vec![ProcessedDoc {
                    doc: doc!(body_field=>"doc 1"),
                    timestamp_opt: None,
                    partition: 0,
                    num_bytes: 30,
                }],
                SourceCheckpointDelta::from_range(0..1),
                true,
            ))
            .await
            .unwrap();
        universe
            .send_exit_with_success(&indexer_mailbox)
            .await
            .unwrap();
        let (exit_status, _indexer_counters) = indexer_handle.join().await;
        assert!(matches!(exit_status, ActorExitStatus::Success));
        let output_messages: Vec<IndexedSplitBatchBuilder> =
            index_serializer_inbox.drain_for_test_typed();

        assert_eq!(output_messages.len(), 1);
        assert_eq!(
            output_messages[0].commit_trigger,
            CommitTrigger::ForceCommit
        );
        assert_eq!(output_messages[0].splits[0].split_attrs.num_docs, 1);
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_indexer_checkpoint_on_all_failed_docs() -> anyhow::Result<()> {
        let pipeline_id = IndexingPipelineId {
            index_uid: IndexUid::new_with_random_ulid("test-index"),
            source_id: "test-source".to_string(),
            node_id: NodeId::from("test-node"),
            pipeline_uid: PipelineUid::default(),
        };
        let doc_mapper = Arc::new(default_doc_mapper_for_test());
        let last_delete_opstamp = 10;
        let indexing_directory = TempDirectory::for_test();
        let indexing_settings = IndexingSettings::for_test();
        let commit_timeout = indexing_settings.commit_timeout();
        let universe = Universe::with_accelerated_time();
        let (index_serializer_mailbox, index_serializer_inbox) = universe.create_test_mailbox();
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_publish_splits()
            .returning(move |publish_splits_request| {
                assert!(publish_splits_request.replaced_split_ids.is_empty());
                Ok(EmptyResponse {})
            });
        mock_metastore.expect_last_delete_opstamp().returning(
            move |_last_delete_opstamp_request| {
                Ok(LastDeleteOpstampResponse::new(last_delete_opstamp))
            },
        );
        let indexer = Indexer::new(
            pipeline_id,
            doc_mapper,
            MetastoreServiceClient::from_mock(mock_metastore),
            indexing_directory,
            indexing_settings,
            None,
            index_serializer_mailbox,
        );
        let (indexer_mailbox, indexer_handle) = universe.spawn_builder().spawn(indexer);
        indexer_mailbox
            .send_message(ProcessedDocBatch::new(
                Vec::new(),
                SourceCheckpointDelta::from_range(4..6),
                false,
            ))
            .await?;
        indexer_mailbox
            .send_message(ProcessedDocBatch::new(
                Vec::new(),
                SourceCheckpointDelta::from_range(6..8),
                false,
            ))
            .await?;
        universe
            .sleep(commit_timeout + Duration::from_secs(2))
            .await;
        let indexer_counters = indexer_handle.process_pending_and_observe().await.state;
        assert_eq!(
            indexer_counters,
            IndexerCounters {
                num_splits_emitted: 0,
                num_split_batches_emitted: 0,
                num_docs_in_workbench: 0, //< the num docs in split counter has been reset.
                num_doc_batches_in_workbench: 2, //< the num docs in split counter has been reset.
                pipeline_metrics_opt: None,
            }
        );

        let index_serializer_messages: Vec<EmptySplit> =
            index_serializer_inbox.drain_for_test_typed();
        assert_eq!(index_serializer_messages.len(), 1);
        let update = index_serializer_messages.into_iter().next().unwrap();
        assert_eq!(update.index_uid.index_id, "test-index");
        assert_eq!(
            update.checkpoint_delta,
            IndexCheckpointDelta::for_test("test-source", 4..8)
        );

        universe.assert_quit().await;
        Ok(())
    }
}
