// Copyright (C) 2021 Quickwit, Inc.
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

use std::ops::RangeInclusive;
use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use fail::fail_point;
use quickwit_actors::{
    Actor, ActorContext, ActorExitStatus, ActorRunner, Handler, Mailbox, QueueCapacity, SendError,
};
use quickwit_config::IndexingSettings;
use quickwit_doc_mapper::{DocMapper, DocParsingError, SortBy};
use tantivy::schema::{Field, Value};
use tantivy::{Document, IndexBuilder, IndexSettings, IndexSortByField};
use tracing::{info, instrument, warn};

use crate::actors::Packager;
use crate::models::{IndexedSplit, IndexedSplitBatch, IndexingDirectory, RawDocBatch};

#[derive(Debug)]
struct CommitTimeout {
    split_id: String,
}

#[derive(Clone, Default, Debug, Eq, PartialEq)]
pub struct IndexerCounters {
    /// Overall number of documents received, partitionned
    /// into 3 categories:
    /// - number docs that did not parse correctly.
    /// - number docs missing a timestamp (if the index has no timestamp,
    /// then this counter is 0)
    /// - number of valid docs.
    pub num_parse_errors: u64,
    pub num_missing_fields: u64,
    pub num_valid_docs: u64,

    /// Number of splits that were emitted by the indexer.
    pub num_splits_emitted: u64,

    /// Number of bytes that went through the indexer
    /// during its entire lifetime.
    ///
    /// Includes both valid and invalid documents.
    pub overall_num_bytes: u64,

    /// Number of (valid) documents in the current split.
    /// This value is used to trigger commit and for observation.
    pub num_docs_in_split: u64,
}

impl IndexerCounters {
    /// Returns the overall number of docs that went through the indexer (valid or not).
    pub fn num_processed_docs(&self) -> u64 {
        self.num_valid_docs + self.num_parse_errors + self.num_missing_fields
    }

    /// Returns the overall number of docs that were sent to the indexer but were invalid.
    /// (For instance, because they were missing a required field or because their because
    /// their format was invalid)
    pub fn num_invalid_docs(&self) -> u64 {
        self.num_parse_errors + self.num_missing_fields
    }
}

struct IndexerState {
    index_id: String,
    doc_mapper: Arc<dyn DocMapper>,
    indexing_directory: IndexingDirectory,
    indexing_settings: IndexingSettings,
    timestamp_field_opt: Option<Field>,
    sort_by_field_opt: Option<IndexSortByField>,
}

enum PrepareDocumentOutcome {
    ParsingError,
    MissingField,
    Document {
        document: Document,
        timestamp_opt: Option<i64>,
    },
}

impl IndexerState {
    fn create_indexed_split(&self, ctx: &ActorContext<Indexer>) -> anyhow::Result<IndexedSplit> {
        let schema = self.doc_mapper.schema();
        let index_settings = IndexSettings {
            sort_by_field: self.sort_by_field_opt.clone(),
            ..Default::default()
        };
        let index_builder = IndexBuilder::new().settings(index_settings).schema(schema);
        let indexed_split = IndexedSplit::new_in_dir(
            self.index_id.clone(),
            self.indexing_directory.scratch_directory.clone(),
            self.indexing_settings.resources.clone(),
            index_builder,
            ctx.progress().clone(),
            ctx.kill_switch().clone(),
        )?;
        info!(split_id = %indexed_split.split_id, "new-split");
        Ok(indexed_split)
    }

    /// Returns the current_indexed_split. If this is the first message, then
    /// the indexed_split does not exist yet.
    ///
    /// This function will then create it, and can hence return an Error.
    async fn get_or_create_current_indexed_split<'a>(
        &self,
        current_split_opt: &'a mut Option<IndexedSplit>,
        ctx: &ActorContext<Indexer>,
    ) -> anyhow::Result<&'a mut IndexedSplit> {
        if current_split_opt.is_none() {
            let new_indexed_split = self.create_indexed_split(ctx)?;
            let commit_timeout_message = CommitTimeout {
                split_id: new_indexed_split.split_id.clone(),
            };
            ctx.schedule_self_msg(
                self.indexing_settings.commit_timeout(),
                commit_timeout_message,
            )
            .await;
            *current_split_opt = Some(new_indexed_split);
        }
        let current_index_split = current_split_opt.as_mut().with_context(|| {
            "No index writer available. Please report: this should never happen."
        })?;
        Ok(current_index_split)
    }

    fn prepare_document(&self, doc_json: String) -> PrepareDocumentOutcome {
        // Parse the document
        let doc_parsing_result = self.doc_mapper.doc_from_json(doc_json);
        let document = match doc_parsing_result {
            Ok(doc) => doc,
            Err(doc_parsing_error) => {
                warn!(err=?doc_parsing_error);
                return match doc_parsing_error {
                    DocParsingError::RequiredFastField(_) => PrepareDocumentOutcome::MissingField,
                    _ => PrepareDocumentOutcome::ParsingError,
                };
            }
        };
        // Extract timestamp if necessary
        let timestamp_field = if let Some(timestamp_field) = self.timestamp_field_opt {
            timestamp_field
        } else {
            // No need to check the timestamp, there are no timestamp.
            return PrepareDocumentOutcome::Document {
                document,
                timestamp_opt: None,
            };
        };
        let timestamp_opt = document.get_first(timestamp_field).and_then(Value::as_i64);
        assert!(
            timestamp_opt.is_some(),
            "We should always have a timestamp here as doc parsing returns a `RequiredFastField` \
             error on a missing timestamp."
        );
        PrepareDocumentOutcome::Document {
            document,
            timestamp_opt,
        }
    }

    #[instrument("process-batch", level = "debug", skip_all)]
    async fn process_batch(
        &self,
        batch: RawDocBatch,
        current_split_opt: &mut Option<IndexedSplit>,
        counters: &mut IndexerCounters,
        ctx: &ActorContext<Indexer>,
    ) -> Result<(), ActorExitStatus> {
        let indexed_split = self
            .get_or_create_current_indexed_split(current_split_opt, ctx)
            .await?;
        indexed_split
            .checkpoint_delta
            .extend(batch.checkpoint_delta)
            .with_context(|| "Batch delta does not follow indexer checkpoint")?;
        for doc_json in batch.docs {
            counters.overall_num_bytes += doc_json.len() as u64;
            indexed_split.docs_size_in_bytes += doc_json.len() as u64;
            let prepared_doc = {
                let _protect_zone = ctx.protect_zone();
                self.prepare_document(doc_json)
            };
            match prepared_doc {
                PrepareDocumentOutcome::ParsingError => {
                    counters.num_parse_errors += 1;
                }
                PrepareDocumentOutcome::MissingField => {
                    counters.num_missing_fields += 1;
                }
                PrepareDocumentOutcome::Document {
                    document,
                    timestamp_opt,
                } => {
                    counters.num_docs_in_split += 1;
                    counters.num_valid_docs += 1;
                    indexed_split.num_docs += 1;
                    if let Some(timestamp) = timestamp_opt {
                        record_timestamp(timestamp, &mut indexed_split.time_range);
                    }
                    let _protect_guard = ctx.protect_zone();
                    indexed_split
                        .index_writer
                        .add_document(document)
                        .with_context(|| "Failed to add document.")?;
                }
            }
            ctx.record_progress();
        }
        Ok(())
    }
}

pub struct Indexer {
    indexer_state: IndexerState,
    packager_mailbox: Mailbox<Packager>,
    current_split_opt: Option<IndexedSplit>,
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

    fn runner(&self) -> ActorRunner {
        ActorRunner::DedicatedThread
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
                self.send_to_packager(CommitTrigger::NoMoreDocs, ctx)
                    .await?;
            }
        }
        Ok(())
    }
}

fn record_timestamp(timestamp: i64, time_range: &mut Option<RangeInclusive<i64>>) {
    let new_timestamp_range = match time_range.as_ref() {
        Some(range) => {
            RangeInclusive::new(timestamp.min(*range.start()), timestamp.max(*range.end()))
        }
        None => RangeInclusive::new(timestamp, timestamp),
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
        self.process_commit_timeout(&commit_timeout.split_id, ctx)
            .await?;
        Ok(())
    }
}

#[async_trait]
impl Handler<RawDocBatch> for Indexer {
    type Reply = ();

    async fn handle(
        &mut self,
        batch: RawDocBatch,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        self.process_batch(batch, ctx).await
    }
}

#[derive(Debug, Clone, Copy)]
enum CommitTrigger {
    Timeout,
    NoMoreDocs,
    NumDocsLimit,
}

impl Indexer {
    pub fn new(
        index_id: String,
        doc_mapper: Arc<dyn DocMapper>,
        indexing_directory: IndexingDirectory,
        indexing_settings: IndexingSettings,
        packager_mailbox: Mailbox<Packager>,
    ) -> Self {
        let schema = doc_mapper.schema();
        let timestamp_field_opt = doc_mapper.timestamp_field(&schema);
        let sort_by_field_opt = match indexing_settings.sort_by() {
            SortBy::DocId => None,
            SortBy::FastField { field_name, order } => Some(IndexSortByField {
                field: field_name,
                order: order.into(),
            }),
        };
        Self {
            indexer_state: IndexerState {
                index_id,
                doc_mapper,
                indexing_directory,
                indexing_settings,
                timestamp_field_opt,
                sort_by_field_opt,
            },
            packager_mailbox,
            current_split_opt: None,
            counters: IndexerCounters::default(),
        }
    }

    async fn process_batch(
        &mut self,
        batch: RawDocBatch,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        fail_point!("indexer:batch:before");
        self.indexer_state
            .process_batch(batch, &mut self.current_split_opt, &mut self.counters, ctx)
            .await?;
        if self.counters.num_docs_in_split
            >= self.indexer_state.indexing_settings.split_num_docs_target as u64
        {
            self.send_to_packager(CommitTrigger::NumDocsLimit, ctx)
                .await?;
        }
        fail_point!("indexer:batch:after");
        Ok(())
    }

    async fn process_commit_timeout(
        &mut self,
        split_id: &str,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        info!("commit-timeout");
        if let Some(split) = self.current_split_opt.as_ref() {
            // This is a timeout for a different split.
            // We can ignore it.
            if split.split_id != split_id {
                return Ok(());
            }
        }
        self.send_to_packager(CommitTrigger::Timeout, ctx).await?;
        Ok(())
    }

    /// Extract the indexed split and send it to the Packager.
    async fn send_to_packager(
        &mut self,
        commit_trigger: CommitTrigger,
        ctx: &ActorContext<Self>,
    ) -> Result<(), SendError> {
        let indexed_split = if let Some(indexed_split) = self.current_split_opt.take() {
            indexed_split
        } else {
            return Ok(());
        };
        info!(commit_trigger=?commit_trigger, split=?indexed_split.split_id, num_docs=self.counters.num_docs_in_split, "send-to-packager");
        ctx.send_message(
            &self.packager_mailbox,
            IndexedSplitBatch {
                splits: vec![indexed_split],
            },
        )
        .await?;
        self.counters.num_docs_in_split = 0;
        self.counters.num_splits_emitted += 1;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use quickwit_actors::{create_test_mailbox, Universe};
    use quickwit_doc_mapper::SortOrder;
    use quickwit_metastore::checkpoint::CheckpointDelta;

    use super::*;
    use crate::actors::indexer::{record_timestamp, IndexerCounters};
    use crate::models::{IndexingDirectory, RawDocBatch};

    #[test]
    fn test_record_timestamp() {
        let mut time_range = None;
        record_timestamp(1628664679, &mut time_range);
        assert_eq!(time_range, Some(1628664679..=1628664679));
        record_timestamp(1628664112, &mut time_range);
        assert_eq!(time_range, Some(1628664112..=1628664679));
        record_timestamp(1628665112, &mut time_range);
        assert_eq!(time_range, Some(1628664112..=1628665112))
    }

    #[tokio::test]
    async fn test_indexer_simple() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let doc_mapper = Arc::new(quickwit_doc_mapper::default_doc_mapper_for_tests());
        let indexing_directory = IndexingDirectory::for_test().await?;
        let mut indexing_settings = IndexingSettings::for_test();
        indexing_settings.split_num_docs_target = 3;
        indexing_settings.sort_field = Some("timestamp".to_string());
        indexing_settings.sort_order = Some(SortOrder::Desc);
        indexing_settings.timestamp_field = Some("timestamp".to_string());
        let (mailbox, inbox) = create_test_mailbox();
        let indexer = Indexer::new(
            "test-index".to_string(),
            doc_mapper,
            indexing_directory,
            indexing_settings,
            mailbox,
        );
        let universe = Universe::new();
        let (indexer_mailbox, indexer_handle) = universe.spawn_actor(indexer).spawn();
        indexer_mailbox
            .send_message(RawDocBatch {
                docs: vec![
                        r#"{"body": "happy", "response_date": "2021-12-19T16:39:57+00:00", "response_time": 12, "response_payload": "YWJj"}"#.to_string(), // missing timestamp
                        r#"{"body": "happy", "timestamp": 1628837062, "response_date": "2021-12-19T16:39:59+00:00", "response_time": 2, "response_payload": "YWJj"}"#.to_string(), // ok
                        r#"{"body": "happy2", "timestamp": 1628837062, "response_date": "2021-12-19T16:40:57+00:00", "response_time": 13, "response_payload": "YWJj"}"#.to_string(), // ok
                        "{".to_string(),                    // invalid json
                    ],
                checkpoint_delta: CheckpointDelta::from(0..4),
            })
            .await?;
        let indexer_counters = indexer_handle.process_pending_and_observe().await.state;
        assert_eq!(
            indexer_counters,
            IndexerCounters {
                num_parse_errors: 1,
                num_missing_fields: 1,
                num_valid_docs: 2,
                num_splits_emitted: 0,
                num_docs_in_split: 2, //< we have not reached the commit limit yet.
                overall_num_bytes: 387
            }
        );
        indexer_mailbox
            .send_message(
                RawDocBatch {
                    docs: vec![r#"{"body": "happy3", "timestamp": 1628837062, "response_date": "2021-12-19T16:39:57+00:00", "response_time": 12, "response_payload": "YWJj"}"#.to_string()],
                    checkpoint_delta: CheckpointDelta::from(4..5),
                }
            )
            .await?;
        let indexer_counters = indexer_handle.process_pending_and_observe().await.state;
        assert_eq!(
            indexer_counters,
            IndexerCounters {
                num_parse_errors: 1,
                num_missing_fields: 1,
                num_valid_docs: 3,
                num_splits_emitted: 1,
                num_docs_in_split: 0, //< the num docs in split counter has been reset.
                overall_num_bytes: 525
            }
        );
        let output_messages = inbox.drain_for_test();
        assert_eq!(output_messages.len(), 1);
        let batch = output_messages[0]
            .downcast_ref::<IndexedSplitBatch>()
            .unwrap();
        assert_eq!(batch.splits[0].num_docs, 3);
        let sort_by_field = batch.splits[0].index.settings().sort_by_field.as_ref();
        assert!(sort_by_field.is_some());
        assert_eq!(sort_by_field.unwrap().field, "timestamp");
        assert!(sort_by_field.unwrap().order.is_desc());
        Ok(())
    }

    #[tokio::test]
    async fn test_indexer_timeout() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let doc_mapper = Arc::new(quickwit_doc_mapper::default_doc_mapper_for_tests());
        let indexing_directory = IndexingDirectory::for_test().await?;
        let indexing_settings = IndexingSettings::for_test();
        let (mailbox, inbox) = create_test_mailbox();
        let indexer = Indexer::new(
            "test-index".to_string(),
            doc_mapper,
            indexing_directory,
            indexing_settings,
            mailbox,
        );
        let universe = Universe::new();
        let (indexer_mailbox, indexer_handle) = universe.spawn_actor(indexer).spawn();
        indexer_mailbox
            .send_message(
                RawDocBatch {
                    docs: vec![r#"{"body": "happy", "timestamp": 1628837062, "response_date": "2021-12-19T16:39:57+00:00", "response_time": 12, "response_payload": "YWJj"}"#.to_string()],
                    checkpoint_delta: CheckpointDelta::from(0..1),
                }
            )
            .await?;
        let indexer_counters = indexer_handle.process_pending_and_observe().await.state;
        assert_eq!(
            indexer_counters,
            IndexerCounters {
                num_parse_errors: 0,
                num_missing_fields: 0,
                num_valid_docs: 1,
                num_splits_emitted: 0,
                num_docs_in_split: 1,
                overall_num_bytes: 137
            }
        );
        universe.simulate_time_shift(Duration::from_secs(61)).await;
        let indexer_counters = indexer_handle.process_pending_and_observe().await.state;
        assert_eq!(
            indexer_counters,
            IndexerCounters {
                num_parse_errors: 0,
                num_missing_fields: 0,
                num_valid_docs: 1,
                num_splits_emitted: 1,
                num_docs_in_split: 0,
                overall_num_bytes: 137
            }
        );
        let output_messages = inbox.drain_for_test();
        assert_eq!(output_messages.len(), 1);
        let indexed_split_batch = output_messages[0]
            .downcast_ref::<IndexedSplitBatch>()
            .unwrap();
        assert_eq!(indexed_split_batch.splits[0].num_docs, 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_indexer_eof() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let doc_mapper = Arc::new(quickwit_doc_mapper::default_doc_mapper_for_tests());
        let indexing_directory = IndexingDirectory::for_test().await?;
        let indexing_settings = IndexingSettings::for_test();
        let (mailbox, inbox) = create_test_mailbox();
        let indexer = Indexer::new(
            "test-index".to_string(),
            doc_mapper,
            indexing_directory,
            indexing_settings,
            mailbox,
        );
        let universe = Universe::new();
        let (indexer_mailbox, indexer_handle) = universe.spawn_actor(indexer).spawn();
        indexer_mailbox
            .send_message(
                RawDocBatch {
                    docs: vec![r#"{"body": "happy", "timestamp": 1628837062, "response_date": "2021-12-19T16:39:57+00:00", "response_time": 12, "response_payload": "YWJj"}"#.to_string()],
                    checkpoint_delta: CheckpointDelta::from(0..1),
                }
            )
            .await?;
        universe.send_exit_with_success(&indexer_mailbox).await?;
        let (exit_status, indexer_counters) = indexer_handle.join().await;
        assert!(exit_status.is_success());
        assert_eq!(
            indexer_counters,
            IndexerCounters {
                num_parse_errors: 0,
                num_missing_fields: 0,
                num_valid_docs: 1,
                num_splits_emitted: 1,
                num_docs_in_split: 0,
                overall_num_bytes: 137
            }
        );
        let output_messages = inbox.drain_for_test();
        assert_eq!(output_messages.len(), 1);
        assert_eq!(
            output_messages[0]
                .downcast_ref::<IndexedSplitBatch>()
                .unwrap()
                .splits[0]
                .num_docs,
            1
        );
        Ok(())
    }
}
