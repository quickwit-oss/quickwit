// Quickwit
//  Copyright (C) 2021 Quickwit Inc.
//
//  Quickwit is offered under the AGPL v3.0 and as commercial software.
//  For commercial licensing, contact us at hello@quickwit.io.
//
//  AGPL:
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Affero General Public License as
//  published by the Free Software Foundation, either version 3 of the
//  License, or (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Affero General Public License for more details.
//
//  You should have received a copy of the GNU Affero General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::ops::RangeInclusive;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use quickwit_actors::Actor;
use quickwit_actors::ActorContext;
use quickwit_actors::ActorExitStatus;
use quickwit_actors::Mailbox;
use quickwit_actors::QueueCapacity;
use quickwit_actors::SendError;
use quickwit_actors::SyncActor;
use quickwit_index_config::IndexConfig;
use tantivy::schema::Field;
use tantivy::schema::Value;
use tantivy::Document;
use tracing::info;
use tracing::warn;

use crate::models::CommitPolicy;
use crate::models::IndexedSplit;
use crate::models::IndexerMessage;
use crate::models::RawDocBatch;
use crate::models::ScratchDirectory;

#[derive(Clone, Default, Debug, Eq, PartialEq)]
pub struct IndexerCounters {
    /// Overall number of documents received, partitionned
    /// into 3 categories:
    /// - number docs that did not parse correctly.
    /// - number docs missing a timestamp (if the index has no timestamp,
    /// then this counter is 0)
    /// - number of valid docs.
    pub num_parse_errors: u64,
    pub num_missing_timestamp: u64,
    pub num_valid_docs: u64,

    /// Number of bytes that went through the indexer
    /// during its entire lifetime.
    ///
    /// Includes both valid and invalid documents.
    pub overall_num_bytes: u64,

    /// Number of (valid) documents in the current split.
    /// This value is used to trigger commit and for observation.
    pub num_docs_in_split: u64,
}

struct IndexerState {
    index_id: String,
    index_config: Arc<dyn IndexConfig>,
    commit_policy: CommitPolicy, //< TODO should move into the index config.
    // splits index writer will write in TempDir within this directory
    indexing_scratch_directory: ScratchDirectory,
    timestamp_field_opt: Option<Field>,
}

enum PrepareDocumentOutcome {
    ParsingError,
    MissingTimestamp,
    Document {
        document: Document,
        timestamp_opt: Option<i64>,
    },
}

impl IndexerState {
    fn create_indexed_split(&self) -> anyhow::Result<IndexedSplit> {
        let schema = self.index_config.schema();
        let indexed_split = IndexedSplit::new_in_dir(
            self.index_id.clone(),
            &self.indexing_scratch_directory,
            schema,
        )?;
        Ok(indexed_split)
    }

    /// Returns the current_indexed_split. If this is the first message, then
    /// the indexed_split does not exist yet.
    ///
    /// This function will then create it, and can hence return an Error.
    fn get_or_create_current_indexed_split<'a>(
        &self,
        current_split_opt: &'a mut Option<IndexedSplit>,
        ctx: &ActorContext<Indexer>,
    ) -> anyhow::Result<&'a mut IndexedSplit> {
        if current_split_opt.is_none() {
            let new_indexed_split = self.create_indexed_split()?;
            let commit_timeout = IndexerMessage::CommitTimeout {
                split_id: new_indexed_split.split_id.clone(),
            };
            ctx.schedule_self_msg_blocking(self.commit_policy.timeout, commit_timeout);
            *current_split_opt = Some(new_indexed_split);
        }
        let current_index_split = current_split_opt.as_mut().with_context(|| {
            "No index writer available. Please report: this should never happen."
        })?;
        Ok(current_index_split)
    }

    fn prepare_document(&self, doc_json: &str) -> PrepareDocumentOutcome {
        // Parse the document
        let doc_parsing_result = self.index_config.doc_from_json(doc_json);
        let document = match doc_parsing_result {
            Ok(doc) => doc,
            Err(doc_parsing_error) => {
                warn!(err=?doc_parsing_error);
                return PrepareDocumentOutcome::ParsingError;
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
        let timestamp_opt = document
            .get_first(timestamp_field)
            .and_then(Value::i64_value);
        let timestamp = if let Some(timestamp) = timestamp_opt {
            timestamp
        } else {
            return PrepareDocumentOutcome::MissingTimestamp;
        };
        PrepareDocumentOutcome::Document {
            document,
            timestamp_opt: Some(timestamp),
        }
    }

    fn process_batch(
        &self,
        batch: RawDocBatch,
        current_split_opt: &mut Option<IndexedSplit>,
        counters: &mut IndexerCounters,
        ctx: &ActorContext<Indexer>,
    ) -> Result<(), ActorExitStatus> {
        let indexed_split = self.get_or_create_current_indexed_split(current_split_opt, ctx)?;
        indexed_split
            .checkpoint_delta
            .add(batch.checkpoint_delta)
            .with_context(|| "Batch delta does not follow indexer checkpoint")?;
        for doc_json in batch.docs {
            counters.overall_num_bytes += doc_json.len() as u64;
            indexed_split.docs_size_in_bytes += doc_json.len() as u64;
            match self.prepare_document(&doc_json) {
                PrepareDocumentOutcome::ParsingError => {
                    counters.num_parse_errors += 1;
                }
                PrepareDocumentOutcome::MissingTimestamp => {
                    counters.num_missing_timestamp += 1;
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
                    indexed_split.index_writer.add_document(document);
                }
            }
        }
        Ok(())
    }
}

pub struct Indexer {
    indexer_state: IndexerState,
    packager_mailbox: Mailbox<IndexedSplit>,
    current_split_opt: Option<IndexedSplit>,
    counters: IndexerCounters,
}

impl Actor for Indexer {
    type Message = IndexerMessage;

    type ObservableState = IndexerCounters;

    fn observable_state(&self) -> Self::ObservableState {
        self.counters.clone()
    }

    fn queue_capacity(&self) -> QueueCapacity {
        QueueCapacity::Bounded(10)
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

impl SyncActor for Indexer {
    fn process_message(
        &mut self,
        indexer_message: IndexerMessage,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        match indexer_message {
            IndexerMessage::Batch(batch) => {
                self.indexer_state.process_batch(
                    batch,
                    &mut self.current_split_opt,
                    &mut self.counters,
                    ctx,
                )?;
                if self.counters.num_docs_in_split
                    >= self.indexer_state.commit_policy.num_docs_threshold
                {
                    self.send_to_packager(CommitTrigger::NumDocsLimit, ctx)?;
                }
            }
            IndexerMessage::CommitTimeout { split_id } => {
                if let Some(split) = self.current_split_opt.as_ref() {
                    // This is a timeout for a different split.
                    // We can ignore it.
                    if split.split_id != split_id {
                        return Ok(());
                    }
                }
                self.send_to_packager(CommitTrigger::Timeout, ctx)?;
            }
            IndexerMessage::EndOfSource => {
                return Err(ActorExitStatus::Success);
            }
        }
        Ok(())
    }

    fn finalize(
        &mut self,
        exit_status: &ActorExitStatus,
        ctx: &ActorContext<Self>,
    ) -> anyhow::Result<()> {
        info!("finalize");
        match exit_status {
            ActorExitStatus::DownstreamClosed
            | ActorExitStatus::Killed
            | ActorExitStatus::Failure(_)
            | ActorExitStatus::Panicked => return Ok(()),
            ActorExitStatus::Quit | ActorExitStatus::Success => {
                self.send_to_packager(CommitTrigger::NoMoreDocs, ctx)
                    .with_context(|| {
                        "Failed to send a last message to packager upon finalize method"
                    })?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
enum CommitTrigger {
    Timeout,
    NoMoreDocs,
    NumDocsLimit,
}

impl Indexer {
    // TODO take all of the parameter and dispatch them in index config, or in a different
    // IndexerParams object.
    pub fn try_new(
        index_id: String,
        index_config: Arc<dyn IndexConfig>,
        indexing_directory: Option<PathBuf>, //< if None, we create a tempdirectory.
        commit_policy: CommitPolicy,
        packager_mailbox: Mailbox<IndexedSplit>,
    ) -> anyhow::Result<Indexer> {
        let indexing_scratch_directory = if let Some(path) = indexing_directory {
            ScratchDirectory::new_in_path(path)
        } else {
            ScratchDirectory::try_new_temp()?
        };
        let schema = index_config.schema();
        let timestamp_field_opt = index_config.timestamp_field(&schema);
        Ok(Indexer {
            indexer_state: IndexerState {
                index_id,
                index_config,
                indexing_scratch_directory,
                commit_policy,
                timestamp_field_opt,
            },
            packager_mailbox,
            counters: IndexerCounters::default(),
            current_split_opt: None,
        })
    }

    /// Extract the indexed split and send it to the Packager.
    fn send_to_packager(
        &mut self,
        commit_trigger: CommitTrigger,
        ctx: &ActorContext<Self>,
    ) -> Result<(), SendError> {
        let indexed_split = if let Some(indexed_split) = self.current_split_opt.take() {
            indexed_split
        } else {
            return Ok(());
        };
        self.counters.num_docs_in_split = 0;
        info!(commit_trigger=?commit_trigger, index=?indexed_split.index_id, split=?indexed_split.split_id,"send-to-packager");
        ctx.send_message_blocking(&self.packager_mailbox, indexed_split)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use crate::actors::indexer::record_timestamp;
    use crate::actors::indexer::IndexerCounters;
    use crate::models::CommitPolicy;
    use crate::models::IndexerMessage;
    use crate::models::RawDocBatch;
    use quickwit_actors::create_test_mailbox;
    use quickwit_actors::Universe;
    use quickwit_metastore::checkpoint::CheckpointDelta;

    use super::Indexer;

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
        let universe = Universe::new();
        let commit_policy = CommitPolicy {
            timeout: Duration::from_secs(60),
            num_docs_threshold: 3,
        };
        let (mailbox, inbox) = create_test_mailbox();
        let index_config = Arc::new(quickwit_index_config::default_config_for_tests());
        let indexer = Indexer::try_new(
            "test-index".to_string(),
            index_config,
            None,
            commit_policy,
            mailbox,
        )?;
        let (indexer_mailbox, indexer_handle) = universe.spawn_sync_actor::<Indexer>(indexer);
        universe
            .send_message(
                &indexer_mailbox,
                RawDocBatch {
                    docs: vec![
                        r#"{"body": "happy"}"#.to_string(), // missing timestamp
                        r#"{"body": "happy", "timestamp": 1628837062}"#.to_string(), // ok
                        r#"{"body": "happy2", "timestamp": 1628837062}"#.to_string(), // ok
                        "{".to_string(),                    // invalid json
                    ],
                    checkpoint_delta: CheckpointDelta::from(0..4),
                }
                .into(),
            )
            .await?;
        let indexer_counters = indexer_handle.process_pending_and_observe().await.state;
        assert_eq!(
            indexer_counters,
            IndexerCounters {
                num_parse_errors: 1,
                num_missing_timestamp: 1,
                num_valid_docs: 2,
                num_docs_in_split: 2, //< we have not reached the commit limit yet.
                overall_num_bytes: 103,
            }
        );
        universe
            .send_message(
                &indexer_mailbox,
                RawDocBatch {
                    docs: vec![r#"{"body": "happy3", "timestamp": 1628837062}"#.to_string()],
                    checkpoint_delta: CheckpointDelta::from(4..5),
                }
                .into(),
            )
            .await?;
        let indexer_counters = indexer_handle.process_pending_and_observe().await.state;
        assert_eq!(
            indexer_counters,
            IndexerCounters {
                num_parse_errors: 1,
                num_missing_timestamp: 1,
                num_valid_docs: 3,
                num_docs_in_split: 0, //< the num docs in split counter has been reset.
                overall_num_bytes: 146,
            }
        );
        let output_messages = inbox.drain_available_message_for_test();
        assert_eq!(output_messages.len(), 1);
        assert_eq!(output_messages[0].num_docs, 3);
        Ok(())
    }

    #[tokio::test]
    async fn test_indexer_timeout() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::new();
        let commit_policy = CommitPolicy {
            timeout: Duration::from_secs(60),
            num_docs_threshold: 10_000,
        };
        let (mailbox, inbox) = create_test_mailbox();
        let index_config = Arc::new(quickwit_index_config::default_config_for_tests());
        let indexer = Indexer::try_new(
            "test-index".to_string(),
            index_config,
            None,
            commit_policy,
            mailbox,
        )?;
        let (indexer_mailbox, indexer_handle) = universe.spawn_sync_actor::<Indexer>(indexer);
        universe
            .send_message(
                &indexer_mailbox,
                RawDocBatch {
                    docs: vec![r#"{"body": "happy", "timestamp": 1628837062}"#.to_string()],
                    checkpoint_delta: CheckpointDelta::from(0..1),
                }
                .into(),
            )
            .await?;
        let indexer_counters = indexer_handle.process_pending_and_observe().await.state;
        assert_eq!(
            indexer_counters,
            IndexerCounters {
                num_parse_errors: 0,
                num_missing_timestamp: 0,
                num_valid_docs: 1,
                num_docs_in_split: 1,
                overall_num_bytes: 42,
            }
        );
        universe.simulate_time_shift(Duration::from_secs(61)).await;
        let indexer_counters = indexer_handle.process_pending_and_observe().await.state;
        assert_eq!(
            indexer_counters,
            IndexerCounters {
                num_parse_errors: 0,
                num_missing_timestamp: 0,
                num_valid_docs: 1,
                num_docs_in_split: 0,
                overall_num_bytes: 42,
            }
        );
        let output_messages = inbox.drain_available_message_for_test();
        assert_eq!(output_messages.len(), 1);
        assert_eq!(output_messages[0].num_docs, 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_indexer_eof() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::new();
        let commit_policy = CommitPolicy::default();
        let (mailbox, inbox) = create_test_mailbox();
        let index_config = Arc::new(quickwit_index_config::default_config_for_tests());
        let indexer = Indexer::try_new(
            "test-index".to_string(),
            index_config,
            None,
            commit_policy,
            mailbox,
        )?;
        let (indexer_mailbox, indexer_handle) = universe.spawn_sync_actor::<Indexer>(indexer);
        universe
            .send_message(
                &indexer_mailbox,
                RawDocBatch {
                    docs: vec![r#"{"body": "happy", "timestamp": 1628837062}"#.to_string()],
                    checkpoint_delta: CheckpointDelta::from(0..1),
                }
                .into(),
            )
            .await?;
        universe
            .send_message(&indexer_mailbox, IndexerMessage::EndOfSource)
            .await?;
        let (exit_status, indexer_counters) = indexer_handle.join().await;
        assert!(exit_status.is_success());
        assert_eq!(
            indexer_counters,
            IndexerCounters {
                num_parse_errors: 0,
                num_missing_timestamp: 0,
                num_valid_docs: 1,
                num_docs_in_split: 0,
                overall_num_bytes: 42,
            }
        );
        let output_messages = inbox.drain_available_message_for_test();
        assert_eq!(output_messages.len(), 1);
        assert_eq!(output_messages[0].num_docs, 1);
        Ok(())
    }
}
