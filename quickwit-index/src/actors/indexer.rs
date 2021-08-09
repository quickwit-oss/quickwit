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
use quickwit_actors::ActorTermination;
use quickwit_actors::Mailbox;
use quickwit_actors::QueueCapacity;
use quickwit_actors::SendError;
use quickwit_actors::SyncActor;
use quickwit_index_config::IndexConfig;
use tantivy::schema::Field;
use tantivy::Document;
use tracing::info;
use tracing::warn;

use crate::models::CommitPolicy;
use crate::models::IndexedSplit;
use crate::models::RawDocBatch;
use crate::models::ScratchDirectory;

#[derive(Clone, Default, Debug)]
pub struct IndexerCounters {
    pub num_parse_errors: u64,
    pub num_docs: u64,
}

pub struct Indexer {
    index_id: String,
    index_config: Arc<dyn IndexConfig>,
    commit_policy: CommitPolicy, //< TODO should move into the index config.
    // splits index writer will write in TempDir within this directory
    indexing_scratch_directory: ScratchDirectory,
    sink: Mailbox<IndexedSplit>,
    current_split_opt: Option<IndexedSplit>,
    counters: IndexerCounters,
    timestamp_field_opt: Option<Field>,
}

impl Actor for Indexer {
    type Message = RawDocBatch;

    type ObservableState = IndexerCounters;

    fn observable_state(&self) -> Self::ObservableState {
        self.counters.clone()
    }

    fn queue_capacity(&self) -> QueueCapacity {
        QueueCapacity::Bounded(10)
    }
}

fn extract_timestamp(doc: &Document, timestamp_field_opt: Option<Field>) -> Option<i64> {
    let timestamp_field = timestamp_field_opt?;
    let timestamp_value = doc.get_first(timestamp_field)?;
    timestamp_value.i64_value()
}

impl SyncActor for Indexer {
    fn process_message(
        &mut self,
        batch: RawDocBatch,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorTermination> {
        let index_config = self.index_config.clone();
        let timestamp_field_opt = self.timestamp_field_opt;
        let commit_policy = self.commit_policy;
        let (indexed_split, counts) = self.indexed_split()?;
        for doc_json in batch.docs {
            // One batch might take a long time to process. Let's register progress
            // after each doc.
            ctx.record_progress();
            indexed_split.size_in_bytes += doc_json.len() as u64;
            let doc_parsing_result = index_config.doc_from_json(&doc_json);
            let doc = match doc_parsing_result {
                Ok(doc) => doc,
                Err(doc_parsing_error) => {
                    // TODO we should at least keep track of the number of parse error.
                    warn!(err=?doc_parsing_error);
                    counts.num_parse_errors += 1;
                    continue;
                }
            };
            if let Some(timestamp) = extract_timestamp(&doc, timestamp_field_opt) {
                let new_timestamp_range = match indexed_split.time_range.as_ref() {
                    Some(range) => RangeInclusive::new(
                        timestamp.min(*range.start()),
                        timestamp.max(*range.end()),
                    ),
                    None => RangeInclusive::new(timestamp, timestamp),
                };
                indexed_split.time_range = Some(new_timestamp_range);
            }
            indexed_split.index_writer.add_document(doc);
            counts.num_docs += 1;
            indexed_split.num_docs += 1;
        }

        // TODO this approach of deadline is not correct, as it never triggers if no need
        // new message arrives.
        // We do need to implement timeout message in actors to get the right behavior.
        if commit_policy.should_commit(indexed_split.num_docs, indexed_split.start_time) {
            self.send_to_packager(ctx)?;
        }

        Ok(())
    }

    fn finalize(
        &mut self,
        termination: &ActorTermination,
        ctx: &ActorContext<Self>,
    ) -> anyhow::Result<()> {
        info!("finalize");
        if termination.is_failure() {
            return Ok(());
        }
        self.send_to_packager(ctx)
            .with_context(|| "Failed to send a last message to packager upon finalize method")?;
        Ok(())
    }
}

impl Indexer {
    // TODO take all of the parameter and dispatch them in index config, or in a different
    // IndexerParams object.
    pub fn try_new(
        index_id: String,
        index_config: Arc<dyn IndexConfig>,
        indexing_directory: Option<PathBuf>, //< if None, we create a tempdirectory.
        commit_policy: CommitPolicy,
        sink: Mailbox<IndexedSplit>,
    ) -> anyhow::Result<Indexer> {
        let indexing_scratch_directory = if let Some(path) = indexing_directory {
            ScratchDirectory::new_in_path(path)
        } else {
            ScratchDirectory::try_new_temp()?
        }
        .into();
        let time_field_opt = index_config.timestamp_field();
        Ok(Indexer {
            index_id,
            index_config,
            sink,
            counters: IndexerCounters::default(),
            current_split_opt: None,
            indexing_scratch_directory,
            timestamp_field_opt: time_field_opt,
            commit_policy,
        })
    }

    fn create_indexed_split(&self) -> anyhow::Result<IndexedSplit> {
        let schema = self.index_config.schema();
        let indexed_split = IndexedSplit::new_in_dir(
            self.index_id.clone(),
            &self.indexing_scratch_directory,
            schema,
        )?;
        Ok(indexed_split)
    }

    fn indexed_split(&mut self) -> anyhow::Result<(&mut IndexedSplit, &mut IndexerCounters)> {
        if self.current_split_opt.is_none() {
            let new_indexed_split = self.create_indexed_split()?;
            self.current_split_opt = Some(new_indexed_split);
        }
        let current_index_split = self.current_split_opt.as_mut().with_context(|| {
            "No index writer available. Please report: this should never happen."
        })?;
        Ok((current_index_split, &mut self.counters))
    }

    fn send_to_packager(&mut self, ctx: &ActorContext<Self>) -> Result<(), SendError> {
        let indexed_split = if let Some(indexed_split) = self.current_split_opt.take() {
            indexed_split
        } else {
            return Ok(());
        };
        ctx.send_message_blocking(&self.sink, indexed_split)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use crate::models::CommitPolicy;
    use crate::models::RawDocBatch;
    use quickwit_actors::create_test_mailbox;
    use quickwit_actors::KillSwitch;
    use quickwit_actors::SyncActor;
    use quickwit_actors::TestContext;

    use super::Indexer;

    #[tokio::test]
    async fn test_indexer() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
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
        let (indexer_mailbox, indexer_handle) = indexer.spawn(KillSwitch::default());
        let ctx = TestContext;
        ctx.send_message(
            &indexer_mailbox,
            RawDocBatch {
                docs: vec![
                    "{\"body\": \"happy\"}".to_string(),
                    "{\"body\": \"happy2\"}".to_string(),
                    "{".to_string(),
                ],
            },
        )
        .await?;
        ctx.send_message(
            &indexer_mailbox,
            RawDocBatch {
                docs: vec!["{\"body\": \"happy3\"}".to_string()],
            },
        )
        .await?;
        let indexer_counters = indexer_handle
            .process_pending_and_observe()
            .await
            .into_inner();
        assert_eq!(indexer_counters.num_docs, 3);
        assert_eq!(indexer_counters.num_parse_errors, 1);
        let output_messages = inbox.drain_available_message_for_test();
        assert_eq!(output_messages.len(), 1);
        assert_eq!(output_messages[0].num_docs, 3);
        Ok(())
    }
}
