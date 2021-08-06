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

use std::io;
use std::ops::RangeInclusive;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use anyhow::Context;
use quickwit_actors::Actor;
use quickwit_actors::Mailbox;
use quickwit_actors::SendError;
use quickwit_actors::SyncActor;
use quickwit_index_config::IndexConfig;
use tantivy::schema::Field;
use tantivy::Document;
use tracing::warn;

use crate::models::IndexedSplit;
use crate::models::RawDocBatch;

const DEFAULT_COMMIT_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_NUM_DOCS_COMMIT_THRESHOLD: u64 = 10_000_000;

#[derive(Clone, Default, Debug)]
pub struct IndexerCounters {
    parse_error: u64,
    docs: u64,
}

enum ScratchDirectory {
    Path(PathBuf),
    TempDir(tempfile::TempDir),
}

impl ScratchDirectory {
    fn try_new_temp() -> io::Result<ScratchDirectory> {
        let temp_dir = tempfile::tempdir()?;
        Ok(ScratchDirectory::TempDir(temp_dir))
    }
    fn path(&self) -> &Path {
        match self {
            ScratchDirectory::Path(path) => path,
            ScratchDirectory::TempDir(tempdir) => tempdir.path(),
        }
    }
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

impl Default for CommitPolicy {
    fn default() -> Self {
        CommitPolicy {
            timeout: DEFAULT_COMMIT_TIMEOUT,
            num_docs_threshold: DEFAULT_NUM_DOCS_COMMIT_THRESHOLD,
        }
    }
}

impl Actor for Indexer {
    type Message = RawDocBatch;

    type ObservableState = IndexerCounters;

    fn observable_state(&self) -> Self::ObservableState {
        self.counters.clone()
    }
}

fn extract_timestamp(doc: &Document, timestamp_field_opt: Option<Field>) -> Option<i64> {
    let timestamp_field = timestamp_field_opt?;
    let timestamp_value = doc.get_first(timestamp_field)?;
    timestamp_value.i64_value()
}

#[derive(Clone, Copy, Debug)]
pub struct CommitPolicy {
    pub timeout: Duration,
    pub num_docs_threshold: u64,
}

impl CommitPolicy {
    fn should_commit(&self, num_docs: u64, split_start_time: Instant) -> bool {
        if num_docs >= self.num_docs_threshold {
            return true;
        }
        let now = Instant::now();
        let deadline = split_start_time + self.timeout;
        now >= deadline
    }
}

impl SyncActor for Indexer {
    fn process_message(
        &mut self,
        batch: RawDocBatch,
        _context: quickwit_actors::ActorContext<'_, Self::Message>,
    ) -> Result<(), quickwit_actors::MessageProcessError> {
        let index_config = self.index_config.clone();
        let timestamp_field_opt = self.timestamp_field_opt;
        let commit_policy = self.commit_policy;
        let indexed_split = self.indexed_split()?;
        for doc_json in batch.docs {
            indexed_split.size_in_bytes += doc_json.len() as u64;
            let doc_parsing_result = index_config.doc_from_json(&doc_json);
            let doc = match doc_parsing_result {
                Ok(doc) => doc,
                Err(doc_parsing_error) => {
                    // TODO we should at least keep track of the number of parse error.
                    warn!(err=?doc_parsing_error);
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
            indexed_split.num_docs += 1;
        }

        // TODO this approach of deadline is not correct, as it never triggers if no need
        // new message arrives.
        // We do need to implement timeout message in actors to get the right behavior.
        if commit_policy.should_commit(indexed_split.num_docs, indexed_split.start_time) {
            self.send_to_packager()?;
        }

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
            ScratchDirectory::Path(path)
        } else {
            ScratchDirectory::try_new_temp()?
        };
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
            self.indexing_scratch_directory.path(),
            schema,
        )?;
        Ok(indexed_split)
    }

    fn indexed_split(&mut self) -> anyhow::Result<&mut IndexedSplit> {
        if self.current_split_opt.is_none() {
            let new_indexed_split = self.create_indexed_split()?;
            self.current_split_opt = Some(new_indexed_split);
        }
        let current_index_split = self.current_split_opt.as_mut().with_context(|| {
            "No index writer available. Please report: this should never happen."
        })?;
        Ok(current_index_split)
    }

    fn send_to_packager(&mut self) -> Result<(), SendError> {
        let indexed_split = if let Some(indexed_split) = self.current_split_opt.take() {
            indexed_split
        } else {
            return Ok(());
        };
        self.sink.send_blocking(indexed_split)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_commit_policy() {
        let commit_policy = CommitPolicy {
            num_docs_threshold: 1_000,
            timeout: Duration::from_secs(60),
        };
        assert_eq!(commit_policy.should_commit(1, Instant::now()), false);
        assert_eq!(commit_policy.should_commit(999, Instant::now()), false);
        assert_eq!(commit_policy.should_commit(1_000, Instant::now()), true);
        let past_instant = Instant::now() - Duration::from_secs(70);
        assert_eq!(commit_policy.should_commit(1, past_instant), true);
        assert_eq!(commit_policy.should_commit(20_000, past_instant), true);
    }
}
