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
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use anyhow::Context;
use quickwit_actors::Actor;
use quickwit_actors::Mailbox;
use quickwit_actors::SyncActor;
use quickwit_index_config::IndexConfig;
use tantivy::IndexWriter;
use tracing::warn;

use crate::models::IndexedSplit;
use crate::models::RawDocBatch;

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
    index_config: Arc<dyn IndexConfig>,
    // splits index writer will write in TempDir within this directory
    indexing_scratch_directory: ScratchDirectory,
    commit_timeout: Duration,
    sink: Mailbox<IndexedSplit>,
    next_commit_deadline_opt: Option<Instant>,
    current_split_opt: Option<IndexedSplit>,
    counters: IndexerCounters,
}

impl Actor for Indexer {
    type Message = RawDocBatch;

    type ObservableState = IndexerCounters;

    fn observable_state(&self) -> Self::ObservableState {
        self.counters.clone()
    }
}

impl SyncActor for Indexer {
    fn process_message(
        &mut self,
        batch: RawDocBatch,
        _context: quickwit_actors::ActorContext<'_, Self::Message>,
    ) -> Result<(), quickwit_actors::MessageProcessError> {
        let index_config = self.index_config.clone();
        let index_writer = self.index_writer()?;
        for doc_json in batch.docs {
            match index_config.doc_from_json(&doc_json) {
                Ok(doc) => {
                    index_writer.add_document(doc);
                }
                Err(doc_parsing_error) => {
                    // TODO what should we do
                    warn!(err=?doc_parsing_error);
                }
            }
        }
        // TODO this approach of deadline is not correct, as it never triggers if no need
        // new message arrives.
        // We do need to implement timeout message in actors to get the right behavior.
        if let Some(deadline) = self.next_commit_deadline_opt {
            let now = Instant::now();
            if now >= deadline {
                self.commit()?;
            }
        } else {
            self.next_commit_deadline_opt = None;
        }
        Ok(())
    }
}

impl Indexer {
    // TODO take all of the parameter and dispatch them in index config, or in a different
    // IndexerParams object.
    pub fn try_new(
        index_config: Arc<dyn IndexConfig>,
        indexing_directory: Option<PathBuf>, //< if None, we create a tempdirectory.
        commit_timeout: Duration,
        sink: Mailbox<IndexedSplit>,
    ) -> anyhow::Result<Indexer> {
        let indexing_scratch_directory =
            if let Some(path) = indexing_directory {
                ScratchDirectory::Path(path)
            } else {
                ScratchDirectory::try_new_temp()?
            };
       Ok(Indexer {
            index_config,
            commit_timeout,
            sink,
            next_commit_deadline_opt: None,
            counters: IndexerCounters::default(),
            current_split_opt: None,
            indexing_scratch_directory
        })
    }

    fn create_indexed_split(&self) -> anyhow::Result<IndexedSplit> {
        let schema = self.index_config.schema();
        let indexed_split = IndexedSplit::new_in_dir(self.indexing_scratch_directory.path(), schema)?;
        Ok(indexed_split)
    }

    fn index_writer(&mut self) -> anyhow::Result<&mut IndexWriter> {
        if self.current_split_opt.is_none() {
            let new_indexed_split = self.create_indexed_split()?;
            self.current_split_opt = Some(new_indexed_split);
            self.next_commit_deadline_opt = Some(Instant::now() + self.commit_timeout);
        }
        let current_index_split = self.current_split_opt.as_mut().with_context(|| {
            "No index writer available. Please report: this should never happen."
        })?;
        Ok(&mut current_index_split.index_writer)
    }

    fn commit(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}
