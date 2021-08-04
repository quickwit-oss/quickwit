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

pub struct Indexer {
    index_config: Arc<dyn IndexConfig>,
    commit_timeout: Duration,
    sink: Mailbox<IndexedSplit>,
    next_commit_deadline_opt: Option<Instant>,
    index_writer_opt: Option<IndexWriter>,
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
    pub fn try_new(
        index_config: Arc<dyn IndexConfig>,
        commit_timeout: Duration,
        sink: Mailbox<IndexedSplit>,
    ) -> anyhow::Result<Indexer> {
        Ok(Indexer {
            index_config,
            commit_timeout,
            sink,
            next_commit_deadline_opt: None,
            index_writer_opt: None,
            counters: IndexerCounters::default(),
        })
    }

    fn create_index_writer(&self) -> anyhow::Result<IndexWriter> {
        todo!()
    }

    fn index_writer(&mut self) -> anyhow::Result<&mut IndexWriter> {
        if self.index_writer_opt.is_none() {
            let index_writer = self.create_index_writer()?;
            self.index_writer_opt = Some(index_writer);
            self.next_commit_deadline_opt = Some(Instant::now() + self.commit_timeout);
        }
        let index_writer = self.index_writer_opt.as_mut().with_context(|| {
            "No index writer available. Please report: this should never happen."
        })?;
        Ok(index_writer)
    }

    fn commit(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}
