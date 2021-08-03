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

use std::time::Duration;
use std::time::Instant;

use quickwit_actors::Actor;
use quickwit_actors::Mailbox;
use quickwit_actors::SyncActor;
use tantivy::IndexWriter;

use crate::models::RawDocBatch;

pub struct Indexer {
    next_commit_deadline_opt: Option<Instant>,
    index_writer: Option<IndexWriter>,
    commit_timeout: Duration,
}

impl Actor for Indexer {
    type Message = RawDocBatch;

    type ObservableState = ();

    fn observable_state(&self) -> Self::ObservableState {
        ()
    }
}

impl SyncActor for Indexer {
    fn process_message(
        &mut self,
        batch: RawDocBatch,
        _context: quickwit_actors::ActorContext<'_, Self::Message>,
    ) -> Result<(), quickwit_actors::MessageProcessError> {
        for doc in batch.docs {
        }
        let now = std::time::Instant::now();
        if let Some(deadline) = self.next_commit_deadline_opt {
            if now >= deadline {
                self.commit()?;
            }
        } else {
            self.next_commit_deadline_opt = Some(now + self.commit_timeout);
        }
        Ok(())
    }
}

impl Indexer {

    pub fn try_new(commit_timeout: Duration, sink: Mailbox<>) -> anyhow::Result<Indexer> {
        Ok(Indexer {
            next_commit_deadline_opt: None,
            index_writer: None,
            commit_timeout,
            sink
        })
    }

    fn commit(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}
