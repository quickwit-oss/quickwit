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

use crate::models::RawDocBatch;
use async_trait::async_trait;
use quickwit_actors::Actor;
use quickwit_actors::ActorContext;
use quickwit_actors::ActorExitStatus;
use quickwit_actors::AsyncActor;
use quickwit_actors::Mailbox;
use quickwit_metastore::checkpoint::CheckpointDelta;
use std::fmt;

/// This private token prevents external actors from creating the Loop message.
struct PrivateToken;

pub struct Loop(PrivateToken);

impl fmt::Debug for Loop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Debug")
    }
}

pub struct VecSource {
    next_item_id: usize,
    items: Vec<String>,
    batch_num_docs: usize,
    sink: Mailbox<RawDocBatch>,
}

impl Actor for VecSource {
    type Message = Loop;

    type ObservableState = usize;

    fn observable_state(&self) -> Self::ObservableState {
        self.next_item_id
    }
}

impl VecSource {
    pub fn new(items: Vec<String>, batch_num_docs: usize, sink: Mailbox<RawDocBatch>) -> VecSource {
        VecSource {
            next_item_id: 0,
            items,
            batch_num_docs,
            sink,
        }
    }
}

#[async_trait]
impl AsyncActor for VecSource {
    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        // Kick starts the source.
        self.process_message(Loop(PrivateToken), ctx).await
    }

    async fn process_message(
        &mut self,
        _message: Self::Message,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        let line_docs: Vec<String> = self.items[self.next_item_id..]
            .iter()
            .take(self.batch_num_docs)
            .cloned()
            .collect();
        let start_item_id = self.next_item_id as u64;
        self.next_item_id += line_docs.len();
        let end_item_id = self.next_item_id as u64;
        let checkpoint_delta = CheckpointDelta::from(start_item_id..end_item_id);
        if line_docs.is_empty() {
            return Err(ActorExitStatus::Success);
        }
        let batch = RawDocBatch {
            docs: line_docs,
            checkpoint_delta,
        };
        ctx.send_message(&self.sink, batch).await?;
        // Loops
        ctx.send_self_message(Loop(PrivateToken)).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickwit_actors::create_test_mailbox;
    use quickwit_actors::Universe;

    #[tokio::test]
    async fn test_vec_source() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::new();
        let (mailbox, inbox) = create_test_mailbox();
        let vec_source = VecSource::new(
            std::iter::repeat_with(|| "{}".to_string())
                .take(100)
                .collect(),
            3,
            mailbox,
        );
        let (_vec_source_mailbox, vec_source_handle) = universe.spawn(vec_source);
        let (actor_termination, last_observation) = vec_source_handle.join().await;
        assert!(actor_termination.is_success());
        assert_eq!(last_observation, 100);
        let batch = inbox.drain_available_message_for_test();
        assert_eq!(batch.len(), 34);
        Ok(())
    }
}
