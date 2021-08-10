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

use crate::models::Checkpoint;
use crate::models::RawDocBatch;
use async_trait::async_trait;
use quickwit_actors::Actor;
use quickwit_actors::ActorContext;
use quickwit_actors::ActorTermination;
use quickwit_actors::AsyncActor;
use quickwit_actors::Mailbox;

pub struct VecSource {
    next_item_id: usize,
    items: Vec<String>,
    batch_num_docs: usize,
    sink: Mailbox<RawDocBatch>,
}

impl Actor for VecSource {
    type Message = ();

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
    async fn process_message(
        &mut self,
        _message: Self::Message,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorTermination> {
        let line_docs: Vec<String> = self.items[self.next_item_id..]
            .iter()
            .take(self.batch_num_docs)
            .cloned()
            .collect();
        self.next_item_id += line_docs.len();
        if line_docs.is_empty() {
            return Err(ActorTermination::Finished);
        }
        let batch = RawDocBatch {
            docs: line_docs,
            // checkpoint: Checkpoint::default(),
        };
        ctx.send_message(&self.sink, batch).await?;
        ctx.send_self_message(()).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use quickwit_actors::TestContext;
    use quickwit_actors::create_test_mailbox;
    use quickwit_actors::KillSwitch;
    use super::*;

    #[tokio::test]
    async fn test_vec_source() -> anyhow::Result<()> {
        let (mailbox, inbox) = create_test_mailbox();
        let vec_source = VecSource::new(
            std::iter::repeat_with(|| "{}".to_string())
                .take(100)
                .collect(),
            3,
            mailbox,
        );
        let (vec_source_mailbox, vec_source_handle) = vec_source.spawn(KillSwitch::default());
        let test_context = TestContext;
        test_context.send_message(&vec_source_mailbox, ()).await?;
        let (actor_termination, last_observation) = vec_source_handle.join().await?;
        assert!(actor_termination.is_finished());
        assert_eq!(last_observation, 1);
        let batch = inbox.drain_available_message_for_test();
        assert_eq!(batch.len(), 34);
        Ok(())
    }
}
