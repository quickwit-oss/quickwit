use crate::models::Checkpoint;
use crate::models::RawDocBatch;
use async_trait::async_trait;
use quickwit_actors::Actor;
use quickwit_actors::ActorContext;
use quickwit_actors::AsyncActor;
use quickwit_actors::Mailbox;
use quickwit_actors::MessageProcessError;

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

    fn default_message(&self) -> Option<Self::Message> {
        Some(())
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
        _context: ActorContext<'_, Self::Message>,
    ) -> Result<(), MessageProcessError> {
        let line_docs: Vec<String> = self.items[self.next_item_id..]
            .iter()
            .take(self.batch_num_docs)
            .cloned()
            .collect();
        self.next_item_id += line_docs.len();
        if line_docs.is_empty() {
            return Err(MessageProcessError::Terminated);
        }
        let batch = RawDocBatch {
            docs: line_docs,
            checkpoint: Checkpoint::default(),
        };
        self.sink.send_async(batch).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use quickwit_actors::create_test_mailbox;
    use quickwit_actors::KillSwitch;
    use quickwit_actors::QueueCapacity;

    use super::*;
    use quickwit_actors::ActorTermination;

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
        let vec_source_handle = vec_source.spawn(QueueCapacity::Unbounded, KillSwitch::default());
        let actor_termination = vec_source_handle.join().await?;
        assert!(matches!(actor_termination, ActorTermination::Disconnect));
        let batch = inbox.to_vec_for_test();
        assert_eq!(batch.len(), 34);
        Ok(())
    }
}
