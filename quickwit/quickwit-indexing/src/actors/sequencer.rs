// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Debug;

use anyhow::Context;
use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox, QueueCapacity};
use tokio::sync::oneshot;

/// The sequencer serves as a proxy to another actor,
/// delivering message in a specific order.
///
/// Producers of message first "reserve" a position in the
/// queue of message by sending `oneshot::Receiver<Message>` to the `Sequencer`.
///
/// The Sequencer then simply resolves these messages and forwards them to the
/// targeted actor.
///
/// It is used by the uploader actor, to run uploads concurrently and yet
/// ensures that publish message are send in the right order.
pub struct Sequencer<A: Actor> {
    mailbox: Mailbox<A>,
}

impl<A: Actor> Sequencer<A> {
    pub fn new(mailbox: Mailbox<A>) -> Self {
        Sequencer { mailbox }
    }
}

#[async_trait]
impl<A: Actor> Actor for Sequencer<A> {
    type ObservableState = ();

    fn queue_capacity(&self) -> QueueCapacity {
        QueueCapacity::Bounded(2)
    }

    fn observable_state(&self) {}
}

#[derive(Debug)]
pub enum SequencerCommand<T: Debug> {
    /// Discard position in the sequence.
    Discard,
    /// Proceed with the enclosed value.
    Proceed(T),
}

#[async_trait]
impl<A, M> Handler<oneshot::Receiver<SequencerCommand<M>>> for Sequencer<A>
where
    A: Actor,
    A: Handler<M>,
    M: Send + Sync + 'static + std::fmt::Debug,
{
    type Reply = ();

    async fn handle(
        &mut self,
        message: oneshot::Receiver<SequencerCommand<M>>,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        let command = ctx
            .protect_future(message)
            .await
            .context("failed to receive command from uploader")?;
        if let SequencerCommand::Proceed(msg) = command {
            ctx.send_message(&self.mailbox, msg)
                .await
                .context("failed to send message to publisher")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use quickwit_actors::Universe;

    use super::*;

    #[derive(Default)]
    struct SequencerTestActor {
        messages: Vec<usize>,
    }

    impl Actor for SequencerTestActor {
        type ObservableState = Vec<usize>;

        fn observable_state(&self) -> Self::ObservableState {
            self.messages.clone()
        }
    }

    #[async_trait]
    impl Handler<usize> for SequencerTestActor {
        type Reply = ();

        async fn handle(
            &mut self,
            message: usize,
            _ctx: &ActorContext<Self>,
        ) -> Result<(), ActorExitStatus> {
            self.messages.push(message);
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_sequencer() {
        let universe = Universe::with_accelerated_time();
        let test_actor = SequencerTestActor::default();
        let (test_mailbox, test_handle) = universe.spawn_builder().spawn(test_actor);
        let sequencer = Sequencer::new(test_mailbox);
        let (sequencer_mailbox, sequencer_handle) = universe.spawn_builder().spawn(sequencer);
        // The sequencer has a capacity of 2.
        // This is the maximum we can do without provoking a deadlock.
        let (fut_tx_1, fut_rx_1) = oneshot::channel();
        let (fut_tx_2, fut_rx_2) = oneshot::channel();
        let (fut_tx_3, fut_rx_3) = oneshot::channel();
        sequencer_mailbox.send_message(fut_rx_1).await.unwrap();
        sequencer_mailbox.send_message(fut_rx_2).await.unwrap();
        fut_tx_3.send(SequencerCommand::<usize>::Discard).unwrap();
        sequencer_mailbox.send_message(fut_rx_3).await.unwrap();
        fut_tx_2.send(SequencerCommand::Proceed(2)).unwrap();
        fut_tx_1.send(SequencerCommand::Proceed(1)).unwrap();
        std::mem::drop(sequencer_mailbox);
        let (exit_status, last_state) = test_handle.join().await;
        assert!(matches!(exit_status, ActorExitStatus::Success));
        assert_eq!(&last_state, &[1, 2]);
        let (sequencer_exit_status, _) = sequencer_handle.join().await;
        assert!(matches!(sequencer_exit_status, ActorExitStatus::Success));
    }
}
