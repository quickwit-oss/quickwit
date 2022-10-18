// Copyright (C) 2022 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::fmt::Debug;
use std::time::Instant;

use anyhow::Context;
use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox};
use quickwit_common::metrics::InstrumentHistogramMetric;
use tokio::sync::oneshot;

use crate::metrics::INDEXER_METRICS;

/// The sequencer serves as a proxy to another actor,
/// delivering message in a specific order.
///
/// Producers of message first "reserve" a position in the
/// queue of message by sending `oneshot::Receiver<Message>` to the `Sequencer`.
///
/// The Sequencer then simply resolves these messages and forwards them to the
/// targetted actor.
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
        let start = Instant::now();
        let command = ctx
            .protect_future(message)
            .await
            .context("Failed to receive command from uploader.")?;
        if let SequencerCommand::Proceed(msg) = command {
            INDEXER_METRICS
                .processing_message_time
                .with_label_values(&["sequencer"])
                .observe(start.elapsed().as_secs_f64());
            ctx.send_message(&self.mailbox, msg)
                .measure_time(
                    &INDEXER_METRICS.waiting_time_to_send_message,
                    &["publisher"],
                )
                .await
                .context("Failed to send message to publisher.")?;
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
        let universe = Universe::new();
        let test_actor = SequencerTestActor::default();
        let (test_mailbox, test_handle) = universe.spawn_builder().spawn(test_actor);
        let sequencer = Sequencer::new(test_mailbox);
        let (sequencer_mailbox, sequencer_handle) = universe.spawn_builder().spawn(sequencer);
        let (fut_tx_1, fut_rx_1) = oneshot::channel();
        let (fut_tx_2, fut_rx_2) = oneshot::channel();
        let (fut_tx_3, fut_rx_3) = oneshot::channel();
        let (fut_tx_4, fut_rx_4) = oneshot::channel();
        sequencer_mailbox.send_message(fut_rx_1).await.unwrap();
        sequencer_mailbox.send_message(fut_rx_2).await.unwrap();
        fut_tx_4.send(SequencerCommand::Proceed(4)).unwrap();
        fut_tx_3.send(SequencerCommand::<usize>::Discard).unwrap();
        sequencer_mailbox.send_message(fut_rx_3).await.unwrap();
        fut_tx_2.send(SequencerCommand::Proceed(2)).unwrap();
        sequencer_mailbox.send_message(fut_rx_4).await.unwrap();
        fut_tx_1.send(SequencerCommand::Proceed(1)).unwrap();
        std::mem::drop(sequencer_mailbox);
        let (exit_status, last_state) = test_handle.join().await;
        assert!(matches!(exit_status, ActorExitStatus::Success));
        assert_eq!(&last_state, &[1, 2, 4]);
        let (sequencer_exit_status, _) = sequencer_handle.join().await;
        assert!(matches!(sequencer_exit_status, ActorExitStatus::Success));
    }
}
