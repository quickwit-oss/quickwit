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

use std::any::Any;
use std::fmt;

use async_trait::async_trait;
use tokio::sync::oneshot;
use tracing::Instrument;

use crate::{Actor, ActorContext, ActorExitStatus, Handler};

/// An `Envelope` is just a way to capture the handler
/// of a message and hide its type.
///
/// Messages can have different types but somehow need to be pushed to a
/// queue with a single type.
/// Before appending, we capture the right handler implementation
/// in the form of a `Box<dyn Envelope>`, and append that to the queue.

pub struct Envelope<A>(Box<dyn EnvelopeT<A>>);

impl<A: Actor> Envelope<A> {
    /// Returns the message as a boxed any.
    ///
    /// This method is only useful in unit tests.
    pub fn message(&mut self) -> Box<dyn Any> {
        self.0.message()
    }

    pub fn message_typed<M: 'static>(&mut self) -> Option<M> {
        if let Ok(boxed_msg) = self.0.message().downcast::<M>() {
            Some(*boxed_msg)
        } else {
            None
        }
    }

    /// Execute the captured handle function.
    pub async fn handle_message(
        &mut self,
        msg_id: u64,
        actor: &mut A,
        ctx: &ActorContext<A>,
    ) -> Result<(), ActorExitStatus> {
        self.0.handle_message(msg_id, actor, ctx).await
    }
}

impl<A: Actor> fmt::Debug for Envelope<A> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let msg_str = self.0.debug_msg();
        f.debug_tuple("Envelope").field(&msg_str).finish()
    }
}

#[async_trait]
trait EnvelopeT<A: Actor>: Send + Sync {
    fn debug_msg(&self) -> String;

    /// Returns the message as a boxed any.
    ///
    /// This method is only useful in unit tests.
    fn message(&mut self) -> Box<dyn Any>;

    /// Execute the captured handle function.
    async fn handle_message(
        &mut self,
        msg_id: u64,
        actor: &mut A,
        ctx: &ActorContext<A>,
    ) -> Result<(), ActorExitStatus>;
}

#[async_trait]
impl<A, M> EnvelopeT<A> for Option<(oneshot::Sender<A::Reply>, M)>
where
    A: Handler<M>,
    M: 'static + Send + Sync + fmt::Debug,
{
    fn debug_msg(&self) -> String {
        #[allow(clippy::needless_option_take)]
        if let Some((_response_tx, msg)) = self.as_ref().take() {
            format!("{msg:?}")
        } else {
            "<consumed>".to_string()
        }
    }

    fn message(&mut self) -> Box<dyn Any> {
        if let Some((_, message)) = self.take() {
            Box::new(message)
        } else {
            Box::new(())
        }
    }

    async fn handle_message(
        &mut self,
        msg_id: u64,
        actor: &mut A,
        ctx: &ActorContext<A>,
    ) -> Result<(), ActorExitStatus> {
        let (response_tx, msg) = self
            .take()
            .expect("handle_message should never be called twice.");
        let message_span = actor.message_span(msg_id, &msg);
        let response = actor.handle(msg, ctx).instrument(message_span).await?;
        // A SendError is fine here. The caller just did not wait
        // for our response and dropped its Receiver channel.
        let _ = response_tx.send(response);
        Ok(())
    }
}

pub(crate) fn wrap_in_envelope<A, M>(msg: M) -> (Envelope<A>, oneshot::Receiver<A::Reply>)
where
    A: Handler<M>,
    M: 'static + Send + Sync + fmt::Debug,
{
    let (response_tx, response_rx) = oneshot::channel();
    let envelope = Some((response_tx, msg));
    (Envelope(Box::new(envelope)), response_rx)
}
