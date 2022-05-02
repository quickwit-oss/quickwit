// Copyright (C) 2021 Quickwit, Inc.
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

use crate::{Actor, ActorContext, ActorExitStatus, Handler};

/// An `Envelope` is just a way to capture the handler
/// of a message and hide its type.
///
/// Message can have different types but somehow need to  be pushed to a
/// queue with a single type.
/// Before appending, we capture the right handler implementation
/// in the form of an Box<dyn Envelope>, and append that to the queue.
#[async_trait]
pub(crate) trait Envelope<A: Actor>: Send + Sync {
    fn debug_msg(&self) -> String;

    /// Returns the message as a boxed any.
    ///
    /// This method is only useful in unit tests.
    fn message(&mut self) -> Box<dyn Any>;

    /// Execute the captured handle function.
    async fn handle_message(
        &mut self,
        actor: &mut A,
        ctx: &ActorContext<A>,
    ) -> Result<(), ActorExitStatus>;
}

#[async_trait]
impl<A, M> Envelope<A> for Option<(oneshot::Sender<A::Reply>, M)>
where
    A: Handler<M>,
    M: 'static + Send + Sync + fmt::Debug,
{
    fn debug_msg(&self) -> String {
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
        actor: &mut A,
        ctx: &ActorContext<A>,
    ) -> Result<(), ActorExitStatus> {
        let (response_tx, msg) = self
            .take()
            .expect("handle_message should never be called twice.");
        let response = actor.handle(msg, ctx).await?;
        // A SendError is fine here. The caller just did not wait
        // for our response and dropped its Receiver channel.
        let _ = response_tx.send(response);
        Ok(())
    }
}

pub(crate) fn wrap_in_envelope<A, M>(msg: M) -> (Box<dyn Envelope<A>>, oneshot::Receiver<A::Reply>)
where
    A: Handler<M>,
    M: 'static + Send + Sync + fmt::Debug,
{
    let (response_tx, response_rx) = oneshot::channel();
    let envelope = Some((response_tx, msg));
    (Box::new(envelope) as Box<dyn Envelope<A>>, response_rx)
}
