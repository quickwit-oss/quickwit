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

use std::any::Any;
use std::fmt;

use async_trait::async_trait;
use tokio::sync::oneshot;

use crate::actor::DeferableReplyHandler;
use crate::scheduler::NoAdvanceTimeGuard;
use crate::{Actor, ActorContext, ActorExitStatus};

/// An `Envelope` is just a way to capture the handler
/// of a message and hide its type.
///
/// Messages can have different types but somehow need to be pushed to a
/// queue with a single type.
/// Before appending, we capture the right handler implementation
/// in the form of a `Box<dyn Envelope>`, and append that to the queue.
pub struct Envelope<A> {
    handler_envelope: Box<dyn EnvelopeT<A>>,
    _no_advance_time_guard: Option<NoAdvanceTimeGuard>,
}

impl<A: Actor> Envelope<A> {
    /// Returns the message as a boxed any.
    ///
    /// This method is only useful in unit tests.
    pub fn message(&mut self) -> Box<dyn Any> {
        self.handler_envelope.message()
    }

    pub fn message_typed<M: 'static>(&mut self) -> Option<M> {
        if let Ok(boxed_msg) = self.handler_envelope.message().downcast::<M>() {
            Some(*boxed_msg)
        } else {
            None
        }
    }

    /// Executes the captured handle function.
    ///
    /// When exiting, also returns the message type name.
    pub async fn handle_message(
        &mut self,
        actor: &mut A,
        ctx: &ActorContext<A>,
    ) -> Result<(), (ActorExitStatus, &'static str)> {
        let handling_res = self.handler_envelope.handle_message(actor, ctx).await;
        if let Err(exit_status) = handling_res {
            return Err((exit_status, self.handler_envelope.message_type_name()));
        }
        Ok(())
    }
}

impl<A: Actor> fmt::Debug for Envelope<A> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let msg_str = self.handler_envelope.debug_msg();
        f.debug_tuple("Envelope").field(&msg_str).finish()
    }
}

#[async_trait]
trait EnvelopeT<A: Actor>: Send {
    fn message_type_name(&self) -> &'static str;

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
impl<A, M> EnvelopeT<A> for Option<(oneshot::Sender<A::Reply>, M)>
where
    A: DeferableReplyHandler<M>,
    M: fmt::Debug + Send + 'static,
{
    fn message_type_name(&self) -> &'static str {
        std::any::type_name::<M>()
    }

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
        actor: &mut A,
        ctx: &ActorContext<A>,
    ) -> Result<(), ActorExitStatus> {
        let (response_tx, msg) = self
            .take()
            .expect("handle_message should never be called twice.");
        actor
            .handle_message(
                msg,
                |response| {
                    // A SendError is fine here. The caller just did not wait
                    // for our response and dropped its Receiver channel.
                    let _ = response_tx.send(response);
                },
                ctx,
            )
            .await?;
        Ok(())
    }
}

pub(crate) fn wrap_in_envelope<A, M>(
    msg: M,
    no_advance_time_guard: Option<NoAdvanceTimeGuard>,
) -> (Envelope<A>, oneshot::Receiver<A::Reply>)
where
    A: DeferableReplyHandler<M>,
    M: fmt::Debug + Send + 'static,
{
    let (response_tx, response_rx) = oneshot::channel();
    let handler_envelope = Some((response_tx, msg));
    let envelope = Envelope {
        handler_envelope: Box::new(handler_envelope),
        _no_advance_time_guard: no_advance_time_guard,
    };
    (envelope, response_rx)
}
