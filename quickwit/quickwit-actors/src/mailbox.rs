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
use std::convert::Infallible;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::oneshot;

use crate::channel_with_priority::{Receiver, Sender};
use crate::envelope::{wrap_in_envelope, Envelope};
use crate::{
    Actor, ActorContext, ActorExitStatus, AskError, Handler, QueueCapacity, RecvError, SendError,
};

/// A mailbox is the object that makes it possible to send a message
/// to an actor.
///
/// It is lightweight to clone.
///
/// The actor holds its `Inbox` counterpart.
///
/// The mailbox can receive high priority and low priority messages.
/// Commands are typically sent as high priority messages, whereas regular
/// actor messages are sent to the low priority channel.
///
/// Whenever a high priority message is available, it is processed
/// before low priority messages.
///
/// If all mailboxes are dropped, the actor will process all of the pending messages
/// and gracefully exit with [`crate::actor::ActorExitStatus::Success`].
pub struct Mailbox<A: Actor> {
    inner: Arc<Inner<A>>,
    // We do not rely on the `Arc:strong_count` here to avoid an intricate
    // race condition. We want to make sure the processing of the `Nudge`
    // message happens AFTER we decrement the refcount.
    ref_count: Arc<AtomicUsize>,
}

impl<A: Actor> Drop for Mailbox<A> {
    fn drop(&mut self) {
        let old_val = self.ref_count.fetch_sub(1, Ordering::SeqCst);
        if old_val == 2 {
            // This was the last mailbox.
            // `ref_count == 1` means that only the mailbox in the ActorContext
            // is remaining.
            let _ = self.send_message_with_high_priority(LastMailbox);
        }
    }
}

#[derive(Debug)]
struct LastMailbox;

#[async_trait]
impl<A: Actor> Handler<LastMailbox> for A {
    type Reply = ();

    async fn handle(
        &mut self,
        _: LastMailbox,
        _ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        // Being the last mailbox does not necessarily mean that we
        // want to stop the processing.
        //
        // There could be pending message in the queue that will
        // spawn actors which will get a new copy of the mailbox
        // etc.
        //
        // For that reason, the logic that really detects
        // the last mailbox happens when all message have been drained.
        //
        // The `LastMailbox` message is just here to make sure the actor
        // loop does not get stuck waiting for a message that does
        // will never come.
        Ok(())
    }
}

impl<A: Actor> Clone for Mailbox<A> {
    fn clone(&self) -> Self {
        self.ref_count.fetch_add(1, Ordering::SeqCst);
        Mailbox {
            inner: self.inner.clone(),
            ref_count: self.ref_count.clone(),
        }
    }
}

impl<A: Actor> Mailbox<A> {
    pub(crate) fn is_last_mailbox(&self) -> bool {
        self.ref_count.load(Ordering::SeqCst) == 1
    }

    pub fn id(&self) -> &str {
        &self.inner.instance_id
    }
}

pub(crate) struct Inner<A: Actor> {
    pub(crate) tx: Sender<Envelope<A>>,
    instance_id: String,
}

impl<A: Actor> fmt::Debug for Mailbox<A> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_tuple("Mailbox")
            .field(&self.actor_instance_id())
            .finish()
    }
}

impl<A: Actor> Mailbox<A> {
    pub fn actor_instance_id(&self) -> &str {
        &self.inner.instance_id
    }

    /// Sends a message to the actor owning the associated inbox.
    ///
    /// From an actor context, use the `ActorContext::send_message` method instead.
    ///
    /// SendError is returned if the actor has already exited.
    pub async fn send_message<M>(
        &self,
        message: M,
    ) -> Result<oneshot::Receiver<A::Reply>, SendError>
    where
        A: Handler<M>,
        M: 'static + Send + Sync + fmt::Debug,
    {
        let (envelope, response_rx) = wrap_in_envelope(message);
        self.inner.tx.send_low_priority(envelope).await?;
        Ok(response_rx)
    }

    pub(crate) fn send_message_with_high_priority<M>(&self, message: M) -> Result<(), SendError>
    where
        A: Handler<M>,
        M: 'static + Send + Sync + fmt::Debug,
    {
        let (envelope, _response_rx) = wrap_in_envelope(message);
        self.inner.tx.send_high_priority(envelope)
    }

    /// Similar to `send_message`, except this method
    /// waits asynchronously for the actor reply.
    ///
    /// From an actor context, use the `ActorContext::ask` method instead.
    pub async fn ask<M, T>(&self, message: M) -> Result<T, AskError<Infallible>>
    where
        A: Handler<M, Reply = T>,
        M: 'static + Send + Sync + fmt::Debug,
    {
        self.send_message(message)
            .await
            .map_err(|_send_error| AskError::MessageNotDelivered)?
            .await
            .map_err(|_| AskError::ProcessMessageError)
    }

    /// Similar to `send_message`, except this method
    /// waits asynchronously for the actor reply.
    ///
    /// From an actor context, use the `ActorContext::ask` method instead.
    pub async fn ask_for_res<M, T, E: fmt::Debug>(&self, message: M) -> Result<T, AskError<E>>
    where
        A: Handler<M, Reply = Result<T, E>>,
        M: 'static + Send + Sync + fmt::Debug,
    {
        self.send_message(message)
            .await
            .map_err(|_send_error| AskError::MessageNotDelivered)?
            .await
            .map_err(|_| AskError::ProcessMessageError)?
            .map_err(AskError::from)
    }
}

pub struct Inbox<A: Actor> {
    rx: Receiver<Envelope<A>>,
}

impl<A: Actor> Inbox<A> {
    pub(crate) async fn recv(&mut self) -> Result<Envelope<A>, RecvError> {
        self.rx.recv().await
    }

    pub(crate) async fn recv_cmd_and_scheduled_msg_only(&mut self) -> Envelope<A> {
        self.rx.recv_high_priority().await
    }

    #[allow(dead_code)] // temporary
    pub(crate) fn try_recv(&mut self) -> Result<Envelope<A>, RecvError> {
        self.rx.try_recv()
    }

    #[allow(dead_code)] // temporary
    pub(crate) fn try_recv_cmd_and_scheduled_msg_only(&mut self) -> Result<Envelope<A>, RecvError> {
        self.rx.try_recv_high_priority_message()
    }

    /// Destroys the inbox and returns the list of pending messages or commands
    /// in the low priority channel.
    ///
    /// Warning this iterator might never be exhausted if there is a living
    /// mailbox associated to it.
    pub fn drain_for_test(&self) -> Vec<Box<dyn Any>> {
        self.rx
            .drain_low_priority()
            .into_iter()
            .map(|mut envelope| envelope.message())
            .collect()
    }

    /// Destroys the inbox and returns the list of pending messages or commands
    /// in the low priority channel.
    ///
    /// Warning this iterator might never be exhausted if there is a living
    /// mailbox associated to it.
    pub fn drain_for_test_typed<M: 'static>(&self) -> Vec<M> {
        self.rx
            .drain_low_priority()
            .into_iter()
            .flat_map(|mut envelope| envelope.message_typed())
            .collect()
    }
}

pub fn create_mailbox<A: Actor>(
    actor_name: String,
    queue_capacity: QueueCapacity,
) -> (Mailbox<A>, Inbox<A>) {
    let (tx, rx) = crate::channel_with_priority::channel(queue_capacity);
    let ref_count = Arc::new(AtomicUsize::new(1));
    let mailbox = Mailbox {
        inner: Arc::new(Inner {
            tx,
            instance_id: quickwit_common::new_coolid(&actor_name),
        }),
        ref_count,
    };
    let inbox = Inbox { rx };
    (mailbox, inbox)
}

pub fn create_test_mailbox<A: Actor>() -> (Mailbox<A>, Inbox<A>) {
    create_mailbox("test-mailbox".to_string(), QueueCapacity::Unbounded)
}
