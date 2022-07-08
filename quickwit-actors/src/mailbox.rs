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
use std::hash::Hash;
use std::sync::Arc;

use tokio::sync::oneshot;

use crate::channel_with_priority::{Priority, Receiver, Sender};
use crate::envelope::{wrap_in_envelope, Envelope};
use crate::{Actor, AskError, Command, Handler, QueueCapacity, RecvError, SendError};

/// A mailbox is the object that makes it possible to send a message
/// to an actor.
///
/// It is lightweight to clone.
///
/// The actor holds its `Inbox` counterpart.
///
/// The mailbox can accept:
/// - Regular messages wrapped in envelopes. Their type depend on the actor and is defined when
/// implementing the actor trait. (See [`Envelope`])
/// - Commands (See [`Command`]). Commands have a higher priority than messages:
/// whenever a command is available, it is guaranteed to be processed
/// as soon as possible regardless of the presence of pending regular messages.
///
/// If all mailboxes are dropped, the actor will process all of the pending messages
/// and gracefully exit with [`crate::actor::ActorExitStatus::Success`].
pub struct Mailbox<A: Actor> {
    pub(crate) inner: Arc<Inner<A>>,
}

impl<A: Actor> Clone for Mailbox<A> {
    fn clone(&self) -> Self {
        Mailbox {
            inner: self.inner.clone(),
        }
    }
}

impl<A: Actor> Mailbox<A> {
    pub(crate) fn is_last_mailbox(&self) -> bool {
        Arc::strong_count(&self.inner) == 1
    }

    pub fn id(&self) -> &str {
        &self.inner.instance_id
    }
}

pub(crate) enum CommandOrMessage<A: Actor> {
    Message(Box<dyn Envelope<A>>),
    Command(Command),
}

impl<A: Actor> From<Command> for CommandOrMessage<A> {
    fn from(cmd: Command) -> Self {
        CommandOrMessage::Command(cmd)
    }
}

pub(crate) struct Inner<A: Actor> {
    pub(crate) tx: Sender<CommandOrMessage<A>>,
    instance_id: String,
}

impl<A: Actor> fmt::Debug for Mailbox<A> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Mailbox({})", self.actor_instance_id())
    }
}

impl<A: Actor> Hash for Mailbox<A> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.inner.instance_id.hash(state)
    }
}

impl<A: Actor> PartialEq for Mailbox<A> {
    fn eq(&self, other: &Self) -> bool {
        self.inner.instance_id.eq(&other.inner.instance_id)
    }
}

impl<A: Actor> Eq for Mailbox<A> {}

impl<A: Actor> Mailbox<A> {
    pub fn actor_instance_id(&self) -> &str {
        &self.inner.instance_id
    }

    pub(crate) async fn send_with_priority(
        &self,
        cmd_or_msg: CommandOrMessage<A>,
        priority: Priority,
    ) -> Result<(), SendError> {
        self.inner.tx.send(cmd_or_msg, priority).await
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
        self.send_with_priority(CommandOrMessage::Message(envelope), Priority::Low)
            .await?;
        Ok(response_rx)
    }

    pub(crate) async fn send_command(&self, command: Command) -> Result<(), SendError> {
        self.send_with_priority(command.into(), Priority::High)
            .await
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
    rx: Receiver<CommandOrMessage<A>>,
}

impl<A: Actor> Inbox<A> {
    pub(crate) async fn recv_timeout(&mut self) -> Result<CommandOrMessage<A>, RecvError> {
        self.rx.recv_timeout(crate::message_timeout()).await
    }

    pub(crate) async fn recv_timeout_cmd_and_scheduled_msg_only(
        &mut self,
    ) -> Result<CommandOrMessage<A>, RecvError> {
        self.rx
            .recv_high_priority_timeout(crate::message_timeout())
            .await
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
            .map(|command_or_message| match command_or_message {
                CommandOrMessage::Message(mut msg) => msg.message(),
                CommandOrMessage::Command(cmd) => Box::new(cmd),
            })
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
            .flat_map(|command_or_message| match command_or_message {
                CommandOrMessage::Message(mut msg) => Some(msg.message()),
                CommandOrMessage::Command(_) => None,
            })
            .flat_map(|any_msg| {
                if let Ok(boxed_m) = any_msg.downcast::<M>() {
                    Some(*boxed_m)
                } else {
                    None
                }
            })
            .collect()
    }
}

pub fn create_mailbox<A: Actor>(
    actor_name: String,
    queue_capacity: QueueCapacity,
) -> (Mailbox<A>, Inbox<A>) {
    let (tx, rx) = crate::channel_with_priority::channel(queue_capacity);
    let mailbox = Mailbox {
        inner: Arc::new(Inner {
            tx,
            instance_id: quickwit_common::new_coolid(&actor_name),
        }),
    };
    let inbox = Inbox { rx };
    (mailbox, inbox)
}

pub fn create_test_mailbox<A: Actor>() -> (Mailbox<A>, Inbox<A>) {
    create_mailbox("test-mailbox".to_string(), QueueCapacity::Unbounded)
}
