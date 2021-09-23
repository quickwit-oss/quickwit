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
use std::hash::Hash;
use std::sync::Arc;

use tokio::sync::oneshot;

use crate::channel_with_priority::{Priority, Receiver, Sender};
use crate::{QueueCapacity, RecvError, SendError};

/// A mailbox is the object that makes it possible to send a message
/// to an actor.
///
/// It is lightweight to clone.
///
/// The actor holds its `Inbox` counterpart.
///
/// The mailbox can accept:
/// - regular message. Their type depend on the actor and is defined when
/// implementing the actor trait.  (See [`Actor::Message`])
/// - Commands (See [`Command`]). Commands have a higher priority than messages:
/// whenever a command is available, it is guaranteed to be processed
/// as soon as possible regardless of the presence of pending regular messages.
///
/// If all mailboxes are dropped, the actor will process all of the pending messages
/// and gracefully exit with `ActorExitStatus::Success`.
pub struct Mailbox<Message> {
    pub(crate) inner: Arc<Inner<Message>>,
}

impl<Message> Clone for Mailbox<Message> {
    fn clone(&self) -> Self {
        Mailbox {
            inner: self.inner.clone(),
        }
    }
}

impl<Message> Mailbox<Message> {
    pub(crate) fn is_last_mailbox(&self) -> bool {
        Arc::strong_count(&self.inner) == 1
    }
}

pub enum CommandOrMessage<Message> {
    Message(Message),
    Command(Command),
}

impl<Message> CommandOrMessage<Message> {
    pub fn message(self) -> Option<Message> {
        match self {
            CommandOrMessage::Message(message) => Some(message),
            CommandOrMessage::Command(_) => None,
        }
    }

    pub fn command(self) -> Option<Command> {
        match self {
            CommandOrMessage::Message(_) => None,
            CommandOrMessage::Command(command) => Some(command),
        }
    }
}

impl<Message> From<Command> for CommandOrMessage<Message> {
    fn from(cmd: Command) -> Self {
        CommandOrMessage::Command(cmd)
    }
}

pub(crate) struct Inner<Message> {
    pub(crate) tx: Sender<CommandOrMessage<Message>>,
    instance_id: String,
}

/// Commands are messages that can be send to control the behavior of an actor.
///
/// They are similar to UNIX signals.
///
/// They are treated with a higher priority than regular actor messages.
pub enum Command {
    /// Temporarily pauses the actor. A paused actor only checks
    /// on its high priority channel and still shows "progress". It appears as
    /// healthy to the supervisor.
    ///
    /// Scheduled message are still processed.
    ///
    /// Semantically, it is similar to SIGSTOP.
    Pause,

    /// Resume a paused actor. If the actor was not paused this command
    /// has no effects.
    ///
    /// Semantically, it is similar to SIGCONT.
    Resume,

    /// Stops the actor with a success exit status code.
    ///
    /// Upstream `actors` that terminates should send the `ExitWithSuccess`
    /// command to downstream actors to inform them that there are no more
    /// incoming messages.
    ///
    /// It is similar to `Quit`, except for the resulting exit status.
    ExitWithSuccess,

    /// Asks the actor to update its ObservableState.
    /// Since it is a command, it will be treated with a higher priority than
    /// a normal message.
    /// If the actor is processing message, it will finish it, and the state
    /// observed will be the state after this message.
    /// The Observe command also ships a oneshot channel to allow client
    /// to wait on this observation.
    ///
    /// The observation is then available using the `ActorHander::last_observation()`
    /// method.
    // We use a `Box<dyn Any>` here to avoid adding an observablestate generic
    // parameter to the mailbox.
    Observe(oneshot::Sender<Box<dyn Any + Send>>),

    /// Asks the actor to gracefully shutdown.
    ///
    /// The actor will stop processing messages and its finalize function will
    /// be called.
    ///
    /// The exit status is then `ActorExitStatus::Quit`.
    ///
    /// This is the equivalent of sending SIGINT/Ctrl-C to a process.
    Quit,

    /// Kill the actor. The behavior is the same as if an actor detected that its kill switch
    /// was pushed.
    ///
    /// It is similar to Quit, except the `ActorExitState` is different.
    ///
    /// It can have important side effect, as the actor `.finalize` method
    /// may have different behavior depending on the exit state.
    ///
    /// This is the equivalent of sending SIGKILL to a process.
    Kill,
}

impl fmt::Debug for Command {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Command::Pause => write!(f, "Pause"),
            Command::Resume => write!(f, "Resume"),
            Command::Observe(_) => write!(f, "Observe"),
            Command::ExitWithSuccess => write!(f, "Success"),
            Command::Quit => write!(f, "Quit"),
            Command::Kill => write!(f, "Kill"),
        }
    }
}

impl<Message: fmt::Debug> fmt::Debug for Mailbox<Message> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Mailbox({})", self.actor_instance_id())
    }
}

impl<Message> Hash for Mailbox<Message> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.inner.instance_id.hash(state)
    }
}

impl<Message> PartialEq for Mailbox<Message> {
    fn eq(&self, other: &Self) -> bool {
        self.inner.instance_id.eq(&other.inner.instance_id)
    }
}

impl<Message> Eq for Mailbox<Message> {}

impl<Message> Mailbox<Message> {
    pub fn actor_instance_id(&self) -> &str {
        &self.inner.instance_id
    }

    pub(crate) async fn send_with_priority(
        &self,
        cmd_or_msg: CommandOrMessage<Message>,
        priority: Priority,
    ) -> Result<(), SendError> {
        self.inner.tx.send(cmd_or_msg, priority).await
    }

    pub(crate) fn send_with_priority_blocking(
        &self,
        cmd_or_msg: CommandOrMessage<Message>,
        priority: Priority,
    ) -> Result<(), SendError> {
        self.inner.tx.send_blocking(cmd_or_msg, priority)
    }

    /// SendError is returned if the actor has already exited.
    ///
    /// (See also [Self::send_blocking()])
    pub(crate) async fn send_message(&self, msg: Message) -> Result<(), SendError> {
        self.send_with_priority(CommandOrMessage::Message(msg), Priority::Low)
            .await
    }

    /// Send a message to the actor in a blocking fashion.
    /// When possible, prefer using [Self::send()].
    pub(crate) fn send_message_blocking(&self, msg: Message) -> Result<(), SendError> {
        self.send_with_priority_blocking(CommandOrMessage::Message(msg), Priority::Low)
    }

    pub(crate) async fn send_command(&self, command: Command) -> Result<(), SendError> {
        self.send_with_priority(command.into(), Priority::High)
            .await
    }

    pub fn try_send_message(&self, message: Message) -> Result<(), SendError> {
        self.inner
            .tx
            .try_send(CommandOrMessage::Message(message), Priority::Low)
    }
}

pub struct Inbox<Message> {
    rx: Receiver<CommandOrMessage<Message>>,
}

impl<Message: fmt::Debug> Inbox<Message> {
    pub(crate) async fn recv_timeout(&mut self) -> Result<CommandOrMessage<Message>, RecvError> {
        self.rx.recv_timeout(crate::message_timeout()).await
    }

    pub(crate) async fn recv_timeout_cmd_and_scheduled_msg_only(
        &mut self,
    ) -> Result<CommandOrMessage<Message>, RecvError> {
        self.rx
            .recv_high_priority_timeout(crate::message_timeout())
            .await
    }

    pub(crate) fn recv_timeout_blocking(&mut self) -> Result<CommandOrMessage<Message>, RecvError> {
        self.rx.recv_timeout_blocking(crate::message_timeout())
    }

    pub(crate) fn recv_timeout_cmd_and_scheduled_msg_only_blocking(
        &mut self,
    ) -> Result<CommandOrMessage<Message>, RecvError> {
        self.rx
            .recv_high_priority_timeout_blocking(crate::message_timeout())
    }

    /// Destroys the inbox and returns the list of pending messages.
    /// Commands are ignored.
    ///
    /// Warning this iterator might never be exhausted if there is a living
    /// mailbox associated to it.
    pub fn drain_available_message_for_test(&self) -> Vec<Message> {
        self.rx
            .drain_low_priority()
            .into_iter()
            .flat_map(|command_or_message| match command_or_message {
                CommandOrMessage::Message(msg) => Some(msg),
                CommandOrMessage::Command(_) => None,
            })
            .collect()
    }

    /// Destroys the inbox and returns the list of pending messages or commands.
    ///
    /// Warning this iterator might never be exhausted if there is a living
    /// mailbox associated to it.
    pub fn drain_available_message_or_command_for_test(mut self) -> Vec<CommandOrMessage<Message>> {
        self.rx.drain_all()
    }
}

pub fn create_mailbox<M>(
    actor_name: String,
    queue_capacity: QueueCapacity,
) -> (Mailbox<M>, Inbox<M>) {
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

pub fn create_test_mailbox<M>() -> (Mailbox<M>, Inbox<M>) {
    create_mailbox("test-mailbox".to_string(), QueueCapacity::Unbounded)
}
