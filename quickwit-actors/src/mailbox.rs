//  Quickwit
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

use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

use flume::RecvTimeoutError;
use flume::TryRecvError;
use std::hash::Hash;
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::actor_handle::ActorMessage;
use crate::SendError;

pub struct Mailbox<Message> {
    inner: Arc<Inner<Message>>,
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

impl<Message> Deref for Mailbox<Message> {
    type Target = Inner<Message>;

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref()
    }
}

pub struct Inner<Message> {
    sender: flume::Sender<ActorMessage<Message>>,
    command_sender: flume::Sender<Command>,
    id: Uuid,
    actor_name: String,
}

pub enum Command {
    Pause,
    Stop(oneshot::Sender<()>),
    Start,
    Observe(oneshot::Sender<()>),
}

impl fmt::Debug for Command {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Command::Pause => write!(f, "Pause"),
            Command::Stop(_) => write!(f, "Stop"),
            Command::Start => write!(f, "Start"),
            Command::Observe(_) => write!(f, "Observe"),
        }
    }
}

impl<Message: fmt::Debug> fmt::Debug for Mailbox<Message> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Mailbox({})", self.actor_name())
    }
}

impl<Message> Hash for Mailbox<Message> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state)
    }
}

impl<Message> PartialEq for Mailbox<Message> {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}

impl<Message> Eq for Mailbox<Message> {}

impl<Message> Mailbox<Message> {
    pub fn actor_name(&self) -> String {
        format!("{}:{}", self.actor_name, self.id)
    }

    pub(crate) async fn send_actor_message(
        &self,
        msg: ActorMessage<Message>,
    ) -> Result<(), SendError> {
        self.sender.send_async(msg).await.map_err(|_| SendError)
    }

    /// Send a message to the actor synchronously.
    ///
    /// SendError is returned if the user is already terminated.
    ///
    /// (See also [Self::send_blocking()])
    pub async fn send_async(&self, msg: Message) -> Result<(), SendError> {
        self.send_actor_message(ActorMessage::Message(msg)).await
    }

    /// Send a message to the actor in a blocking fashion.
    /// When possible, prefer using [Self::send_async()].
    ///
    // TODO do we need a version with a deadline?
    pub fn send_blocking(&self, msg: Message) -> Result<(), SendError> {
        self.sender
            .send(ActorMessage::Message(msg))
            .map_err(|_e| SendError)
    }

    pub fn send_command_blocking(&self, command: Command) -> Result<(), SendError> {
        self.command_sender.send(command).map_err(|_e| SendError)
    }

    pub async fn send_command(&self, command: Command) -> Result<(), SendError> {
        self.command_sender
            .send_async(command)
            .await
            .map_err(|_e| SendError)
    }
}

pub struct Inbox<Message> {
    pub rx: flume::Receiver<ActorMessage<Message>>,
    command_rx: flume::Receiver<Command>,
}

#[derive(Debug)]
pub enum ReceptionResult<M> {
    Command(Command),
    Message(M),
    None,
    Disconnect,
}

impl<Message: fmt::Debug> Inbox<Message> {
    fn get_command_if_available(&self) -> Option<Command> {
        match self.command_rx.try_recv() {
            Ok(command) => Some(command),
            Err(TryRecvError::Disconnected) => None,
            Err(TryRecvError::Empty) => None,
        }
    }

    pub async fn try_recv_msg_async(
        &self,
        message_enabled: bool,
        default_message_opt: Option<Message>,
    ) -> ReceptionResult<Message> {
        if let Some(command) = self.get_command_if_available() {
            return ReceptionResult::Command(command);
        }
        if !message_enabled {
            return ReceptionResult::None;
        }
        if let Some(default_message) = default_message_opt {
            match self.rx.try_recv() {
                Ok(ActorMessage::Message(msg)) => ReceptionResult::Message(msg),
                Ok(ActorMessage::Observe(cb)) => ReceptionResult::Command(Command::Observe(cb)),
                Err(TryRecvError::Empty) => ReceptionResult::Message(default_message),
                Err(TryRecvError::Disconnected) => ReceptionResult::Disconnect,
            }
        } else {
            match tokio::time::timeout(crate::message_timeout(), self.rx.recv_async()).await {
                Ok(Ok(ActorMessage::Message(msg))) => ReceptionResult::Message(msg),
                Ok(Ok(ActorMessage::Observe(cb))) => ReceptionResult::Command(Command::Observe(cb)),
                Ok(Err(_recv_error)) => ReceptionResult::Disconnect,
                Err(_timeout_error) => ReceptionResult::None,
            }
        }
    }

    pub fn try_recv_msg(
        &self,
        message_enabled: bool,
        default_message_opt: Option<Message>, //< TODO this is not looking good...
            // We don't want to force message to be cloned and therefore we passed the message
            // by value.
            // We should probably leave it to the called to replace the message.
            //
            // The problem is that in presence of a message, we do not want to wait either.
            // A refactoring might be tricky, but is necessary.
    ) -> ReceptionResult<Message> {
        if let Some(command) = self.get_command_if_available() {
            return ReceptionResult::Command(command);
        }
        if !message_enabled {
            return ReceptionResult::None;
        }
        if let Some(default_message) = default_message_opt {
            match self.rx.try_recv() {
                Ok(ActorMessage::Message(msg)) => ReceptionResult::Message(msg),
                Ok(ActorMessage::Observe(cb)) => ReceptionResult::Command(Command::Observe(cb)),
                Err(TryRecvError::Empty) => ReceptionResult::Message(default_message),
                Err(TryRecvError::Disconnected) => ReceptionResult::Disconnect,
            }
        } else {
            let msg = self.rx.recv_timeout(crate::message_timeout());
            match msg {
                Ok(ActorMessage::Message(msg)) => ReceptionResult::Message(msg),
                Ok(ActorMessage::Observe(cb)) => ReceptionResult::Command(Command::Observe(cb)),
                Err(RecvTimeoutError::Disconnected) => ReceptionResult::Disconnect,
                Err(RecvTimeoutError::Timeout) => ReceptionResult::None,
            }
        }
    }

    /// Destroys the inbox and returns the list of pending messages.
    /// Commands are ignored.
    ///
    /// Warning this iterator might never be exhausted if there is a living
    /// mailbox associated to it.
    pub fn to_vec_for_test(self) -> Vec<Message> {
        let mut messages = Vec::new();
        loop {
            match self.rx.try_recv() {
                Ok(ActorMessage::Message(msg)) => messages.push(msg),
                Ok(ActorMessage::Observe(_)) => {}
                Err(_) => {
                    break;
                }
            }
        }
        messages
    }
}

#[derive(Clone, Copy, Debug)]
pub enum QueueCapacity {
    Bounded(usize),
    Unbounded,
}

impl QueueCapacity {
    fn create_channel<M>(&self) -> (flume::Sender<M>, flume::Receiver<M>) {
        match *self {
            QueueCapacity::Bounded(cap) => flume::bounded(cap),
            QueueCapacity::Unbounded => flume::unbounded(),
        }
    }
}

pub fn create_mailbox<M>(actor_name: String, capacity: QueueCapacity) -> (Mailbox<M>, Inbox<M>) {
    let (msg_tx, msg_rx) = capacity.create_channel();
    let (cmd_tx, cmd_rx) = QueueCapacity::Unbounded.create_channel();
    let mailbox = Mailbox {
        inner: Arc::new(Inner {
            sender: msg_tx,
            command_sender: cmd_tx,
            id: Uuid::new_v4(),
            actor_name,
        }),
    };
    let inbox = Inbox {
        rx: msg_rx,
        command_rx: cmd_rx,
    };
    (mailbox, inbox)
}

pub fn create_test_mailbox<M>() -> (Mailbox<M>, Inbox<M>) {
    create_mailbox("test-mailbox".to_string(), QueueCapacity::Unbounded)
}
