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
    pub(crate) tx: flume::Sender<ActorMessage<Message>>,
    command_tx: flume::Sender<Command<Message>>,
    id: Uuid,
    actor_name: String,
}

pub enum Command<Msg> {
    /// Temporarily pauses the actor. A paused actor only checks
    /// on its command channel and still shows "progress". It appears as
    /// healthy to the supervisor.
    Pause,
    /// Resume a paused actor. If the actor was not paused this command
    /// has no effects.
    Resume,
    /// Asks the actor to gracefully stop working.
    /// The actor will stop processing messages and call its finalize function.
    /// The actor termination is then `ActorTermination::OnDemand`.
    Terminate(oneshot::Sender<()>),
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
    Observe(oneshot::Sender<()>),
    /// It is possible to use the command channel to send regular message
    /// needing to be processed with a high priority.
    /// Internally, this is used by the scheduled message, to make sure that
    /// the message arrive as soon as possible after their timeout has expired.
    HighPriorityMessage(Msg),
}

impl<Msg> fmt::Debug for Command<Msg> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Command::Pause => write!(f, "Pause"),
            Command::Terminate(_) => write!(f, "Stop"),
            Command::Resume => write!(f, "Start"),
            Command::Observe(_) => write!(f, "Observe"),
            Command::HighPriorityMessage(_) => write!(f, "ScheduleMsg"),
        }
    }
}

impl<Message: fmt::Debug> fmt::Debug for Mailbox<Message> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Mailbox({})", self.actor_instance_name())
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
    pub fn actor_instance_name(&self) -> String {
        format!("{}:{}", self.actor_name, self.id)
    }

    pub(crate) async fn send_actor_message(
        &self,
        msg: ActorMessage<Message>,
    ) -> Result<(), SendError> {
        self.tx.send_async(msg).await?;
        Ok(())
    }

    /// Send a message to the actor synchronously.
    ///
    /// SendError is returned if the user is already terminated.
    ///
    /// (See also [Self::send_blocking()])
    pub(crate) async fn send_message(&self, msg: Message) -> Result<(), SendError> {
        self.send_actor_message(ActorMessage::Message(msg)).await
    }

    /// Send a message to the actor in a blocking fashion.
    /// When possible, prefer using [Self::send()].
    pub(crate) fn send_message_blocking(&self, msg: Message) -> Result<(), SendError> {
        self.tx.send(ActorMessage::Message(msg))?;
        Ok(())
    }

    pub fn send_command_blocking(&self, command: Command<Message>) -> Result<(), SendError> {
        self.command_tx.send(command)?;
        Ok(())
    }

    pub async fn send_command(&self, command: Command<Message>) -> Result<(), SendError> {
        self.command_tx.send_async(command).await?;
        Ok(())
    }
}

pub struct Inbox<Message> {
    pub rx: flume::Receiver<ActorMessage<Message>>,
    command_rx: flume::Receiver<Command<Message>>,
    // This channel is only used to make sure that no disconnection can happen
    // on the command channel.
    // It simplifies the reception code.
    _command_tx: flume::Sender<Command<Message>>,
    // This channel is here due to make it possible to properly emulate the priority channel even on a blocking actor.
    pending: Option<ReceptionResult<Message>>,
}

#[derive(Debug)]
pub enum ReceptionResult<M> {
    Command(Command<M>), // A command was received.
    Message(M),          //< A message was received.
    Timeout, //< No message was available. The timeout used is define by `crate::message_timeout()`
    Disconnect, //< Was disconnect from either the regular mailbox. Deconnection is not detected if the actor is paused.
}

impl<Message: fmt::Debug> Inbox<Message> {
    fn get_command_if_available(&self) -> Option<ReceptionResult<Message>> {
        match self.command_rx.try_recv() {
            Ok(command) => Some(ReceptionResult::Command(command)),
            Err(TryRecvError::Disconnected) => {
                unreachable!(
                    "This can never happen, as the command channel is owned by the Inbox."
                );
            }
            Err(TryRecvError::Empty) => None,
        }
    }

    /// Receive the first message that comes from (in that order):
    /// - the high priority command queue
    /// - the message queue.
    ///
    /// No message is returned as long as there are commands available.
    ///

    pub(crate) async fn try_recv_msg(&mut self, message_enabled: bool) -> ReceptionResult<Message> {
        if let Some(command_result) = self.get_command_if_available() {
            return command_result;
        }
        if !message_enabled {
            return ReceptionResult::Timeout;
        }
        if let Some(pending) = self.pending.take() {
            return pending;
        }
        tokio::select! {
            command_recv = self.command_rx.recv_async() => {
                match command_recv {
                    Ok(command) => ReceptionResult::Command(command),
                    _ => ReceptionResult::Timeout,
                }
            }
            actor_msg_recv = self.rx.recv_async() => {
                let msg_recv_result: ReceptionResult<Message> = match actor_msg_recv {
                    Ok(ActorMessage::Message(msg)) => ReceptionResult::Message(msg),
                    Ok(ActorMessage::Observe(cb)) => ReceptionResult::Command(Command::Observe(cb)),
                    Err(_recv_error) => ReceptionResult::Disconnect,
                };
                if let Some(command_result) = self.get_command_if_available() {
                    self.pending = Some(msg_recv_result);
                    command_result
                } else {
                    msg_recv_result
                }
            }
            _ = tokio::time::sleep(crate::message_timeout()) => ReceptionResult::Timeout,
        }
    }

    /// Same as `.try_recv_msg` but blocking.
    pub(crate) fn try_recv_msg_blocking(
        &mut self,
        message_enabled: bool,
    ) -> ReceptionResult<Message> {
        // The code below has extra complexity in order to emulate the fact that our command queue has
        // a higher priority.
        //
        // The original logic goes like this. If there is a command, we return it.
        // If there isn't then we recv from the message channel with a timeout. However, while we
        // were blocking, a command could have arrived.
        //
        // For this reason, we then recheck for the command channel and return the available
        // command if any.
        // The message that we popped up from the queue is put in a pending slot
        // that will be returned on the first subsequent round that does not have any command available.
        if let Some(command_result) = self.get_command_if_available() {
            return command_result;
        }
        if !message_enabled {
            return ReceptionResult::Timeout;
        }
        if let Some(pending_msg) = self.pending.take() {
            return pending_msg;
        }
        let msg = self.rx.recv_timeout(crate::message_timeout());
        let reception_result = match msg {
            Ok(ActorMessage::Message(msg)) => ReceptionResult::Message(msg),
            Ok(ActorMessage::Observe(cb)) => ReceptionResult::Command(Command::Observe(cb)),
            Err(RecvTimeoutError::Disconnected) => ReceptionResult::Disconnect,
            Err(RecvTimeoutError::Timeout) => ReceptionResult::Timeout,
        };
        if let Some(command_result) = self.get_command_if_available() {
            // There is a pending command. We shelf the message and return the command instead.
            self.pending = Some(reception_result);
            return command_result;
        }
        reception_result
    }

    /// Destroys the inbox and returns the list of pending messages.
    /// Commands are ignored.
    ///
    /// Warning this iterator might never be exhausted if there is a living
    /// mailbox associated to it.
    pub fn drain_available_message_for_test(&self) -> Vec<Message> {
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

pub fn create_mailbox<M>(
    actor_name: String,
    queue_capacity: QueueCapacity,
) -> (Mailbox<M>, Inbox<M>) {
    let (tx, rx) = queue_capacity.create_channel();
    let (command_tx, command_rx) = QueueCapacity::Unbounded.create_channel();
    let mailbox = Mailbox {
        inner: Arc::new(Inner {
            tx,
            command_tx: command_tx.clone(),
            id: Uuid::new_v4(),
            actor_name,
        }),
    };
    let inbox = Inbox {
        rx,
        command_rx,
        pending: None,
        _command_tx: command_tx,
    };
    (mailbox, inbox)
}

pub fn create_test_mailbox<M>() -> (Mailbox<M>, Inbox<M>) {
    create_mailbox("test-mailbox".to_string(), QueueCapacity::Unbounded)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use std::time::Instant;

    use super::*;

    #[tokio::test]
    async fn test_try_recv_prority() -> anyhow::Result<()> {
        let (mailbox, mut inbox) = create_test_mailbox::<usize>();
        mailbox.send_message(1).await?;
        mailbox.send_command(Command::Resume).await?;
        assert!(matches!(
            inbox.try_recv_msg(true).await,
            ReceptionResult::Command(Command::Resume)
        ));
        assert!(matches!(
            inbox.try_recv_msg(true).await,
            ReceptionResult::Message(1)
        ));
        assert!(matches!(
            inbox.try_recv_msg(true).await,
            ReceptionResult::Timeout
        ));
        Ok(())
    }

    #[tokio::test]
    async fn test_try_recv_ignore_messages() -> anyhow::Result<()> {
        let (mailbox, mut inbox) = create_test_mailbox::<usize>();
        mailbox.send_message(1).await?;
        assert!(matches!(
            inbox.try_recv_msg(false).await,
            ReceptionResult::Timeout
        ));
        Ok(())
    }

    #[tokio::test]
    async fn test_try_recv_ignore_messages_disconnection() -> anyhow::Result<()> {
        let (_mailbox, mut inbox) = create_test_mailbox::<usize>();
        assert!(matches!(
            inbox.try_recv_msg(false).await,
            ReceptionResult::Timeout
        ));
        Ok(())
    }

    #[tokio::test]
    async fn test_try_recv_disconnect() -> anyhow::Result<()> {
        let (mailbox, mut inbox) = create_test_mailbox::<usize>();
        std::mem::drop(mailbox);
        assert!(matches!(
            inbox.try_recv_msg(true).await,
            ReceptionResult::Disconnect
        ));
        Ok(())
    }

    #[tokio::test]
    async fn test_try_recv_simple_timeout() -> anyhow::Result<()> {
        let (_mailbox, mut inbox) = create_test_mailbox::<usize>();
        let start_time = Instant::now();
        assert!(matches!(
            inbox.try_recv_msg(true).await,
            ReceptionResult::Timeout
        ));
        let elapsed = start_time.elapsed();
        assert!(elapsed < crate::HEARTBEAT);
        Ok(())
    }

    #[tokio::test]
    async fn test_try_recv_prority_corner_case() -> anyhow::Result<()> {
        let (mailbox, mut inbox) = create_test_mailbox::<usize>();
        tokio::task::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            mailbox.send_command(Command::Resume).await?;
            mailbox.send_message(1).await?;
            Result::<(), SendError>::Ok(())
        });
        assert!(matches!(
            inbox.try_recv_msg(true).await,
            ReceptionResult::Command(Command::Resume)
        ));
        assert!(matches!(
            inbox.try_recv_msg(true).await,
            ReceptionResult::Message(1)
        ));
        assert!(matches!(
            inbox.try_recv_msg(true).await,
            ReceptionResult::Disconnect
        ));
        Ok(())
    }

    #[test]
    fn test_try_recv_prority_blocking() -> anyhow::Result<()> {
        let (mailbox, mut inbox) = create_test_mailbox::<usize>();
        mailbox.send_message_blocking(1)?;
        mailbox.send_command_blocking(Command::Resume)?;
        assert!(matches!(
            inbox.try_recv_msg_blocking(true),
            ReceptionResult::Command(Command::Resume)
        ));
        assert!(matches!(
            inbox.try_recv_msg_blocking(true),
            ReceptionResult::Message(1)
        ));
        assert!(matches!(
            inbox.try_recv_msg_blocking(true),
            ReceptionResult::Timeout
        ));
        Ok(())
    }

    #[test]
    fn test_try_recv_ignore_messages_blocking() -> anyhow::Result<()> {
        let (mailbox, mut inbox) = create_test_mailbox::<usize>();
        mailbox.send_message_blocking(1)?;
        assert!(matches!(
            inbox.try_recv_msg_blocking(false),
            ReceptionResult::Timeout
        ));
        Ok(())
    }

    #[test]
    fn test_try_recv_ignore_messages_disconnection_blocking() -> anyhow::Result<()> {
        let (_mailbox, mut inbox) = create_test_mailbox::<usize>();
        assert!(matches!(
            inbox.try_recv_msg_blocking(false),
            ReceptionResult::Timeout
        ));
        Ok(())
    }

    #[test]
    fn test_try_recv_disconnect_blocking() -> anyhow::Result<()> {
        let (mailbox, mut inbox) = create_test_mailbox::<usize>();
        std::mem::drop(mailbox);
        assert!(matches!(
            inbox.try_recv_msg_blocking(true),
            ReceptionResult::Disconnect
        ));
        Ok(())
    }

    #[test]
    fn test_try_recv_simple_timeout_blocking() -> anyhow::Result<()> {
        let (_mailbox, mut inbox) = create_test_mailbox::<usize>();
        let start_time = Instant::now();
        assert!(matches!(
            inbox.try_recv_msg_blocking(true),
            ReceptionResult::Timeout
        ));
        let elapsed = start_time.elapsed();
        assert!(elapsed < crate::HEARTBEAT);
        Ok(())
    }

    #[test]
    fn test_try_recv_prority_corner_case_blocking() -> anyhow::Result<()> {
        let (mailbox, mut inbox) = create_test_mailbox::<usize>();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(10));
            mailbox.send_command_blocking(Command::Resume)?;
            mailbox.send_message_blocking(1)?;
            Result::<(), SendError>::Ok(())
        });
        assert!(matches!(
            inbox.try_recv_msg_blocking(true),
            ReceptionResult::Command(Command::Resume)
        ));
        assert!(matches!(
            inbox.try_recv_msg_blocking(true),
            ReceptionResult::Message(1)
        ));
        assert!(matches!(
            inbox.try_recv_msg_blocking(true),
            ReceptionResult::Disconnect
        ));
        Ok(())
    }
}
