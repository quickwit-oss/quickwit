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
use std::time::Duration;
use std::{any::type_name, sync::Arc};
use thiserror::Error;
use tokio::sync::watch::Sender;
use tracing::{debug, error};

use crate::channel_with_priority::Priority;
use crate::mailbox::{Command, CommandOrMessage};
use crate::scheduler::{Callback, SchedulerMessage};
use crate::spawn_builder::SpawnBuilder;
use crate::{
    actor_state::{ActorState, AtomicState},
    progress::{Progress, ProtectedZoneGuard},
    KillSwitch, Mailbox, QueueCapacity, SendError,
};

/// The actor exit status represents the outcome of the execution of an actor,
/// after the end of the execution.
///
/// It is in many ways, similar to the exit status code of a program.
#[derive(Error, Debug, Clone)]
pub enum ActorExitStatus {
    /// The actor successfully exited.
    ///
    /// It happens either because:
    /// - all of the existing mailboxes were dropped and the actor message queue was exhausted.
    /// No new message could ever arrive to the actor. (This exit is triggered by the framework.)
    /// or
    /// - the actor `process_message` method returned `Err(ExitStatusCode::Success)`.
    /// (This exit is triggered by the actor implementer.)
    ///
    /// (This is equivalent to exit status code 0.)
    /// Note that this is not really an error.
    #[error("Success")]
    Success,

    /// The actor was asked to gracefully shutdown.
    ///
    /// (Semantically equivalent to exit status code 130, triggered by SIGINT aka Ctrl-C, or SIGQUIT)
    #[error("Quit")]
    Quit,

    /// The actor tried to send a message to a dowstream actor and failed.
    /// The logic ruled that the actor should be killed.
    ///
    /// (Semantically equivalent to exit status code 141, triggered by SIGPIPE)
    #[error("Downstream actor exited.")]
    DownstreamClosed,

    /// The actor was killed.
    ///
    /// It can happen because:
    /// - it received `Command::Kill`.
    /// - its kill switch was activated.
    ///
    /// (Semantically equivalent to exit status code 137, triggered by SIGKILL)
    #[error("Killed")]
    Killed,

    /// An unexpected error happened while processing a message.
    #[error("Failure(cause={0:?})")]
    Failure(Arc<anyhow::Error>),

    /// The thread or the task executing the actor loop panicked.
    #[error("Panicked")]
    Panicked,
}

impl From<anyhow::Error> for ActorExitStatus {
    fn from(err: anyhow::Error) -> Self {
        ActorExitStatus::Failure(Arc::new(err))
    }
}

impl ActorExitStatus {
    pub fn is_success(&self) -> bool {
        matches!(self, ActorExitStatus::Success)
    }
}

impl From<SendError> for ActorExitStatus {
    fn from(_: SendError) -> Self {
        ActorExitStatus::DownstreamClosed
    }
}

/// An actor has an internal state and processes a stream of messages.
/// Each actor has a mailbox where the messages are enqueued before being processed.
///
/// While processing a message, the actor typically
/// - update its state;
/// - emits one or more messages to other actors.
///
/// Actors exist in two flavors:
/// - async actors, are executed in event thread in tokio runtime.
/// - sync actors, executed on the blocking thread pool of tokio runtime.
pub trait Actor: Send + Sync + 'static {
    /// Type of message that can be received by the actor.
    type Message: Send + Sync + fmt::Debug;
    /// Piece of state that can be copied for assert in unit test, admin, etc.
    type ObservableState: Send + Sync + Clone + fmt::Debug;
    /// A name identifying the type of actor.
    /// It does not need to be "instance-unique", and can be the name of
    /// the actor implementation.
    fn name(&self) -> String {
        type_name::<Self>().to_string()
    }
    /// The Actor's incoming mailbox queue capacity. It is set when the actor is spawned.
    fn queue_capacity(&self) -> QueueCapacity {
        QueueCapacity::Unbounded
    }

    /// Extracts an observable state. Useful for unit test, and admin UI.
    ///
    /// This function should return fast.
    fn observable_state(&self) -> Self::ObservableState;
}

// TODO hide all of this public stuff
pub struct ActorContext<Message> {
    inner: Arc<ActorContextInner<Message>>,
}

impl<Message> Clone for ActorContext<Message> {
    fn clone(&self) -> Self {
        ActorContext {
            inner: self.inner.clone(),
        }
    }
}

impl<Message> Deref for ActorContext<Message> {
    type Target = ActorContextInner<Message>;

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref()
    }
}

pub struct ActorContextInner<Message> {
    self_mailbox: Mailbox<Message>,
    progress: Progress,
    kill_switch: KillSwitch,
    scheduler_mailbox: Mailbox<SchedulerMessage>,
    actor_state: AtomicState,
}

impl<Message> ActorContext<Message> {
    pub(crate) fn new(
        self_mailbox: Mailbox<Message>,
        kill_switch: KillSwitch,
        scheduler_mailbox: Mailbox<SchedulerMessage>,
    ) -> Self {
        ActorContext {
            inner: ActorContextInner {
                self_mailbox,
                progress: Progress::default(),
                kill_switch,
                scheduler_mailbox,
                actor_state: AtomicState::default(),
            }
            .into(),
        }
    }

    pub fn mailbox(&self) -> &Mailbox<Message> {
        &self.self_mailbox
    }

    pub fn actor_instance_id(&self) -> &str {
        self.mailbox().actor_instance_id()
    }

    /// This function returns a guard that prevents any supervisor from identifying the
    /// actor as dead.
    /// The protection ends when the `ProtectZoneGuard` is dropped.
    ///
    /// In an ideal world, you should never need to call this function.
    /// It is only useful in some corner like, like a calling a long blocking
    /// from an external library that you trust.
    pub fn protect_zone(&self) -> ProtectedZoneGuard {
        self.progress.protect_zone()
    }

    /// Gets a copy of the actor kill switch.
    /// This should rarely be used.
    ///
    /// For instance, when quitting from the process_message function, prefer simply
    /// returning `Error(ActorExitStatus::Failure(..))`
    pub fn kill_switch(&self) -> &KillSwitch {
        &self.kill_switch
    }

    pub(crate) fn progress(&self) -> &Progress {
        &self.progress
    }

    pub fn spawn_actor<A: Actor>(&self, actor: A) -> SpawnBuilder<A> {
        SpawnBuilder::new(
            actor,
            self.scheduler_mailbox.clone(),
            self.kill_switch.clone(),
        )
    }

    /// Records some progress.
    /// This function is only useful when implementing actors that may take more than
    /// `HEARTBEAT` to process a single message.
    /// In that case, you can call this function in the middle of the process_message method
    /// to prevent the actor from being identified as blocked or dead.
    pub fn record_progress(&self) {
        self.progress.record_progress();
    }

    pub(crate) fn state(&self) -> ActorState {
        self.actor_state.get_state()
    }

    pub(crate) fn pause(&mut self) {
        self.actor_state.pause();
    }

    pub(crate) fn resume(&mut self) {
        self.actor_state.resume();
    }

    pub(crate) fn exit(&mut self, exit_status: &ActorExitStatus) {
        if should_activate_kill_switch(exit_status) {
            error!(actor=%self.actor_instance_id(), exit_status=?exit_status, "exit activating-kill-switch");
            self.kill_switch().kill();
        }
        self.actor_state.exit();
    }
}

/// If an actor exits in an unexpected manner, its kill
/// switch will be activated, and all other actors under the same
/// kill switch will be killed.
fn should_activate_kill_switch(exit_status: &ActorExitStatus) -> bool {
    match exit_status {
        ActorExitStatus::DownstreamClosed => true,
        ActorExitStatus::Failure(_) => true,
        ActorExitStatus::Panicked => true,
        ActorExitStatus::Success => false,
        ActorExitStatus::Quit => false,
        ActorExitStatus::Killed => false,
    }
}

impl<Message: Send + Sync + fmt::Debug + 'static> ActorContext<Message> {
    /// Sends a message in the actor's mailbox.
    ///
    /// This method hides logic to prevent an actor from being identified
    /// as frozen if the destination actor channel is saturated, and we
    /// are simply experiencing back pressure.
    pub fn send_message_blocking<M: fmt::Debug>(
        &self,
        mailbox: &Mailbox<M>,
        msg: M,
    ) -> Result<(), crate::SendError> {
        let _guard = self.protect_zone();
        debug!(from=%self.self_mailbox.actor_instance_id(), to=%mailbox.actor_instance_id(), msg=?msg, "send");
        mailbox.send_message_blocking(msg)
    }

    /// Sends a message to itself.
    ///
    /// Since it is very easy to deadlock an actor, the behavior is quite
    /// different from `send_message_blocking`.
    ///
    /// If the message queue does not have the capacity to accept the message,
    /// instead of blocking forever, we return an error right away.
    pub fn send_self_message_blocking(&self, msg: Message) -> Result<(), crate::SendError> {
        debug!(self=%self.self_mailbox.actor_instance_id(), msg=?msg, "self_send");
        let send_res = self.self_mailbox.try_send_message(msg);
        if let Err(crate::SendError::Full) = &send_res {
            error!(self=%self.self_mailbox.actor_instance_id(),
                   "Attempted to send self message while the channel was full. This would have triggered a deadlock");
        }
        send_res
    }

    pub fn schedule_self_msg_blocking(&self, after_duration: Duration, msg: Message) {
        let self_mailbox = self.inner.self_mailbox.clone();
        let scheduler_msg = SchedulerMessage::ScheduleEvent {
            timeout: after_duration,
            callback: Callback(Box::pin(async move {
                let _ = self_mailbox
                    .send_with_priority(CommandOrMessage::Message(msg), Priority::High)
                    .await;
            })),
        };
        let _ = self.send_message_blocking(&self.inner.scheduler_mailbox, scheduler_msg);
    }
}

impl<Message: Send + Sync + fmt::Debug + 'static> ActorContext<Message> {
    /// `async` version of `send_message`
    pub async fn send_message<M: fmt::Debug>(
        &self,
        mailbox: &Mailbox<M>,
        msg: M,
    ) -> Result<(), crate::SendError> {
        let _guard = self.protect_zone();
        debug!(from=%self.self_mailbox.actor_instance_id(), send=%mailbox.actor_instance_id(), msg=?msg);
        mailbox.send_message(msg).await
    }

    /// `async` version of `send_self_message`
    pub async fn send_self_message(&self, msg: Message) -> Result<(), crate::SendError> {
        debug!(self=%self.self_mailbox.actor_instance_id(), msg=?msg, "self_send");
        self.self_mailbox.send_message(msg).await
    }

    pub async fn schedule_self_msg(&self, after_duration: Duration, msg: Message) {
        let self_mailbox = self.inner.self_mailbox.clone();
        let callback = Callback(Box::pin(async move {
            let _ = self_mailbox
                .send_with_priority(CommandOrMessage::Message(msg), Priority::High)
                .await;
        }));
        let scheduler_msg = SchedulerMessage::ScheduleEvent {
            timeout: after_duration,
            callback,
        };
        let _ = self
            .send_message(&self.inner.scheduler_mailbox, scheduler_msg)
            .await;
    }
}

pub(crate) fn process_command<A: Actor>(
    actor: &mut A,
    command: Command,
    ctx: &mut ActorContext<A::Message>,
    state_tx: &Sender<A::ObservableState>,
) -> Option<ActorExitStatus> {
    match command {
        Command::Pause => {
            ctx.pause();
            None
        }
        Command::Quit => Some(ActorExitStatus::Quit),
        Command::Kill => Some(ActorExitStatus::Killed),
        Command::Resume => {
            ctx.resume();
            None
        }
        Command::Observe(cb) => {
            let state = actor.observable_state();
            let _ = state_tx.send(state.clone());
            // We voluntarily ignore the error here. (An error only occurs if the
            // sender dropped its receiver.)
            let _ = cb.send(Box::new(state));
            None
        }
    }
}
