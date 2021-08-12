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
use crate::{
    actor_state::{ActorState, AtomicState},
    progress::{Progress, ProtectedZoneGuard},
    AsyncActor, KillSwitch, Mailbox, QueueCapacity, SendError, SyncActor,
};

/// The actor exit status represents the outcome of the execution of an actor,
/// after the end of the execution.
///
/// It is in many ways, similar to the exit status code of a program.
#[derive(Error, Debug)]
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
    /// Note this is not really an error.
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
    #[error("Failure")]
    Failure(#[from] anyhow::Error),

    /// The thread or the task executing the actor loop panicked.
    #[error("Panicked")]
    Panicked,
}

impl ActorExitStatus {
    pub fn is_success(&self) -> bool {
        matches!(self, ActorExitStatus::Success)
    }

    /// If an actor exits with a failure, its kill switch
    /// will be activated, and all other actors under the same
    /// kill switch will be killed.
    ///
    /// The semantic limit between a failure and not a failure here,
    /// is that a failure is an unexpected exit.
    /// It is not a success, and it was not triggered by an actual
    /// command or the kill switch.
    pub fn is_failure(&self) -> bool {
        match self {
            ActorExitStatus::DownstreamClosed => true,
            ActorExitStatus::Failure(_) => true,
            ActorExitStatus::Panicked => true,
            ActorExitStatus::Success => false,
            ActorExitStatus::Quit => false,
            ActorExitStatus::Killed => false,
        }
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
pub struct ActorContext<A: Actor> {
    inner: Arc<ActorContextInner<A>>,
}

impl<A: Actor> Clone for ActorContext<A> {
    fn clone(&self) -> Self {
        ActorContext {
            inner: self.inner.clone(),
        }
    }
}

impl<A: Actor> Deref for ActorContext<A> {
    type Target = ActorContextInner<A>;

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref()
    }
}

pub struct ActorContextInner<A: Actor> {
    self_mailbox: Mailbox<A::Message>,
    progress: Progress,
    kill_switch: KillSwitch,
    scheduler_mailbox: Mailbox<SchedulerMessage>,
    actor_state: AtomicState,
}

impl<A: Actor> ActorContext<A> {
    pub(crate) fn new(
        self_mailbox: Mailbox<A::Message>,
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

    pub fn mailbox(&self) -> &Mailbox<A::Message> {
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

    /// Records some progress.
    /// This function is only useful when implementing actors that may take more than
    /// `HEARTBEAT` to process a single message.
    /// In that case, you can call this function in the middle of the process_message method
    /// to prevent the actor from being identified as blocked or dead.
    pub fn record_progress(&self) {
        self.progress.record_progress();
    }

    pub(crate) fn get_state(&self) -> ActorState {
        self.actor_state.get_state()
    }

    pub(crate) fn pause(&mut self) {
        self.actor_state.pause();
    }

    pub(crate) fn resume(&mut self) {
        self.actor_state.resume();
    }

    pub(crate) fn exit(&mut self, exit_status: &ActorExitStatus) {
        if exit_status.is_failure() {
            error!(actor=%self.actor_instance_id(), exit_status=%exit_status, "Failure");
            self.kill_switch().kill();
        }
        self.actor_state.exit();
    }
}

impl<A: SyncActor> ActorContext<A> {
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
    pub fn send_self_message_blocking(&self, msg: A::Message) -> Result<(), crate::SendError> {
        debug!(self=%self.self_mailbox.actor_instance_id(), msg=?msg, "self_send");
        let send_res = self.self_mailbox.try_send_message(msg);
        if let Err(crate::SendError::Full) = &send_res {
            error!(self=%self.self_mailbox.actor_instance_id(),
                   "Attempted to send self message while the channel was full. This would have triggered a deadlock");
        }
        send_res
    }

    pub fn schedule_self_msg_blocking(&self, after_duration: Duration, msg: A::Message) {
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

impl<A: AsyncActor> ActorContext<A> {
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
    pub async fn send_self_message(&self, msg: A::Message) -> Result<(), crate::SendError> {
        debug!(self=%self.self_mailbox.actor_instance_id(), msg=?msg, "self_send");
        self.self_mailbox.send_message(msg).await
    }

    pub async fn schedule_self_msg(&self, after_duration: Duration, msg: A::Message) {
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
    ctx: &mut ActorContext<A>,
    state_tx: &Sender<A::ObservableState>,
) -> Option<ActorExitStatus> {
    match command {
        Command::Pause => {
            ctx.pause();
            None
        }
        Command::Quit(cb) => {
            let _ = cb.send(());
            Some(ActorExitStatus::Quit)
        }
        Command::Kill(cb) => {
            let _ = cb.send(());
            Some(ActorExitStatus::Killed)
        }
        Command::Resume => {
            ctx.resume();
            None
        }
        Command::Observe(cb) => {
            let state = actor.observable_state();
            let _ = state_tx.send(state);
            // We voluntarily ignore the error here. (An error only occurs if the
            // sender dropped its receiver.)
            let _ = cb.send(());
            None
        }
    }
}
