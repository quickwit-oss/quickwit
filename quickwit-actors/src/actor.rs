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

use std::any::type_name;
use std::convert::Infallible;
use std::fmt;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::oneshot;
use tokio::sync::watch::Sender;
use tracing::{debug, error, info_span, Span};

use crate::actor_state::{ActorState, AtomicState};
use crate::channel_with_priority::Priority;
use crate::envelope::wrap_in_envelope;
use crate::mailbox::{Command, CommandOrMessage};
use crate::progress::{Progress, ProtectedZoneGuard};
use crate::scheduler::{Callback, ScheduleEvent, Scheduler};
use crate::spawn_builder::SpawnBuilder;
use crate::{ActorRunner, AskError, KillSwitch, Mailbox, QueueCapacity, SendError};

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
    /// (Semantically equivalent to exit status code 130, triggered by SIGINT aka Ctrl-C, or
    /// SIGQUIT)
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
#[async_trait]
pub trait Actor: Send + Sync + Sized + 'static {
    /// Piece of state that can be copied for assert in unit test, admin, etc.
    type ObservableState: Send + Sync + Clone + fmt::Debug;
    /// A name identifying the type of actor.
    ///
    /// Ideally respect the `CamelCase` convention.
    ///
    /// It does not need to be "instance-unique", and can be the name of
    /// the actor implementation.
    fn name(&self) -> String {
        type_name::<Self>().to_string()
    }

    /// The runner method makes it possible to decide the environment
    /// of execution of the Actor.
    ///
    /// Actor with a handler that may block for more than 50microsecs should
    /// use the `ActorRunner::DedicatedThread`.
    fn runner(&self) -> ActorRunner {
        ActorRunner::GlobalRuntime
    }

    /// The Actor's incoming mailbox queue capacity. It is set when the actor is spawned.
    fn queue_capacity(&self) -> QueueCapacity {
        QueueCapacity::Unbounded
    }

    /// Extracts an observable state. Useful for unit tests, and admin UI.
    ///
    /// This function should return quickly.
    fn observable_state(&self) -> Self::ObservableState;

    /// Creates a span associated to all logging happening during the lifetime of an actor instance.
    fn span(&self, _ctx: &ActorContext<Self>) -> Span {
        info_span!("", actor = %self.name())
    }

    /// Initialize is called before running the actor.
    ///
    /// This function is useful for instance to schedule an initial message in a looping
    /// actor.
    ///
    /// It can be compared just to an implicit Initial message.
    ///
    /// Returning an ActorExitStatus will therefore have the same effect as if it
    /// was in `process_message` (e.g. the actor will stop, the finalize method will be called.
    /// the kill switch may be activated etc.)
    async fn initialize(&mut self, _ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        Ok(())
    }

    /// Hook  that can be set up to define what should happen upon actor exit.
    /// This hook is called only once.
    ///
    /// It is always called regardless of the reason why the actor exited.
    /// The exit status is passed as an argument to make it possible to act conditionnally
    /// upon it.
    /// For instance, it is often better to do as little work as possible on a killed actor.
    /// It can be done by checking the `exit_status` and performing an early-exit if it is
    /// equal to `ActorExitStatus::Killed`.
    async fn finalize(
        &mut self,
        _exit_status: &ActorExitStatus,
        _ctx: &ActorContext<Self>,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

// TODO hide all of this public stuff
pub struct ActorContext<A: Actor> {
    inner: Arc<ActorContextInner<A>>,
    phantom_data: PhantomData<A>,
}

impl<A: Actor> Clone for ActorContext<A> {
    fn clone(&self) -> Self {
        ActorContext {
            inner: self.inner.clone(),
            phantom_data: PhantomData,
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
    self_mailbox: Mailbox<A>,
    progress: Progress,
    kill_switch: KillSwitch,
    scheduler_mailbox: Mailbox<Scheduler>,
    actor_state: AtomicState,
}

impl<A: Actor> ActorContext<A> {
    pub(crate) fn new(
        self_mailbox: Mailbox<A>,
        kill_switch: KillSwitch,
        scheduler_mailbox: Mailbox<Scheduler>,
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
            phantom_data: PhantomData,
        }
    }

    pub fn mailbox(&self) -> &Mailbox<A> {
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

    pub fn progress(&self) -> &Progress {
        &self.progress
    }

    pub fn spawn_actor<SpawnedActor: Actor>(
        &self,
        actor: SpawnedActor,
    ) -> SpawnBuilder<SpawnedActor> {
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

    pub(crate) fn process(&mut self) {
        self.actor_state.process();
    }

    pub(crate) fn idle(&mut self) {
        self.actor_state.idle();
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

impl<A: Actor> ActorContext<A> {
    /// Posts a message in an actor's mailbox.
    ///
    /// This method does not wait for the message to be handled by the
    /// target actor. However, it returns a oneshot receiver that the caller
    /// that makes it possible to `.await` it.
    /// If the reply is important, chances are the `.ask(...)` method is
    /// more indicated.
    ///
    /// Droppping the receiver channel will not cancel the
    /// processing of the messsage. It is a very common usage.
    /// In fact most actors are expected to send message in a
    /// fire-and-forget fashion.
    ///
    /// Regular messages (as opposed to commands) are queued and guaranteed
    /// to be processed in FIFO order.
    ///
    /// This method hides logic to prevent an actor from being identified
    /// as frozen if the destination actor channel is saturated, and we
    /// are simply experiencing back pressure.
    pub async fn send_message<DestActor: Actor, M>(
        &self,
        mailbox: &Mailbox<DestActor>,
        msg: M,
    ) -> Result<oneshot::Receiver<DestActor::Reply>, crate::SendError>
    where
        DestActor: Handler<M>,
        M: 'static + Send + Sync + fmt::Debug,
    {
        let _guard = self.protect_zone();
        debug!(from=%self.self_mailbox.actor_instance_id(), send=%mailbox.actor_instance_id(), msg=?msg);
        mailbox.send_message(msg).await
    }

    pub async fn ask<DestActor: Actor, M, T>(
        &self,
        mailbox: &Mailbox<DestActor>,
        msg: M,
    ) -> Result<T, AskError<Infallible>>
    where
        DestActor: Handler<M, Reply = T>,
        M: 'static + Send + Sync + fmt::Debug,
    {
        let _guard = self.protect_zone();
        debug!(from=%self.self_mailbox.actor_instance_id(), send=%mailbox.actor_instance_id(), msg=?msg, "ask");
        mailbox.ask(msg).await
    }

    /// Similar to `send_message`, except this method
    /// waits asynchronously for the actor reply.
    pub async fn ask_for_res<DestActor: Actor, M, T, E: fmt::Debug>(
        &self,
        mailbox: &Mailbox<DestActor>,
        msg: M,
    ) -> Result<T, AskError<E>>
    where
        DestActor: Handler<M, Reply = Result<T, E>>,
        M: 'static + Send + Sync + fmt::Debug,
    {
        let _guard = self.protect_zone();
        debug!(from=%self.self_mailbox.actor_instance_id(), send=%mailbox.actor_instance_id(), msg=?msg, "ask");
        mailbox.ask_for_res(msg).await
    }

    /// Send the Success message to terminate the destination actor with the Success exit status.
    ///
    /// The message is queued like any regular message, so that pending messages will be processed
    /// first.
    pub async fn send_exit_with_success<Dest: Actor>(
        &self,
        mailbox: &Mailbox<Dest>,
    ) -> Result<(), crate::SendError> {
        let _guard = self.protect_zone();
        debug!(from=%self.self_mailbox.actor_instance_id(), to=%mailbox.actor_instance_id(), "success");
        mailbox
            .send_with_priority(
                CommandOrMessage::Command(Command::ExitWithSuccess),
                Priority::Low,
            )
            .await
    }

    /// `async` version of `send_self_message`.
    pub async fn send_self_message<M>(
        &self,
        msg: M,
    ) -> Result<oneshot::Receiver<A::Reply>, crate::SendError>
    where
        A: Handler<M>,
        M: 'static + Sync + Send + fmt::Debug,
    {
        debug!(self=%self.self_mailbox.actor_instance_id(), msg=?msg, "self_send");
        self.self_mailbox.send_message(msg).await
    }

    pub async fn schedule_self_msg<M>(&self, after_duration: Duration, msg: M)
    where
        A: Handler<M>,
        M: 'static + Send + Sync + fmt::Debug,
    {
        let self_mailbox = self.inner.self_mailbox.clone();
        let (envelope, _response_rx) = wrap_in_envelope(msg);
        let callback = Callback(Box::pin(async move {
            let _ = self_mailbox
                .send_with_priority(CommandOrMessage::Message(envelope), Priority::High)
                .await;
        }));
        let scheduler_msg = ScheduleEvent {
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
        Command::ExitWithSuccess => Some(ActorExitStatus::Success),
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

#[async_trait::async_trait]
pub trait Handler<M>: Actor {
    type Reply: 'static + Send;

    /// Processes a message.
    ///
    /// If an exit status is returned as an error, the actor will exit.
    /// It will stop processing more message, the finalize method will be called,
    /// and its exit status will be the one defined in the error.
    async fn handle(
        &mut self,
        message: M,
        ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus>;
}
