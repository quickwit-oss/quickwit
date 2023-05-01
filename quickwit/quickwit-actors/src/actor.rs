// Copyright (C) 2023 Quickwit, Inc.
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
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;
use tracing::error;

use crate::{ActorContext, QueueCapacity, SendError};

/// The actor exit status represents the outcome of the execution of an actor,
/// after the end of the execution.
///
/// It is in many ways, similar to the exit status code of a program.
#[derive(Clone, Debug, Error)]
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
    type ObservableState: Send + Sync + Clone + serde::Serialize + fmt::Debug;
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
    /// Actor with a handler that may block for more than 50 microseconds
    /// should use the `ActorRunner::DedicatedThread`.
    fn runtime_handle(&self) -> tokio::runtime::Handle {
        tokio::runtime::Handle::current()
    }

    /// If set to true, the actor will yield after every single
    /// message.
    ///
    /// For actors that are calling `.await` regularly,
    /// returning `false` can yield better performance.
    fn yield_after_each_message(&self) -> bool {
        true
    }

    /// The Actor's incoming mailbox queue capacity. It is set when the actor is spawned.
    fn queue_capacity(&self) -> QueueCapacity {
        QueueCapacity::Unbounded
    }

    /// Extracts an observable state. Useful for unit tests, and admin UI.
    ///
    /// This function should return quickly.
    fn observable_state(&self) -> Self::ObservableState;

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

    /// This function is called after a series of one, or several messages have been processed and
    /// no more message is available.
    ///
    /// It is a great place to have the actor "sleep".
    ///
    /// Quickwit's Indexer actor for instance use `on_drained_messages` to
    /// schedule indexing in such a way that an indexer drains all of its
    /// available messages and sleeps for some amount of time.
    async fn on_drained_messages(
        &mut self,
        _ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
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

/// Message handler that allows actor to defer the reply
#[async_trait::async_trait]
pub trait DeferableReplyHandler<M>: Actor {
    type Reply: Send + 'static;

    async fn handle_message(
        &mut self,
        message: M,
        reply: impl FnOnce(Self::Reply) + Send + Sync + 'static,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus>
    where
        M: Send + Sync + 'static;
}

/// Message handler that requires actor to provide immediate response
#[async_trait::async_trait]
pub trait Handler<M>: Actor {
    type Reply: Send + 'static;

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

#[async_trait::async_trait]
impl<H, M> DeferableReplyHandler<M> for H
where H: Handler<M>
{
    type Reply = H::Reply;

    async fn handle_message(
        &mut self,
        message: M,
        reply: impl FnOnce(Self::Reply) + Send + 'static,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus>
    where
        M: Send + 'static + Send + Sync,
    {
        self.handle(message, ctx).await.map(reply)
    }
}
