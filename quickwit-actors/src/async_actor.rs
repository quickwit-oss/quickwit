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

use anyhow::Context;
use async_trait::async_trait;
use tokio::sync::watch::Sender;
use tracing::{debug, error};

use crate::actor::ActorExitStatus;
use crate::actor_state::ActorState;
use crate::actor_with_state_tx::ActorWithStateTx;
use crate::channel_with_priority::Priority;
use crate::mailbox::{Command, CommandOrMessage, Inbox};
use crate::{Actor, ActorContext, RecvError};

/// An async actor is executed on a regular tokio task.
///
/// It can make async calls, but it should not block.
/// Actors doing CPU-heavy work should implement `SyncActor` instead.
#[async_trait]
pub trait AsyncActor: Actor + Sized {
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
    async fn initialize(
        &mut self,
        _ctx: &ActorContext<Self::Message>,
    ) -> Result<(), ActorExitStatus> {
        Ok(())
    }

    /// Processes a message.
    ///
    /// If an exit status is returned as an error, the actor will exit.
    /// It will stop processing more message, the finalize method will be called,
    /// and its exit status will be the one defined in the error.
    async fn process_message(
        &mut self,
        message: Self::Message,
        ctx: &ActorContext<Self::Message>,
    ) -> Result<(), ActorExitStatus>;

    /// Makes it possible to override the default behavior of
    /// handling command. (At your own risk!)
    #[doc(hidden)]
    async fn process_command(
        &mut self,
        command: Command,
        _priority: Priority,
        ctx: &mut ActorContext<Self::Message>,
        state_tx: &Sender<Self::ObservableState>,
    ) -> Result<(), ActorExitStatus> {
        crate::actor::process_command(self, command, ctx, state_tx)
    }

    /// Hook  that can be set up to define what should happen upon actor exit.
    /// This hook is called only once.
    ///
    /// It is always called regardless of the reason why the actor exited.
    /// The exit status is passed as an argument to make it possible to act conditionnally
    /// upon it.
    ///
    /// It is recommended to do as little work as possible on a killed actor.
    async fn finalize(
        &mut self,
        _exit_status: &ActorExitStatus,
        _ctx: &ActorContext<Self::Message>,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

async fn process_msg<A: Actor + AsyncActor>(
    actor: &mut A,
    inbox: &mut Inbox<A::Message>,
    ctx: &mut ActorContext<A::Message>,
    state_tx: &Sender<A::ObservableState>,
) -> Option<ActorExitStatus> {
    if ctx.kill_switch().is_dead() {
        return Some(ActorExitStatus::Killed);
    }
    ctx.progress().record_progress();

    let command_or_msg_recv_res = if ctx.state() == ActorState::Running {
        inbox.recv_timeout().await
    } else {
        // The actor is paused. We only process command and scheduled message.
        inbox
            .recv_timeout_cmd_and_scheduled_msg_only()
            .await
            .map(|cmd_or_msg| (Priority::High, cmd_or_msg))
    };

    ctx.progress().record_progress();
    if ctx.kill_switch().is_dead() {
        return Some(ActorExitStatus::Killed);
    }

    match command_or_msg_recv_res {
        Ok((priority, CommandOrMessage::Command(cmd))) => {
            debug!(cmd=?cmd, actor=%ctx.actor_instance_id(), "command-received");
            actor
                .process_command(cmd, priority, ctx, state_tx)
                .await
                .err()
        }
        Ok((_, CommandOrMessage::Message(msg))) => {
            debug!(msg=?msg, actor=%ctx.actor_instance_id(),"message-received");
            actor.process_message(msg, ctx).await.err()
        }
        Err(RecvError::Disconnected) => Some(ActorExitStatus::Success),
        Err(RecvError::Timeout) => {
            if ctx.mailbox().is_last_mailbox() {
                // No one will be able to send us more messages.
                // We can exit the actor.
                Some(ActorExitStatus::Success)
            } else {
                None
            }
        }
    }
}

pub(crate) async fn async_actor_loop<A: AsyncActor>(
    actor: A,
    mut inbox: Inbox<A::Message>,
    mut ctx: ActorContext<A::Message>,
    state_tx: Sender<A::ObservableState>,
) -> ActorExitStatus {
    // We rely on this object internally to fetch a post-mortem state,
    // even in case of a panic.
    let mut actor_with_state_tx = ActorWithStateTx { actor, state_tx };

    let mut exit_status_opt: Option<ActorExitStatus> =
        actor_with_state_tx.actor.initialize(&ctx).await.err();

    let exit_status: ActorExitStatus = loop {
        tokio::task::yield_now().await;
        if let Some(exit_status) = exit_status_opt {
            break exit_status;
        }
        exit_status_opt = process_msg(
            &mut actor_with_state_tx.actor,
            &mut inbox,
            &mut ctx,
            &actor_with_state_tx.state_tx,
        )
        .await;
    };
    ctx.exit(&exit_status);

    if let Err(finalize_error) = actor_with_state_tx
        .actor
        .finalize(&exit_status, &ctx)
        .await
        .with_context(|| format!("Finalization of actor {}", actor_with_state_tx.actor.name()))
    {
        error!(err=?finalize_error, "finalize_error");
    }
    exit_status
}
