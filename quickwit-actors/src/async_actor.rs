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
use tokio::sync::watch::{self, Sender};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, Instrument};

use crate::actor::{process_command, ActorExitStatus};
use crate::actor_handle::ActorHandle;
use crate::actor_with_state_tx::ActorWithStateTx;
use crate::mailbox::{CommandOrMessage, Inbox};
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

pub(crate) fn spawn_async_actor<A: AsyncActor>(
    actor: A,
    ctx: ActorContext<A::Message>,
    inbox: Inbox<A::Message>,
) -> ActorHandle<A> {
    let (state_tx, state_rx) = watch::channel(actor.observable_state());
    let ctx_clone = ctx.clone();
    let span = actor.span(&ctx_clone);
    let (exit_status_tx, exit_status_rx) = watch::channel(None);
    let loop_async_actor_future = async move {
        debug!("spawn-async");
        let actor_instance_id = ctx.actor_instance_id().to_string();
        let exit_status = async_actor_loop(actor, inbox, ctx, state_tx).await;
        info!(
            actor_name = actor_instance_id.as_str(),
            exit_status = %exit_status,
            "actor-exit"
        );
        let _ = exit_status_tx.send(Some(exit_status));
    }
    .instrument(span);
    let join_handle: JoinHandle<()> = tokio::spawn(loop_async_actor_future);
    ActorHandle::new(state_rx, join_handle, ctx_clone, exit_status_rx)
}

async fn process_msg<A: Actor + AsyncActor>(
    actor: &mut A,
    msg_id: u64,
    inbox: &mut Inbox<A::Message>,
    ctx: &mut ActorContext<A::Message>,
    state_tx: &Sender<A::ObservableState>,
) -> Option<ActorExitStatus> {
    if ctx.kill_switch().is_dead() {
        return Some(ActorExitStatus::Killed);
    }
    ctx.progress().record_progress();

    let command_or_msg_recv_res = if ctx.state().is_running() {
        inbox.recv_timeout().await
    } else {
        // The actor is paused. We only process command and scheduled message.
        inbox.recv_timeout_cmd_and_scheduled_msg_only().await
    };

    ctx.progress().record_progress();
    if ctx.kill_switch().is_dead() {
        return Some(ActorExitStatus::Killed);
    }

    match command_or_msg_recv_res {
        Ok(CommandOrMessage::Command(cmd)) => {
            ctx.process();
            process_command(actor, cmd, ctx, state_tx)
        }
        Ok(CommandOrMessage::Message(msg)) => {
            ctx.process();
            let span = actor.message_span(msg_id, &msg);
            actor.process_message(msg, ctx).instrument(span).await.err()
        }
        Err(RecvError::Disconnected) => Some(ActorExitStatus::Success),
        Err(RecvError::Timeout) => {
            ctx.idle();
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

async fn async_actor_loop<A: AsyncActor>(
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

    let mut msg_id: u64 = 1;
    let mut exit_status: ActorExitStatus = loop {
        tokio::task::yield_now().await;
        if let Some(exit_status) = exit_status_opt {
            break exit_status;
        }
        exit_status_opt = process_msg(
            &mut actor_with_state_tx.actor,
            msg_id,
            &mut inbox,
            &mut ctx,
            &actor_with_state_tx.state_tx,
        )
        .await;
        msg_id += 1;
    };
    ctx.record_progress();
    if let Err(finalize_error) = actor_with_state_tx
        .actor
        .finalize(&exit_status, &ctx)
        .await
        .with_context(|| format!("Finalization of actor {}", actor_with_state_tx.actor.name()))
    {
        error!(error=?finalize_error, "Finalizing failed, set exit status to panicked.");
        exit_status = ActorExitStatus::Panicked;
    }

    info!(exit_status=%exit_status, "exit");
    ctx.exit(&exit_status);

    exit_status
}
