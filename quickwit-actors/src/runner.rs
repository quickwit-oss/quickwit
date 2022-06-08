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

use std::collections::HashMap;
use std::future::Future;

use anyhow::Context;
use once_cell::sync::OnceCell;
use tokio::runtime::Runtime;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, Instrument};

use crate::actor::process_command;
use crate::actor_with_state_tx::ActorWithStateTx;
use crate::mailbox::{CommandOrMessage, Inbox};
use crate::{Actor, ActorContext, ActorExitStatus, ActorHandle, RecvError};

static RUNTIMES: OnceCell<HashMap<RuntimeType, tokio::runtime::Runtime>> = OnceCell::new();

fn get_tokio_runtime_handle(runtime_type: RuntimeType) -> tokio::runtime::Handle {
    let runtime: &Runtime = RUNTIMES
        .get_or_init(|| {
            let mut runtimes = HashMap::default();
            // TODO make configurable
            let blocking_runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            runtimes.insert(RuntimeType::Blocking, blocking_runtime);
            let non_blocking_runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            runtimes.insert(RuntimeType::NonBlocking, non_blocking_runtime);
            runtimes
        })
        .get(&runtime_type)
        .unwrap();
    runtime.handle().clone()
}

#[derive(Hash, Clone, Copy, PartialEq, Eq, Debug)]
pub enum RuntimeType {
    /// Actor running on this runtime should not block for more than 50 microsecs.
    NonBlocking,
    /// This runner is suitable for actors that are heavily blocking.
    Blocking,
}

pub fn spawn_actor<A: Actor>(
    actor: A,
    ctx: ActorContext<A>,
    inbox: Inbox<A>,
    handle: tokio::runtime::Handle,
) -> ActorHandle<A> {
    debug!(actor_id = %ctx.actor_instance_id(), "spawn-async");
    let (state_tx, state_rx) = watch::channel(actor.observable_state());
    let ctx_clone = ctx.clone();
    let span = actor.span(&ctx_clone);
    let loop_async_actor_future =
        async move { async_actor_loop(actor, inbox, ctx, state_tx).await }.instrument(span);
    let join_handle = handle.spawn(loop_async_actor_future);
    ActorHandle::new(state_rx, join_handle, ctx_clone)
}

async fn process_msg<A: Actor>(
    actor: &mut A,
    msg_id: u64,
    inbox: &mut Inbox<A>,
    ctx: &ActorContext<A>,
    state_tx: &watch::Sender<A::ObservableState>,
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
        Ok(CommandOrMessage::Message(mut msg)) => {
            ctx.process();
            msg.handle_message(msg_id, actor, ctx).await.err()
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

async fn async_actor_loop<A: Actor>(
    actor: A,
    mut inbox: Inbox<A>,
    ctx: ActorContext<A>,
    state_tx: watch::Sender<A::ObservableState>,
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
            &ctx,
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
    match &exit_status {
        ActorExitStatus::Success
        | ActorExitStatus::Quit
        | ActorExitStatus::DownstreamClosed
        | ActorExitStatus::Killed => {}
        ActorExitStatus::Failure(err) => {
            error!(cause=?err, exit_status=?exit_status, "actor-failure");
        }
        ActorExitStatus::Panicked => {
            error!(exit_status=?exit_status, "actor-failure");
        }
    }
    info!(actor_id = %ctx.actor_instance_id(), exit_status = %exit_status, "actor-exit");
    ctx.exit(&exit_status);
    exit_status
}
