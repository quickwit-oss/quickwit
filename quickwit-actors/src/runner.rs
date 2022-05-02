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

use std::future::Future;

use anyhow::Context;
use tokio::sync::watch;
use tracing::{debug, error, info};

use crate::actor::process_command;
use crate::actor_with_state_tx::ActorWithStateTx;
use crate::join_handle::{JoinHandle, JoinOutcome};
use crate::mailbox::{CommandOrMessage, Inbox};
use crate::{Actor, ActorContext, ActorExitStatus, ActorHandle, RecvError};

/// The `ActorRunner` defines the environment in which an actor
/// should be executed.
///
/// While all actor's handlers have a asynchronous trait,
/// some actor can be implemented to do some heavy blocking work
/// and no rely on any asynchronous contructs.
///
/// In that case, they should simply rely on the `DedicatedThread` runner.
#[derive(Clone, Copy, Debug)]
pub enum ActorRunner {
    /// Returns the actor as a tokio task running on the global runtime.
    ///
    /// This is the most lightweight solution.
    /// Actor running on this runtime should not block for more than 50 microsecs.
    GlobalRuntime,
    /// Spawns a dedicated thread, a Tokio runtime, and rely on
    /// `tokio::Runtime::block_on` to run the actor loop.
    ///
    /// This runner is suitable for actors that are heavily blocking.
    DedicatedThread,
}

impl ActorRunner {
    pub fn spawn_actor<A: Actor>(
        &self,
        actor: A,
        ctx: ActorContext<A>,
        inbox: Inbox<A>,
    ) -> ActorHandle<A> {
        debug!(actor_id = %ctx.actor_instance_id(), "spawn-async");
        let (state_tx, state_rx) = watch::channel(actor.observable_state());
        let ctx_clone = ctx.clone();
        let (exit_status_tx, exit_status_rx) = watch::channel(None);
        let actor_instance_id = ctx.actor_instance_id().to_string();
        let loop_async_actor_future = async move {
            let exit_status = async_actor_loop(actor, inbox, ctx, state_tx).await;
            let _ = exit_status_tx.send(Some(exit_status));
        };
        let join_handle = self.spawn_named_task(loop_async_actor_future, &actor_instance_id);
        ActorHandle::new(state_rx, join_handle, ctx_clone, exit_status_rx)
    }

    fn spawn_named_task(
        &self,
        task: impl Future<Output = ()> + Send + 'static,
        name: &str,
    ) -> JoinHandle {
        match *self {
            ActorRunner::GlobalRuntime => tokio_task_runtime_spawn_named(task, name),
            ActorRunner::DedicatedThread => dedicated_runtime_spawn_named(task, name),
        }
    }
}

fn dedicated_runtime_spawn_named(
    task: impl Future<Output = ()> + Send + 'static,
    name: &str,
) -> JoinHandle {
    let (join_handle, sender) = JoinHandle::create_for_thread();
    std::thread::Builder::new()
        .name(name.to_string())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(task);
            let _ = sender.send(JoinOutcome::Ok);
        })
        .unwrap();
    join_handle
}

#[allow(unused_variables)]
fn tokio_task_runtime_spawn_named(
    task: impl Future<Output = ()> + Send + 'static,
    name: &str,
) -> JoinHandle {
    let tokio_task_join_handle = {
        #[cfg(tokio_unstable)]
        {
            tokio::task::Builder::new().name(_name).spawn(task)
        }
        #[cfg(not(tokio_unstable))]
        {
            tokio::spawn(task)
        }
    };
    JoinHandle::create_for_task(tokio_task_join_handle)
}

async fn process_msg<A: Actor>(
    actor: &mut A,
    inbox: &mut Inbox<A>,
    ctx: &mut ActorContext<A>,
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
            msg.handle_message(actor, ctx).await.err()
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
    mut ctx: ActorContext<A>,
    state_tx: watch::Sender<A::ObservableState>,
) -> ActorExitStatus {
    // We rely on this object internally to fetch a post-mortem state,
    // even in case of a panic.
    let mut actor_with_state_tx = ActorWithStateTx { actor, state_tx };

    let mut exit_status_opt: Option<ActorExitStatus> =
        actor_with_state_tx.actor.initialize(&ctx).await.err();

    let mut exit_status: ActorExitStatus = loop {
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
    info!(actor_id = %ctx.actor_instance_id(), exit_status = %exit_status, "actor-exit");
    ctx.exit(&exit_status);
    exit_status
}
