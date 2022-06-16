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
use tokio::sync::watch;
use tracing::{debug, error, info, Instrument};

use crate::actor_with_state_tx::ActorWithStateTx;
use crate::mailbox::{CommandOrMessage, Inbox};
use crate::scheduler::Scheduler;
use crate::{
    create_mailbox, Actor, ActorContext, ActorExitStatus, ActorHandle, Command, KillSwitch,
    Mailbox, RecvError,
};

/// `SpawnBuilder` makes it possible to configure misc parameters before spawning an actor.
pub struct SpawnBuilder<A: Actor> {
    actor: A,
    scheduler_mailbox: Mailbox<Scheduler>,
    kill_switch: KillSwitch,
    #[allow(clippy::type_complexity)]
    mailboxes: Option<(Mailbox<A>, Inbox<A>)>,
}

impl<A: Actor> SpawnBuilder<A> {
    pub(crate) fn new(
        actor: A,
        scheduler_mailbox: Mailbox<Scheduler>,
        kill_switch: KillSwitch,
    ) -> Self {
        SpawnBuilder {
            actor,
            scheduler_mailbox,
            kill_switch,
            mailboxes: None,
        }
    }

    /// Sets a specific kill switch for the actor.
    ///
    /// By default, the kill switch is inherited from the context that was used to
    /// spawn the actor.
    pub fn set_kill_switch(mut self, kill_switch: KillSwitch) -> Self {
        self.kill_switch = kill_switch;
        self
    }

    /// Sets a specific set of mailbox.
    ///
    /// By default, a brand new set of mailboxes will be created
    /// when the actor is spawned.
    ///
    /// This function makes it possible to create non-DAG networks
    /// of actors.
    pub fn set_mailboxes(mut self, mailbox: Mailbox<A>, inbox: Inbox<A>) -> Self {
        self.mailboxes = Some((mailbox, inbox));
        self
    }

    fn create_actor_context_and_inbox(mut self) -> (A, ActorContext<A>, Inbox<A>) {
        let (mailbox, inbox) = self.mailboxes.take().unwrap_or_else(|| {
            let actor_name = self.actor.name();
            let queue_capacity = self.actor.queue_capacity();
            create_mailbox(actor_name, queue_capacity)
        });
        let ctx = ActorContext::new(
            mailbox,
            self.kill_switch.clone(),
            self.scheduler_mailbox.clone(),
        );
        (self.actor, ctx, inbox)
    }
}

impl<A: Actor> SpawnBuilder<A> {
    /// Spawns an async actor.
    pub fn spawn(self) -> (Mailbox<A>, ActorHandle<A>) {
        let runtime_handle = self.actor.runtime_handle();
        let (actor, ctx, inbox) = self.create_actor_context_and_inbox();
        debug!(actor_id = %ctx.actor_instance_id(), "spawn-actor");
        let mailbox = ctx.mailbox().clone();
        let (state_tx, state_rx) = watch::channel(actor.observable_state());
        let ctx_clone = ctx.clone();
        let span = actor.span(&ctx);
        let loop_async_actor_future =
            async move { actor_loop(actor, inbox, ctx, state_tx).await }.instrument(span);
        let join_handle = runtime_handle.spawn(loop_async_actor_future);
        let actor_handle = ActorHandle::new(state_rx, join_handle, ctx_clone);
        (mailbox, actor_handle)
    }
}

fn process_command<A: Actor>(
    actor: &mut A,
    command: Command,
    ctx: &ActorContext<A>,
    state_tx: &watch::Sender<A::ObservableState>,
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
        ctx.protect_future(inbox.recv_timeout()).await
    } else {
        // The actor is paused. We only process command and scheduled message.
        ctx.protect_future(inbox.recv_timeout_cmd_and_scheduled_msg_only())
            .await
    };

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

async fn actor_loop<A: Actor>(
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
