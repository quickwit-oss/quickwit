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

use tokio::sync::watch::{self, Sender};
use tokio::task::spawn_blocking;
use tracing::{debug, error, info};

use crate::actor::{process_command, ActorExitStatus};
use crate::actor_state::ActorState;
use crate::mailbox::{create_mailbox, CommandOrMessage, Inbox};
use crate::scheduler::SchedulerMessage;
use crate::{Actor, ActorContext, ActorHandle, KillSwitch, Mailbox, RecvError};

/// An sync actor is executed on a tokio blocking task.
///
/// It may block and perform CPU heavy computation.
/// (See also [`AsyncActor`])
///
/// Known pitfalls: Contrary to AsyncActor commands are typically not executed right away.
/// If both the command and the message channel are exhausted, and a command and N messages arrives,
/// one message is likely to be executed before the command is.
pub trait SyncActor: Actor + Sized {
    fn initialize(&mut self, _ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        Ok(())
    }

    /// Processes a message.
    ///
    /// If an exit status is returned as an error, the actor will exit.
    /// It will stop processing more message, the finalize method will be called,
    /// and its exit status will be the one defined in the error.
    fn process_message(
        &mut self,
        message: Self::Message,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus>;

    /// Hook that can be set up to define what should happen upon actor exit.
    /// This hook is called only once.
    ///
    /// It is always called regardless of the reason why the actor exited.
    /// The exit status is passed as an argument to make it possible to act conditionnally
    /// upon it.
    ///
    /// It is recommended to do as little work as possible on a killed actor.
    fn finalize(
        &mut self,
        _exit_status: &ActorExitStatus,
        _ctx: &ActorContext<Self>,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

pub(crate) fn spawn_sync_actor<A: SyncActor>(
    actor: A,
    kill_switch: KillSwitch,
    scheduler_mailbox: Mailbox<SchedulerMessage>,
) -> (Mailbox<A::Message>, ActorHandle<A>) {
    let actor_name = actor.name();
    debug!(actor_name=%actor_name,"spawning-sync-actor");
    let queue_capacity = actor.queue_capacity();
    let (mailbox, inbox) = create_mailbox(actor_name, queue_capacity);
    let mailbox_clone = mailbox.clone();
    let (state_tx, state_rx) = watch::channel(actor.observable_state());
    let ctx = ActorContext::new(mailbox, kill_switch, scheduler_mailbox);
    let ctx_clone = ctx.clone();
    let join_handle = spawn_blocking::<_, ActorExitStatus>(move || {
        let actor_name = actor.name();
        let exit_status = sync_actor_loop(actor, inbox, ctx, state_tx);
        info!(exit_status=%exit_status, actor=%actor_name, "exit");
        exit_status
    });
    let handle = ActorHandle::new(state_rx, join_handle, ctx_clone);
    (mailbox_clone, handle)
}

/// Process a given message.
///
/// If some `ActorExitStatus` is returned, the actor will exit and no more message will be processed.
fn process_msg<A: Actor + SyncActor>(
    actor: &mut A,
    inbox: &mut Inbox<A::Message>,
    ctx: &mut ActorContext<A>,
    state_tx: &Sender<A::ObservableState>,
) -> Option<ActorExitStatus> {
    if ctx.kill_switch().is_dead() {
        return Some(ActorExitStatus::Killed);
    }

    ctx.progress().record_progress();

    let command_or_msg_recv_res =
        inbox.recv_timeout_blocking(ctx.get_state() == ActorState::Running);

    ctx.progress().record_progress();
    if ctx.kill_switch().is_dead() {
        return Some(ActorExitStatus::Killed);
    }
    match command_or_msg_recv_res {
        Ok(CommandOrMessage::Command(cmd)) => process_command(actor, cmd, ctx, state_tx),
        Ok(CommandOrMessage::Message(msg)) => {
            debug!(msg=?msg, actor=%actor.name(),"message-received");
            actor.process_message(msg, ctx).err()
        }
        Err(RecvError::Timeout) => {
            if ctx.mailbox().is_last_mailbox() {
                Some(ActorExitStatus::Success)
            } else {
                None
            }
        }
        Err(RecvError::Disconnected) => Some(ActorExitStatus::Success),
    }
}

fn sync_actor_loop<A: SyncActor>(
    mut actor: A,
    mut inbox: Inbox<A::Message>,
    mut ctx: ActorContext<A>,
    state_tx: Sender<A::ObservableState>,
) -> ActorExitStatus {
    let mut exit_status_opt: Option<ActorExitStatus> = actor.initialize(&ctx).err();
    let exit_status: ActorExitStatus = loop {
        if let Some(exit_status) = exit_status_opt {
            break exit_status;
        }
        exit_status_opt = process_msg(&mut actor, &mut inbox, &mut ctx, &state_tx);
    };
    ctx.exit(&exit_status);
    if let Err(error) = actor.finalize(&exit_status, &ctx) {
        error!(error=?error, "Finalizing failed");
    }
    let final_state = actor.observable_state();
    let _ = state_tx.send(final_state);
    exit_status
}
