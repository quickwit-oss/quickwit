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

use tokio::sync::watch::Sender;
use tracing::{debug, error};

use crate::actor::ActorExitStatus;
use crate::actor_state::ActorState;
use crate::actor_with_state_tx::ActorWithStateTx;
use crate::mailbox::{Command, CommandOrMessage, Inbox};
use crate::{Actor, ActorContext, RecvError};

/// An sync actor is executed on a tokio blocking task.
///
/// It may block and perform CPU heavy computation.
/// (See also [`AsyncActor`])
///
/// Known pitfalls: Contrary to AsyncActor commands are typically not executed right away.
/// If both the command and the message channel are exhausted, and a command and N messages arrives,
/// one message is likely to be executed before the command is.
pub trait SyncActor: Actor + Sized {
    fn initialize(&mut self, _ctx: &ActorContext<Self::Message>) -> Result<(), ActorExitStatus> {
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
        ctx: &ActorContext<Self::Message>,
    ) -> Result<(), ActorExitStatus>;

    /// Makes it possible to override the default behavior of
    /// handling command. (At your own risk!)
    #[doc(hidden)]
    fn process_command(
        &mut self,
        command: Command,
        ctx: &mut ActorContext<Self::Message>,
        state_tx: &Sender<Self::ObservableState>,
    ) -> Result<(), ActorExitStatus> {
        crate::actor::process_command(self, command, ctx, state_tx)
    }

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
        _ctx: &ActorContext<Self::Message>,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

/// Process a given message.
///
/// If some `ActorExitStatus` is returned, the actor will exit and no more message will be
/// processed.
fn process_msg<A: Actor + SyncActor>(
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
        inbox.recv_timeout_blocking()
    } else {
        // The actor is paused. We only process command and scheduled message.
        inbox.recv_timeout_cmd_and_scheduled_msg_only_blocking()
    };

    ctx.progress().record_progress();
    if ctx.kill_switch().is_dead() {
        return Some(ActorExitStatus::Killed);
    }
    match command_or_msg_recv_res {
        Ok(CommandOrMessage::Command(cmd)) => {
            debug!(cmd=?cmd, actor=%ctx.actor_instance_id(),"command-received");
            actor.process_command(cmd, ctx, state_tx).err()
        }
        Ok(CommandOrMessage::Message(msg)) => {
            debug!(msg=?msg, actor=%ctx.actor_instance_id(),"message-received");
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

pub(crate) fn sync_actor_loop<A: SyncActor>(
    actor: A,
    mut inbox: Inbox<A::Message>,
    mut ctx: ActorContext<A::Message>,
    state_tx: Sender<A::ObservableState>,
) -> ActorExitStatus {
    // We rely on this object internally to fetch a post-mortem state,
    // even in case of a panic.
    let mut actor_with_state_tx = ActorWithStateTx { actor, state_tx };

    let mut exit_status_opt: Option<ActorExitStatus> =
        actor_with_state_tx.actor.initialize(&ctx).err();

    let exit_status: ActorExitStatus = loop {
        if let Some(exit_status) = exit_status_opt {
            break exit_status;
        }
        exit_status_opt = process_msg(
            &mut actor_with_state_tx.actor,
            &mut inbox,
            &mut ctx,
            &actor_with_state_tx.state_tx,
        );
    };
    ctx.exit(&exit_status);
    if let Err(error) = actor_with_state_tx.actor.finalize(&exit_status, &ctx) {
        error!(error=?error, "Finalizing failed");
    }
    exit_status
}
