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

use async_trait::async_trait;

use crate::{Actor, ActorContext, ActorExitStatus, Handler};

/// Commands are messages that can be send to control the behavior of an actor.
///
/// They are similar to UNIX signals.
///
/// They are treated with a higher priority than regular actor messages.
#[derive(Debug)]
pub enum Command {
    /// Temporarily pauses the actor. A paused actor only checks
    /// on its high priority channel and still shows "progress". It appears as
    /// healthy to the supervisor.
    ///
    /// Scheduled message are still processed.
    ///
    /// Semantically, it is similar to SIGSTOP.
    Pause,

    /// Resume a paused actor. If the actor was not paused this command
    /// has no effects.
    ///
    /// Semantically, it is similar to SIGCONT.
    Resume,

    /// Stops the actor with a success exit status code.
    ///
    /// Upstream `actors` that terminates should send the `ExitWithSuccess`
    /// command to downstream actors to inform them that there are no more
    /// incoming messages.
    ///
    /// It is similar to `Quit`, except for the resulting exit status.
    ExitWithSuccess,

    /// Asks the actor to gracefully shutdown.
    ///
    /// The actor will stop processing messages and its finalize function will
    /// be called.
    ///
    /// The exit status is then `ActorExitStatus::Quit`.
    ///
    /// This is the equivalent of sending SIGINT/Ctrl-C to a process.
    Quit,

    /// Nudging is a No-op message.
    ///
    /// Its only effect is to wake-up actors that are stuck waiting
    /// for a message.
    ///
    /// This is useful to kill actors properly or for tests.
    /// Actors stuck waiting for a message do not have any timeout to
    /// check for their killswitch signal.
    ///
    ///
    /// Note: Historically, actors used to have a timeout, then
    /// the wake up logic worked using a Kill command.
    /// However, after the introduction of supervision, it became common
    /// to recycle a mailbox.
    ///
    /// After a panic for instance, the supervisor of an actor might kill
    /// it by activating its killswitch and sending a Kill message.
    ///
    /// The respawned actor would receive its predecessor mailbox and
    /// possibly end up process a Kill message as its first message.
    Nudge,
}

#[async_trait]
impl<A: Actor> Handler<Command> for A {
    type Reply = ();

    /// and its exit status will be the one defined in the error.
    async fn handle(
        &mut self,
        command: Command,
        ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        match command {
            Command::Pause => {
                ctx.pause();
                Ok(())
            }
            Command::ExitWithSuccess => Err(ActorExitStatus::Success),
            Command::Quit => Err(ActorExitStatus::Quit),
            Command::Nudge => Ok(()),
            Command::Resume => {
                ctx.resume();
                Ok(())
            }
        }
    }
}

/// Asks the actor to update its ObservableState.
///
/// The observation is then available using the `ActorHandler::last_observation()`
/// method.
#[derive(Debug)]
pub(crate) struct Observe;

#[async_trait]
impl<A: Actor> Handler<Observe> for A {
    type Reply = A::ObservableState;

    async fn handle(
        &mut self,
        _observe: Observe,
        ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(ctx.observe(self))
    }
}
