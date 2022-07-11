// Copyright (C) 2022 Quickwit, Inc.
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

use std::any::Any;
use std::fmt;

use tokio::sync::oneshot;

/// Commands are messages that can be send to control the behavior of an actor.
///
/// They are similar to UNIX signals.
///
/// They are treated with a higher priority than regular actor messages.
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

    /// Asks the actor to update its ObservableState.
    /// Since it is a command, it will be treated with a higher priority than
    /// a normal message.
    /// If the actor is processing message, it will finish it, and the state
    /// observed will be the state after this message.
    /// The Observe command also ships a oneshot channel to allow client
    /// to wait on this observation.
    ///
    /// The observation is then available using the `ActorHander::last_observation()`
    /// method.
    // We use a `Box<dyn Any>` here to avoid adding an observablestate generic
    // parameter to the mailbox.
    Observe(oneshot::Sender<Box<dyn Any + Send>>),

    /// Asks the actor to gracefully shutdown.
    ///
    /// The actor will stop processing messages and its finalize function will
    /// be called.
    ///
    /// The exit status is then `ActorExitStatus::Quit`.
    ///
    /// This is the equivalent of sending SIGINT/Ctrl-C to a process.
    Quit,

    /// Kill the actor. The behavior is the same as if an actor detected that its kill switch
    /// was pushed.
    ///
    /// It is similar to Quit, except the `ActorExitState` is different.
    ///
    /// It can have important side effect, as the actor `.finalize` method
    /// may have different behavior depending on the exit state.
    ///
    /// This is the equivalent of sending SIGKILL to a process.
    Kill,
}

impl fmt::Debug for Command {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Command::Pause => write!(f, "Pause"),
            Command::Resume => write!(f, "Resume"),
            Command::Observe(_) => write!(f, "Observe"),
            Command::ExitWithSuccess => write!(f, "Success"),
            Command::Quit => write!(f, "Quit"),
            Command::Kill => write!(f, "Kill"),
        }
    }
}
