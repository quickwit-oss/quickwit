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

//! quickwit-actors is a simplified actor framework for quickwit.
//!
//! It solves the following problem:
//! - have sync and async tasks communicate together.
//! - make these task observable
//! - make these task modular and testable
//! - detect when some task is stuck and does not progress anymore

use std::fmt;

use tokio::time::Duration;
mod actor;
mod actor_handle;
mod actor_state;
mod channel_with_priority;
mod command;
mod envelope;
mod kill_switch;
mod mailbox;
mod observation;
mod progress;
mod scheduler;
mod spawn_builder;

#[cfg(test)]
mod tests;
mod universe;

pub use actor::{Actor, ActorExitStatus, Handler};
pub use actor_handle::{ActorHandle, Health, Supervisable};
pub use command::Command;
pub use kill_switch::KillSwitch;
pub use observation::{Observation, ObservationType};
pub use progress::{Progress, ProtectedZoneGuard};
pub(crate) use scheduler::Scheduler;
use thiserror::Error;
pub use universe::Universe;

pub use self::actor::ActorContext;
pub use self::actor_state::ActorState;
pub use self::channel_with_priority::{QueueCapacity, RecvError, SendError};
pub use self::mailbox::{create_mailbox, create_test_mailbox, Mailbox};

/// Heartbeat used to verify that actors are progressing.
///
/// If an actor does not advertise a progress within an interval of duration `HEARTBEAT`,
/// its supervisor will consider it as blocked and will proceed to kill it, as well
/// as all of the actors all the actors that share the killswitch.
pub const HEARTBEAT: Duration = Duration::from_secs(3);

/// Error that occured while calling `ActorContext::ask(..)` or `Universe::ask`
#[derive(Error, Debug)]
pub enum AskError<E: fmt::Debug> {
    #[error("Message could not be delivered")]
    MessageNotDelivered,
    #[error("Error while the message was being processed.")]
    ProcessMessageError,
    #[error("The handler returned an error: `{0:?}`.")]
    ErrorReply(#[from] E),
}
