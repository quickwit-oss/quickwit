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

/*!
quickwit-actors is a simplified actor framework for quickwit.

It solves the following problem:
- have sync and async tasks communicate together.
- make these task observable
- make these task modular and testable
- detect when some task is stuck and does not progress anymore

*/

use tokio::time::Duration;
mod actor;
mod actor_handle;
mod actor_state;
mod actor_with_state_tx;
mod async_actor;
pub(crate) mod channel_with_priority;
mod kill_switch;
mod mailbox;
mod observation;
mod progress;
mod scheduler;
mod sync_actor;
#[cfg(test)]
mod tests;
mod universe;

pub use self::actor::ActorContext;
pub use self::channel_with_priority::{QueueCapacity, RecvError, SendError};
pub use self::mailbox::{create_mailbox, create_test_mailbox, Mailbox};
pub use actor::{Actor, ActorExitStatus};
pub use actor_handle::ActorHandle;
pub use async_actor::AsyncActor;
pub use kill_switch::KillSwitch;
pub use observation::{Observation, ObservationType};
pub(crate) use scheduler::Scheduler;
pub use sync_actor::SyncActor;
pub use universe::Universe;

/// Heartbeat used to verify that actors are progressing.
///
/// If an actor does not advertise a progress within an interval of duration `HEARTBEAT`,
/// its supervisor will consider it as blocked and will proceed to kill it, as well
/// as all of the actors all the actors that share the killswitch.
pub const HEARTBEAT: Duration = Duration::from_secs(1);

pub fn message_timeout() -> Duration {
    HEARTBEAT.mul_f32(0.2f32)
}
