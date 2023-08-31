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

#![deny(clippy::disallowed_methods)]

//! quickwit-actors is a simplified actor framework for quickwit.
//!
//! It solves the following problem:
//! - have sync and async tasks communicate together.
//! - make these task observable
//! - make these task modular and testable
//! - detect when some task is stuck and does not progress anymore

use std::fmt;
use std::num::NonZeroU64;

use once_cell::sync::Lazy;
use tokio::time::Duration;
mod actor;
mod actor_context;
mod actor_handle;
mod actor_state;
#[doc(hidden)]
pub mod channel_with_priority;
mod command;
mod envelope;
mod mailbox;
mod observation;
mod registry;
pub(crate) mod scheduler;
mod spawn_builder;
mod supervisor;

pub use scheduler::{start_scheduler, SchedulerClient};

#[cfg(test)]
pub(crate) mod tests;
mod universe;

pub use actor::{Actor, ActorExitStatus, DeferableReplyHandler, Handler};
pub use actor_handle::{ActorHandle, Health, Healthz, Supervisable};
pub use command::{Command, Observe};
pub use observation::{Observation, ObservationType};
use quickwit_common::KillSwitch;
pub use spawn_builder::SpawnContext;
use thiserror::Error;
use tracing::info;
use tracing::log::warn;
pub use universe::Universe;

pub use self::actor_context::ActorContext;
pub use self::actor_state::ActorState;
pub use self::channel_with_priority::{QueueCapacity, RecvError, SendError, TrySendError};
pub use self::mailbox::{Inbox, Mailbox};
pub use self::registry::{ActorObservation, ActorRegistry};
pub use self::supervisor::{Supervisor, SupervisorState};

/// Heartbeat used to verify that actors are progressing.
///
/// If an actor does not advertise a progress within an interval of duration `HEARTBEAT`,
/// its supervisor will consider it as blocked and will proceed to kill it, as well
/// as all of the actors all the actors that share the killswitch.
pub static HEARTBEAT: Lazy<Duration> = Lazy::new(heartbeat_from_env_or_default);

/// Returns the actor's heartbeat duration:
/// - Derived from `QW_ACTOR_HEARTBEAT_SECS` if set and valid.
/// - Defaults to 30 seconds or 500ms for tests.
fn heartbeat_from_env_or_default() -> Duration {
    if cfg!(any(test, feature = "testsuite")) {
        // Right now some unit test end when we detect that a
        // pipeline has terminated, which can require waiting
        // for a heartbeat.
        //
        // We use a shorter heartbeat to reduce the time running unit tests.
        return Duration::from_millis(500);
    }
    match std::env::var("QW_ACTOR_HEARTBEAT_SECS") {
        Ok(actor_hearbeat_secs_str) => {
            if let Ok(actor_hearbeat_secs) = actor_hearbeat_secs_str.parse::<NonZeroU64>() {
                info!("Set the actor heartbeat to {actor_hearbeat_secs} seconds.");
                return Duration::from_secs(actor_hearbeat_secs.get());
            } else {
                warn!(
                    "Failed to parse `QW_ACTOR_HEARTBEAT_SECS={actor_hearbeat_secs_str}` in \
                     seconds > 0, using default heartbeat (30 seconds)."
                );
            };
        }
        Err(std::env::VarError::NotUnicode(os_str)) => {
            warn!(
                "Failed to parse `QW_ACTOR_HEARTBEAT_SECS={os_str:?}` in a valid unicode string, \
                 using default heartbeat (30 seconds)."
            );
        }
        Err(std::env::VarError::NotPresent) => {}
    }
    Duration::from_secs(30)
}

/// Time we accept to wait for a new observation.
///
/// Once this time is elapsed, we just return the last observation.
const OBSERVE_TIMEOUT: Duration = Duration::from_secs(3);

/// Error that occurred while calling `ActorContext::ask(..)` or `Universe::ask`
#[derive(Error, Debug)]
pub enum AskError<E: fmt::Debug> {
    #[error("Message could not be delivered")]
    MessageNotDelivered,
    #[error("Error while the message was being processed.")]
    ProcessMessageError,
    #[error("The handler returned an error: `{0:?}`.")]
    ErrorReply(#[from] E),
}
