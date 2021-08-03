/*!
quickwit-actors is a simplified actor framework for quickwit.

It solves the following problem:
- have sync and async tasks communicate together.
- make these task observable
- make these task modular and testable
- detect when some task is stuck and does not progress anymore
- offers a killswitch

Actors are organized under a Kill switch. If one actor of the group is terminated,
all of them get terminated.

Actors are also passed a progress object. If an actor does not record some progress
within one HEARTBEAT, the actor and all the actors under his kill switch will be terminated.
Consuming one message count as a progress of course, but implementors can manually
record some progress if they are processing a large messsage.

# Example

*/

// TODO handle the case where an actor gracefully finished its work.
// In this case, the kill switch should not be triggered even if there is no progress.

use std::{error::Error, fmt::Display};

use tokio::time::Duration;

mod actor;
mod actor_handle;
mod async_actor;
mod mailbox;
mod observation;
mod sync_actor;
#[cfg(test)]
mod tests;

pub use self::actor::ActorContext;
pub use self::mailbox::{
    create_mailbox, create_test_mailbox, Mailbox, QueueCapacity, ReceptionResult,
};
pub use actor::{Actor, KillSwitch, MessageProcessError, Progress};
pub use actor_handle::{ActorHandle, ActorTermination};
pub use async_actor::AsyncActor;
pub use observation::Observation;
pub use sync_actor::SyncActor;

/// Heartbeat used to verify that actors are progressing.
///
/// If an actor does not advertise a progress within an interval of duration `HEARTBEAT`,
/// the killswith is hit, and all of the actors in this generation are killed.
pub const HEARTBEAT: Duration = if cfg!(test) {
    Duration::from_millis(2)
} else {
    Duration::from_secs(1)
};

pub fn message_timeout() -> Duration {
    HEARTBEAT.mul_f32(0.2f32)
}

/// Error returned when a message is sent to an actor that is detected as terminated.
#[derive(Debug)]
pub struct SendError;

impl Display for SendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for SendError {}
