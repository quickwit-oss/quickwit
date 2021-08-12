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

use tokio::time::Duration;
mod actor;
mod actor_handle;
mod actor_state;
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
pub use actor::{Actor, ActorTermination};
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
/// the killswith is hit, and all of the actors in this generation are killed.
pub const HEARTBEAT: Duration = Duration::from_secs(1);

pub fn message_timeout() -> Duration {
    HEARTBEAT.mul_f32(0.2f32)
}
