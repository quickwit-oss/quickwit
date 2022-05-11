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

use std::time::Duration;

use crate::channel_with_priority::Priority;
use crate::mailbox::{Command, CommandOrMessage};
use crate::scheduler::{SimulateAdvanceTime, TimeShift};
use crate::spawn_builder::SpawnBuilder;
use crate::{Actor, KillSwitch, Mailbox, QueueCapacity, Scheduler};

/// Universe serves as the top-level context in which Actor can be spawned.
/// It is *not* a singleton. A typical application will usually have only one universe hosting all
/// of the actors but it is not a requirement.
///
/// In particular, unit test all have their own universe and hence can be executed in parallel.
pub struct Universe {
    scheduler_mailbox: Mailbox<Scheduler>,
    // This killswitch is used for the scheduler, and will be used by default for all spawned
    // actors.
    kill_switch: KillSwitch,
}

impl Universe {
    /// Creates a new universe.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Universe {
        let scheduler = Scheduler::default();
        let kill_switch = KillSwitch::default();
        let (mailbox, _inbox) =
            crate::create_mailbox("fake-mailbox".to_string(), QueueCapacity::Unbounded);
        let (scheduler_mailbox, _scheduler_inbox) =
            SpawnBuilder::new(scheduler, mailbox, kill_switch.clone()).spawn();
        Universe {
            scheduler_mailbox,
            kill_switch,
        }
    }

    pub fn kill(&self) {
        self.kill_switch.kill();
    }

    /// Simulate advancing the time for unit tests.
    ///
    /// It is not just about jumping the clock and triggering one round of messages:
    /// These message might have generated more messages for instance.
    ///
    /// This simulation triggers progress step by step, and after each step, leaves 100ms for actors
    /// to schedule extra messages.
    pub async fn simulate_time_shift(&self, duration: Duration) {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let _ = self
            .scheduler_mailbox
            .send_message(SimulateAdvanceTime {
                time_shift: TimeShift::ByDuration(duration),
                tx,
            })
            .await;
        let _ = rx.await;
    }

    pub fn spawn_actor<A: Actor>(&self, actor: A) -> SpawnBuilder<A> {
        SpawnBuilder::new(
            actor,
            self.scheduler_mailbox.clone(),
            self.kill_switch.clone(),
        )
    }

    /// Inform an actor to process pending message and then stop processing new messages
    /// and exit successfully.
    pub async fn send_exit_with_success<A: Actor>(
        &self,
        mailbox: &Mailbox<A>,
    ) -> Result<(), crate::SendError> {
        mailbox
            .send_with_priority(
                CommandOrMessage::Command(Command::ExitWithSuccess),
                Priority::Low,
            )
            .await
    }
}

impl Drop for Universe {
    fn drop(&mut self) {
        self.kill_switch.kill();
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use async_trait::async_trait;

    use crate::{Actor, ActorContext, ActorExitStatus, Handler, Universe};

    #[derive(Default)]
    pub struct ActorWithSchedule {
        count: usize,
    }

    #[async_trait]
    impl Actor for ActorWithSchedule {
        type ObservableState = usize;

        fn observable_state(&self) -> usize {
            self.count
        }

        async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
            self.handle(Loop, ctx).await
        }
    }

    #[derive(Debug)]
    struct Loop;

    #[async_trait]
    impl Handler<Loop> for ActorWithSchedule {
        type Reply = ();
        async fn handle(
            &mut self,
            _msg: Loop,
            ctx: &ActorContext<Self>,
        ) -> Result<(), ActorExitStatus> {
            self.count += 1;
            ctx.schedule_self_msg(Duration::from_secs(60), Loop).await;
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_schedule_for_actor() {
        let universe = Universe::new();
        let actor_with_schedule = ActorWithSchedule::default();
        let (_maibox, handler) = universe.spawn_actor(actor_with_schedule).spawn();
        let count_after_initialization = handler.process_pending_and_observe().await.state;
        assert_eq!(count_after_initialization, 1);
        universe.simulate_time_shift(Duration::from_secs(200)).await;
        let count_after_advance_time = handler.process_pending_and_observe().await.state;
        // Note the count is 2 here and not 1 + 3  = 4.
        // See comment on `universe.simulate_advance_time`.
        assert_eq!(count_after_advance_time, 4);
    }
}
