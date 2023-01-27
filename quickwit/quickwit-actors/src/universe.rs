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

use std::time::Duration;

use crate::mailbox::create_mailbox;
use crate::registry::ActorObservation;
use crate::scheduler::start_scheduler;
use crate::spawn_builder::{SpawnBuilder, SpawnContext};
use crate::{Actor, Command, Inbox, Mailbox, QueueCapacity};

/// Universe serves as the top-level context in which Actor can be spawned.
/// It is *not* a singleton. A typical application will usually have only one universe hosting all
/// of the actors but it is not a requirement.
///
/// In particular, unit test all have their own universe and hence can be executed in parallel.
pub struct Universe {
    pub(crate) spawn_ctx: SpawnContext,
}

impl Default for Universe {
    fn default() -> Universe {
        Universe::new()
    }
}

impl Universe {
    /// Creates a new universe.
    pub fn new() -> Universe {
        let scheduler_client = start_scheduler();
        Universe {
            spawn_ctx: SpawnContext::new(scheduler_client),
        }
    }

    /// Creates a universe were time is accelerated.
    ///
    /// Time is accelerated in a way to exhibit a behavior as close as possible
    /// to what would have happened with normal time but faster.
    ///
    /// The time "jumps" only happen when no actor is processing any message,
    /// running initialization or finalize.
    #[cfg(any(test, feature = "testsuite"))]
    pub fn with_accelerated_time() -> Universe {
        let universe = Universe::new();
        universe.spawn_ctx().scheduler_client.accelerate_time();
        universe
    }

    pub fn spawn_ctx(&self) -> &SpawnContext {
        &self.spawn_ctx
    }

    pub fn create_test_mailbox<A: Actor>(&self) -> (Mailbox<A>, Inbox<A>) {
        create_mailbox("test-mailbox".to_string(), QueueCapacity::Unbounded, None)
    }

    pub fn create_mailbox<A: Actor>(
        &self,
        actor_name: impl ToString,
        queue_capacity: QueueCapacity,
    ) -> (Mailbox<A>, Inbox<A>) {
        self.spawn_ctx.create_mailbox(actor_name, queue_capacity)
    }

    pub fn get<A: Actor>(&self) -> Vec<Mailbox<A>> {
        self.spawn_ctx.registry.get::<A>()
    }

    pub fn get_one<A: Actor>(&self) -> Option<Mailbox<A>> {
        self.spawn_ctx.registry.get_one::<A>()
    }

    pub async fn observe(&self, timeout: Duration) -> Vec<ActorObservation> {
        self.spawn_ctx.registry.observe(timeout).await
    }

    pub fn kill(&self) {
        self.spawn_ctx.kill_switch.kill();
    }

    /// This function acts as a drop-in replacement of
    /// `tokio::time::sleep`.
    ///
    /// It can however be accelerated when using a time-accelerated
    /// universe.
    pub async fn sleep(&self, duration: Duration) {
        self.spawn_ctx.scheduler_client.sleep(duration).await;
    }

    pub fn spawn_builder<A: Actor>(&self) -> SpawnBuilder<A> {
        self.spawn_ctx.spawn_builder()
    }

    /// Inform an actor to process pending message and then stop processing new messages
    /// and exit successfully.
    pub async fn send_exit_with_success<A: Actor>(
        &self,
        mailbox: &Mailbox<A>,
    ) -> Result<(), crate::SendError> {
        mailbox.send_message(Command::ExitWithSuccess).await?;
        Ok(())
    }
}

impl Drop for Universe {
    fn drop(&mut self) {
        self.spawn_ctx.kill_switch.kill();
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use async_trait::async_trait;

    use crate::{Actor, ActorContext, ActorExitStatus, Handler, Universe};

    #[derive(Default)]
    pub struct CountingMinutesActor {
        count: usize,
    }

    #[async_trait]
    impl Actor for CountingMinutesActor {
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
    impl Handler<Loop> for CountingMinutesActor {
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
        let universe = Universe::with_accelerated_time();
        let actor_with_schedule = CountingMinutesActor::default();
        let (_maibox, handler) = universe.spawn_builder().spawn(actor_with_schedule);
        let count_after_initialization = handler.process_pending_and_observe().await.state;
        assert_eq!(count_after_initialization, 1);
        universe.sleep(Duration::from_secs(200)).await;
        let count_after_advance_time = handler.process_pending_and_observe().await.state;
        assert_eq!(count_after_advance_time, 4);
    }
}
