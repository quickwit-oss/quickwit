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

use std::fmt;

use serde::Serialize;
use tokio::sync::{oneshot, watch};
use tracing::error;

use crate::actor_state::ActorState;
use crate::command::Observe;
use crate::mailbox::Priority;
use crate::observation::ObservationType;
use crate::registry::ActorJoinHandle;
use crate::{Actor, ActorContext, ActorExitStatus, Command, Mailbox, Observation};

/// An Actor Handle serves as an address to communicate with an actor.
pub struct ActorHandle<A: Actor> {
    actor_context: ActorContext<A>,
    last_state: watch::Receiver<A::ObservableState>,
    join_handle: ActorJoinHandle,
}

/// Describes the health of a given actor.
#[derive(Clone, Eq, PartialEq, Debug, Hash, Serialize)]
pub enum Health {
    /// The actor is running and behaving as expected.
    Healthy,
    /// No progress was registered, or the process terminated with an error
    FailureOrUnhealthy,
    /// The actor terminated successfully.
    Success,
}

/// Message received by health probe handlers.
#[derive(Clone, Debug)]
pub struct Healthz;

impl<A: Actor> fmt::Debug for ActorHandle<A> {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("ActorHandle")
            .field("name", &self.actor_context.actor_instance_id())
            .finish()
    }
}

pub trait Supervisable {
    fn name(&self) -> &str;

    /// Check for the ActorState (has it terminated?), and provided `check_for_progress`
    /// is set to `true`, it will also check for the progress of the actor.
    fn check_health(&self, check_for_progress: bool) -> Health;

    fn state(&self) -> ActorState;
}

impl<A: Actor> Supervisable for ActorHandle<A> {
    fn name(&self) -> &str {
        self.actor_context.actor_instance_id()
    }

    fn state(&self) -> ActorState {
        self.actor_context.state()
    }

    /// Harvests the health of the actor by checking its state (see [`ActorState`]) and,
    /// provided `check_for_progress` is set to true, it will check its progress too
    /// (see `Progress`).
    ///
    /// When `check_for_progress` is set to true, calling this method resets its progress state
    /// to "no update" (see `ProgressState`). As a consequence, only one supervisor or probe
    /// should periodically invoke this method during the lifetime of the actor.
    fn check_health(&self, check_for_progress: bool) -> Health {
        let actor_state = self.state();
        if actor_state == ActorState::Success {
            return Health::Success;
        }
        if actor_state == ActorState::Failure {
            error!(actor = self.name(), "actor-exit-without-success");
            return Health::FailureOrUnhealthy;
        }
        if !check_for_progress
            || self
                .actor_context
                .progress()
                .registered_activity_since_last_call()
        {
            Health::Healthy
        } else {
            error!(actor = self.name(), "actor-timeout");
            Health::FailureOrUnhealthy
        }
    }
}

impl<A: Actor> ActorHandle<A> {
    pub(crate) fn new(
        last_state: watch::Receiver<A::ObservableState>,
        join_handle: ActorJoinHandle,
        actor_context: ActorContext<A>,
    ) -> Self {
        ActorHandle {
            actor_context,
            last_state,
            join_handle,
        }
    }

    pub fn state(&self) -> ActorState {
        self.actor_context.state()
    }

    /// Process all of the pending messages, and returns a snapshot of
    /// the observable state of the actor after this.
    ///
    /// This method is mostly useful for tests.
    ///
    /// To actually observe the state of an actor for ops purpose,
    /// prefer using the `.observe()` method.
    ///
    /// This method timeout if reaching the end of the message takes more than an HEARTBEAT.
    pub async fn process_pending_and_observe(&self) -> Observation<A::ObservableState> {
        self.observe_with_priority(Priority::Low).await
    }

    /// Observe the current state.
    ///
    /// The observation will be scheduled as a high priority message, therefore it will be executed
    /// after the current active message and the current command queue have been processed.
    ///
    /// This method does not do anything to avoid Observe messages from stacking up.
    /// In supervisors, prefer using `refresh_observation`.
    pub async fn observe(&self) -> Observation<A::ObservableState> {
        self.observe_with_priority(Priority::High).await
    }

    /// Triggers an observation.
    /// It is scheduled as a high priority
    /// message, and will hence be executed as soon as possible.
    ///
    /// This method does not enqueue an Observe request if there is already one in
    /// the queue.
    ///
    /// The resulting observation can eventually be accessible using the
    /// observation watch channel.
    ///
    /// This function returning does NOT mean that the observation was executed.
    pub fn refresh_observe(&self) {
        let observation_already_enqueued = self
            .actor_context
            .set_observe_enqueued_and_return_previous();
        if !observation_already_enqueued {
            let _ = self
                .actor_context
                .mailbox()
                .send_message_with_high_priority(Observe);
        }
    }

    async fn observe_with_priority(&self, priority: Priority) -> Observation<A::ObservableState> {
        if !self.actor_context.state().is_exit() {
            if let Ok(oneshot_rx) = self
                .actor_context
                .mailbox()
                .send_message_with_priority(Observe, priority)
                .await
            {
                // The timeout is required here. If the actor fails, its inbox is properly dropped
                // but the send channel might actually prevent the onechannel
                // Receiver from being dropped.
                return self.wait_for_observable_state_callback(oneshot_rx).await;
            } else {
                error!(
                    actor_id=%self.actor_context.actor_instance_id(),
                    "Failed to send observe message"
                );
            }
        }
        let state = self.last_observation();
        Observation {
            obs_type: ObservationType::PostMortem,
            state,
        }
    }

    /// Pauses the actor. The actor will stop processing messages from the low priority
    /// channel, but its work can be resumed by calling the method `.resume()`.
    pub fn pause(&self) {
        let _ = self
            .actor_context
            .mailbox()
            .send_message_with_high_priority(Command::Pause);
    }

    /// Resumes a paused actor.
    pub fn resume(&self) {
        let _ = self
            .actor_context
            .mailbox()
            .send_message_with_high_priority(Command::Resume);
    }

    /// Kills the actor. Its finalize function will still be called.
    ///
    /// This function also actionnates the actor kill switch.
    ///
    /// The other difference with quit is the exit status. It is important,
    /// as the finalize logic may behave differently depending on the exit status.
    pub async fn kill(self) -> (ActorExitStatus, A::ObservableState) {
        self.actor_context.kill_switch().kill();
        let _ = self
            .actor_context
            .mailbox()
            .send_message_with_high_priority(Command::Nudge);
        self.join().await
    }

    /// Gracefully quit the actor, regardless of whether there are pending messages or not.
    /// Its finalize function will be called.
    ///
    /// The kill switch is not actionated.
    ///
    /// The other difference with kill is the exit status. It is important,
    /// as the finalize logic may behave differently depending on the exit status.
    pub async fn quit(self) -> (ActorExitStatus, A::ObservableState) {
        let _ = self
            .actor_context
            .mailbox()
            .send_message_with_high_priority(Command::Quit);
        self.join().await
    }

    /// Waits until the actor exits by itself. This is the equivalent of `Thread::join`.
    pub async fn join(self) -> (ActorExitStatus, A::ObservableState) {
        let exit_status = self.join_handle.join().await;
        let observation = self.last_state.borrow().clone();
        (exit_status, observation)
    }

    pub fn last_observation(&self) -> A::ObservableState {
        self.last_state.borrow().clone()
    }

    async fn wait_for_observable_state_callback(
        &self,
        rx: oneshot::Receiver<A::ObservableState>,
    ) -> Observation<A::ObservableState> {
        let scheduler_client = &self.actor_context.spawn_ctx().scheduler_client;
        let observable_state_or_timeout =
            scheduler_client.timeout(crate::OBSERVE_TIMEOUT, rx).await;
        match observable_state_or_timeout {
            Ok(Ok(state)) => {
                let obs_type = ObservationType::Alive;
                Observation { obs_type, state }
            }
            Ok(Err(_)) => {
                let state = self.last_observation();
                let obs_type = ObservationType::PostMortem;
                Observation { obs_type, state }
            }
            Err(_) => {
                let state = self.last_observation();
                let obs_type = if self.actor_context.state().is_exit() {
                    ObservationType::PostMortem
                } else {
                    ObservationType::Timeout
                };
                Observation { obs_type, state }
            }
        }
    }

    pub fn mailbox(&self) -> &Mailbox<A> {
        self.actor_context.mailbox()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    use async_trait::async_trait;

    use super::*;
    use crate::{Handler, Universe};

    #[derive(Default)]
    struct PanickingActor {
        count: usize,
    }

    impl Actor for PanickingActor {
        type ObservableState = usize;
        fn observable_state(&self) -> usize {
            self.count
        }
    }

    #[derive(Debug)]
    struct Panic;

    #[async_trait]
    impl Handler<Panic> for PanickingActor {
        type Reply = ();
        async fn handle(
            &mut self,
            _message: Panic,
            _ctx: &ActorContext<Self>,
        ) -> Result<(), ActorExitStatus> {
            self.count += 1;
            panic!("Oops");
        }
    }

    #[derive(Default)]
    struct ExitActor {
        count: usize,
    }

    impl Actor for ExitActor {
        type ObservableState = usize;
        fn observable_state(&self) -> usize {
            self.count
        }
    }

    #[derive(Debug)]
    struct Exit;

    #[async_trait]
    impl Handler<Exit> for ExitActor {
        type Reply = ();

        async fn handle(
            &mut self,
            _msg: Exit,
            _ctx: &ActorContext<Self>,
        ) -> Result<(), ActorExitStatus> {
            self.count += 1;
            Err(ActorExitStatus::DownstreamClosed)
        }
    }

    #[tokio::test]
    async fn test_panic_in_actor() -> anyhow::Result<()> {
        let universe = Universe::with_accelerated_time();
        let (mailbox, handle) = universe.spawn_builder().spawn(PanickingActor::default());
        mailbox.send_message(Panic).await?;
        let (exit_status, count) = handle.join().await;
        assert!(matches!(exit_status, ActorExitStatus::Panicked));
        assert!(matches!(count, 1)); //< Upon panick we cannot get a post mortem state.
        Ok(())
    }

    #[tokio::test]
    async fn test_exit() -> anyhow::Result<()> {
        let universe = Universe::with_accelerated_time();
        let (mailbox, handle) = universe.spawn_builder().spawn(ExitActor::default());
        mailbox.send_message(Exit).await?;
        let (exit_status, count) = handle.join().await;
        assert!(matches!(exit_status, ActorExitStatus::DownstreamClosed));
        assert!(matches!(count, 1)); //< Upon panick we cannot get a post mortem state.
        Ok(())
    }

    #[derive(Default)]
    struct ObserveActor {
        observe: AtomicU32,
    }

    #[async_trait]
    impl Actor for ObserveActor {
        type ObservableState = u32;

        fn observable_state(&self) -> u32 {
            self.observe.fetch_add(1, Ordering::Relaxed)
        }

        async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
            ctx.send_self_message(YieldLoop).await?;
            Ok(())
        }
    }

    #[derive(Debug)]
    struct YieldLoop;

    #[async_trait]
    impl Handler<YieldLoop> for ObserveActor {
        type Reply = ();
        async fn handle(
            &mut self,
            _: YieldLoop,
            ctx: &ActorContext<Self>,
        ) -> Result<Self::Reply, ActorExitStatus> {
            ctx.sleep(Duration::from_millis(25)).await; // OBSERVE_TIMEOUT.mul_f32(10.0f32)).await;
            ctx.send_self_message(YieldLoop).await?;
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_observation_debounce() {
        // TODO investigate why Universe::with_accelerated_time() does not work here.
        let universe = Universe::new();
        let (_, actor_handle) = universe.spawn_builder().spawn(ObserveActor::default());
        for _ in 0..10 {
            actor_handle.refresh_observe();
            universe.sleep(Duration::from_millis(10)).await;
        }
        let (_last_obs, num_obs) = actor_handle.quit().await;
        assert!(num_obs < 8);
        universe.assert_quit().await;
    }
}
