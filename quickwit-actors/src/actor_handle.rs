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
use std::borrow::Borrow;
use std::fmt;

use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing::error;

use crate::actor_state::ActorState;
use crate::channel_with_priority::Priority;
use crate::mailbox::Command;
use crate::observation::ObservationType;
use crate::{Actor, ActorContext, ActorExitStatus, Mailbox, Observation};

/// An Actor Handle serves as an address to communicate with an actor.
pub struct ActorHandle<A: Actor> {
    actor_context: ActorContext<A>,
    last_state: watch::Receiver<A::ObservableState>,
    join_handle: JoinHandle<ActorExitStatus>,
}

/// Describes the health of a given actor.
#[derive(Clone, Eq, PartialEq, Debug)]
pub enum Health {
    /// The actor is running and behaving as expected.
    Healthy,
    /// No progress was registered, or the process terminated with an error
    FailureOrUnhealthy,
    /// The actor terminated successfully.
    Success,
}

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
    fn health(&self) -> Health;
}

impl<A: Actor> Supervisable for ActorHandle<A> {
    fn name(&self) -> &str {
        self.actor_context.actor_instance_id()
    }

    fn health(&self) -> Health {
        let actor_state = self.state();
        if actor_state == ActorState::Success {
            Health::Success
        } else if actor_state == ActorState::Failure {
            error!(actor = self.name(), "actor-exit-without-success");
            Health::FailureOrUnhealthy
        } else if self
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
        join_handle: JoinHandle<ActorExitStatus>,
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
        let (tx, rx) = oneshot::channel();
        if !self.actor_context.state().is_exit()
            && self
                .actor_context
                .mailbox()
                .send_with_priority(Command::Observe(tx).into(), Priority::Low)
                .await
                .is_err()
        {
            error!(
                actor = self.actor_context.actor_instance_id(),
                "Failed to send observe message"
            );
        }
        // The timeout is required here. If the actor fails, its inbox is properly dropped but the
        // send channel might actually prevent the onechannel Receiver from being dropped.
        self.wait_for_observable_state_callback(rx).await
    }

    /// Pauses the actor. The actor will stop processing the message, but its
    /// work can be resumed by calling the method `.resume()`.
    pub async fn pause(&self) {
        let _ = self
            .actor_context
            .mailbox()
            .send_command(Command::Pause)
            .await;
    }

    /// Resumes a paused actor.
    pub async fn resume(&self) {
        let _ = self
            .actor_context
            .mailbox()
            .send_command(Command::Resume)
            .await;
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
            .send_command(Command::Kill)
            .await;
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
            .send_command(Command::Quit)
            .await;
        self.join().await
    }

    /// Waits until the actor exits by itself. This is the equivalent of `Thread::join`.
    pub async fn join(self) -> (ActorExitStatus, A::ObservableState) {
        let exit_status = self.join_handle.await.unwrap_or_else(|join_err| {
            if join_err.is_panic() {
                ActorExitStatus::Panicked
            } else {
                ActorExitStatus::Killed
            }
        });
        let observation = self.last_state.borrow().clone();
        (exit_status, observation)
    }

    /// Observe the current state.
    ///
    /// The observation will be scheduled as a command message, therefore it will be executed
    /// after the current active message and the current command queue have been processed.
    pub async fn observe(&self) -> Observation<A::ObservableState> {
        let (tx, rx) = oneshot::channel();
        if self.actor_context.state().is_exit() {
            let state = self.last_observation().borrow().clone();
            return Observation {
                obs_type: ObservationType::PostMortem,
                state,
            };
        }
        if self
            .actor_context
            .mailbox()
            .send_command(Command::Observe(tx))
            .await
            .is_err()
        {
            error!(
                actor_id = self.actor_context.actor_instance_id(),
                "Failed to send observe message"
            );
        }
        self.wait_for_observable_state_callback(rx).await
    }

    pub fn last_observation(&self) -> A::ObservableState {
        self.last_state.borrow().clone()
    }

    async fn wait_for_observable_state_callback(
        &self,
        rx: oneshot::Receiver<Box<dyn Any + Send>>,
    ) -> Observation<A::ObservableState> {
        let observable_state_or_timeout = timeout(crate::HEARTBEAT, rx).await;
        match observable_state_or_timeout {
            Ok(Ok(observable_state_any)) => {
                let state: A::ObservableState = *observable_state_any
                    .downcast()
                    .expect("The type is guaranteed logically by the ActorHandle.");
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
        let universe = Universe::new();
        let (mailbox, handle) = universe.spawn_actor(PanickingActor::default()).spawn();
        mailbox.send_message(Panic).await?;
        let (exit_status, count) = handle.join().await;
        assert!(matches!(exit_status, ActorExitStatus::Panicked));
        assert!(matches!(count, 1)); //< Upon panick we cannot get a post mortem state.
        Ok(())
    }

    #[tokio::test]
    async fn test_exit() -> anyhow::Result<()> {
        let universe = Universe::new();
        let (mailbox, handle) = universe.spawn_actor(ExitActor::default()).spawn();
        mailbox.send_message(Exit).await?;
        let (exit_status, count) = handle.join().await;
        assert!(matches!(exit_status, ActorExitStatus::DownstreamClosed));
        assert!(matches!(count, 1)); //< Upon panick we cannot get a post mortem state.
        Ok(())
    }
}
