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

use async_trait::async_trait;
use tracing::{info, warn};

use crate::mailbox::Inbox;
use crate::{
    Actor, ActorContext, ActorExitStatus, ActorHandle, ActorState, Handler, Health, Mailbox,
    Supervisable, KillSwitch,
};

const SUPERVISE_PERIOD: Duration = crate::HEARTBEAT;

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct SupervisorState {
    num_panics: usize,
    num_errors: usize,
    num_kills: usize,
}

pub struct Supervisor<A: Actor + Clone> {
    pub(crate) actor: A,
    pub(crate) inbox: Inbox<A>,
    pub(crate) mailbox: Mailbox<A>,
    pub(crate) handle_opt: Option<ActorHandle<A>>,
    pub(crate) state: SupervisorState,
}

#[derive(Debug, Copy, Clone)]
pub struct SuperviseLoop;

#[async_trait]
impl<A: Actor + Clone> Actor for Supervisor<A> {
    type ObservableState = SupervisorState;

    fn observable_state(&self) -> Self::ObservableState {
        self.state
    }

    fn name(&self) -> String {
        format!("Supervisor({})", self.actor.name())
    }

    fn queue_capacity(&self) -> crate::QueueCapacity {
        crate::QueueCapacity::Unbounded
    }

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        ctx.schedule_self_msg(SUPERVISE_PERIOD, SuperviseLoop).await;
        Ok(())
    }

    async fn finalize(
        &mut self,
        exit_status: &ActorExitStatus,
        _ctx: &ActorContext<Self>,
    ) -> anyhow::Result<()> {
        match exit_status {
            ActorExitStatus::Quit => {
                if let Some(handle) = self.handle_opt.take() {
                    handle.quit().await;
                }
            }
            ActorExitStatus::Killed => {
                if let Some(handle) = self.handle_opt.take() {
                    handle.activate_kill_switch();
                }
            }
            ActorExitStatus::Failure(_)
            | ActorExitStatus::Success
            | ActorExitStatus::DownstreamClosed => {}
            ActorExitStatus::Panicked => {}
        }

        Ok(())
    }
}

impl<A: Actor + Clone> Supervisor<A> {
    async fn supervise(
        &mut self,
        ctx: &ActorContext<Supervisor<A>>,
    ) -> Result<(), ActorExitStatus> {
        match self.handle_opt.as_ref().unwrap().health() {
            Health::Healthy => {
                return Ok(());
            }
            Health::FailureOrUnhealthy => {}
            Health::Success => {
                return Err(ActorExitStatus::Success);
            }
        }
        warn!("unhealthy-actor");
        // The actor is failing we need to restart it.
        let actor_handle = self.handle_opt.take().unwrap();
        let (actor_exit_status, _last_state) = if actor_handle.state() == ActorState::Processing {
            // The actor is probably frozen.
            // Let's kill it.
            warn!("killing");
            actor_handle.kill().await
        } else {
            actor_handle.join().await
        };
        match actor_exit_status {
            ActorExitStatus::Success => {
                return Err(ActorExitStatus::Success);
            }
            ActorExitStatus::Quit => {
                return Err(ActorExitStatus::Quit);
            }
            ActorExitStatus::DownstreamClosed => {
                return Err(ActorExitStatus::DownstreamClosed);
            }
            ActorExitStatus::Killed => {
                self.state.num_kills += 1;
            }
            ActorExitStatus::Failure(_err) => {
                self.state.num_errors += 1;
            }
            ActorExitStatus::Panicked => {
                self.state.num_panics += 1;
            }
        }
        info!("respawning-actor");
        let (_, actor_handle) = ctx
            .spawn_actor(self.actor.clone())
            .set_mailboxes(self.mailbox.clone(), self.inbox.clone())
            .set_kill_switch(KillSwitch::default())
            .spawn();
        self.handle_opt = Some(actor_handle);
        Ok(())
    }
}

#[async_trait]
impl<A: Actor + Clone> Handler<SuperviseLoop> for Supervisor<A> {
    type Reply = ();

    async fn handle(
        &mut self,
        _msg: SuperviseLoop,
        ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        self.supervise(ctx).await?;
        ctx.schedule_self_msg(SUPERVISE_PERIOD, SuperviseLoop).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use async_trait::async_trait;
    use tracing::info;

    use crate::supervisor::SupervisorState;
    use crate::{Actor, ActorContext, ActorExitStatus, AskError, Handler, Universe};

    #[derive(Copy, Clone, Debug)]
    enum FailingActorMessage {
        Panic,
        ReturnError,
        Increment,
        Freeze(Duration),
    }

    #[derive(Default, Clone)]
    struct FailingActor {
        counter: usize,
    }

    #[async_trait]
    impl Actor for FailingActor {
        type ObservableState = usize;

        fn name(&self) -> String {
            "FailingActor".to_string()
        }

        fn observable_state(&self) -> Self::ObservableState {
            self.counter
        }

        async fn finalize(
            &mut self,
            _exit_status: &ActorExitStatus,
            _ctx: &ActorContext<Self>,
        ) -> anyhow::Result<()> {
            info!("finalize-failing-actor");
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<FailingActorMessage> for FailingActor {
        type Reply = usize;

        async fn handle(
            &mut self,
            msg: FailingActorMessage,
            _ctx: &ActorContext<Self>,
        ) -> Result<Self::Reply, ActorExitStatus> {
            match msg {
                FailingActorMessage::Panic => {
                    panic!("Failing actor panicked");
                }
                FailingActorMessage::ReturnError => {
                    return Err(ActorExitStatus::from(anyhow::anyhow!(
                        "Failing actor error"
                    )));
                }
                FailingActorMessage::Increment => {
                    self.counter += 1;
                }
                FailingActorMessage::Freeze(wait_duration) => {
                    tokio::time::sleep(wait_duration).await;
                }
            }
            Ok(self.counter)
        }
    }

    #[tokio::test]
    async fn test_supervisor_restart_on_panic() {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::new();
        let actor = FailingActor::default();
        let (mailbox, supervisor_handle) = universe.spawn_actor(actor).supervise();
        assert_eq!(
            mailbox.ask(FailingActorMessage::Increment).await.unwrap(),
            1
        );
        assert_eq!(
            mailbox.ask(FailingActorMessage::Increment).await.unwrap(),
            2
        );
        assert!(mailbox.ask(FailingActorMessage::Panic).await.is_err());
        assert_eq!(
            mailbox.ask(FailingActorMessage::Increment).await.unwrap(),
            1
        );
        assert_eq!(
            *supervisor_handle.observe().await,
            SupervisorState {
                num_panics: 1,
                num_errors: 0,
                num_kills: 0
            }
        );
    }

    #[tokio::test]
    async fn test_supervisor_restart_on_error() {
        let universe = Universe::new();
        let actor = FailingActor::default();
        let (mailbox, supervisor_handle) = universe.spawn_actor(actor).supervise();
        assert_eq!(
            mailbox.ask(FailingActorMessage::Increment).await.unwrap(),
            1
        );
        assert_eq!(
            mailbox.ask(FailingActorMessage::Increment).await.unwrap(),
            2
        );
        assert!(mailbox.ask(FailingActorMessage::ReturnError).await.is_err());
        assert_eq!(
            mailbox.ask(FailingActorMessage::Increment).await.unwrap(),
            1
        );
        assert_eq!(
            *supervisor_handle.observe().await,
            SupervisorState {
                num_panics: 0,
                num_errors: 1,
                num_kills: 0
            }
        );
    }

    #[tokio::test]
    async fn test_supervisor_kills_and_restart_frozen_actor() {
        let universe = Universe::new();
        let actor = FailingActor::default();
        let (mailbox, supervisor_handle) = universe.spawn_actor(actor).supervise();
        assert_eq!(
            mailbox.ask(FailingActorMessage::Increment).await.unwrap(),
            1
        );
        universe.simulate_time_shift(Duration::from_secs(10)).await;
        assert_eq!(
            mailbox.ask(FailingActorMessage::Increment).await.unwrap(),
            2
        );
        assert_eq!(
            *supervisor_handle.observe().await,
            SupervisorState {
                num_panics: 0,
                num_errors: 0,
                num_kills: 0
            }
        );
        assert_eq!(
            mailbox
                .ask(FailingActorMessage::Freeze(Duration::from_secs(10)))
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            mailbox.ask(FailingActorMessage::Increment).await.unwrap(),
            1
        );
        assert_eq!(
            *supervisor_handle.observe().await,
            SupervisorState {
                num_panics: 0,
                num_errors: 0,
                num_kills: 1
            }
        );
    }

    #[tokio::test]
    async fn test_supervisor_forwards_quit_commands() {
        let universe = Universe::new();
        let actor = FailingActor::default();
        let (mailbox, supervisor_handle) = universe.spawn_actor(actor).supervise();
        assert_eq!(
            mailbox.ask(FailingActorMessage::Increment).await.unwrap(),
            1
        );
        let (exit_status, _state) = supervisor_handle.quit().await;
        assert!(matches!(
            mailbox
                .ask(FailingActorMessage::Increment)
                .await
                .unwrap_err(),
            AskError::MessageNotDelivered
        ));
        assert!(matches!(exit_status, ActorExitStatus::Quit));
    }

    #[tokio::test]
    async fn test_supervisor_forwards_kill_command() {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::new();
        let actor = FailingActor::default();
        let (mailbox, supervisor_handle) = universe.spawn_actor(actor).supervise();
        assert_eq!(
            mailbox.ask(FailingActorMessage::Increment).await.unwrap(),
            1
        );
        let (exit_status, _state) = supervisor_handle.kill().await;
        assert!(mailbox
            .ask(FailingActorMessage::Increment)
            .await
            .is_err());
        assert!(matches!(
            mailbox
                .ask(FailingActorMessage::Increment)
                .await
                .unwrap_err(),
            AskError::MessageNotDelivered
        ));
        assert!(matches!(exit_status, ActorExitStatus::Killed));
    }
}
