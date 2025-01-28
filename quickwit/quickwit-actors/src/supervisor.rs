// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use async_trait::async_trait;
use serde::Serialize;
use tracing::{info, warn};

use crate::mailbox::Inbox;
use crate::{Actor, ActorContext, ActorExitStatus, ActorHandle, Handler, Health, Supervisable};

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Serialize)]
pub struct SupervisorMetrics {
    pub num_panics: usize,
    pub num_errors: usize,
    pub num_kills: usize,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct SupervisorState<S> {
    pub metrics: SupervisorMetrics,
    pub state_opt: Option<S>,
}

impl<S> Default for SupervisorState<S> {
    fn default() -> Self {
        SupervisorState {
            metrics: Default::default(),
            state_opt: None,
        }
    }
}

pub struct Supervisor<A: Actor> {
    actor_name: String,
    actor_factory: Box<dyn Fn() -> A + Send>,
    inbox: Inbox<A>,
    handle_opt: Option<ActorHandle<A>>,
    metrics: SupervisorMetrics,
}

#[derive(Debug, Copy, Clone)]
struct SuperviseLoop;

#[async_trait]
impl<A: Actor> Actor for Supervisor<A> {
    type ObservableState = SupervisorState<A::ObservableState>;

    fn observable_state(&self) -> Self::ObservableState {
        let state_opt: Option<A::ObservableState> = self
            .handle_opt
            .as_ref()
            .map(|handle| handle.last_observation().clone());
        SupervisorState {
            metrics: self.metrics,
            state_opt,
        }
    }

    fn name(&self) -> String {
        format!("Supervisor({})", self.actor_name)
    }

    fn queue_capacity(&self) -> crate::QueueCapacity {
        crate::QueueCapacity::Unbounded
    }

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        ctx.schedule_self_msg(*crate::HEARTBEAT, SuperviseLoop);
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
                    handle.kill().await;
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

impl<A: Actor> Supervisor<A> {
    pub(crate) fn new(
        actor_name: String,
        actor_factory: Box<dyn Fn() -> A + Send>,
        inbox: Inbox<A>,
        handle: ActorHandle<A>,
    ) -> Self {
        Supervisor {
            actor_name,
            actor_factory,
            inbox,
            handle_opt: Some(handle),
            metrics: Default::default(),
        }
    }

    async fn supervise(
        &mut self,
        ctx: &ActorContext<Supervisor<A>>,
    ) -> Result<(), ActorExitStatus> {
        let handle_ref = self
            .handle_opt
            .as_ref()
            .expect("The actor handle should always be set.");
        match handle_ref.check_health(true) {
            Health::Healthy => {
                handle_ref.refresh_observe();
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
        let actor_mailbox = actor_handle.mailbox().clone();
        let (actor_exit_status, _last_state) = if !actor_handle.state().is_exit() {
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
                self.metrics.num_kills += 1;
            }
            ActorExitStatus::Failure(_err) => {
                self.metrics.num_errors += 1;
            }
            ActorExitStatus::Panicked => {
                self.metrics.num_panics += 1;
            }
        }
        info!("respawning-actor");
        let (_, actor_handle) = ctx
            .spawn_actor()
            .set_mailboxes(actor_mailbox, self.inbox.clone())
            .set_kill_switch(ctx.kill_switch().child())
            .spawn((*self.actor_factory)());
        self.handle_opt = Some(actor_handle);
        Ok(())
    }
}

#[async_trait]
impl<A: Actor> Handler<SuperviseLoop> for Supervisor<A> {
    type Reply = ();

    async fn handle(
        &mut self,
        _msg: SuperviseLoop,
        ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        self.supervise(ctx).await?;
        ctx.schedule_self_msg(*crate::HEARTBEAT, SuperviseLoop);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use async_trait::async_trait;
    use tracing::info;

    use crate::supervisor::SupervisorMetrics;
    use crate::tests::{Ping, PingReceiverActor};
    use crate::{Actor, ActorContext, ActorExitStatus, AskError, Handler, Observe, Universe};

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
            ctx: &ActorContext<Self>,
        ) -> Result<Self::Reply, ActorExitStatus> {
            match msg {
                FailingActorMessage::Panic => {
                    panic!("Failing actor panicked");
                }
                FailingActorMessage::ReturnError => {
                    return Err(ActorExitStatus::from(anyhow::anyhow!(
                        "failing actor error"
                    )));
                }
                FailingActorMessage::Increment => {
                    self.counter += 1;
                }
                FailingActorMessage::Freeze(wait_duration) => {
                    ctx.sleep(wait_duration).await;
                }
            }
            Ok(self.counter)
        }
    }

    #[tokio::test]
    async fn test_supervisor_restart_on_panic() {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::with_accelerated_time();
        let actor = FailingActor::default();
        let (mailbox, supervisor_handle) = universe.spawn_builder().supervise(actor);
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
            supervisor_handle.observe().await.metrics,
            SupervisorMetrics {
                num_panics: 1,
                num_errors: 0,
                num_kills: 0
            }
        );
        assert!(!matches!(
            supervisor_handle.quit().await.0,
            ActorExitStatus::Panicked
        ));
    }

    #[tokio::test]
    async fn test_supervisor_restart_on_error() {
        let universe = Universe::with_accelerated_time();
        let actor = FailingActor::default();
        let (mailbox, supervisor_handle) = universe.spawn_builder().supervise(actor);
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
            supervisor_handle.observe().await.metrics,
            SupervisorMetrics {
                num_panics: 0,
                num_errors: 1,
                num_kills: 0
            }
        );
        assert!(!matches!(
            supervisor_handle.quit().await.0,
            ActorExitStatus::Panicked
        ));
    }

    #[tokio::test]
    async fn test_supervisor_kills_and_restart_frozen_actor() {
        let universe = Universe::with_accelerated_time();
        let actor = FailingActor::default();
        let (mailbox, supervisor_handle) = universe.spawn_builder().supervise(actor);
        assert_eq!(
            mailbox.ask(FailingActorMessage::Increment).await.unwrap(),
            1
        );
        assert_eq!(
            mailbox.ask(FailingActorMessage::Increment).await.unwrap(),
            2
        );
        assert_eq!(
            supervisor_handle.observe().await.metrics,
            SupervisorMetrics {
                num_panics: 0,
                num_errors: 0,
                num_kills: 0
            }
        );
        mailbox
            .send_message(FailingActorMessage::Freeze(
                crate::HEARTBEAT.mul_f32(3.0f32),
            ))
            .await
            .unwrap();
        assert_eq!(
            mailbox.ask(FailingActorMessage::Increment).await.unwrap(),
            1
        );
        assert_eq!(
            supervisor_handle.observe().await.metrics,
            SupervisorMetrics {
                num_panics: 0,
                num_errors: 0,
                num_kills: 1
            }
        );
        assert!(!matches!(
            supervisor_handle.quit().await.0,
            ActorExitStatus::Panicked
        ));
    }

    #[tokio::test]
    async fn test_supervisor_forwards_quit_commands() {
        let universe = Universe::with_accelerated_time();
        let actor = FailingActor::default();
        let (mailbox, supervisor_handle) = universe.spawn_builder().supervise(actor);
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
        let universe = Universe::with_accelerated_time();
        let actor = FailingActor::default();
        let (mailbox, supervisor_handle) = universe.spawn_builder().supervise(actor);
        assert_eq!(
            mailbox.ask(FailingActorMessage::Increment).await.unwrap(),
            1
        );
        let (exit_status, _state) = supervisor_handle.kill().await;
        assert!(mailbox.ask(FailingActorMessage::Increment).await.is_err());
        assert!(matches!(
            mailbox
                .ask(FailingActorMessage::Increment)
                .await
                .unwrap_err(),
            AskError::MessageNotDelivered
        ));
        assert!(matches!(exit_status, ActorExitStatus::Killed));
    }

    #[tokio::test]
    async fn test_supervisor_exits_successfully_when_supervised_actor_mailbox_is_dropped() {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::with_accelerated_time();
        let actor = FailingActor::default();
        let (_, supervisor_handle) = universe.spawn_builder().supervise(actor);
        let (exit_status, _state) = supervisor_handle.join().await;
        assert!(matches!(exit_status, ActorExitStatus::Success));
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_supervisor_state() {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::with_accelerated_time();
        let ping_actor = PingReceiverActor::default();
        let (mailbox, handler) = universe.spawn_builder().supervise(ping_actor);
        let obs = handler.observe().await;
        assert_eq!(obs.state.state_opt, Some(0));
        let _ = mailbox.ask(Ping).await;
        assert_eq!(mailbox.ask(Observe).await.unwrap(), 1);
        universe.sleep(Duration::from_secs(60)).await;
        let obs = handler.observe().await;
        assert_eq!(obs.state.state_opt, Some(1));
        handler.quit().await;
        universe.assert_quit().await;
    }
}
