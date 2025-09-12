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

use std::fmt;
use std::time::Duration;

use anyhow::Context;
use quickwit_common::metrics::IntCounter;
use sync_wrapper::SyncWrapper;
use tokio::sync::watch;
use tracing::{debug, error, info};

use crate::envelope::Envelope;
use crate::mailbox::{Inbox, create_mailbox};
use crate::registry::{ActorJoinHandle, ActorRegistry};
use crate::scheduler::{NoAdvanceTimeGuard, SchedulerClient};
use crate::supervisor::Supervisor;
use crate::{
    Actor, ActorContext, ActorExitStatus, ActorHandle, KillSwitch, Mailbox, QueueCapacity,
};

#[derive(Clone)]
pub struct SpawnContext {
    pub(crate) scheduler_client: SchedulerClient,
    pub(crate) kill_switch: KillSwitch,
    pub(crate) registry: ActorRegistry,
}

impl SpawnContext {
    pub fn new(scheduler_client: SchedulerClient) -> Self {
        SpawnContext {
            scheduler_client,
            kill_switch: Default::default(),
            registry: ActorRegistry::default(),
        }
    }

    pub fn spawn_builder<A: Actor>(&self) -> SpawnBuilder<A> {
        SpawnBuilder::new(self.child_context())
    }

    pub fn create_mailbox<A: Actor>(
        &self,
        actor_name: impl ToString,
        queue_capacity: QueueCapacity,
    ) -> (Mailbox<A>, Inbox<A>) {
        create_mailbox(
            actor_name.to_string(),
            queue_capacity,
            Some(self.scheduler_client.clone()),
        )
    }

    pub fn child_context(&self) -> SpawnContext {
        SpawnContext {
            scheduler_client: self.scheduler_client.clone(),
            kill_switch: self.kill_switch.child(),
            registry: self.registry.clone(),
        }
    }

    /// Schedules a new event.
    /// Once `timeout` is elapsed, the future `fut` is
    /// executed.
    ///
    /// `fut` will be executed in the scheduler task, so it is
    /// required to be short.
    pub fn schedule_event<F: FnOnce() + Send + Sync + 'static>(
        &self,
        callback: F,
        timeout: Duration,
    ) {
        self.scheduler_client.schedule_event(callback, timeout)
    }
}

/// `SpawnBuilder` makes it possible to configure misc parameters before spawning an actor.
#[derive(Clone)]
pub struct SpawnBuilder<A: Actor> {
    spawn_ctx: SpawnContext,
    #[allow(clippy::type_complexity)]
    mailboxes: Option<(Mailbox<A>, Inbox<A>)>,
    backpressure_micros_counter_opt: Option<IntCounter>,
}

impl<A: Actor> SpawnBuilder<A> {
    pub(crate) fn new(spawn_ctx: SpawnContext) -> Self {
        SpawnBuilder {
            spawn_ctx,
            mailboxes: None,
            backpressure_micros_counter_opt: None,
        }
    }

    /// Sets a specific kill switch for the actor.
    ///
    /// By default, the kill switch is inherited from the context that was used to
    /// spawn the actor.
    pub fn set_kill_switch(mut self, kill_switch: KillSwitch) -> Self {
        self.spawn_ctx.kill_switch = kill_switch;
        self
    }

    /// Sets a specific set of mailbox.
    ///
    /// By default, a brand new set of mailboxes will be created
    /// when the actor is spawned.
    ///
    /// This function makes it possible to create non-DAG networks
    /// of actors.
    pub fn set_mailboxes(mut self, mailbox: Mailbox<A>, inbox: Inbox<A>) -> Self {
        self.mailboxes = Some((mailbox, inbox));
        self
    }

    /// Adds a counter to track the amount of time the actor is
    /// spending in "backpressure".
    ///
    /// When using `.ask` the amount of time counted may be misleading.
    /// (See `Mailbox::ask_with_backpressure_counter` for more details)
    pub fn set_backpressure_micros_counter(
        mut self,
        backpressure_micros_counter: IntCounter,
    ) -> Self {
        self.backpressure_micros_counter_opt = Some(backpressure_micros_counter);
        self
    }

    fn take_or_create_mailboxes(&mut self, actor: &A) -> (Mailbox<A>, Inbox<A>) {
        if let Some((mailbox, inbox)) = self.mailboxes.take() {
            return (mailbox, inbox);
        }
        let actor_name = actor.name();
        let queue_capacity = actor.queue_capacity();
        self.spawn_ctx.create_mailbox(actor_name, queue_capacity)
    }

    fn create_actor_context_and_inbox(
        mut self,
        actor: &A,
    ) -> (
        ActorContext<A>,
        Inbox<A>,
        watch::Receiver<A::ObservableState>,
    ) {
        let (mailbox, inbox) = self.take_or_create_mailboxes(actor);
        let obs_state = actor.observable_state();
        let (state_tx, state_rx) = watch::channel(obs_state);
        let ctx = ActorContext::new(
            mailbox,
            self.spawn_ctx.clone(),
            state_tx,
            self.backpressure_micros_counter_opt,
        );
        (ctx, inbox, state_rx)
    }

    /// Spawns an async actor.
    pub fn spawn(self, actor: A) -> (Mailbox<A>, ActorHandle<A>) {
        // We prevent fast forward of the scheduler during  initialization.
        let no_advance_time_guard = self.spawn_ctx.scheduler_client.no_advance_time_guard();
        let runtime_handle = actor.runtime_handle();
        let (ctx, inbox, state_rx) = self.create_actor_context_and_inbox(&actor);
        debug!(actor_id = %ctx.actor_instance_id(), "spawn-actor");
        let mailbox = ctx.mailbox().clone();
        let ctx_clone = ctx.clone();
        let loop_async_actor_future =
            async move { actor_loop(actor, inbox, no_advance_time_guard, ctx).await };
        let join_handle = ActorJoinHandle::new(quickwit_common::spawn_named_task_on(
            loop_async_actor_future,
            std::any::type_name::<A>(),
            &runtime_handle,
        ));
        ctx_clone.registry().register(&mailbox, join_handle.clone());
        let actor_handle = ActorHandle::new(state_rx, join_handle, ctx_clone);
        (mailbox, actor_handle)
    }

    pub fn supervise_fn<F: Fn() -> A + Send + 'static>(
        mut self,
        actor_factory: F,
    ) -> (Mailbox<A>, ActorHandle<Supervisor<A>>) {
        let actor = actor_factory();
        let actor_name = actor.name();
        let (mailbox, inbox) = self.take_or_create_mailboxes(&actor);
        self.mailboxes = Some((mailbox, inbox.clone()));
        let child_ctx = self.spawn_ctx.child_context();
        let parent_spawn_ctx = std::mem::replace(&mut self.spawn_ctx, child_ctx);
        let (mailbox, actor_handle) = self.spawn(actor);
        let supervisor = Supervisor::new(actor_name, Box::new(actor_factory), inbox, actor_handle);
        let (_supervisor_mailbox, supervisor_handle) =
            parent_spawn_ctx.spawn_builder().spawn(supervisor);
        (mailbox, supervisor_handle)
    }
}

impl<A: Actor + Clone> SpawnBuilder<A> {
    pub fn supervise(self, actor: A) -> (Mailbox<A>, ActorHandle<Supervisor<A>>) {
        self.supervise_fn(move || actor.clone())
    }
}

impl<A: Actor + Default> SpawnBuilder<A> {
    pub fn supervise_default(self) -> (Mailbox<A>, ActorHandle<Supervisor<A>>) {
        self.supervise_fn(Default::default)
    }
}

enum ActorExitPhase {
    Initializing,
    Handling { message: &'static str },
    Running,
    OnDrainedMessaged,
    Completed,
}

impl fmt::Debug for ActorExitPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ActorExitPhase::Initializing => write!(f, "initializing"),
            ActorExitPhase::Handling { message } => write!(f, "handling({message})"),
            ActorExitPhase::Running => write!(f, "running"),
            ActorExitPhase::OnDrainedMessaged => write!(f, "on_drained_messages"),
            ActorExitPhase::Completed => write!(f, "completed"),
        }
    }
}

/// Receives an envelope from either the high priority queue or the low priority queue.
///
/// In the paused state, the actor will only attempt to receive high priority messages.
///
/// If no message is available, this function will yield until a message arrives.
/// If a high priority message is arrives first it is guaranteed to be processed first.
/// This other way around is however not guaranteed.
async fn recv_envelope<A: Actor>(inbox: &mut Inbox<A>, ctx: &ActorContext<A>) -> Envelope<A> {
    if ctx.state().is_running() {
        ctx.protect_future(inbox.recv()).await.expect(
            "Disconnection should be impossible because the ActorContext holds a Mailbox too",
        )
    } else {
        // The actor is paused. We only process command and scheduled message.
        ctx.protect_future(inbox.recv_cmd_and_scheduled_msg_only())
            .await
    }
}

fn try_recv_envelope<A: Actor>(inbox: &mut Inbox<A>) -> Option<Envelope<A>> {
    inbox.try_recv().ok()
}

struct ActorExecutionEnv<A: Actor> {
    actor: SyncWrapper<A>,
    inbox: Inbox<A>,
    ctx: ActorContext<A>,
}

impl<A: Actor> ActorExecutionEnv<A> {
    async fn initialize(&mut self) -> Result<(), ActorExitStatus> {
        self.actor.get_mut().initialize(&self.ctx).await
    }

    async fn process_messages(&mut self) -> (ActorExitStatus, ActorExitPhase) {
        loop {
            if let Err((exit_status, exit_phase)) = self.process_all_available_messages().await {
                return (exit_status, exit_phase);
            }
        }
    }

    async fn process_one_message(
        &mut self,
        mut envelope: Envelope<A>,
    ) -> Result<(), (ActorExitStatus, ActorExitPhase)> {
        self.yield_and_check_if_killed().await?;
        envelope
            .handle_message(self.actor.get_mut(), &self.ctx)
            .await
            .map_err(|(exit_status, message)| {
                (exit_status, ActorExitPhase::Handling { message })
            })?;
        Ok(())
    }

    async fn yield_and_check_if_killed(&mut self) -> Result<(), (ActorExitStatus, ActorExitPhase)> {
        if self.ctx.kill_switch().is_dead() {
            return Err((ActorExitStatus::Killed, ActorExitPhase::Running));
        }
        if self.actor.get_mut().yield_after_each_message() {
            self.ctx.yield_now().await;
            if self.ctx.kill_switch().is_dead() {
                return Err((ActorExitStatus::Killed, ActorExitPhase::Running));
            }
        } else {
            self.ctx.record_progress();
        }
        Ok(())
    }

    async fn process_all_available_messages(
        &mut self,
    ) -> Result<(), (ActorExitStatus, ActorExitPhase)> {
        self.yield_and_check_if_killed().await?;
        let envelope = recv_envelope(&mut self.inbox, &self.ctx).await;
        self.process_one_message(envelope).await?;
        // If the actor is Running (not Paused), we consume all the messages in the mailbox
        // and call `on_drained_message`.
        if self.ctx.state().is_running() {
            loop {
                while let Some(envelope) = try_recv_envelope(&mut self.inbox) {
                    self.process_one_message(envelope).await?;
                }
                // We have reached the last message.
                // Let's still yield and see if we have more messages:
                // an upstream actor might have experienced backpressure, and is now waiting for our
                // mailbox to have some room.
                self.ctx.yield_now().await;
                if self.inbox.is_empty() {
                    break;
                }
            }
            self.actor
                .get_mut()
                .on_drained_messages(&self.ctx)
                .await
                .map_err(|exit_status| (exit_status, ActorExitPhase::OnDrainedMessaged))?;
        }
        if self.ctx.mailbox().is_last_mailbox() {
            // We double check here that the mailbox does not contain any messages,
            // as someone on different runtime thread could have added a last message
            // and dropped the last mailbox right before this block.
            // See #4248
            if self.inbox.is_empty() {
                // No one will be able to send us more messages.
                // We can exit the actor.
                return Err((ActorExitStatus::Success, ActorExitPhase::Completed));
            }
        }

        Ok(())
    }

    async fn finalize(&mut self, exit_status: ActorExitStatus) -> ActorExitStatus {
        let _no_advance_time_guard = self
            .ctx
            .mailbox()
            .scheduler_client()
            .map(|scheduler_client| scheduler_client.no_advance_time_guard());
        if let Err(finalize_error) = self
            .actor
            .get_mut()
            .finalize(&exit_status, &self.ctx)
            .await
            .with_context(|| format!("finalization of actor {}", self.actor.get_mut().name()))
        {
            error!(error=?finalize_error, "finalizing failed, set exit status to panicked");
            return ActorExitStatus::Panicked;
        }
        exit_status
    }
}

impl<A: Actor> Drop for ActorExecutionEnv<A> {
    // We rely on this object internally to fetch a post-mortem state,
    // even in case of a panic.
    fn drop(&mut self) {
        self.ctx.observe(self.actor.get_mut());
    }
}

async fn actor_loop<A: Actor>(
    actor: A,
    inbox: Inbox<A>,
    no_advance_time_guard: NoAdvanceTimeGuard,
    ctx: ActorContext<A>,
) -> ActorExitStatus {
    let mut actor_env = ActorExecutionEnv {
        actor: SyncWrapper::new(actor),
        inbox,
        ctx,
    };

    let initialize_exit_status_res: Result<(), ActorExitStatus> = actor_env.initialize().await;
    drop(no_advance_time_guard);

    let (after_process_exit_status, exit_phase) =
        if let Err(initialize_exit_status) = initialize_exit_status_res {
            // We do not process messages if initialize yield an error.
            // We still call finalize however!
            (initialize_exit_status, ActorExitPhase::Initializing)
        } else {
            actor_env.process_messages().await
        };

    let actor_id = actor_env.ctx.actor_instance_id();
    match after_process_exit_status {
        ActorExitStatus::Success
        | ActorExitStatus::Quit
        | ActorExitStatus::DownstreamClosed
        | ActorExitStatus::Killed => {
            info!(actor_id, phase = ?exit_phase, exit_status = ?after_process_exit_status, "actor-exit");
        }
        ActorExitStatus::Failure(_) | ActorExitStatus::Panicked => {
            error!(actor_id, phase = ?exit_phase, exit_status = ?after_process_exit_status, "actor-exit");
        }
    };

    // TODO the no advance time guard for finalize has a race condition. Ideally we would
    // like to have the guard before we drop the last envelope.
    let final_exit_status = actor_env.finalize(after_process_exit_status).await;
    // The last observation is collected on `ActorExecutionEnv::Drop`.
    actor_env.ctx.exit(&final_exit_status);
    final_exit_status
}
