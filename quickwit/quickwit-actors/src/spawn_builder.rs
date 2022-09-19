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

use anyhow::Context;
use tokio::sync::watch;
use tracing::{debug, error, info, Instrument};

use crate::envelope::Envelope;
use crate::mailbox::Inbox;
use crate::scheduler::Scheduler;
use crate::supervisor::Supervisor;
use crate::{
    create_mailbox, Actor, ActorContext, ActorExitStatus, ActorHandle, KillSwitch, Mailbox,
};

/// `SpawnBuilder` makes it possible to configure misc parameters before spawning an actor.
pub struct SpawnBuilder<A: Actor> {
    scheduler_mailbox: Mailbox<Scheduler>,
    kill_switch: KillSwitch,
    #[allow(clippy::type_complexity)]
    mailboxes: Option<(Mailbox<A>, Inbox<A>)>,
}

impl<A: Actor> SpawnBuilder<A> {
    pub(crate) fn new(scheduler_mailbox: Mailbox<Scheduler>, kill_switch: KillSwitch) -> Self {
        SpawnBuilder {
            scheduler_mailbox,
            kill_switch,
            mailboxes: None,
        }
    }

    /// Sets a specific kill switch for the actor.
    ///
    /// By default, the kill switch is inherited from the context that was used to
    /// spawn the actor.
    pub fn set_kill_switch(mut self, kill_switch: KillSwitch) -> Self {
        self.kill_switch = kill_switch;
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

    fn take_or_create_mailboxes(&mut self, actor: &A) -> (Mailbox<A>, Inbox<A>) {
        if let Some((mailbox, inbox)) = self.mailboxes.take() {
            return (mailbox, inbox);
        }
        let actor_name = actor.name();
        let queue_capacity = actor.queue_capacity();
        create_mailbox(actor_name, queue_capacity)
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
            self.kill_switch.clone(),
            self.scheduler_mailbox.clone(),
            state_tx,
        );
        (ctx, inbox, state_rx)
    }

    /// Spawns an async actor.
    pub fn spawn(self, actor: A) -> (Mailbox<A>, ActorHandle<A>) {
        let runtime_handle = actor.runtime_handle();
        let (ctx, inbox, state_rx) = self.create_actor_context_and_inbox(&actor);
        debug!(actor_id = %ctx.actor_instance_id(), "spawn-actor");
        let mailbox = ctx.mailbox().clone();
        let ctx_clone = ctx.clone();
        let span = actor.span(&ctx);
        let loop_async_actor_future =
            async move { actor_loop(actor, inbox, ctx).await }.instrument(span);
        let join_handle = runtime_handle.spawn(loop_async_actor_future);
        let actor_handle = ActorHandle::new(state_rx, join_handle, ctx_clone);
        (mailbox, actor_handle)
    }

    pub fn supervise_fn<F: Fn() -> A + Send + Sync + 'static>(
        mut self,
        actor_factory: F,
    ) -> (Mailbox<A>, ActorHandle<Supervisor<A>>) {
        let actor = actor_factory();
        let actor_name = actor.name();
        let (mailbox, inbox) = self.take_or_create_mailboxes(&actor);
        self.mailboxes = Some((mailbox, inbox.clone()));
        let kill_switch = self.kill_switch.clone();
        let child_kill_switch = kill_switch.child();
        let scheduler_mailbox = self.scheduler_mailbox.clone();
        let (mailbox, actor_handle) = self.set_kill_switch(child_kill_switch).spawn(actor);
        let supervisor = Supervisor {
            handle_opt: Some(actor_handle),
            actor_name,
            actor_factory: Box::new(actor_factory),
            inbox,
            mailbox: mailbox.clone(),
            state: Default::default(),
        };
        let (_superviser_mailbox, supervisor_handle) =
            SpawnBuilder::new(scheduler_mailbox, kill_switch).spawn(supervisor);
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

/// Returns `None` if no message is available at the moment.
async fn get_envelope<A: Actor>(inbox: &mut Inbox<A>, ctx: &ActorContext<A>) -> Envelope<A> {
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

fn try_get_envelope<A: Actor>(inbox: &mut Inbox<A>, ctx: &ActorContext<A>) -> Option<Envelope<A>> {
    if ctx.state().is_running() {
        inbox.try_recv()
    } else {
        // The actor is paused. We only process command and scheduled message.
        inbox.try_recv_cmd_and_scheduled_msg_only()
    }
    .ok()
}

struct ActorExecutionEnv<A: Actor> {
    actor: A,
    inbox: Inbox<A>,
    ctx: ActorContext<A>,
    msg_id: u64,
}

impl<A: Actor> ActorExecutionEnv<A> {
    async fn initialize(&mut self) -> Result<(), ActorExitStatus> {
        self.actor.initialize(&self.ctx).await
    }

    async fn process_messages(&mut self) -> ActorExitStatus {
        loop {
            if let Err(exit_status) = self.process_all_available_messages().await {
                return exit_status;
            }
        }
    }

    async fn process_one_message(
        &mut self,
        mut envelope: Envelope<A>,
    ) -> Result<(), ActorExitStatus> {
        self.yield_and_check_if_killed().await?;
        envelope
            .handle_message(self.msg_id, &mut self.actor, &self.ctx)
            .await?;
        self.msg_id += 1u64;
        Ok(())
    }

    async fn yield_and_check_if_killed(&self) -> Result<(), ActorExitStatus> {
        self.ctx.protect_future(tokio::task::yield_now()).await;
        if self.ctx.kill_switch().is_dead() {
            return Err(ActorExitStatus::Killed);
        }
        Ok(())
    }

    async fn process_all_available_messages(&mut self) -> Result<(), ActorExitStatus> {
        self.yield_and_check_if_killed().await?;
        let envelope = get_envelope(&mut self.inbox, &self.ctx).await;
        self.ctx.process();
        self.process_one_message(envelope).await?;
        while let Some(envelope) = try_get_envelope(&mut self.inbox, &self.ctx) {
            self.process_one_message(envelope).await?;
        }
        self.actor.on_drained_messages(&self.ctx).await?;
        self.ctx.idle();

        if self.ctx.mailbox().is_last_mailbox() {
            // No one will be able to send us more messages.
            // We can exit the actor.
            return Err(ActorExitStatus::Success);
        }

        Ok(())
    }

    async fn finalize(&mut self, exit_status: ActorExitStatus) -> ActorExitStatus {
        if let Err(finalize_error) = self
            .actor
            .finalize(&exit_status, &self.ctx)
            .await
            .with_context(|| format!("Finalization of actor {}", self.actor.name()))
        {
            error!(error=?finalize_error, "Finalizing failed, set exit status to panicked.");
            return ActorExitStatus::Panicked;
        }
        exit_status
    }

    fn process_exit_status(&self, exit_status: &ActorExitStatus) {
        match &exit_status {
            ActorExitStatus::Success
            | ActorExitStatus::Quit
            | ActorExitStatus::DownstreamClosed
            | ActorExitStatus::Killed => {}
            ActorExitStatus::Failure(err) => {
                error!(cause=?err, exit_status=?exit_status, "actor-failure");
            }
            ActorExitStatus::Panicked => {
                error!(exit_status=?exit_status, "actor-failure");
            }
        }
        info!(actor_id = %self.ctx.actor_instance_id(), exit_status = %exit_status, "actor-exit");
        self.ctx.exit(exit_status);
    }
}

impl<A: Actor> Drop for ActorExecutionEnv<A> {
    // We rely on this object internally to fetch a post-mortem state,
    // even in case of a panic.
    fn drop(&mut self) {
        self.ctx.observe(&mut self.actor);
    }
}

async fn actor_loop<A: Actor>(actor: A, inbox: Inbox<A>, ctx: ActorContext<A>) -> ActorExitStatus {
    let mut actor_env = ActorExecutionEnv {
        actor,
        inbox,
        ctx,
        msg_id: 1u64,
    };

    let initialize_exit_status_res: Result<(), ActorExitStatus> = actor_env.initialize().await;

    let after_process_exit_status = if let Err(initialize_exit_status) = initialize_exit_status_res
    {
        // We do not process messages if initialize yield an error.
        // We still call finalize however!
        initialize_exit_status
    } else {
        actor_env.process_messages().await
    };

    let final_exit_status = actor_env.finalize(after_process_exit_status).await;
    actor_env.process_exit_status(&final_exit_status);
    final_exit_status
}
