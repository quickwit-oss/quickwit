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

use tokio::sync::watch;
use tokio::task::{spawn_blocking, JoinHandle};
use tracing::{debug, info};

use crate::async_actor::async_actor_loop;
use crate::mailbox::{create_mailboxes, Inbox};
use crate::scheduler::SchedulerMessage;
use crate::sync_actor::sync_actor_loop;
use crate::{
    create_mailbox, Actor, ActorContext, ActorHandle, AsyncActor, KillSwitch, Mailbox,
    ParallelActor, QueueCapacity, SyncActor,
};

/// `SpawnBuilder` makes it possible to configure misc parameters before spawning an actor.
pub struct SpawnBuilder<A: Actor> {
    actor: Option<A>,
    scheduler_mailbox: Mailbox<SchedulerMessage>,
    kill_switch: KillSwitch,
    #[allow(clippy::type_complexity)]
    mailboxes: Option<(Mailbox<A::Message>, Inbox<A::Message>)>,
}

impl<A: Actor> SpawnBuilder<A> {
    pub(crate) fn new(
        actor: A,
        scheduler_mailbox: Mailbox<SchedulerMessage>,
        kill_switch: KillSwitch,
    ) -> Self {
        SpawnBuilder {
            actor: Some(actor),
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
    pub fn set_mailboxes(mut self, mailbox: Mailbox<A::Message>, inbox: Inbox<A::Message>) -> Self {
        self.mailboxes = Some((mailbox, inbox));
        self
    }

    fn create_actor_context_and_inbox(
        mut self,
        actor_name: String,
        queue_capacity: QueueCapacity,
    ) -> (ActorContext<A::Message>, Inbox<A::Message>) {
        let (mailbox, inbox) = self
            .mailboxes
            .take()
            .unwrap_or_else(move || create_mailbox(actor_name, queue_capacity));
        let ctx = ActorContext::new(mailbox, self.kill_switch.clone(), self.scheduler_mailbox);
        (ctx, inbox)
    }
}

/// Spawnable is an internal trait that helps us factorizing the logic between
/// sync and async actors.
trait Spawnable<A: Actor> {
    fn actor(&self) -> &A;
    fn spawn(self, ctx: ActorContext<A::Message>, inbox: Inbox<A::Message>) -> ActorHandle<A>;
}

// Ideally we would have prefered to have `impl<T: AsyncActor> Spawnable for T`,
/// but there is no way to inform rust type system that no type can implement both
/// AsyncActor and SyncActor.
///
/// `SpawnableSync` and `SpawnableAsync` are just wrappers to helps the compiler with
/// the disambiguation, and factorize the spawning code.
#[derive(Clone)]
struct SpawnableSync<A: SyncActor + Actor>(A);

impl<A: SyncActor> Spawnable<A> for SpawnableSync<A> {
    fn actor(&self) -> &A {
        &self.0
    }

    fn spawn(self, ctx: ActorContext<A::Message>, inbox: Inbox<A::Message>) -> ActorHandle<A> {
        let actor = self.0;
        debug!(actor=%ctx.actor_instance_id(),"spawning-sync-actor");
        let (state_tx, state_rx) = watch::channel(actor.observable_state());
        let ctx_clone = ctx.clone();
        let (exit_status_tx, exit_status_rx) = watch::channel(None);
        let join_handle: JoinHandle<()> = spawn_blocking(move || {
            let actor_instance_id = ctx.actor_instance_id().to_string();
            let exit_status = sync_actor_loop(actor, inbox, ctx, state_tx);
            info!(exit_status=%exit_status, actor=actor_instance_id.as_str(), "exit");
            let _ = exit_status_tx.send(Some(exit_status));
        });
        ActorHandle::new(state_rx, join_handle, ctx_clone, exit_status_rx)
    }
}

#[derive(Clone)]
struct SpawnableAsync<A: AsyncActor + Actor>(A);

impl<A: AsyncActor> Spawnable<A> for SpawnableAsync<A> {
    fn actor(&self) -> &A {
        &self.0
    }

    fn spawn(self, ctx: ActorContext<A::Message>, inbox: Inbox<A::Message>) -> ActorHandle<A> {
        {
            let actor = self.0;
            let ctx = ctx;
            debug!(actor_name = %ctx.actor_instance_id(), "spawning-async-actor");
            let (state_tx, state_rx) = watch::channel(actor.observable_state());
            let ctx_clone = ctx.clone();
            let (exit_status_tx, exit_status_rx) = watch::channel(None);
            let join_handle: JoinHandle<()> = tokio::spawn(async move {
                let actor_instance_id = ctx.actor_instance_id().to_string();
                let exit_status = async_actor_loop(actor, inbox, ctx, state_tx).await;
                info!(
                    actor_name = actor_instance_id.as_str(),
                    exit_status = %exit_status,
                    "actor-exit"
                );
                let _ = exit_status_tx.send(Some(exit_status));
            });
            ActorHandle::new(state_rx, join_handle, ctx_clone, exit_status_rx)
        }
    }
}

impl<A: Actor> SpawnBuilder<A> {
    /// Spawns an async actor.
    fn spawn_actor<S: Spawnable<A>>(self, spawnable: S) -> (Mailbox<A::Message>, ActorHandle<A>) {
        let actor_name = spawnable.actor().name();
        let queue_capacity = spawnable.actor().queue_capacity();
        let (ctx, inbox) = self.create_actor_context_and_inbox(actor_name, queue_capacity);
        let mailbox = ctx.mailbox().clone();
        let actor_handle = spawnable.spawn(ctx, inbox);
        (mailbox, actor_handle)
    }
}

impl<A: Actor + Clone> SpawnBuilder<A> {
    /// Spawns an async actor.
    fn spawn_parallel_actor<S: Spawnable<A> + Clone>(
        self,
        actor: S,
        num_actors: usize,
    ) -> (Mailbox<A::Message>, ActorHandle<ParallelActor<A>>) {
        let actor_name = actor.actor().name();
        let queue_capacity = actor.actor().queue_capacity();
        let mailboxes_and_inboxes: Vec<(Mailbox<_>, Inbox<_>)> =
            create_mailboxes(actor_name.clone(), queue_capacity, num_actors);
        let kill_switch = self.kill_switch.clone();
        let mut mailboxes = Vec::new();
        let handles: Vec<ActorHandle<A>> = mailboxes_and_inboxes
            .into_iter()
            .map(|(mailbox, inbox)| {
                mailboxes.push(mailbox.clone());
                let ctx =
                    ActorContext::new(mailbox, kill_switch.clone(), self.scheduler_mailbox.clone());
                actor.clone().spawn(ctx, inbox)
            })
            .collect();
        let parallel_actor = ParallelActor::new(actor_name, handles, mailboxes);
        let (supervisor_mailbox, supervisor_inbox) =
            create_mailbox(parallel_actor.name(), parallel_actor.queue_capacity());
        let parallel_actor_ctx = ActorContext::new(
            supervisor_mailbox.clone(),
            self.kill_switch.clone(),
            self.scheduler_mailbox,
        );
        let supervisor_handle =
            SpawnableAsync(parallel_actor).spawn(parallel_actor_ctx, supervisor_inbox);
        (supervisor_mailbox, supervisor_handle)
    }
}

impl<A: AsyncActor> SpawnBuilder<A> {
    /// Spawns an async actor.
    pub fn spawn_async(mut self) -> (Mailbox<A::Message>, ActorHandle<A>) {
        let actor = self.actor.take().unwrap();
        let spawnable_actor = SpawnableAsync(actor);
        self.spawn_actor(spawnable_actor)
    }
}

impl<A: SyncActor> SpawnBuilder<A> {
    /// Spawns an async actor.
    pub fn spawn_sync(mut self) -> (Mailbox<A::Message>, ActorHandle<A>) {
        let actor = self.actor.take().unwrap();
        let spawnable_actor = SpawnableSync(actor);
        self.spawn_actor(spawnable_actor)
    }
}

impl<A: AsyncActor + Clone> SpawnBuilder<A> {
    /// Spawns an async actor.
    pub fn spawn_parallel_async(
        mut self,
        num_actors: usize,
    ) -> (Mailbox<A::Message>, ActorHandle<ParallelActor<A>>) {
        let actor = self.actor.take().unwrap();
        let spawnable_actor = SpawnableAsync(actor);
        self.spawn_parallel_actor(spawnable_actor, num_actors)
    }
}

impl<A: SyncActor + Clone> SpawnBuilder<A> {
    /// Spawns an async actor.
    pub fn spawn_parallel_sync(
        mut self,
        num_actors: usize,
    ) -> (Mailbox<A::Message>, ActorHandle<ParallelActor<A>>) {
        let actor = self.actor.take().unwrap();
        let spawnable_actor = SpawnableSync(actor);
        self.spawn_parallel_actor(spawnable_actor, num_actors)
    }
}
