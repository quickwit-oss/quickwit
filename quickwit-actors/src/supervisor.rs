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

use async_trait::async_trait;
use tracing::error;

use crate::scheduler::Scheduler;
use crate::spawn_builder::SpawnBuilder;
use crate::{
    Actor, ActorContext, ActorExitStatus, ActorHandle, Handler, KillSwitch, Mailbox, Supervisable,
    HEARTBEAT,
};

#[derive(Debug, Clone, Default)]
pub struct SupervisorState {
    pub num_failure: usize,
}

pub struct Supervisor<A: Actor> {
    create_actor_fn: Box<dyn FnMut() -> anyhow::Result<A> + Sync + Send>,
    actor_handle: ActorHandle<A>,
    state: SupervisorState,
}

impl<A: Actor> Supervisor<A> {
    pub fn new(
        create_actor_fn: Box<dyn FnMut() -> anyhow::Result<A> + Sync + Send>,
        actor_handle: ActorHandle<A>,
    ) -> Self {
        Self {
            create_actor_fn,
            actor_handle,
            state: SupervisorState::default(),
        }
    }

    async fn spawn_actor_if_exited(&mut self, ctx: &ActorContext<Self>) -> anyhow::Result<()> {
        if self.actor_handle.state().is_running() {
            return Ok(());
        }
        self.state.num_failure += 1;
        let new_actor = (self.create_actor_fn)()?;
        let (_, new_handle) = ctx
            .spawn_actor(new_actor)
            .set_mailboxes(
                self.actor_handle.mailbox().clone(),
                self.actor_handle.inbox().clone(),
            )
            .set_kill_switch(KillSwitch::default())
            .spawn();
        self.actor_handle = new_handle;
        Ok(())
    }
}

#[async_trait]
impl<A: Actor> Actor for Supervisor<A> {
    type ObservableState = SupervisorState;

    fn observable_state(&self) -> Self::ObservableState {
        self.state.clone()
    }

    fn name(&self) -> String {
        format!("Supervisor<{}>", self.actor_handle.name())
    }

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        let _ = ctx.send_self_message(SupervisorLoop {}).await;
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct SupervisorLoop {}

#[async_trait]
impl<A: Actor> Handler<SupervisorLoop> for Supervisor<A> {
    type Reply = ();

    async fn handle(
        &mut self,
        _: SupervisorLoop,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        if let Err(error) = self.spawn_actor_if_exited(ctx).await {
            error!("Error when spawning actor: {}", error);
        }
        ctx.schedule_self_msg(HEARTBEAT, SupervisorLoop {}).await;
        Ok(())
    }
}

pub(crate) fn spawn_supervised_actor<A: Actor>(
    scheduler_mailbox: Mailbox<Scheduler>,
    kill_switch: KillSwitch,
    mut create_actor_fn: Box<dyn FnMut() -> anyhow::Result<A> + Sync + Send>,
) -> anyhow::Result<(Mailbox<A>, ActorHandle<Supervisor<A>>)> {
    let actor = create_actor_fn()?;
    let (actor_mailbox, actor_handle) =
        SpawnBuilder::new(actor, scheduler_mailbox.clone(), KillSwitch::default()).spawn();
    let supervisor = Supervisor::new(create_actor_fn, actor_handle);
    let (_, supervisor_handle) =
        SpawnBuilder::new(supervisor, scheduler_mailbox.clone(), kill_switch.clone()).spawn();
    Ok((actor_mailbox, supervisor_handle))
}
