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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use tokio::sync::watch;
use tokio::time::interval;
use tracing::warn;

use crate::channel_with_priority::Priority;
use crate::mailbox::Command;
use crate::progress::Progress;
use crate::{
    Actor, ActorContext, ActorExitStatus, ActorHandle, AsyncActor, Mailbox, QueueCapacity,
    HEARTBEAT,
};

pub type ParallelActorHandle<A> = ActorHandle<ParallelActor<A>>;

/// The parallel actor acts as a supervisor for several
/// underlying actor instances we call `worker`.
///
/// It tries as much as possible to create the illusion that
/// clients are interacting with a single unified worker, but fails
/// to do so in different places.
///
/// For instance, the parallel actor is in charge of detecting if the
/// worker actors are all progressing or not.
/// If one of them is not progressing, regardless of the reason, including them having returned
/// success, the supervision will have their kill switch activated.
///
/// Sending the success command however, behaves as expected.
pub struct ParallelActor<A: Actor> {
    worker_handles: Vec<ActorHandle<A>>,
    worker_mailboxes: Vec<Mailbox<A::Message>>,
    actor_name: String,
    worker_observations: Vec<A::ObservableState>,
    is_alive_opt: Option<PairIsAlive>, //< This is used to stop the worker supervising task.
}

/// This is a weird boolean flag that works in pair of instance. It switches to false
/// automatically if one of the two instance is dropped.
struct PairIsAlive(Arc<AtomicBool>);

impl PairIsAlive {
    fn is_true(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

impl Drop for PairIsAlive {
    fn drop(&mut self) {
        self.0.store(false, std::sync::atomic::Ordering::Release);
    }
}

fn create_pair_is_alive() -> (PairIsAlive, PairIsAlive) {
    let keep_on_flag = Arc::new(AtomicBool::new(true));
    let left = PairIsAlive(keep_on_flag.clone());
    let right = PairIsAlive(keep_on_flag);
    (left, right)
}

/// Clone any command (except Observe which is not cloneable due to
/// the oneshot callback).
fn try_command_clone(command: &Command) -> Option<Command> {
    match command {
        Command::Pause => Some(Command::Pause),
        Command::Resume => Some(Command::Resume),
        Command::Success => Some(Command::Success),
        Command::Quit => Some(Command::Quit),
        Command::Kill => Some(Command::Kill),
        Command::Observe(_) => None,
    }
}

impl<A: Actor> ParallelActor<A> {
    pub fn new(
        actor_name: String,
        worker_handles: Vec<ActorHandle<A>>,
        worker_mailboxes: Vec<Mailbox<A::Message>>,
    ) -> Self {
        assert!(!worker_handles.is_empty());
        let worker_observations = worker_handles
            .iter()
            .map(|worker_handle| worker_handle.last_observation())
            .collect();
        ParallelActor {
            worker_handles,
            worker_mailboxes,
            actor_name,
            worker_observations,
            is_alive_opt: None,
        }
    }

    // We put `mut self` to insure that this flush happens on the same task
    // as the execution of process_message, because it is ensure that it's
    // correct usage: No messages will extra message can be concurrently added
    // to the worker queue.
    async fn process_pending_message(
        &mut self,
        ctx: &ActorContext<A::Message>,
    ) -> Result<(), ActorExitStatus> {
        // We send a single first observation message with low priority,
        // in order to flush the existing messages.
        let (tx, rx) = tokio::sync::oneshot::channel();
        let obs_msg = Command::Observe(tx);
        let _guard = ctx.protect_zone();
        self.worker_mailboxes[0]
            .send_with_priority(obs_msg.into(), Priority::Low)
            .await?;
        rx.await.context("Worker observation failed")?;
        Ok(())
    }
}

impl<A: Actor> Actor for ParallelActor<A> {
    type Message = A::Message;

    type ObservableState = Vec<A::ObservableState>;

    fn observable_state(&self) -> Self::ObservableState {
        self.worker_observations.clone()
    }

    fn name(&self) -> String {
        format!("parallel_{}", self.actor_name)
    }

    fn queue_capacity(&self) -> crate::QueueCapacity {
        QueueCapacity::Bounded(1)
    }
}

#[async_trait]
impl<A: Actor> AsyncActor for ParallelActor<A> {
    async fn initialize(
        &mut self,
        ctx: &ActorContext<Self::Message>,
    ) -> Result<(), ActorExitStatus> {
        let actor_progresses: Vec<Progress> = self
            .worker_handles
            .iter()
            .map(|handle| handle.progress().clone())
            .collect();
        let (left_is_alive, right_is_alive) = create_pair_is_alive();
        self.is_alive_opt = Some(left_is_alive);
        let kill_switch = ctx.kill_switch().clone();
        tokio::spawn(async move {
            let mut interval = interval(HEARTBEAT);
            while right_is_alive.is_true() {
                interval.tick().await;
                for progress in &actor_progresses {
                    if !progress.registered_activity_since_last_call() {
                        if right_is_alive.is_true() {
                            warn!("no progress from worker, killing.");
                            kill_switch.kill();
                        }
                        return;
                    }
                }
            }
        });
        Ok(())
    }

    async fn process_message(
        &mut self,
        message: Self::Message,
        ctx: &ActorContext<Self::Message>,
    ) -> Result<(), ActorExitStatus> {
        if let Some(is_alive) = self.is_alive_opt.as_ref() {
            if !is_alive.is_true() {
                let err = Arc::new(anyhow::anyhow!("One of the worker failed"));
                return Err(ActorExitStatus::Failure(err));
            }
        }
        ctx.send_message(&self.worker_mailboxes[0], message).await?;
        Ok(())
    }

    async fn process_command(
        &mut self,
        command: Command,
        priority: Priority,
        ctx: &mut ActorContext<Self::Message>,
        state_tx: &watch::Sender<Self::ObservableState>,
    ) -> Result<(), ActorExitStatus> {
        match command {
            cmd
            @
            (Command::Success
            | Command::Pause
            | Command::Resume
            | Command::Quit
            | Command::Kill) => {
                for mailbox in &self.worker_mailboxes {
                    if let Some(cmd_clone) = try_command_clone(&cmd) {
                        mailbox.send_command(cmd_clone).await?;
                    }
                }
                match cmd {
                    Command::Pause | Command::Resume => Ok(()),
                    Command::Success => Err(ActorExitStatus::Success),
                    Command::Quit => Err(ActorExitStatus::Quit),
                    Command::Kill => Err(ActorExitStatus::Killed),
                    Command::Observe(_) => {
                        unreachable!("Cannot happen because of the pattern matching above");
                    }
                }
            }
            Command::Observe(cb) => {
                let mut rxs = Vec::new();
                if priority == Priority::Low {
                    // We send a single first observation message with low priority,
                    // in order to flush the existing messages.
                    self.process_pending_message(ctx).await?;
                }
                for mailbox in &self.worker_mailboxes {
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    mailbox.send_command(Command::Observe(tx)).await?;
                    rxs.push(rx);
                }
                let observations_any = {
                    let _protect_guard = ctx.protect_zone();
                    futures::future::try_join_all(rxs)
                        .await
                        .context("Failed to observe child task")?
                };
                let observations: Vec<A::ObservableState> = observations_any
                    .into_iter()
                    .map(|obs_any| {
                        let obs: A::ObservableState = *obs_any
                            .downcast()
                            .expect("The type is guaranteed logically by the ActorHandle.");
                        obs
                    })
                    .collect::<Vec<A::ObservableState>>();
                // This is not an error if the receiver oneshot was dropped.
                let _ = cb.send(Box::new(observations.clone()));
                state_tx
                    .send(observations)
                    .context("Observation task failed")?;
                Ok(())
            }
        }
    }

    async fn finalize(
        &mut self,
        exit_status: &ActorExitStatus,
        ctx: &ActorContext<Self::Message>,
    ) -> anyhow::Result<()> {
        // Time to quit. If we quit because it was detected that there would
        // be no more incoming message, we want to process
        // pending message (in the worker queue) before effectively quitting.
        #[allow(clippy::collapsible_if)]
        if exit_status.is_success() {
            if self.process_pending_message(ctx).await.is_err() {
                ctx.kill_switch().kill();
            }
        }
        let wait_for_worker_futures: Vec<_> = self
            .worker_handles
            .drain(..)
            .map(|handle| handle.quit())
            .collect();
        let wait_for_workers: Vec<(ActorExitStatus, A::ObservableState)> =
            futures::future::join_all(wait_for_worker_futures).await;
        self.worker_observations.clear();
        for (_worker_exit_status, worker_exit_state) in wait_for_workers {
            self.worker_observations.push(worker_exit_state);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::mem;
    use std::time::Duration;

    use async_trait::async_trait;

    use crate::parallel_actors::{create_pair_is_alive, PairIsAlive};
    use crate::{Actor, ActorExitStatus, AsyncActor, ObservationType, Universe};

    #[derive(Debug)]
    enum CountingActorMsg {
        Inc,
        Panic,
    }

    #[derive(Default, Clone)]
    struct CountingActor {
        count: usize,
    }

    impl Actor for CountingActor {
        type Message = CountingActorMsg;

        type ObservableState = usize;

        fn observable_state(&self) -> Self::ObservableState {
            self.count
        }
    }

    #[async_trait]
    impl AsyncActor for CountingActor {
        async fn process_message(
            &mut self,
            message: Self::Message,
            _ctx: &crate::ActorContext<Self::Message>,
        ) -> Result<(), crate::ActorExitStatus> {
            match message {
                CountingActorMsg::Inc => {
                    self.count += 1;
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                CountingActorMsg::Panic => {
                    panic!("count");
                }
            }
            Ok(())
        }
    }

    fn test_pair_is_alive_aux(left: PairIsAlive, right: PairIsAlive) {
        assert!(left.is_true());
        mem::drop(right);
        assert!(!left.is_true());
    }

    #[test]
    fn test_stop_on_drop() {
        {
            let (left, right) = create_pair_is_alive();
            test_pair_is_alive_aux(left, right);
        }
        {
            let (left, right) = create_pair_is_alive();
            test_pair_is_alive_aux(right, left);
        }
    }

    #[tokio::test]
    async fn test_parallel_actors_wait_and_observe() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::new();
        let counting_actor = CountingActor::default();
        let (mailbox, handle) = universe.spawn_actor(counting_actor).spawn_parallel_async(3);
        for _ in 0..10 {
            universe
                .send_message(&mailbox, CountingActorMsg::Inc)
                .await?;
        }
        let counts = handle.process_pending_and_observe().await;
        assert_eq!(counts.len(), 3);
        assert_eq!(counts.state.iter().cloned().sum::<usize>(), 10);
        assert_eq!(counts.obs_type, ObservationType::Alive);
        assert!(counts.state.iter().cloned().all(|count| count > 0));
        Ok(())
    }

    #[tokio::test]
    async fn test_parallel_actors_process_pending_and_observe() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::new();
        let counting_actor = CountingActor::default();
        let (mailbox, handle) = universe.spawn_actor(counting_actor).spawn_parallel_async(3);
        for _ in 0..10 {
            universe
                .send_message(&mailbox, CountingActorMsg::Inc)
                .await?;
        }
        for _ in 0..3 {
            tokio::task::yield_now().await;
        }
        let counts = handle.observe().await;
        assert_eq!(counts.len(), 3);
        assert_eq!(counts.state.iter().cloned().sum::<usize>(), 3);
        assert_eq!(counts.obs_type, ObservationType::Alive);
        assert!(counts.state.iter().cloned().all(|count| count == 1));
        Ok(())
    }

    #[tokio::test]
    async fn test_parallel_actors_join_should_process_all_pending_message() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::new();
        let counting_actor = CountingActor::default();
        let (mailbox, handle) = universe.spawn_actor(counting_actor).spawn_parallel_async(3);
        for _ in 0..10 {
            universe
                .send_message(&mailbox, CountingActorMsg::Inc)
                .await?;
        }
        mem::drop(mailbox);
        let (exit_status, counts) = handle.join().await;
        assert_eq!(counts.len(), 3);
        assert_eq!(counts.iter().cloned().sum::<usize>(), 10);
        assert!(matches!(exit_status, ActorExitStatus::Success));
        Ok(())
    }

    #[tokio::test]
    async fn test_parallel_actors_single_failing_workers() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::new();
        let counting_actor = CountingActor::default();
        let (mailbox, handle) = universe.spawn_actor(counting_actor).spawn_parallel_async(3);
        for _ in 0..3 {
            universe
                .send_message(&mailbox, CountingActorMsg::Inc)
                .await?;
        }
        universe
            .send_message(&mailbox, CountingActorMsg::Panic)
            .await?;
        for _ in 0..7 {
            universe
                .send_message(&mailbox, CountingActorMsg::Inc)
                .await?;
        }
        let (exit_status, counts) = handle.join().await;
        assert_eq!(counts.len(), 3);
        assert_eq!(counts.iter().cloned().sum::<usize>(), 10);
        assert!(matches!(exit_status, ActorExitStatus::Killed));
        Ok(())
    }
}
