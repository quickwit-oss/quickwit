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

use std::collections::HashSet;

use async_trait::async_trait;

use crate::mailbox::Command;
use crate::observation::ObservationType;
use crate::{
    message_timeout, Actor, ActorContext, ActorExitStatus, ActorHandle, ActorRunner, ActorState,
    Handler, Health, Mailbox, Observation, Supervisable, Universe,
};

// An actor that receives ping messages.
#[derive(Default)]
pub struct PingReceiverActor {
    ping_count: usize,
}

impl Actor for PingReceiverActor {
    type ObservableState = usize;

    fn name(&self) -> String {
        "Ping".to_string()
    }

    fn observable_state(&self) -> Self::ObservableState {
        self.ping_count
    }
}

#[derive(Debug)]
pub struct Ping;

#[async_trait]
impl Handler<Ping> for PingReceiverActor {
    type Reply = ();

    async fn handle(
        &mut self,
        _message: Ping,
        _ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        self.ping_count += 1;
        Ok(())
    }
}

#[derive(Default)]
pub struct PingerSenderActor {
    count: usize,
    peers: HashSet<Mailbox<PingReceiverActor>>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SenderState {
    pub count: usize,
    pub num_peers: usize,
}

#[derive(Debug, Clone)]
pub struct AddPeer(Mailbox<PingReceiverActor>);

impl Actor for PingerSenderActor {
    type ObservableState = SenderState;

    fn name(&self) -> String {
        "PingSender".to_string()
    }

    fn observable_state(&self) -> Self::ObservableState {
        SenderState {
            count: self.count,
            num_peers: self.peers.len(),
        }
    }
}

#[async_trait]
impl Handler<Ping> for PingerSenderActor {
    type Reply = ();

    async fn handle(
        &mut self,
        _message: Ping,
        _ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        self.count += 1;
        for peer in &self.peers {
            let _ = peer.send_message(Ping).await;
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<AddPeer> for PingerSenderActor {
    type Reply = ();

    async fn handle(
        &mut self,
        message: AddPeer,
        _ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        let AddPeer(peer) = message;
        self.peers.insert(peer);
        Ok(())
    }
}

#[tokio::test]
async fn test_ping_actor() {
    quickwit_common::setup_logging_for_tests();
    let universe = Universe::new();
    let (ping_recv_mailbox, ping_recv_handle) =
        universe.spawn_actor(PingReceiverActor::default()).spawn();
    let (ping_sender_mailbox, ping_sender_handle) =
        universe.spawn_actor(PingerSenderActor::default()).spawn();
    assert_eq!(
        ping_recv_handle.observe().await,
        Observation {
            obs_type: ObservationType::Alive,
            state: 0
        }
    );
    // No peers. This one will have no impact.
    let ping_recv_mailbox = ping_recv_mailbox.clone();
    assert!(ping_sender_mailbox.send_message(Ping).await.is_ok());
    assert!(ping_sender_mailbox
        .send_message(AddPeer(ping_recv_mailbox.clone()))
        .await
        .is_ok());
    assert_eq!(
        ping_sender_handle.process_pending_and_observe().await,
        Observation {
            obs_type: ObservationType::Alive,
            state: SenderState {
                num_peers: 1,
                count: 1
            }
        }
    );
    assert!(ping_sender_mailbox.send_message(Ping).await.is_ok());
    assert!(ping_sender_mailbox.send_message(Ping).await.is_ok());
    assert_eq!(
        ping_sender_handle.process_pending_and_observe().await,
        Observation {
            obs_type: ObservationType::Alive,
            state: SenderState {
                num_peers: 1,
                count: 3
            }
        }
    );
    assert_eq!(
        ping_recv_handle.process_pending_and_observe().await,
        Observation {
            obs_type: ObservationType::Alive,
            state: 2
        }
    );
    universe.kill();
    assert_eq!(
        ping_recv_handle.process_pending_and_observe().await,
        Observation {
            obs_type: ObservationType::PostMortem,
            state: 2
        }
    );
    assert_eq!(
        ping_sender_handle.process_pending_and_observe().await,
        Observation {
            obs_type: ObservationType::PostMortem,
            state: SenderState {
                num_peers: 1,
                count: 3
            }
        }
    );
    assert!(ping_sender_mailbox.send_message(Ping).await.is_err());
}

struct BuggyActor;

#[derive(Debug, Clone)]
struct DoNothing;

#[derive(Debug, Clone)]
struct Block;

impl Actor for BuggyActor {
    type ObservableState = ();

    fn name(&self) -> String {
        "BuggyActor".to_string()
    }

    fn observable_state(&self) {}
}

#[async_trait]
impl Handler<DoNothing> for BuggyActor {
    type Reply = ();

    async fn handle(
        &mut self,
        _message: DoNothing,
        _ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        Ok(())
    }
}

#[async_trait]
impl Handler<Block> for BuggyActor {
    type Reply = ();

    async fn handle(
        &mut self,
        _message: Block,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        while ctx.kill_switch().is_alive() {
            tokio::task::yield_now().await;
        }
        Ok(())
    }
}

#[tokio::test]
async fn test_timeouting_actor() {
    let universe = Universe::new();
    let (buggy_mailbox, buggy_handle) = universe.spawn_actor(BuggyActor).spawn();
    let buggy_mailbox = buggy_mailbox;
    assert_eq!(
        buggy_handle.observe().await.obs_type,
        ObservationType::Alive
    );
    assert!(buggy_mailbox.send_message(DoNothing).await.is_ok());
    assert_eq!(
        buggy_handle.observe().await.obs_type,
        ObservationType::Alive
    );
    assert!(buggy_mailbox.send_message(Block).await.is_ok());

    assert_eq!(buggy_handle.health(), Health::Healthy);
    assert_eq!(
        buggy_handle.process_pending_and_observe().await.obs_type,
        ObservationType::Timeout
    );
    assert_eq!(buggy_handle.health(), Health::Healthy);
    tokio::time::sleep(crate::HEARTBEAT).await;
    tokio::time::sleep(crate::HEARTBEAT).await;
    assert_eq!(buggy_handle.health(), Health::FailureOrUnhealthy);
}

#[tokio::test]
async fn test_sync_actor_running_states() {
    quickwit_common::setup_logging_for_tests();
    let universe = Universe::new();
    let actor = PingReceiverActor::default();
    let (ping_mailbox, ping_handle) = universe.spawn_actor(actor).spawn();
    assert!(ping_handle.state() == ActorState::Processing);
    for _ in 0..10 {
        assert!(ping_mailbox.send_message(Ping).await.is_ok());
    }
    assert!(ping_handle.state() == ActorState::Processing);
    ping_handle.process_pending_and_observe().await;
    // Actor is still in processing state and will go idle after message timeout.
    assert!(ping_handle.state() == ActorState::Processing);
    tokio::time::sleep(message_timeout().mul_f32(1.1)).await;
    assert!(ping_handle.state() == ActorState::Idle);
    assert!(ping_mailbox.send_command(Command::Resume).await.is_ok());
    // Return into processing on sending a command and go back to idle after message timeout.
    tokio::time::sleep(message_timeout()).await;
    assert!(ping_handle.state() == ActorState::Processing);
    tokio::time::sleep(message_timeout()).await;
    assert!(ping_handle.state() == ActorState::Idle);
}

#[tokio::test]
async fn test_pause_actor() {
    quickwit_common::setup_logging_for_tests();
    let universe = Universe::new();
    let (ping_mailbox, ping_handle) = universe.spawn_actor(PingReceiverActor::default()).spawn();
    for _ in 0u32..1000u32 {
        assert!(ping_mailbox.send_message(Ping).await.is_ok());
    }
    assert!(ping_mailbox.send_command(Command::Pause).await.is_ok());
    let first_state = ping_handle.observe().await.state;
    assert!(first_state < 1000);
    let second_state = ping_handle.observe().await.state;
    assert_eq!(first_state, second_state);
    assert!(ping_mailbox.send_command(Command::Resume).await.is_ok());
    let end_state = ping_handle.process_pending_and_observe().await.state;
    assert_eq!(end_state, 1000);
}

#[tokio::test]
async fn test_actor_running_states() {
    quickwit_common::setup_logging_for_tests();
    let universe = Universe::new();
    let (ping_mailbox, ping_handle) = universe.spawn_actor(PingReceiverActor::default()).spawn();
    assert!(ping_handle.state() == ActorState::Processing);
    for _ in 0u32..10u32 {
        assert!(ping_mailbox.send_message(Ping).await.is_ok());
    }
    // Actor is still in processing state and will go idle after message timeout.
    assert!(ping_handle.state() == ActorState::Processing);
    tokio::time::sleep(message_timeout().mul_f32(1.1)).await;
    assert!(ping_handle.state() == ActorState::Idle);
    assert!(ping_mailbox.send_command(Command::Resume).await.is_ok());
    // Return into processing on sending a command and go back to idle after message timeout.
    tokio::time::sleep(message_timeout()).await;
    assert!(ping_handle.state() == ActorState::Processing);
    tokio::time::sleep(message_timeout()).await;
    assert!(ping_handle.state() == ActorState::Idle);
}

#[derive(Default, Debug, Clone)]
struct LoopingActor {
    pub loop_count: usize,
    pub single_shot_count: usize,
}

#[derive(Debug)]
struct Loop;

#[derive(Debug)]
struct SingleShot;

#[async_trait]
impl Actor for LoopingActor {
    type ObservableState = Self;

    fn observable_state(&self) -> Self::ObservableState {
        self.clone()
    }

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        self.handle(Loop, ctx).await
    }
}

#[async_trait]
impl Handler<Loop> for LoopingActor {
    type Reply = ();
    async fn handle(
        &mut self,
        _msg: Loop,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        self.loop_count += 1;
        ctx.send_self_message(Loop).await?;
        Ok(())
    }
}

#[async_trait]
impl Handler<SingleShot> for LoopingActor {
    type Reply = ();

    async fn handle(
        &mut self,
        _msg: SingleShot,
        _ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        self.single_shot_count += 1;
        Ok(())
    }
}

#[track_caller]
async fn test_looping_aux(runner: ActorRunner) -> anyhow::Result<()> {
    let universe = Universe::new();
    let looping_actor = LoopingActor::default();
    let (looping_actor_mailbox, looping_actor_handle) = universe
        .spawn_actor(looping_actor)
        .spawn_with_forced_runner(runner);
    assert!(looping_actor_mailbox.send_message(SingleShot).await.is_ok());
    looping_actor_handle.process_pending_and_observe().await;
    let (exit_status, state) = looping_actor_handle.quit().await;
    assert!(matches!(exit_status, ActorExitStatus::Quit));
    assert_eq!(state.single_shot_count, 1);
    assert!(state.loop_count > 0);
    Ok(())
}

#[tokio::test]
async fn test_looping_tokio_task() -> anyhow::Result<()> {
    test_looping_aux(ActorRunner::GlobalRuntime).await
}

#[tokio::test]
async fn test_looping_dedicated_thread() -> anyhow::Result<()> {
    test_looping_aux(ActorRunner::DedicatedThread).await
}

#[derive(Default)]
struct SummingActor {
    sum: u64,
}

#[async_trait]
impl Handler<u64> for SummingActor {
    type Reply = ();

    async fn handle(&mut self, add: u64, _ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        self.sum += add;
        Ok(())
    }
}

impl Actor for SummingActor {
    type ObservableState = u64;

    fn observable_state(&self) -> Self::ObservableState {
        self.sum
    }
}

#[derive(Default)]
struct SpawningActor {
    res: u64,
    handle_opt: Option<(Mailbox<SummingActor>, ActorHandle<SummingActor>)>,
}

#[async_trait]
impl Actor for SpawningActor {
    type ObservableState = u64;

    fn observable_state(&self) -> Self::ObservableState {
        self.res
    }

    async fn finalize(
        &mut self,
        _exit_status: &ActorExitStatus,
        _ctx: &ActorContext<Self>,
    ) -> anyhow::Result<()> {
        if let Some((_, child_handler)) = self.handle_opt.take() {
            self.res = child_handler.process_pending_and_observe().await.state;
            child_handler.kill().await;
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<u64> for SpawningActor {
    type Reply = ();

    async fn handle(
        &mut self,
        message: u64,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        let (mailbox, _) = self
            .handle_opt
            .get_or_insert_with(|| ctx.spawn_actor(SummingActor::default()).spawn());
        ctx.send_message(mailbox, message).await?;
        Ok(())
    }
}

#[tokio::test]
async fn test_actor_spawning_actor() -> anyhow::Result<()> {
    let universe = Universe::new();
    let (mailbox, handle) = universe.spawn_actor(SpawningActor::default()).spawn();
    mailbox.send_message(1).await?;
    mailbox.send_message(2).await?;
    mailbox.send_message(3).await?;
    drop(mailbox);
    let (exit, result) = handle.join().await;
    assert!(matches!(exit, ActorExitStatus::Success));
    assert_eq!(result, 6);
    Ok(())
}

struct BuggyFinalizeActor(ActorRunner);

#[async_trait]
impl Actor for BuggyFinalizeActor {
    type ObservableState = ();

    fn name(&self) -> String {
        "BuggyFinalizeActor".to_string()
    }

    fn observable_state(&self) {}

    async fn finalize(
        &mut self,
        _exit_status: &ActorExitStatus,
        _: &ActorContext<Self>,
    ) -> anyhow::Result<()> {
        anyhow::bail!("Finalize error")
    }
}

#[track_caller]
async fn test_actor_finalize_error_set_exit_status_to_panicked_aux(
    actor_runner: ActorRunner,
) -> anyhow::Result<()> {
    let universe = Universe::new();
    let (mailbox, handle) = universe
        .spawn_actor(BuggyFinalizeActor(actor_runner))
        .spawn();
    assert!(matches!(handle.state(), ActorState::Processing));
    drop(mailbox);
    let (exit, _) = handle.join().await;
    assert!(matches!(exit, ActorExitStatus::Panicked));
    Ok(())
}

#[tokio::test]
async fn test_actor_finalize_error_set_exit_status_to_panicked_tokio_task() -> anyhow::Result<()> {
    test_actor_finalize_error_set_exit_status_to_panicked_aux(ActorRunner::GlobalRuntime).await
}

#[tokio::test]
async fn test_actor_finalize_error_set_exit_status_to_panicked_dedicated_thread(
) -> anyhow::Result<()> {
    test_actor_finalize_error_set_exit_status_to_panicked_aux(ActorRunner::DedicatedThread).await
}

#[derive(Default)]
struct Adder(u64);

impl Actor for Adder {
    type ObservableState = ();

    fn observable_state(&self) -> Self::ObservableState {}
}

#[derive(Debug)]
struct AddOperand(u64);

#[async_trait]
impl Handler<AddOperand> for Adder {
    type Reply = u64;

    async fn handle(
        &mut self,
        add_op: AddOperand,
        _ctx: &ActorContext<Self>,
    ) -> Result<u64, ActorExitStatus> {
        self.0 += add_op.0;
        Ok(self.0)
    }
}

#[tokio::test]
async fn test_actor_return_response() -> anyhow::Result<()> {
    let universe = Universe::new();
    let adder = Adder::default();
    let (mailbox, _handle) = universe.spawn_actor(adder).spawn();
    let plus_two = mailbox.send_message(AddOperand(2)).await?;
    let plus_two_plus_four = mailbox.send_message(AddOperand(4)).await?;
    assert_eq!(plus_two.await.unwrap(), 2);
    assert_eq!(plus_two_plus_four.await.unwrap(), 6);
    Ok(())
}
