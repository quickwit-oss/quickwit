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

use std::cell::Cell;
use std::collections::HashMap;
use std::ops::Mul;
use std::time::Duration;

use async_trait::async_trait;
use quickwit_common::new_coolid;
use serde::Serialize;

use crate::observation::ObservationType;
use crate::{
    Actor, ActorContext, ActorExitStatus, ActorHandle, ActorState, Command, Handler, Health,
    Mailbox, Observation, Supervisable, Universe,
};

// An actor that receives ping messages.
#[derive(Default, Clone)]
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
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        self.ping_count += 1;
        assert_eq!(ctx.state(), ActorState::Running);
        Ok(())
    }
}

#[derive(Default)]
pub struct PingerSenderActor {
    count: usize,
    peers: HashMap<String, Mailbox<PingReceiverActor>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SenderState {
    pub count: usize,
    pub num_peers: usize,
}

#[derive(Clone, Debug)]
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
        for peer in self.peers.values() {
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
        let peer_id = peer.actor_instance_id().to_string();
        self.peers.insert(peer_id, peer);
        Ok(())
    }
}

#[tokio::test]
async fn test_actor_stops_when_last_mailbox_is_dropped() {
    quickwit_common::setup_logging_for_tests();
    let universe = Universe::with_accelerated_time();
    let (ping_recv_mailbox, ping_recv_handle) =
        universe.spawn_builder().spawn(PingReceiverActor::default());
    drop(ping_recv_mailbox);
    let (exit_status, _) = ping_recv_handle.join().await;
    assert!(exit_status.is_success());
}

#[tokio::test]
async fn test_ping_actor() {
    quickwit_common::setup_logging_for_tests();
    let universe = Universe::with_accelerated_time();
    let (ping_recv_mailbox, ping_recv_handle) =
        universe.spawn_builder().spawn(PingReceiverActor::default());
    let (ping_sender_mailbox, ping_sender_handle) =
        universe.spawn_builder().spawn(PingerSenderActor::default());
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
    assert!(
        ping_sender_mailbox
            .send_message(AddPeer(ping_recv_mailbox.clone()))
            .await
            .is_ok()
    );
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
    ping_sender_handle.join().await;
    assert!(ping_sender_mailbox.send_message(Ping).await.is_err());
}

struct BuggyActor;

#[derive(Clone, Debug)]
struct DoNothing;

#[derive(Clone, Debug)]
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
    let universe = Universe::with_accelerated_time();
    let (buggy_mailbox, buggy_handle) = universe.spawn_builder().spawn(BuggyActor);
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

    assert_eq!(buggy_handle.check_health(true), Health::Healthy);
    assert_eq!(
        buggy_handle.process_pending_and_observe().await.obs_type,
        ObservationType::Timeout
    );
    assert_eq!(buggy_handle.check_health(true), Health::Healthy);
    universe.sleep(crate::HEARTBEAT.mul(2)).await;
    assert_eq!(buggy_handle.check_health(true), Health::FailureOrUnhealthy);
    buggy_handle.kill().await;
}

#[tokio::test]
async fn test_pause_actor() {
    quickwit_common::setup_logging_for_tests();
    let universe = Universe::with_accelerated_time();
    let (ping_mailbox, ping_handle) = universe.spawn_builder().spawn(PingReceiverActor::default());
    for _ in 0u32..1000u32 {
        assert!(ping_mailbox.send_message(Ping).await.is_ok());
    }
    assert!(
        ping_mailbox
            .send_message_with_high_priority(Command::Pause)
            .is_ok()
    );
    let first_state = ping_handle.observe().await.state;
    assert!(first_state < 1000);
    let second_state = ping_handle.observe().await.state;
    assert_eq!(first_state, second_state);
    assert!(
        ping_mailbox
            .send_message_with_high_priority(Command::Resume)
            .is_ok()
    );
    let end_state = ping_handle.process_pending_and_observe().await.state;
    assert_eq!(end_state, 1000);
    universe.assert_quit().await;
}

#[tokio::test]
async fn test_actor_running_states() {
    quickwit_common::setup_logging_for_tests();
    let universe = Universe::with_accelerated_time();
    let (ping_mailbox, ping_handle) = universe.spawn_builder().spawn(PingReceiverActor::default());
    assert_eq!(ping_handle.state(), ActorState::Running);
    for _ in 0u32..10u32 {
        assert!(ping_mailbox.send_message(Ping).await.is_ok());
    }
    let obs = ping_handle.process_pending_and_observe().await;
    assert_eq!(*obs, 10);
    universe.sleep(Duration::from_millis(1)).await;
    assert_eq!(ping_handle.state(), ActorState::Running);
    universe.assert_quit().await;
}

#[derive(Clone, Debug, Default, Serialize)]
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

    fn yield_after_each_message(&self) -> bool {
        false
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

#[tokio::test(flavor = "multi_thread")]
async fn test_looping() -> anyhow::Result<()> {
    let universe = Universe::with_accelerated_time();
    let looping_actor = LoopingActor::default();
    let (looping_actor_mailbox, looping_actor_handle) =
        universe.spawn_builder().spawn(looping_actor);
    assert!(looping_actor_mailbox.send_message(SingleShot).await.is_ok());
    looping_actor_handle.process_pending_and_observe().await;
    let (exit_status, state) = looping_actor_handle.quit().await;
    assert!(matches!(exit_status, ActorExitStatus::Quit));
    assert_eq!(state.single_shot_count, 1);
    assert!(state.loop_count > 0);
    Ok(())
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
            .get_or_insert_with(|| ctx.spawn_actor().spawn(SummingActor::default()));
        ctx.send_message(mailbox, message).await?;
        Ok(())
    }
}

#[tokio::test]
async fn test_actor_spawning_actor() -> anyhow::Result<()> {
    let universe = Universe::with_accelerated_time();
    let (mailbox, handle) = universe.spawn_builder().spawn(SpawningActor::default());
    mailbox.send_message(1).await?;
    mailbox.send_message(2).await?;
    mailbox.send_message(3).await?;
    drop(mailbox);
    let (exit, result) = handle.join().await;
    assert!(matches!(exit, ActorExitStatus::Success));
    assert_eq!(result, 6);
    Ok(())
}

struct BuggyFinalizeActor;

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
        anyhow::bail!("finalize error")
    }
}

#[tokio::test]
async fn test_actor_finalize_error_set_exit_status_to_panicked() -> anyhow::Result<()> {
    let universe = Universe::with_accelerated_time();
    let (mailbox, handle) = universe.spawn_builder().spawn(BuggyFinalizeActor);
    assert!(matches!(handle.state(), ActorState::Running));
    drop(mailbox);
    let (exit, _) = handle.join().await;
    assert!(matches!(exit, ActorExitStatus::Panicked));
    Ok(())
}

#[derive(Default)]
struct Adder(u64);

impl Actor for Adder {
    type ObservableState = u64;

    fn yield_after_each_message(&self) -> bool {
        false
    }

    fn observable_state(&self) -> Self::ObservableState {
        self.0
    }
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
    let universe = Universe::with_accelerated_time();
    let adder = Adder::default();
    let (mailbox, _handle) = universe.spawn_builder().spawn(adder);
    let plus_two = mailbox.send_message(AddOperand(2)).await?;
    let plus_two_plus_four = mailbox.send_message(AddOperand(4)).await?;
    assert_eq!(plus_two.await.unwrap(), 2);
    assert_eq!(plus_two_plus_four.await.unwrap(), 6);
    universe.assert_quit().await;
    Ok(())
}

#[derive(Default)]
struct TestActorWithDrain {
    counts: ProcessAndDrainCounts,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
struct ProcessAndDrainCounts {
    process_calls_count: usize,
    drain_calls_count: usize,
}

#[async_trait]
impl Actor for TestActorWithDrain {
    type ObservableState = ProcessAndDrainCounts;

    fn observable_state(&self) -> ProcessAndDrainCounts {
        self.counts
    }

    async fn on_drained_messages(
        &mut self,
        _ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        self.counts.drain_calls_count += 1;
        Ok(())
    }
}

#[async_trait]
impl Handler<()> for TestActorWithDrain {
    type Reply = ();

    async fn handle(
        &mut self,
        _message: (),
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        self.counts.process_calls_count += 1;
        Ok(())
    }
}

#[tokio::test]
async fn test_drain_is_called() {
    quickwit_common::setup_logging_for_tests();
    let universe = Universe::with_accelerated_time();
    let test_actor_with_drain = TestActorWithDrain::default();
    let (mailbox, handle) = universe.spawn_builder().spawn(test_actor_with_drain);
    assert_eq!(
        *handle.process_pending_and_observe().await,
        ProcessAndDrainCounts {
            process_calls_count: 0,
            drain_calls_count: 0
        }
    );
    handle.pause();
    mailbox.send_message(()).await.unwrap();
    mailbox.send_message(()).await.unwrap();
    mailbox.send_message(()).await.unwrap();
    handle.resume();
    universe.sleep(Duration::from_millis(1)).await;
    assert_eq!(
        *handle.process_pending_and_observe().await,
        ProcessAndDrainCounts {
            process_calls_count: 3,
            drain_calls_count: 1
        }
    );
    mailbox.send_message(()).await.unwrap();
    universe.sleep(Duration::from_millis(1)).await;
    assert_eq!(
        *handle.process_pending_and_observe().await,
        ProcessAndDrainCounts {
            process_calls_count: 4,
            drain_calls_count: 2
        }
    );
    universe.assert_quit().await;
}

#[tokio::test]
async fn test_unsync_actor() {
    #[derive(Default)]
    struct UnsyncActor(Cell<u64>);

    impl Actor for UnsyncActor {
        type ObservableState = u64;

        fn observable_state(&self) -> Self::ObservableState {
            self.0.get()
        }
    }

    #[async_trait]
    impl Handler<u64> for UnsyncActor {
        type Reply = u64;

        async fn handle(
            &mut self,
            number: u64,
            _ctx: &ActorContext<Self>,
        ) -> Result<u64, ActorExitStatus> {
            *self.0.get_mut() += number;
            Ok(self.0.get())
        }
    }
    let universe = Universe::with_accelerated_time();
    let unsync_message_actor = UnsyncActor::default();
    let (mailbox, _handle) = universe.spawn_builder().spawn(unsync_message_actor);

    let response = mailbox.ask(1).await.unwrap();
    assert_eq!(response, 1);

    universe.assert_quit().await;
}

#[tokio::test]
async fn test_unsync_actor_message() {
    #[derive(Default)]
    struct UnsyncMessageActor(u64);

    impl Actor for UnsyncMessageActor {
        type ObservableState = u64;

        fn observable_state(&self) -> Self::ObservableState {
            self.0
        }
    }

    #[async_trait]
    impl Handler<Cell<u64>> for UnsyncMessageActor {
        type Reply = anyhow::Result<u64>;

        async fn handle(
            &mut self,
            number: Cell<u64>,
            _ctx: &ActorContext<Self>,
        ) -> Result<anyhow::Result<u64>, ActorExitStatus> {
            self.0 += number.get();
            Ok(Ok(self.0))
        }
    }
    let universe = Universe::with_accelerated_time();
    let unsync_message_actor = UnsyncMessageActor::default();
    let (mailbox, _handle) = universe.spawn_builder().spawn(unsync_message_actor);

    let response_rx = mailbox.send_message(Cell::new(1)).await.unwrap();
    assert_eq!(response_rx.await.unwrap().unwrap(), 1);

    let response = mailbox.ask(Cell::new(1)).await.unwrap().unwrap();
    assert_eq!(response, 2);

    let response = mailbox.ask_for_res(Cell::new(1)).await.unwrap();
    assert_eq!(response, 3);

    let response_rx = mailbox
        .send_message_with_high_priority(Cell::new(1))
        .unwrap();
    assert_eq!(response_rx.await.unwrap().unwrap(), 4);

    let response_rx = mailbox.try_send_message(Cell::new(1)).unwrap();
    assert_eq!(response_rx.await.unwrap().unwrap(), 5);

    universe.assert_quit().await;
}

struct FakeActorService {
    // We use a cool id to make sure in the test that we get twice the same instance.
    cool_id: String,
}

#[derive(Debug)]
struct GetCoolId;

impl Actor for FakeActorService {
    type ObservableState = ();

    fn observable_state(&self) {}
}

#[async_trait]
impl Handler<GetCoolId> for FakeActorService {
    type Reply = String;

    async fn handle(
        &mut self,
        _: GetCoolId,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(self.cool_id.clone())
    }
}

impl Default for FakeActorService {
    fn default() -> Self {
        FakeActorService {
            cool_id: new_coolid("fake-actor"),
        }
    }
}

#[tokio::test]
async fn test_get_or_spawn() {
    let universe = Universe::new();
    let mailbox1: Mailbox<FakeActorService> = universe.get_or_spawn_one();
    let id1 = mailbox1.ask(GetCoolId).await.unwrap();
    let mailbox2: Mailbox<FakeActorService> = universe.get_or_spawn_one();
    let id2 = mailbox2.ask(GetCoolId).await.unwrap();
    assert_eq!(id1, id2);
    universe.assert_quit().await;
}
