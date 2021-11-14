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

use std::collections::HashSet;

use async_trait::async_trait;

use crate::mailbox::Command;
use crate::observation::ObservationType;
use crate::{
    message_timeout, Actor, ActorContext, ActorExitStatus, ActorHandle, ActorState, AsyncActor,
    Health, Mailbox, Observation, Supervisable, SyncActor, Universe,
};

// An actor that receives ping messages.
#[derive(Default)]
pub struct PingReceiverSyncActor {
    ping_count: usize,
}

#[derive(Debug, Clone)]
pub struct Ping;

impl Actor for PingReceiverSyncActor {
    type Message = Ping;

    type ObservableState = usize;

    fn name(&self) -> String {
        "Ping".to_string()
    }

    fn observable_state(&self) -> Self::ObservableState {
        self.ping_count
    }
}

impl SyncActor for PingReceiverSyncActor {
    fn process_message(
        &mut self,
        _message: Self::Message,
        _ctx: &ActorContext<Self::Message>,
    ) -> Result<(), ActorExitStatus> {
        self.ping_count += 1;
        Ok(())
    }
}

// An actor that receives ping messages.
#[derive(Default)]
pub struct PingReceiverAsyncActor {
    ping_count: usize,
}

impl Actor for PingReceiverAsyncActor {
    type Message = Ping;

    type ObservableState = usize;

    fn name(&self) -> String {
        "Ping".to_string()
    }

    fn observable_state(&self) -> Self::ObservableState {
        self.ping_count
    }
}

#[async_trait]
impl AsyncActor for PingReceiverAsyncActor {
    async fn process_message(
        &mut self,
        _message: Self::Message,
        _progress: &ActorContext<Self::Message>,
    ) -> Result<(), ActorExitStatus> {
        self.ping_count += 1;
        Ok(())
    }
}
#[derive(Default)]
pub struct PingerAsyncSenderActor {
    count: usize,
    peers: HashSet<Mailbox<<PingReceiverSyncActor as Actor>::Message>>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SenderState {
    pub count: usize,
    pub num_peers: usize,
}

#[derive(Debug, Clone)]
pub enum SenderMessage {
    AddPeer(Mailbox<Ping>),
    Ping,
}

impl Actor for PingerAsyncSenderActor {
    type Message = SenderMessage;
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
impl AsyncActor for PingerAsyncSenderActor {
    async fn process_message(
        &mut self,
        message: SenderMessage,
        _ctx: &ActorContext<Self::Message>,
    ) -> Result<(), ActorExitStatus> {
        match message {
            SenderMessage::AddPeer(peer) => {
                self.peers.insert(peer);
            }
            SenderMessage::Ping => {
                self.count += 1;
                for peer in &self.peers {
                    let _ = peer.send_message(Ping).await;
                }
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn test_ping_actor() {
    quickwit_common::setup_logging_for_tests();
    let universe = Universe::new();
    let (ping_recv_mailbox, ping_recv_handle) = universe
        .spawn_actor(PingReceiverSyncActor::default())
        .spawn_sync();
    let (ping_sender_mailbox, ping_sender_handle) = universe
        .spawn_actor(PingerAsyncSenderActor::default())
        .spawn_async();
    assert_eq!(
        ping_recv_handle.observe().await,
        Observation {
            obs_type: ObservationType::Alive,
            state: 0
        }
    );
    // No peers. This one will have no impact.
    let ping_recv_mailbox = ping_recv_mailbox.clone();
    assert!(ping_sender_mailbox
        .send_message(SenderMessage::Ping)
        .await
        .is_ok());
    assert!(ping_sender_mailbox
        .send_message(SenderMessage::AddPeer(ping_recv_mailbox.clone()))
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
    assert!(ping_sender_mailbox
        .send_message(SenderMessage::Ping)
        .await
        .is_ok());
    assert!(ping_sender_mailbox
        .send_message(SenderMessage::Ping)
        .await
        .is_ok());
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
    assert!(ping_sender_mailbox
        .send_message(SenderMessage::Ping)
        .await
        .is_err());
}

struct BuggyActor;

#[derive(Debug, Clone)]
enum BuggyMessage {
    DoNothing,
    Block,
}

impl Actor for BuggyActor {
    type Message = BuggyMessage;
    type ObservableState = ();

    fn name(&self) -> String {
        "BuggyActor".to_string()
    }

    fn observable_state(&self) {}
}

#[async_trait]
impl AsyncActor for BuggyActor {
    async fn process_message(
        &mut self,
        message: BuggyMessage,
        ctx: &ActorContext<Self::Message>,
    ) -> Result<(), ActorExitStatus> {
        match message {
            BuggyMessage::Block => {
                while ctx.kill_switch().is_alive() {
                    // we could keep the actor alive by calling `progress.record_progress()` here.
                    tokio::task::yield_now().await;
                }
            }
            BuggyMessage::DoNothing => {}
        }
        Ok(())
    }
}

#[tokio::test]
async fn test_timeouting_actor() {
    let universe = Universe::new();
    let (buggy_mailbox, buggy_handle) = universe.spawn_actor(BuggyActor).spawn_async();
    let buggy_mailbox = buggy_mailbox;
    assert_eq!(
        buggy_handle.observe().await.obs_type,
        ObservationType::Alive
    );
    assert!(buggy_mailbox
        .send_message(BuggyMessage::DoNothing)
        .await
        .is_ok());
    assert_eq!(
        buggy_handle.observe().await.obs_type,
        ObservationType::Alive
    );
    assert!(buggy_mailbox
        .send_message(BuggyMessage::Block)
        .await
        .is_ok());

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
async fn test_pause_sync_actor() {
    quickwit_common::setup_logging_for_tests();
    let universe = Universe::new();
    let actor = PingReceiverSyncActor::default();
    let (ping_mailbox, ping_handle) = universe.spawn_actor(actor).spawn_sync();
    for _ in 0..1000 {
        assert!(ping_mailbox.send_message(Ping).await.is_ok());
    }
    // Commands should be processed before message.
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
async fn test_sync_actor_running_states() {
    quickwit_common::setup_logging_for_tests();
    let universe = Universe::new();
    let actor = PingReceiverSyncActor::default();
    let (ping_mailbox, ping_handle) = universe.spawn_actor(actor).spawn_sync();
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
async fn test_pause_async_actor() {
    quickwit_common::setup_logging_for_tests();
    let universe = Universe::new();
    let (ping_mailbox, ping_handle) = universe
        .spawn_actor(PingReceiverAsyncActor::default())
        .spawn_async();
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
async fn test_async_actor_running_states() {
    quickwit_common::setup_logging_for_tests();
    let universe = Universe::new();
    let (ping_mailbox, ping_handle) = universe
        .spawn_actor(PingReceiverAsyncActor::default())
        .spawn_async();
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
    pub default_count: usize,
    pub normal_count: usize,
}

#[derive(Clone, Debug)]
enum Msg {
    Looping,
    Normal,
}

impl Actor for LoopingActor {
    type Message = Msg;

    type ObservableState = Self;

    fn observable_state(&self) -> Self::ObservableState {
        self.clone()
    }
}

#[async_trait]
impl AsyncActor for LoopingActor {
    async fn initialize(
        &mut self,
        ctx: &ActorContext<Self::Message>,
    ) -> Result<(), ActorExitStatus> {
        <LoopingActor as AsyncActor>::process_message(self, Msg::Looping, ctx).await
    }

    async fn process_message(
        &mut self,
        message: Self::Message,
        _ctx: &ActorContext<Self::Message>,
    ) -> Result<(), ActorExitStatus> {
        match message {
            Msg::Looping => {
                self.default_count += 1;
            }
            Msg::Normal => {
                self.normal_count += 1;
            }
        }
        Ok(())
    }
}

impl SyncActor for LoopingActor {
    fn initialize(&mut self, ctx: &ActorContext<Self::Message>) -> Result<(), ActorExitStatus> {
        <LoopingActor as SyncActor>::process_message(self, Msg::Looping, ctx)
    }

    fn process_message(
        &mut self,
        message: Self::Message,
        ctx: &ActorContext<Self::Message>,
    ) -> Result<(), ActorExitStatus> {
        match message {
            Msg::Looping => {
                self.default_count += 1;
                ctx.send_self_message_blocking(Msg::Looping)?;
            }
            Msg::Normal => {
                self.normal_count += 1;
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn test_looping_async() -> anyhow::Result<()> {
    let universe = Universe::new();
    let looping_actor = LoopingActor::default();
    let (looping_actor_mailbox, looping_actor_handle) =
        universe.spawn_actor(looping_actor).spawn_sync();
    assert!(looping_actor_mailbox
        .send_message(Msg::Normal)
        .await
        .is_ok());
    looping_actor_handle.process_pending_and_observe().await;
    let (exit_status, state) = looping_actor_handle.quit().await;
    assert!(matches!(exit_status, ActorExitStatus::Quit));
    assert_eq!(state.normal_count, 1);
    assert!(state.default_count > 0);
    Ok(())
}

#[tokio::test]
async fn test_looping_sync() -> anyhow::Result<()> {
    let universe = Universe::new();
    let looping_actor = LoopingActor::default();
    let (looping_actor_mailbox, looping_actor_handle) =
        universe.spawn_actor(looping_actor).spawn_sync();
    assert!(looping_actor_mailbox
        .send_message(Msg::Normal)
        .await
        .is_ok());
    looping_actor_handle.process_pending_and_observe().await;
    let (exit_status, state) = looping_actor_handle.quit().await;
    assert!(matches!(exit_status, ActorExitStatus::Quit));
    assert_eq!(state.normal_count, 1);
    assert!(state.default_count > 0);
    Ok(())
}

#[derive(Default)]
struct SummingActor {
    sum: u64,
}

impl Actor for SummingActor {
    type Message = u64;

    type ObservableState = u64;

    fn observable_state(&self) -> Self::ObservableState {
        self.sum
    }
}

impl SyncActor for SummingActor {
    fn process_message(
        &mut self,
        add: Self::Message,
        _ctx: &ActorContext<Self::Message>,
    ) -> Result<(), ActorExitStatus> {
        self.sum += add;
        Ok(())
    }
}

#[derive(Default)]
struct SpawningActor {
    res: u64,
    handle_opt: Option<(Mailbox<u64>, ActorHandle<SummingActor>)>,
}

impl Actor for SpawningActor {
    type Message = u64;
    type ObservableState = u64;

    fn observable_state(&self) -> Self::ObservableState {
        self.res
    }
}

#[async_trait]
impl AsyncActor for SpawningActor {
    async fn process_message(
        &mut self,
        message: Self::Message,
        ctx: &ActorContext<Self::Message>,
    ) -> Result<(), ActorExitStatus> {
        let (mailbox, _) = self
            .handle_opt
            .get_or_insert_with(|| ctx.spawn_actor(SummingActor::default()).spawn_sync());
        ctx.send_message(mailbox, message).await?;
        Ok(())
    }

    async fn finalize(
        &mut self,
        _exit_status: &ActorExitStatus,
        _ctx: &ActorContext<Self::Message>,
    ) -> anyhow::Result<()> {
        if let Some((_, child_handler)) = self.handle_opt.take() {
            self.res = child_handler.process_pending_and_observe().await.state;
            child_handler.kill().await;
        }
        Ok(())
    }
}

#[tokio::test]
async fn test_actor_spawning_actor() -> anyhow::Result<()> {
    let universe = Universe::new();
    let (mailbox, handle) = universe.spawn_actor(SpawningActor::default()).spawn_async();
    mailbox.send_message(1).await?;
    mailbox.send_message(2).await?;
    mailbox.send_message(3).await?;
    drop(mailbox);
    let (exit, result) = handle.join().await;
    assert!(matches!(exit, ActorExitStatus::Success));
    assert_eq!(result, 6);
    Ok(())
}
