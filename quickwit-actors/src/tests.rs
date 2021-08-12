use crate::mailbox::Command;
use crate::Actor;
use crate::Universe;
use crate::{ActorContext, ActorTermination, AsyncActor, Mailbox, Observation, SyncActor};
use async_trait::async_trait;
use std::collections::HashSet;
use std::time::Duration;

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
        _ctx: &ActorContext<Self>,
    ) -> Result<(), ActorTermination> {
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
        _progress: &ActorContext<Self>,
    ) -> Result<(), ActorTermination> {
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
        _ctx: &ActorContext<Self>,
    ) -> Result<(), ActorTermination> {
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
    let (ping_recv_mailbox, ping_recv_handle) =
        universe.spawn_sync_actor(PingReceiverSyncActor::default());
    let (ping_sender_mailbox, ping_sender_handle) =
        universe.spawn(PingerAsyncSenderActor::default());
    assert_eq!(ping_recv_handle.observe().await, Observation::Running(0));
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
        Observation::Running(SenderState {
            num_peers: 1,
            count: 1
        })
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
        Observation::Running(SenderState {
            num_peers: 1,
            count: 3
        })
    );
    assert_eq!(
        ping_recv_handle.process_pending_and_observe().await,
        Observation::Running(2)
    );
    universe.kill();
    assert_eq!(
        ping_recv_handle.process_pending_and_observe().await,
        Observation::Terminated(2)
    );
    assert_eq!(
        ping_sender_handle.process_pending_and_observe().await,
        Observation::Terminated(SenderState {
            num_peers: 1,
            count: 3
        })
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

    fn observable_state(&self) -> Self::ObservableState {
        ()
    }
}

#[async_trait]
impl AsyncActor for BuggyActor {
    async fn process_message(
        &mut self,
        message: BuggyMessage,
        _ctx: &ActorContext<Self>,
    ) -> Result<(), ActorTermination> {
        match message {
            BuggyMessage::Block => {
                loop {
                    // we could keep the actor alive by calling `progress.record_progress()` here.
                    tokio::time::sleep(tokio::time::Duration::from_secs(3_600)).await;
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
    let (buggy_mailbox, buggy_handle) = universe.spawn(BuggyActor);
    let buggy_mailbox = buggy_mailbox;
    assert_eq!(buggy_handle.observe().await, Observation::Running(()));
    assert!(buggy_mailbox
        .send_message(BuggyMessage::DoNothing)
        .await
        .is_ok());
    assert_eq!(buggy_handle.observe().await, Observation::Running(()));
    assert!(buggy_mailbox
        .send_message(BuggyMessage::Block)
        .await
        .is_ok());
    // We sleep here to make sure the block message is not executed after the observe command
    // due to priority rules.
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(buggy_handle.observe().await, Observation::Timeout(()));
    tokio::time::sleep(crate::HEARTBEAT).await;
    tokio::time::sleep(crate::HEARTBEAT).await;
    assert_eq!(buggy_handle.observe().await, Observation::Terminated(()));
}

#[tokio::test]
async fn test_pause_sync_actor() {
    quickwit_common::setup_logging_for_tests();
    let universe = Universe::new();
    let actor = PingReceiverSyncActor::default();
    let (ping_mailbox, ping_handle) = universe.spawn_sync_actor(actor);
    for _ in 0..1000 {
        assert!(ping_mailbox.send_message(Ping).await.is_ok());
    }
    // Commands should be processed before message.
    assert!(ping_mailbox.send_command(Command::Pause).await.is_ok());
    let first_state = *ping_handle.observe().await.state();
    assert!(first_state < 1000);
    let second_state = *ping_handle.observe().await.state();
    assert_eq!(first_state, second_state);
    assert!(ping_mailbox.send_command(Command::Resume).await.is_ok());
    let end_state = *ping_handle.process_pending_and_observe().await.state();
    assert_eq!(end_state, 1000);
}

#[tokio::test]
async fn test_pause_async_actor() {
    quickwit_common::setup_logging_for_tests();
    let universe = Universe::new();
    let (ping_mailbox, ping_handle) = universe.spawn(PingReceiverAsyncActor::default());
    for _ in 0u32..1000u32 {
        assert!(ping_mailbox.send_message(Ping).await.is_ok());
    }
    assert!(ping_mailbox.send_command(Command::Pause).await.is_ok());
    let first_state = *ping_handle.observe().await.state();
    assert!(first_state < 1000);
    let second_state = *ping_handle.observe().await.state();
    assert_eq!(first_state, second_state);
    assert!(ping_mailbox.send_command(Command::Resume).await.is_ok());
    let end_state = *ping_handle.process_pending_and_observe().await.state();
    assert_eq!(end_state, 1000);
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
    async fn process_message(
        &mut self,
        message: Self::Message,
        _ctx: &ActorContext<Self>,
    ) -> Result<(), ActorTermination> {
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
    fn process_message(
        &mut self,
        message: Self::Message,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorTermination> {
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
async fn test_default_message_async() -> anyhow::Result<()> {
    let universe = Universe::new();
    let actor_with_default_msg = LoopingActor::default();
    let (actor_with_default_msg_mailbox, actor_with_default_msg_handle) =
        universe.spawn(actor_with_default_msg);
    universe
        .send_message(&actor_with_default_msg_mailbox, Msg::Looping)
        .await?;
    assert!(actor_with_default_msg_mailbox
        .send_message(Msg::Normal)
        .await
        .is_ok());
    tokio::time::sleep(Duration::from_millis(10)).await;
    let state = actor_with_default_msg_handle
        .process_pending_and_observe()
        .await
        .state()
        .clone();
    actor_with_default_msg_handle.finish().await;
    assert_eq!(state.normal_count, 1);
    assert!(state.default_count > 0);
    Ok(())
}

#[tokio::test]
async fn test_default_message_sync() -> anyhow::Result<()> {
    let universe = Universe::new();
    let actor_with_default_msg = LoopingActor::default();
    let (actor_with_default_msg_mailbox, actor_with_default_msg_handle) =
        universe.spawn_sync_actor(actor_with_default_msg);
    let universe = Universe::new();
    universe
        .send_message(&actor_with_default_msg_mailbox, Msg::Looping)
        .await?;
    assert!(actor_with_default_msg_mailbox
        .send_message(Msg::Normal)
        .await
        .is_ok());
    tokio::time::sleep(Duration::from_millis(10)).await;
    let state = actor_with_default_msg_handle
        .process_pending_and_observe()
        .await
        .state()
        .clone();
    actor_with_default_msg_handle.finish().await;
    assert_eq!(state.normal_count, 1);
    assert!(state.default_count > 1);
    Ok(())
}
