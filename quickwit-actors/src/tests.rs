use crate::actor::{Actor, KillSwitch};
use crate::mailbox::{Command, QueueCapacity};
use crate::{ActorContext, AsyncActor, Mailbox, MessageProcessError, Observation, SyncActor};
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

    fn default_message(&self) -> Option<Self::Message> {
        None
    }
}

impl SyncActor for PingReceiverSyncActor {
    fn process_message(
        &mut self,
        _message: Self::Message,
        _progress: ActorContext<'_, Self::Message>,
    ) -> Result<(), MessageProcessError> {
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

    fn default_message(&self) -> Option<Self::Message> {
        None
    }
}

#[async_trait]
impl AsyncActor for PingReceiverAsyncActor {
    async fn process_message(
        &mut self,
        _message: Self::Message,
        _progress: ActorContext<'_, Self::Message>,
    ) -> Result<(), MessageProcessError> {
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

    fn default_message(&self) -> Option<Self::Message> {
        None
    }
}

#[async_trait]
impl AsyncActor for PingerAsyncSenderActor {
    async fn process_message(
        &mut self,
        message: SenderMessage,
        _context: ActorContext<'_, SenderMessage>,
    ) -> Result<(), MessageProcessError> {
        match message {
            SenderMessage::AddPeer(peer) => {
                self.peers.insert(peer);
            }
            SenderMessage::Ping => {
                self.count += 1;
                for peer in &self.peers {
                    let _ = peer.send_async(Ping).await;
                }
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn test_ping_actor() {
    let kill_switch = KillSwitch::default();
    let ping_recv_handle =
        PingReceiverSyncActor::default().spawn(QueueCapacity::Bounded(10), kill_switch.clone());
    let ping_sender_handle =
        PingerAsyncSenderActor::default().spawn(QueueCapacity::Bounded(10), kill_switch.clone());
    assert_eq!(ping_recv_handle.observe().await, Observation::Running(0));
    // No peers. This one will have no impact.
    let ping_recv_mailbox = ping_recv_handle.mailbox().clone();
    assert!(ping_sender_handle
        .mailbox()
        .send_async(SenderMessage::Ping)
        .await
        .is_ok());
    assert!(ping_sender_handle
        .mailbox()
        .send_async(SenderMessage::AddPeer(ping_recv_mailbox.clone()))
        .await
        .is_ok());
    assert_eq!(
        ping_sender_handle.process_and_observe().await,
        Observation::Running(SenderState {
            num_peers: 1,
            count: 1
        })
    );
    assert!(ping_sender_handle
        .mailbox()
        .send_async(SenderMessage::Ping)
        .await
        .is_ok());
    assert!(ping_sender_handle
        .mailbox()
        .send_async(SenderMessage::Ping)
        .await
        .is_ok());
    assert_eq!(
        ping_sender_handle.process_and_observe().await,
        Observation::Running(SenderState {
            num_peers: 1,
            count: 3
        })
    );
    assert_eq!(
        ping_recv_handle.process_and_observe().await,
        Observation::Running(2)
    );
    kill_switch.kill();
    assert_eq!(
        ping_recv_handle.process_and_observe().await,
        Observation::Terminated(2)
    );
    assert_eq!(
        ping_sender_handle.process_and_observe().await,
        Observation::Terminated(SenderState {
            num_peers: 1,
            count: 3
        })
    );
    assert!(ping_sender_handle
        .mailbox()
        .send_async(SenderMessage::Ping)
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
        _progress: ActorContext<'_, BuggyMessage>,
    ) -> Result<(), MessageProcessError> {
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
    let kill_switch = KillSwitch::default();
    let buggy_handle = BuggyActor.spawn(QueueCapacity::Bounded(10), kill_switch.clone());
    let buggy_mailbox = buggy_handle.mailbox().clone();
    assert_eq!(buggy_handle.observe().await, Observation::Running(()));
    assert!(buggy_mailbox
        .send_async(BuggyMessage::DoNothing)
        .await
        .is_ok());
    assert_eq!(buggy_handle.observe().await, Observation::Running(()));
    assert!(buggy_mailbox.send_async(BuggyMessage::Block).await.is_ok());
    assert_eq!(buggy_handle.observe().await, Observation::Timeout(()));
    tokio::time::sleep(crate::HEARTBEAT).await;
    tokio::time::sleep(crate::HEARTBEAT).await;
    assert_eq!(buggy_handle.observe().await, Observation::Terminated(()));
}

#[tokio::test]
async fn test_pause_sync_actor() {
    let actor = PingReceiverSyncActor::default();
    let kill_switch = KillSwitch::default();
    let ping_handle = actor.spawn(QueueCapacity::Unbounded, kill_switch);
    for _ in 0..1000 {
        assert!(ping_handle.mailbox().send_async(Ping).await.is_ok());
    }
    // Commands should be processed before message.
    assert!(ping_handle
        .mailbox()
        .send_command(Command::Pause)
        .await
        .is_ok());
    let first_state = *ping_handle.observe().await.state();
    assert!(first_state < 1000);
    let second_state = *ping_handle.observe().await.state();
    assert_eq!(first_state, second_state);
    assert!(ping_handle
        .mailbox()
        .send_command(Command::Start)
        .await
        .is_ok());
    let end_state = *ping_handle.process_and_observe().await.state();
    assert_eq!(end_state, 1000);
}

#[tokio::test]
async fn test_pause_async_actor() {
    let actor = PingReceiverAsyncActor::default();
    let kill_switch = KillSwitch::default();
    let ping_handle = actor.spawn(QueueCapacity::Unbounded, kill_switch);
    for _ in 0u32..1000u32 {
        assert!(ping_handle.mailbox().send_async(Ping).await.is_ok());
    }
    assert!(ping_handle
        .mailbox()
        .send_command(Command::Pause)
        .await
        .is_ok());
    let first_state = *ping_handle.observe().await.state();
    assert!(first_state < 1000);
    let second_state = *ping_handle.observe().await.state();
    assert_eq!(first_state, second_state);
    assert!(ping_handle
        .mailbox()
        .send_command(Command::Start)
        .await
        .is_ok());
    let end_state = *ping_handle.process_and_observe().await.state();
    assert_eq!(end_state, 1000);
}

#[derive(Default, Debug, Clone)]
struct DefaultMessageActor {
    pub default_count: usize,
    pub normal_count: usize,
}

#[derive(Clone, Debug)]
enum Msg {
    Default,
    Normal,
}

impl Actor for DefaultMessageActor {
    type Message = Msg;

    type ObservableState = Self;

    fn observable_state(&self) -> Self::ObservableState {
        self.clone()
    }

    fn default_message(&self) -> Option<Self::Message> {
        Some(Msg::Default)
    }
}

#[async_trait]
impl AsyncActor for DefaultMessageActor {
    async fn process_message(
        &mut self,
        message: Self::Message,
        _ctx: ActorContext<'_, Self::Message>,
    ) -> Result<(), MessageProcessError> {
        match message {
            Msg::Default => {
                self.default_count += 1;
            }
            Msg::Normal => {
                self.normal_count += 1;
            }
        }
        Ok(())
    }
}

impl SyncActor for DefaultMessageActor {
    fn process_message(
        &mut self,
        message: Self::Message,
        _ctx: ActorContext<'_, Self::Message>,
    ) -> Result<(), MessageProcessError> {
        match message {
            Msg::Default => {
                self.default_count += 1;
            }
            Msg::Normal => {
                self.normal_count += 1;
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn test_default_message_async() {
    let actor_with_default_msg = DefaultMessageActor::default();
    let actor_with_default_msg_handle = AsyncActor::spawn(
        actor_with_default_msg,
        QueueCapacity::Unbounded,
        KillSwitch::default(),
    );
    assert!(actor_with_default_msg_handle
        .mailbox()
        .send_async(Msg::Normal)
        .await
        .is_ok());
    tokio::time::sleep(Duration::from_millis(10)).await;
    let state = actor_with_default_msg_handle
        .process_and_observe()
        .await
        .state()
        .clone();
    assert_eq!(state.normal_count, 1);
    assert!(state.default_count > 0);
}

#[tokio::test]
async fn test_default_message_sync() {
    let actor_with_default_msg = DefaultMessageActor::default();
    let actor_with_default_msg_handle = SyncActor::spawn(
        actor_with_default_msg,
        QueueCapacity::Unbounded,
        KillSwitch::default(),
    );
    assert!(actor_with_default_msg_handle
        .mailbox()
        .send_async(Msg::Normal)
        .await
        .is_ok());
    tokio::time::sleep(Duration::from_millis(10)).await;
    let state = actor_with_default_msg_handle
        .process_and_observe()
        .await
        .state()
        .clone();
    assert_eq!(state.normal_count, 1);
    assert!(state.default_count > 1);
}
