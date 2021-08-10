use std::fmt;
use std::ops::Deref;
use std::{any::type_name, sync::Arc};
use thiserror::Error;
use tracing::{debug, error};

use crate::actor_handle::ActorMessage;
use crate::{
    actor_state::{ActorState, AtomicState},
    progress::{Progress, ProtectZoneGuard},
    AsyncActor, KillSwitch, Mailbox, QueueCapacity, SendError, SyncActor,
};

// While the lack of message cannot pause a problem with heartbeating,  sending a message to a saturated channel
// can be interpreted as a blocked actor.

#[derive(Error, Debug)]
pub enum ActorTermination {
    /// The actor was stopped upon reception of a Command.
    #[error("On Demand")]
    OnDemand,
    /// The actor tried to send a message to a dowstream actor and failed.
    /// The logic ruled that the actor should be killed.
    #[error("Downstream actor closed connection")]
    DownstreamClosed,
    /// Some unexpected error happened.
    #[error("Failure")]
    Failure(#[from] anyhow::Error),
    /// The actor terminated, as it identified it reached a state where it
    /// would not send any more message.
    ///
    /// It happens either because all of the existing mailbox where dropped and
    /// the actor message queue was exhausted, or because the actor
    /// returned `Err(ActorTermination::Finished)` from its `process_msg` function.
    #[error("Finished")]
    Finished,
    /// The actor was terminated due to its kill switch.
    #[error("Actor stopped working as the killswitch was pushed by another actor.")]
    KillSwitch,
}

impl ActorTermination {

    pub fn is_finished(&self) -> bool {
        match self {
            ActorTermination::Finished => true,
            _ => false
        }
    }

    pub fn is_failure(&self) -> bool {
        match self {
            ActorTermination::OnDemand => false,
            ActorTermination::DownstreamClosed => true,
            ActorTermination::Failure(_) => true,
            ActorTermination::Finished => false,
            ActorTermination::KillSwitch => false,
        }
    }
}

impl From<SendError> for ActorTermination {
    fn from(_: SendError) -> Self {
        ActorTermination::DownstreamClosed
    }
}

/// An actor has an internal state and processes a stream of message.
///
/// While processing a message, the actor typically
/// - Update its state
/// - emit one or more message to other actors.
///
/// Actors exists in two flavor:
/// - async actors, are executed in event thread in tokio runtime.
/// - sync actors, executed on the blocking thread pool of tokio runtime.
pub trait Actor: Send + Sync + 'static {
    /// Type of message that can be received by the actor.
    type Message: Send + Sync + fmt::Debug;
    /// Piece of state that can be copied for assert in unit test, admin, etc.
    type ObservableState: Send + Sync + Clone + fmt::Debug;
    /// A name identifying the type of actor.
    /// It does not need to be "instance-unique", and can be the name of
    /// the actor implementation.
    fn name(&self) -> String {
        type_name::<Self>().to_string()
    }

    fn queue_capacity(&self) -> QueueCapacity {
        QueueCapacity::Unbounded
    }

    /// Extracts an observable state. Useful for unit test, and admin UI.
    ///
    /// This function should return fast, but it is not called after receiving
    /// single message. Snapshotting happens when the actor is terminated, or
    /// in an on demand fashion by calling `ActorHandle::observe()`.
    fn observable_state(&self) -> Self::ObservableState;
}

// TODO hide all of this public stuff
pub struct ActorContext<A: Actor> {
    inner: Arc<ActorContextInner<A>>,
}

impl<A: Actor> Clone for ActorContext<A> {
    fn clone(&self) -> Self {
        ActorContext {
            inner: self.inner.clone(),
        }
    }
}

impl<A: Actor> Deref for ActorContext<A> {
    type Target = ActorContextInner<A>;

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref()
    }
}

pub struct ActorContextInner<A: Actor> {
    actor_instance_name: String,
    self_mailbox: Mailbox<A::Message>,
    progress: Progress,
    kill_switch: KillSwitch,
    actor_state: AtomicState,
}

impl<A: Actor> ActorContext<A> {
    pub fn new(self_mailbox: Mailbox<A::Message>, kill_switch: KillSwitch) -> Self {
        let actor_instance_name = self_mailbox.actor_instance_name();
        ActorContext {
            inner: ActorContextInner {
                actor_instance_name,
                self_mailbox,
                progress: Progress::default(),
                kill_switch,
                actor_state: AtomicState::default(),
            }
            .into(),
        }
    }

    pub fn mailbox(&self) -> &Mailbox<A::Message> {
        &self.self_mailbox
    }

    pub fn actor_instance_name(&self) -> &str {
        &self.actor_instance_name
    }

    /// This function returns a guard that prevents any supervisor from identifying the
    /// actor has dead.
    /// The protection last as long as the `ProtectZoneGuard` it returns.
    ///
    /// In an ideal world, you should never need to call this function.
    /// It is only useful in some corner like, like a calling a long blocking
    /// from an external library that you trust.
    pub fn protect_zone(&self) -> ProtectZoneGuard {
        self.progress.protect_zone()
    }

    /// Get a copy of the actor kill switch.
    /// This should rarely be used.
    /// For instance, when quitting from the process_message function, prefer simply
    /// returning `Error(ActorTermination::Failure(..))`
    pub fn kill_switch(&self) -> &KillSwitch {
        &self.kill_switch
    }

    pub(crate) fn progress(&self) -> &Progress {
        &self.progress
    }
    /// Record some progress.
    /// This function is only useful when implementing actors that may takes more than
    /// `HEARTBEAT` to process a single message.
    /// In that case, you can call this function in the middle of the process_message method
    /// to prevent the actor from being identified as blocked or dead.
    pub fn record_progress(&self) {
        self.progress.record_progress();
    }

    pub(crate) fn get_state(&self) -> ActorState {
        self.actor_state.get_state()
    }

    pub(crate) fn pause(&mut self) {
        self.actor_state.pause();
    }

    pub(crate) fn resume(&mut self) {
        self.actor_state.resume();
    }

    pub(crate) fn terminate(&mut self) {
        self.actor_state.terminate();
    }
}

impl<A: SyncActor> ActorContext<A> {
    /// Sends a message to the actor being the mailbox.
    ///
    /// This method hides logic to prevent an actor from being identified
    /// as frozen if the destination actor channel is saturated, and we
    /// are simply experiencing back pressure.
    pub fn send_message_blocking<M: fmt::Debug>(
        &self,
        mailbox: &Mailbox<M>,
        msg: M,
    ) -> Result<(), crate::SendError> {
        let _guard = self.protect_zone();
        debug!(from=%self.self_mailbox.actor_instance_name(), to=%mailbox.actor_instance_name(), msg=?msg, "send");
        mailbox.send_message_blocking(msg)
    }

    /// Sends a message to itself.
    /// Since it is very easy to deadlock an actor, the behavior is quite
    /// different from `send_message_blocking`.
    ///
    /// The method not
    pub fn send_self_message_blocking(
        &self,
        msg: A::Message,
    ) -> Result<(), crate::SendError> {
        debug!(self=%self.self_mailbox.actor_instance_name(), msg=?msg, "self_send");
        self.self_mailbox.sender.try_send(ActorMessage::Message(msg))
            .map_err(|_| crate::SendError::WouldDeadlock)?;
        Ok(())
    }
}

impl<A: AsyncActor> ActorContext<A> {
    /// `async` version of `send_message`
    pub async fn send_message<M: fmt::Debug>(
        &self,
        mailbox: &Mailbox<M>,
        msg: M,
    ) -> Result<(), crate::SendError> {
        let _guard = self.protect_zone();
        debug!(from=%self.self_mailbox.actor_instance_name(), send=%mailbox.actor_instance_name(), msg=?msg);
        mailbox.send_message(msg).await
    }

    /// `async` version of `send_self_message`
    pub async fn send_self_message(
        &self,
        msg: A::Message,
    ) -> Result<(), crate::SendError> {
        debug!(self=%self.self_mailbox.actor_instance_name(), msg=?msg, "self_send");
        self.self_mailbox.send_message(msg).await
    }
}

pub struct TestContext;

impl TestContext {
    /// Sends a message to the actor being the mailbox.
    pub fn send_message_blocking<M>(
        &self,
        mailbox: &Mailbox<M>,
        msg: M,
    ) -> Result<(), crate::SendError> {
        mailbox.send_message_blocking(msg)
    }

    /// `async` version of `send_message`
    pub async fn send_message<M>(
        &self,
        mailbox: &Mailbox<M>,
        msg: M,
    ) -> Result<(), crate::SendError> {
        mailbox.send_message(msg).await
    }
}
