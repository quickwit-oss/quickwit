use std::any::type_name;
use std::fmt;
use thiserror::Error;
use tokio::sync::watch;
use tracing::error;

use crate::{KillSwitch, Mailbox, Progress, QueueCapacity, SendError};

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
    #[error("Terminated")]
    Terminated,
    #[error("Actor stopped working as the killswitch was pushed by another actor.")]
    KillSwitch,
}

impl ActorTermination {
    pub fn is_failure(&self) -> bool {
        match self {
            ActorTermination::OnDemand => false,
            ActorTermination::DownstreamClosed => true,
            ActorTermination::Failure(_) => true,
            ActorTermination::Terminated => false,
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

    fn default_message(&self) -> Option<Self::Message> {
        None
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
    pub self_mailbox: Mailbox<A::Message>,
    pub progress: Progress,
    pub kill_switch: KillSwitch,
    pub state_tx: watch::Sender<A::ObservableState>,
    pub is_paused: bool,
}

impl<A: Actor> ActorContext<A> {
    pub async fn self_send_async(&self, msg: A::Message) {
        if let Err(_send_err) = self.self_mailbox.send_async(msg).await {
            error!("Failed to send error to self. This should never happen.");
        }
    }

    pub fn record_progress(&self) {
        self.progress.record_progress();
    }

    pub(crate) fn is_paused(&self,) -> bool {
        self.is_paused
    }

    pub(crate) fn pause(&mut self,) {
        self.is_paused = true;
    }

    pub(crate) fn resume(&mut self,) {
        self.is_paused = false;
    }
}
