use std::any::type_name;
use std::fmt;
use thiserror::Error;
use tracing::error;

use crate::{KillSwitch, Mailbox, Progress, QueueCapacity, SendError};

// While the lack of message cannot pause a problem with heartbeating,  sending a message to a saturated channel
// can be interpreted as a blocked actor.

#[derive(Error, Debug)]
pub enum MessageProcessError {
    /// The actor was stopped upon reception of a Command.
    #[error("On Demand")]
    OnDemand,
    /// The actor tried to send a message to a dowstream actor and failed.
    /// The logic ruled that the actor should be killed.
    #[error("Downstream actor closed connection")]
    DownstreamClosed,
    /// Some unexpected error happened.
    #[error("Failure")]
    Error(#[from] anyhow::Error),
    /// The actor terminated, as it identified it reached a state where it
    /// would not send any more message.
    #[error("Terminated")]
    Terminated,
}

impl From<SendError> for MessageProcessError {
    fn from(_: SendError) -> Self {
        MessageProcessError::DownstreamClosed
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

pub struct ActorContext< Message> {
    pub self_mailbox: Mailbox<Message>,
    pub progress: Progress,
    pub kill_switch: KillSwitch,
}

impl<Message> ActorContext<Message> {
    pub async fn self_send_async(&self, msg: Message) {
        if let Err(_send_err) = self.self_mailbox.send_async(msg).await {
            error!("Failed to send error to self. This should never happen.");
        }
    }

    pub fn record_progress(&self) {
        self.progress.record_progress();
    }
}
