use std::fmt;
use tokio::sync::{oneshot, watch};
use tokio::task::{JoinError, JoinHandle};
use tokio::time::timeout;
use tracing::error;

use crate::mailbox::Command;
use crate::progress::Progress;
use crate::{ActorTermination, KillSwitch, Mailbox, Observation};

/// An Actor Handle serves as an address to communicate with an actor.
///
/// It is lightweight to clone it.
/// If all actor handles are dropped, the actor does not die right away.
/// It will process all of the message in its mailbox before being terminated.
///
/// Because `ActorHandle`'s generic types are Message and Observable, as opposed
/// to the actor type, `ActorHandle` are interchangeable.
/// It makes it possible to plug different implementations, have actor proxy etc.
pub struct ActorHandle<Message, ObservableState> {
    mailbox: Mailbox<Message>,
    join_handle: JoinHandle<ActorTermination>,
    kill_switch: KillSwitch,
    last_state: watch::Receiver<ObservableState>,
}

impl<M, ObservableState> fmt::Debug for ActorHandle<M, ObservableState> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ActorHandle({})", self.mailbox.actor_instance_name())
    }
}

impl<Message, ObservableState: Clone + Send + fmt::Debug> ActorHandle<Message, ObservableState> {
    pub(crate) fn new(
        mailbox: Mailbox<Message>,
        last_state: watch::Receiver<ObservableState>,
        join_handle: JoinHandle<ActorTermination>,
        progress: Progress,
        kill_switch: KillSwitch,
    ) -> Self {
        let mut interval = tokio::time::interval(crate::HEARTBEAT);
        let kill_switch_clone = kill_switch.clone();
        tokio::task::spawn(async move {
            interval.tick().await;
            while kill_switch.is_alive() {
                interval.tick().await;
                if !progress.harvest_changes() {
                    kill_switch.kill();
                    return;
                }
            }
        });
        ActorHandle {
            mailbox,
            join_handle,
            kill_switch: kill_switch_clone,
            last_state,
        }
    }

    pub fn mailbox(&self) -> &Mailbox<Message> {
        &self.mailbox
    }

    /// Process all of the pending message, and returns a snapshot of
    /// the observable state of the actor after this.
    ///
    /// This method is mostly useful for tests.
    ///
    /// To actually observe the state of an actor for ops purpose,
    /// prefer using the `.observe()` method.
    ///
    /// This method timeout if reaching the end of the message takes more than an HEARTBEAT.
    /// Hence, it is only useful or unit tests.
    pub async fn process_pending_and_observe(&self) -> Observation<ObservableState> {
        let (tx, rx) = oneshot::channel();
        if self
            .mailbox
            .send_actor_message(ActorMessage::Observe(tx))
            .await
            .is_err()
        {
            error!("Failed to send message");
        }
        // TODO The timeout is required here. If the actor fails, the inbox is properly dropped but the send channel might actually
        // prevent the onechannel Receiver from being dropped.
        let observable_state_res = tokio::time::timeout(crate::HEARTBEAT, rx).await;
        let state = self.last_state.borrow().clone();
        match observable_state_res {
            Ok(Ok(_)) => Observation::Running(state),
            Ok(Err(_)) => Observation::Terminated(state),
            Err(_) => Observation::Timeout(state),
        }
    }

    /// Terminates the actor, regardless of whether there are pending messages or not.
    pub async fn finish(&self) {
        let (tx, rx) = oneshot::channel();
        let _ = self.mailbox().send_command(Command::Stop(tx)).await;
        let _ = rx.await;
    }

    pub async fn join(self) -> Result<ActorTermination, JoinError> {
        self.join_handle.await
    }

    /// Observe the current state.
    ///
    /// If a message is currently being processed, the observation will be
    /// after its processing has finished.
    pub async fn observe(&self) -> Observation<ObservableState> {
        let (tx, rx) = oneshot::channel();
        if self
            .mailbox
            .send_command(Command::Observe(tx))
            .await
            .is_err()
        {
            error!("Failed to send message");
        }
        let observable_state_or_timeout = timeout(crate::HEARTBEAT, rx).await;
        let state = self.last_state.borrow().clone();
        match observable_state_or_timeout {
            Ok(Ok(())) => Observation::Running(state),
            Ok(Err(_)) => Observation::Terminated(state),
            Err(_) => {
                if self.kill_switch.is_alive() {
                    Observation::Timeout(state)
                } else {
                    self.join_handle.abort();
                    Observation::Terminated(state)
                }
            }
        }
    }

    pub fn last_observation(&self) -> ObservableState {
        self.last_state.borrow().clone()
    }
}

pub enum ActorMessage<Message> {
    Message(Message),
    Observe(oneshot::Sender<()>),
}

impl<Message: fmt::Debug> fmt::Debug for ActorMessage<Message> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Message(msg) => {
                write!(f, "Message({:?})", msg)
            }
            Self::Observe(_) => {
                write!(f, "Observe")
            }
        }
    }
}
