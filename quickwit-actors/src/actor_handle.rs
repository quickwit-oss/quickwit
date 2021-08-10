use crate::actor_state::ActorState;
use crate::mailbox::Command;
use crate::{Actor, ActorContext, ActorTermination, Observation};
use std::fmt;
/// An Actor Handle serves as an address to communicate with an actor.
///
/// It is lightweight to clone it.
/// If all actor handles are dropped, the actor does not die right away.
/// It will process all of the message in its mailbox before being terminated.
///
/// Because `ActorHandle`'s generic types are Message and Observable, as opposed
/// to the actor type, `ActorHandle` are interchangeable.
/// It makes it possible to plug different implementations, have actor proxy etc.use std::fmt;
use tokio::sync::{oneshot, watch};
use tokio::task::{JoinError, JoinHandle};
use tokio::time::timeout;
use tracing::error;

pub struct ActorHandle<A: Actor> {
    actor_context: ActorContext<A>,
    last_state: watch::Receiver<A::ObservableState>,
    join_handle: JoinHandle<ActorTermination>,
}

impl<A: Actor> fmt::Debug for ActorHandle<A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActorHandle")
            .field("name", &self.actor_context.actor_instance_name())
            .finish()
    }
}

impl<A: Actor> ActorHandle<A> {
    pub(crate) fn new(
        last_state: watch::Receiver<A::ObservableState>,
        join_handle: JoinHandle<ActorTermination>,
        ctx: ActorContext<A>,
    ) -> Self {
        let mut interval = tokio::time::interval(crate::HEARTBEAT);
        let actor_instance_name = ctx.actor_instance_name().to_string();
        let ctx_clone = ctx.clone();
        tokio::task::spawn(async move {
            interval.tick().await;
            while ctx.kill_switch().is_alive() {
                interval.tick().await;
                if !ctx.progress().harvest_changes() {
                    if ctx.get_state() == ActorState::Terminated {
                        return;
                    }
                    error!(actor=%actor_instance_name, "actor-timeout");
                    ctx.kill_switch().kill();
                    return;
                }
            }
        });
        ActorHandle {
            join_handle,
            last_state,
            actor_context: ctx_clone,
        }
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
    pub async fn process_pending_and_observe(&self) -> Observation<A::ObservableState> {
        let (tx, rx) = oneshot::channel();
        if self
            .actor_context
            .mailbox()
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
        let _ = self
            .actor_context
            .mailbox()
            .send_command(Command::Stop(tx))
            .await;
        let _ = rx.await;
    }

    pub async fn join(self) -> Result<(ActorTermination, A::ObservableState), JoinError> {
        let termination = self.join_handle.await?;
        let observation = self.last_state.borrow().clone();
        Ok((termination, observation))
    }

    /// Observe the current state.
    ///
    /// If a message is currently being processed, the observation will be
    /// after its processing has finished.
    pub async fn observe(&self) -> Observation<A::ObservableState> {
        let (tx, rx) = oneshot::channel();
        if self
            .actor_context
            .mailbox()
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
                if self.actor_context.kill_switch().is_alive() {
                    Observation::Timeout(state)
                } else {
                    self.join_handle.abort();
                    Observation::Terminated(state)
                }
            }
        }
    }

    pub fn last_observation(&self) -> A::ObservableState {
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
