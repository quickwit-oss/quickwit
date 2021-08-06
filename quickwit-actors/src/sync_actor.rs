use tokio::sync::watch;
use tokio::task::spawn_blocking;
use tracing::debug;

use crate::actor::MessageProcessError;
use crate::actor_handle::ActorTermination;
use crate::mailbox::{create_mailbox, Command, Inbox, QueueCapacity};
use crate::{Actor, ActorContext, ActorHandle, KillSwitch, Mailbox, Progress, ReceptionResult};

/// An sync actor is executed on a tokio blocking task.
///
/// It may block and perform CPU heavy computation.
/// (See also [`AsyncActor`])
pub trait SyncActor: Actor + Sized {
    /// Processes a message.
    ///
    /// If true is returned, the actors will continue processing messages.
    /// If false is returned, the actor will terminate "gracefully".
    ///
    /// If an error is returned, the actor will be killed, as well as all of the actor
    /// under the same kill switch.
    fn process_message(
        &mut self,
        message: Self::Message,
        context: crate::ActorContext<'_, Self::Message>,
    ) -> Result<(), MessageProcessError>;

    /// Function called if there are no more messages available.
    fn finalize(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    #[doc(hidden)]
    fn spawn(
        mut self,
        message_queue_capacity: QueueCapacity,
        kill_switch: KillSwitch,
    ) -> ActorHandle<Self::Message, Self::ObservableState> {
        let actor_name = self.name();
        let (mailbox, inbox) = create_mailbox(actor_name, message_queue_capacity);
        let (state_tx, state_rx) = watch::channel(self.observable_state());
        let progress = Progress::default();
        let progress_clone = progress.clone();
        let kill_switch_clone = kill_switch.clone();
        let mailbox_clone = mailbox.clone();
        let join_handle = spawn_blocking::<_, ActorTermination>(move || {
            let actor_name = self.name();
            let termination = sync_actor_loop(
                &mut self,
                inbox,
                mailbox_clone,
                &state_tx,
                kill_switch,
                progress,
            );
            debug!("Termination of actor {}", actor_name);
            let _ = state_tx.send(self.observable_state());
            termination
        });
        let actor_handle = ActorHandle::new(
            mailbox.clone(),
            state_rx,
            join_handle,
            progress_clone,
            kill_switch_clone,
        );
        actor_handle
    }
}

fn sync_actor_loop<A: SyncActor>(
    actor: &mut A,
    inbox: Inbox<A::Message>,
    self_mailbox: Mailbox<A::Message>,
    state_tx: &watch::Sender<A::ObservableState>,
    kill_switch: KillSwitch,
    progress: Progress,
) -> ActorTermination {
    let mut running = true;
    loop {
        if !kill_switch.is_alive() {
            return ActorTermination::KillSwitch;
        }
        progress.record_progress();
        let default_message_opt = actor.default_message().and_then(|default_message| {
            if self_mailbox.is_last_mailbox() {
                None
            } else {
                Some(default_message)
            }
        });
        let reception_result = inbox.try_recv_msg(running, default_message_opt);
        progress.record_progress();
        if !kill_switch.is_alive() {
            return ActorTermination::KillSwitch;
        }
        if let ReceptionResult::None = reception_result {
            if self_mailbox.is_last_mailbox() {
                return ActorTermination::Disconnect;
            }
        }
        match reception_result {
            ReceptionResult::Command(cmd) => {
                match cmd {
                    Command::Pause => {
                        running = false;
                    }
                    Command::Stop(cb) => {
                        let _ = cb.send(());
                        return ActorTermination::OnDemand;
                    }
                    Command::Start => {
                        running = true;
                    }
                    Command::Observe(cb) => {
                        let state = actor.observable_state();
                        let _ = state_tx.send(state);
                        // We voluntarily ignore the error here. (An error only occurs if the
                        // sender dropped its receiver.)
                        let _ = cb.send(());
                    }
                }
            }
            ReceptionResult::Message(msg) => {
                let context = ActorContext {
                    self_mailbox: &self_mailbox,
                    progress: &progress,
                    kill_switch: &kill_switch,
                };
                match actor.process_message(msg, context) {
                    Ok(()) => (),
                    Err(MessageProcessError::OnDemand) => return ActorTermination::OnDemand,
                    Err(MessageProcessError::Terminated) => return ActorTermination::OnDemand,
                    Err(MessageProcessError::Error(err)) => {
                        kill_switch.kill();
                        return ActorTermination::ActorError(err);
                    }
                    Err(MessageProcessError::DownstreamClosed) => {
                        kill_switch.kill();
                        return ActorTermination::DownstreamClosed;
                    }
                }
            }
            ReceptionResult::None => {
                continue;
            }
            ReceptionResult::Disconnect => {
                return ActorTermination::Disconnect;
            }
        }
    }
}
