use tokio::sync::watch;
use tokio::task::spawn_blocking;
use tracing::debug;

use crate::actor::ActorTermination;
use crate::mailbox::{create_mailbox, Command, Inbox};
use crate::{Actor, ActorContext, ActorHandle, KillSwitch, Progress, ReceptionResult};

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
        ctx: &ActorContext<Self::Message>,
    ) -> Result<(), ActorTermination>;

    /// Function called if there are no more messages available.
    fn finalize(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    #[doc(hidden)]
    fn spawn(
        mut self,
        kill_switch: KillSwitch,
    ) -> ActorHandle<Self::Message, Self::ObservableState> {
        let actor_name = self.name();
        debug!(actor_name=%actor_name,"spawning-sync-actor");
        let queue_capacity = self.queue_capacity();
        let (mailbox, inbox) = create_mailbox(actor_name, queue_capacity);
        let (state_tx, state_rx) = watch::channel(self.observable_state());
        let progress = Progress::default();
        let ctx = ActorContext {
            progress: progress.clone(),
            self_mailbox: mailbox.clone(),
            kill_switch: kill_switch.clone(),
        };
        let join_handle = spawn_blocking::<_, ActorTermination>(move || {
            let actor_name = self.name();
            let termination = sync_actor_loop(&mut self, inbox, &state_tx, ctx);
            debug!(cause=?termination, actor=%actor_name, "termination");
            let _ = state_tx.send(self.observable_state());
            termination
        });
        let actor_handle = ActorHandle::new(
            mailbox.clone(),
            state_rx,
            join_handle,
            progress,
            kill_switch,
        );
        actor_handle
    }
}

fn sync_actor_loop<A: SyncActor>(
    actor: &mut A,
    inbox: Inbox<A::Message>,
    state_tx: &watch::Sender<A::ObservableState>,
    ctx: ActorContext<A::Message>,
) -> ActorTermination {
    let mut is_paused = true;
    loop {
        if !ctx.kill_switch.is_alive() {
            return ActorTermination::KillSwitch;
        }
        ctx.progress.record_progress();
        let default_message_opt = actor.default_message().and_then(|default_message| {
            if ctx.self_mailbox.is_last_mailbox() {
                None
            } else {
                Some(default_message)
            }
        });
        let reception_result = inbox.try_recv_msg(is_paused, default_message_opt);
        ctx.progress.record_progress();
        if !ctx.kill_switch.is_alive() {
            return ActorTermination::KillSwitch;
        }
        if let ReceptionResult::None = reception_result {
            if ctx.self_mailbox.is_last_mailbox() {
                return ActorTermination::Terminated;
            }
        }
        match reception_result {
            ReceptionResult::Command(cmd) => {
                match cmd {
                    Command::Pause => {
                        is_paused = false;
                    }
                    Command::Stop(cb) => {
                        let _ = cb.send(());
                        return ActorTermination::OnDemand;
                    }
                    Command::Start => {
                        is_paused = true;
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
                // TODO avoid building the contexdt all of the time.
                let process_result = actor.process_message(msg, &ctx);
                if let Err(actor_termination) = process_result {
                    if actor_termination.is_failure() {
                        ctx.kill_switch.kill();
                    }
                    return actor_termination;
                }
            }
            ReceptionResult::None => {
                continue;
            }
            ReceptionResult::Disconnect => {
                return ActorTermination::Terminated;
            }
        }
    }
}
