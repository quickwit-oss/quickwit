use tokio::sync::watch;
use tokio::task::spawn_blocking;
use tracing::debug;

use crate::actor::ActorTermination;
use crate::mailbox::{create_mailbox, Command, Inbox};
use crate::progress::Progress;
use crate::{Actor, ActorContext, ActorHandle, KillSwitch, ReceptionResult};

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
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorTermination>;

    /// Functionj
    fn finalize(&mut self, termination: ActorTermination) -> ActorTermination {
        termination
    }

    #[doc(hidden)]
    fn spawn(self, kill_switch: KillSwitch) -> ActorHandle<Self::Message, Self::ObservableState> {
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
            state_tx,
            is_paused: false,
        };
        let join_handle = spawn_blocking::<_, ActorTermination>(move || {
            let actor_name = self.name();
            let termination = sync_actor_loop(self, inbox, ctx);
            debug!(cause=?termination, actor=%actor_name, "termination");
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

fn process_msg<A: Actor + SyncActor>(
    actor: &mut A,
    inbox: &Inbox<A::Message>,
    ctx: &mut ActorContext<A>,
) -> Option<ActorTermination> {
    if !ctx.kill_switch.is_alive() {
        return Some(ActorTermination::KillSwitch);
    }

    ctx.progress.record_progress();
    let default_message_opt = actor.default_message().and_then(|default_message| {
        if ctx.self_mailbox.is_last_mailbox() {
            None
        } else {
            Some(default_message)
        }
    });
    ctx.progress.record_progress();

    let reception_result = inbox.try_recv_msg(!ctx.is_paused(), default_message_opt);

    ctx.progress.record_progress();
    if !ctx.kill_switch.is_alive() {
        return Some(ActorTermination::KillSwitch);
    }
    match reception_result {
        ReceptionResult::Command(cmd) => {
            match cmd {
                Command::Pause => {
                    ctx.pause();
                    None
                }
                Command::Stop(cb) => {
                    let _ = cb.send(());
                    Some(ActorTermination::OnDemand)
                }
                Command::Start => {
                    ctx.resume();
                    None
                }
                Command::Observe(cb) => {
                    let state = actor.observable_state();
                    let _ = ctx.state_tx.send(state);
                    // We voluntarily ignore the error here. (An error only occurs if the
                    // sender dropped its receiver.)
                    let _ = cb.send(());
                    None
                }
            }
        }
        ReceptionResult::Message(msg) => actor.process_message(msg, &ctx).err(),
        ReceptionResult::None => {
            if ctx.self_mailbox.is_last_mailbox() {
                Some(ActorTermination::Terminated)
            } else {
                None
            }
        }
        ReceptionResult::Disconnect => Some(ActorTermination::Terminated),
    }
}

fn sync_actor_loop<A: SyncActor>(
    mut actor: A,
    inbox: Inbox<A::Message>,
    mut ctx: ActorContext<A>,
) -> ActorTermination {
    loop {
        let termination_opt = process_msg(&mut actor, &inbox, &mut ctx);
        if let Some(termination) = termination_opt {
            if termination.is_failure() {
                ctx.kill_switch.kill();
            }
            let termination = actor.finalize(termination);
            let final_state = actor.observable_state();
            let _ = ctx.state_tx.send(final_state);
            return termination;
        }
    }
}
