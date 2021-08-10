use tokio::sync::watch::{self, Sender};
use tokio::task::spawn_blocking;
use tracing::{debug, error, info};

use crate::actor::ActorTermination;
use crate::actor_state::ActorState;
use crate::mailbox::{create_mailbox, Command, Inbox};
use crate::scheduler::SchedulerMessage;
use crate::{Actor, ActorContext, ActorHandle, KillSwitch, Mailbox, ReceptionResult};

/// An sync actor is executed on a tokio blocking task.
///
/// It may block and perform CPU heavy computation.
/// (See also [`AsyncActor`])
///
/// Known pitfalls: Contrary to AsyncActor commands are typically not executed right away.
/// If both the command and the message channel are exhausted, and a command and N messages arrives,
/// one message is likely to be executed before the command is.
pub trait SyncActor: Actor + Sized {
    fn initialize(&mut self, _ctx: &ActorContext<Self>) -> Result<(), ActorTermination> {
        Ok(())
    }

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

    /// Hook  that can be set up to define what should happen upon actor termination.
    /// This hook is called only once.
    ///
    /// It is always called regardless of the type of termination.
    /// termination is passed as an argument to make it possible to act conditionnally
    /// to the type of Termination.
    fn finalize(
        &mut self,
        _termination: &ActorTermination,
        _ctx: &ActorContext<Self>,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

pub(crate) fn spawn_sync_actor<A: SyncActor>(
    actor: A,
    kill_switch: KillSwitch,
    scheduler_mailbox: Mailbox<SchedulerMessage>,
) -> (Mailbox<A::Message>, ActorHandle<A>) {
    let actor_name = actor.name();
    debug!(actor_name=%actor_name,"spawning-sync-actor");
    let queue_capacity = actor.queue_capacity();
    let (mailbox, inbox) = create_mailbox(actor_name, queue_capacity);
    let mailbox_clone = mailbox.clone();
    let (state_tx, state_rx) = watch::channel(actor.observable_state());
    let ctx = ActorContext::new(mailbox, kill_switch, scheduler_mailbox);
    let ctx_clone = ctx.clone();
    let join_handle = spawn_blocking::<_, ActorTermination>(move || {
        let actor_name = actor.name();
        let termination = sync_actor_loop(actor, inbox, ctx, state_tx);
        info!(cause=?termination, actor=%actor_name, "termination");
        termination
    });
    let handle = ActorHandle::new(state_rx, join_handle, ctx_clone);
    (mailbox_clone, handle)
}

/// Process a given message.
///
/// If some `ActorTermination` is returned, the actor will stop permanently.
fn process_msg<A: Actor + SyncActor>(
    actor: &mut A,
    inbox: &Inbox<A::Message>,
    ctx: &mut ActorContext<A>,
    state_tx: &Sender<A::ObservableState>,
) -> Option<ActorTermination> {
    if !ctx.kill_switch().is_alive() {
        return Some(ActorTermination::KillSwitch);
    }

    ctx.progress().record_progress();

    let reception_result = inbox.try_recv_msg_blocking(ctx.get_state() == ActorState::Running);

    ctx.progress().record_progress();
    if !ctx.kill_switch().is_alive() {
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
                    let _ = state_tx.send(state);
                    // We voluntarily ignore the error here. (An error only occurs if the
                    // sender dropped its receiver.)
                    let _ = cb.send(());
                    None
                }
                Command::ScheduledMessage(msg) => {
                    debug!(msg=?msg, actor=%actor.name(),"message-received");
                    actor.process_message(msg, &ctx).err()
                }
            }
        }
        ReceptionResult::Message(msg) => {
            debug!(msg=?msg, actor=%actor.name(),"message-received");
            actor.process_message(msg, &ctx).err()
        }
        ReceptionResult::None => {
            if ctx.mailbox().is_last_mailbox() {
                Some(ActorTermination::Finished)
            } else {
                None
            }
        }
        ReceptionResult::Disconnect => Some(ActorTermination::Finished),
    }
}

fn sync_actor_loop<A: SyncActor>(
    mut actor: A,
    inbox: Inbox<A::Message>,
    mut ctx: ActorContext<A>,
    state_tx: Sender<A::ObservableState>,
) -> ActorTermination {
    let mut termination_opt: Option<ActorTermination> = actor.initialize(&ctx).err();
    let termination: ActorTermination = loop {
        if let Some(termination) = termination_opt {
            break termination;
        }
        termination_opt = process_msg(&mut actor, &inbox, &mut ctx, &state_tx);
    };
    ctx.terminate(&termination);
    if let Err(error) = actor.finalize(&termination, &ctx) {
        error!(error=?error, "Finalizing failed");
    }
    let final_state = actor.observable_state();
    let _ = state_tx.send(final_state);
    termination
}
