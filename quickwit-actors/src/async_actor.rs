use crate::actor::ActorTermination;
use crate::actor_handle::ActorHandle;
use crate::actor_state::ActorState;
use crate::mailbox::{create_mailbox, Command, Inbox};
use crate::scheduler::SchedulerMessage;
use crate::{Actor, ActorContext, KillSwitch, Mailbox, ReceptionResult};
use anyhow::Context;
use async_trait::async_trait;
use tokio::sync::watch::{self, Sender};
use tracing::{debug, error};

/// An async actor is executed on a regular tokio task.
///
/// It can make async calls, but it should not block.
/// Actors doing CPU heavy work should implement `SyncActor` instead.
#[async_trait]
pub trait AsyncActor: Actor + Sized {
    /// Initialize is called before running the actor.
    ///
    /// This function is useful for instance to schedule an initial message in a looping
    /// actor.
    ///
    /// It can is treated just as if it was an initial implicit Initial message.
    /// Returning an ActorTermination will therefore have the same effect as if it
    /// was in `process_message`. (e.g. the actor will stop, the finalize method will be called.
    /// the kill switch may be activated etc.)
    async fn initialize(&mut self, _ctx: &ActorContext<Self>) -> Result<(), ActorTermination> {
        Ok(())
    }

    /// Processes a message.
    ///
    /// If true is returned, the actors will continue processing messages.
    /// If false is returned, the actor will terminate "gracefully".
    ///
    /// If Err is returned, the actor will be killed, as well as all of the actor
    /// under the same kill switch.
    async fn process_message(
        &mut self,
        message: Self::Message,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorTermination>;

    async fn finalize(
        &mut self,
        _termination: &ActorTermination,
        _ctx: &ActorContext<Self>,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

pub(crate) fn spawn_async_actor<A: AsyncActor>(
    actor: A,
    kill_switch: KillSwitch,
    scheduler_mailbox: Mailbox<SchedulerMessage>,
) -> (Mailbox<A::Message>, ActorHandle<A>) {
    debug!(actor_name=%actor.name(),"spawning-async-actor");
    let (state_tx, state_rx) = watch::channel(actor.observable_state());
    let actor_name = actor.name();
    let queue_capacity = actor.queue_capacity();
    let (mailbox, inbox) = create_mailbox(actor_name, queue_capacity);
    let mailbox_clone = mailbox.clone();
    let ctx = ActorContext::new(mailbox, kill_switch, scheduler_mailbox);
    let ctx_clone = ctx.clone();
    let join_handle = tokio::spawn(async_actor_loop(actor, inbox, ctx, state_tx));
    let handle = ActorHandle::new(state_rx, join_handle, ctx_clone);
    (mailbox_clone, handle)
}

async fn process_msg<A: Actor + AsyncActor>(
    actor: &mut A,
    inbox: &mut Inbox<A::Message>,
    ctx: &mut ActorContext<A>,
    state_tx: &Sender<A::ObservableState>,
) -> Option<ActorTermination> {
    if !ctx.kill_switch().is_alive() {
        return Some(ActorTermination::KillSwitch);
    }
    ctx.progress().record_progress();

    let reception_result = inbox
        .try_recv_msg(ctx.get_state() == ActorState::Running)
        .await;

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
                Command::Terminate(cb) => {
                    let _ = cb.send(());
                    Some(ActorTermination::OnDemand)
                }
                Command::Resume => {
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
                Command::HighPriorityMessage(msg) => {
                    debug!(msg=?msg, actor=%actor.name(),"scheduled-message-received");
                    actor.process_message(msg, &ctx).await.err()
                }
            }
        }
        ReceptionResult::Message(msg) => {
            debug!(msg=?msg, actor=%actor.name(),"message-received");
            actor.process_message(msg, &ctx).await.err()
        }
        ReceptionResult::Timeout => {
            if ctx.mailbox().is_last_mailbox() {
                Some(ActorTermination::Finished)
            } else {
                None
            }
        }
        ReceptionResult::Disconnect => Some(ActorTermination::Finished),
    }
}

async fn async_actor_loop<A: AsyncActor>(
    mut actor: A,
    mut inbox: Inbox<A::Message>,
    mut ctx: ActorContext<A>,
    state_tx: Sender<A::ObservableState>,
) -> ActorTermination {
    let mut termination_opt: Option<ActorTermination> = actor.initialize(&ctx).await.err();
    let termination: ActorTermination = loop {
        tokio::task::yield_now().await;
        if let Some(termination) = termination_opt {
            break termination;
        }
        termination_opt = process_msg(&mut actor, &mut inbox, &mut ctx, &state_tx).await;
    };
    ctx.terminate(&termination);

    if let Err(finalize_error) = actor
        .finalize(&termination, &ctx)
        .await
        .with_context(|| format!("Finalization of actor {}", actor.name()))
    {
        error!(err=?finalize_error, "finalize_error");
    }
    let final_state = actor.observable_state();
    let _ = state_tx.send(final_state);
    termination
}
