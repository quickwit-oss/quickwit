use crate::actor::ActorTermination;
use crate::actor_handle::ActorHandle;
use crate::actor_state::ActorState;
use crate::mailbox::{create_mailbox, Command, Inbox};
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

    #[doc(hidden)]
    fn spawn(self, kill_switch: KillSwitch) -> (Mailbox<Self::Message>, ActorHandle<Self>) {
        debug!(actor_name=%self.name(),"spawning-async-actor");
        let (state_tx, state_rx) = watch::channel(self.observable_state());
        let actor_name = self.name();
        let queue_capacity = self.queue_capacity();
        let (mailbox, inbox) = create_mailbox(actor_name, queue_capacity);
        let mailbox_clone = mailbox.clone();
        let ctx = ActorContext::new(mailbox, kill_switch);
        let ctx_clone = ctx.clone();
        let join_handle = tokio::spawn(async_actor_loop(self, inbox, ctx, state_tx));
        let handle = ActorHandle::new(state_rx, join_handle, ctx_clone);
        (mailbox_clone, handle)
    }
}

async fn process_msg<A: Actor + AsyncActor>(
    actor: &mut A,
    inbox: &Inbox<A::Message>,
    ctx: &mut ActorContext<A>,
    state_tx: &Sender<A::ObservableState>,
) -> Option<ActorTermination> {
    if !ctx.kill_switch().is_alive() {
        return Some(ActorTermination::KillSwitch);
    }
    ctx.progress().record_progress();
    let default_message_opt = actor.default_message();
    ctx.progress().record_progress();

    let reception_result = inbox
        .try_recv_msg(ctx.get_state() == ActorState::Running, default_message_opt)
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
            }
        }
        ReceptionResult::Message(msg) => {
            debug!(msg=?msg, actor=%actor.name(),"message-received");
            actor.process_message(msg, &ctx).await.err()
        }
        ReceptionResult::None => {
            if ctx.mailbox().is_last_mailbox() {
                dbg!("lastmailbox");
                Some(ActorTermination::Terminated)
            } else {
                dbg!("not lastmailbox");
                None
            }
        }
        ReceptionResult::Disconnect => Some(ActorTermination::Terminated),
    }
}

async fn async_actor_loop<A: AsyncActor>(
    mut actor: A,
    inbox: Inbox<A::Message>,
    mut ctx: ActorContext<A>,
    state_tx: Sender<A::ObservableState>,
) -> ActorTermination {
    loop {
        tokio::task::yield_now().await;
        let termination_opt = process_msg(&mut actor, &inbox, &mut ctx, &state_tx).await;
        if let Some(termination) = termination_opt {
            ctx.terminate();
            if termination.is_failure() {
                ctx.kill_switch().kill();
            }
            if let Err(finalize_error) = actor
                .finalize(&termination, &ctx)
                .await
                .with_context(|| format!("Finalization of actor {}", actor.name()))
            {
                error!(err=?finalize_error, "finalize_error");
            }
            let final_state = actor.observable_state();
            let _ = state_tx.send(final_state);
            return termination;
        }
    }
}
