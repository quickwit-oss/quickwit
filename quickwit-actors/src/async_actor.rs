use crate::actor::ActorTermination;
use crate::actor_handle::ActorHandle;
use crate::mailbox::{create_mailbox, Command, Inbox};
use crate::{Actor, ActorContext, KillSwitch, Progress, ReceptionResult};
use async_trait::async_trait;
use tokio::sync::watch;
use tracing::debug;

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

    async fn finalize(&mut self, termination: ActorTermination) -> ActorTermination {
        termination
    }

    #[doc(hidden)]
    fn spawn(self, kill_switch: KillSwitch) -> ActorHandle<Self::Message, Self::ObservableState> {
        debug!(actor_name=%self.name(),"spawning-async-actor");
        let (state_tx, state_rx) = watch::channel(self.observable_state());
        let actor_name = self.name();
        let progress = Progress::default();
        let queue_capacity = self.queue_capacity();
        let (mailbox, inbox) = create_mailbox(actor_name, queue_capacity);
        let join_handle = tokio::spawn(async_actor_loop(
            self,
            inbox,
            ActorContext {
                self_mailbox: mailbox.clone(),
                progress: progress.clone(),
                kill_switch: kill_switch.clone(),
                state_tx,
                is_paused: false,
            },
        ));
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


async fn process_msg<A: Actor + AsyncActor>(
    actor: &mut A,
    inbox: &Inbox<A::Message>,
    ctx: &mut ActorContext<A>
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

    let reception_result = inbox.try_recv_msg_async(!ctx.is_paused(), default_message_opt).await;

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
        ReceptionResult::Message(msg) => {
            actor
                .process_message(msg, &ctx)
                .await
                .err()
        }
        ReceptionResult::None => {
            if ctx.self_mailbox.is_last_mailbox() {
                Some(ActorTermination::Terminated)
            } else {
                None
            }
        }
        ReceptionResult::Disconnect => {
            Some(ActorTermination::Terminated)
        }
    }
}



async fn async_actor_loop<A: AsyncActor>(
    mut actor: A,
    inbox: Inbox<A::Message>,
    mut ctx: ActorContext<A>,
) -> ActorTermination {
    loop {
        debug!(name=%ctx.self_mailbox.actor_instance_name(), "message-loop");
        tokio::task::yield_now().await;
        let termination_opt = process_msg(&mut actor, &inbox,  &mut ctx).await;
        if let Some(termination) = termination_opt {
            if termination.is_failure() {
                ctx.kill_switch.kill();
            }
            let termination = actor.finalize(termination).await;
            let final_state = actor.observable_state();
            let _ = ctx.state_tx.send(final_state);
            return termination;
        }
    }
}
