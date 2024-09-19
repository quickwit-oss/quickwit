// Copyright (C) 2024 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use quickwit_actors::{Actor, ActorContext, Handler};

/// A debouncer is a helper to debounce events.
///
/// The debouncing takes a `cooldown_period` parameter and works as you may expect:
///
///
///    time                t=0                   t=COOLDOWN            t=2*COOLDOWN
///                        |                          |                     |
///  ----------------------------------------------------------------------------------------------------
///    event               *     * *    *                                   *   * *
///    debounced effect    o                          o                     o
///
/// In particular, note the event triggered at `t=COOLDOWN`.
#[derive(Clone)]
pub struct Debouncer {
    cooldown_period: Duration,
    cooldown_state: Arc<Mutex<DebouncerState>>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum DebouncerState {
    /// More than `cooldown_period` has elapsed since we last emitted an event.
    NoCooldown,
    // Less than `cooldown_period` has elapsed since we last emitted an event,
    // and no event has been received since then.
    CooldownNotScheduled,
    // Less than `cooldown_period` has elapsed since we last emitted an event,
    // and we have already received an event during this cooldown period.
    CooldownScheduled,
}

impl DebouncerState {
    fn accept(self, transition: Transition) -> DebouncerState {
        use DebouncerState::*;
        use Transition::*;
        match (self, transition) {
            (NoCooldown, Emit) => CooldownNotScheduled,
            (NoCooldown, CooldownExpired) => unreachable!(),
            (CooldownNotScheduled, Emit) => CooldownScheduled,
            (CooldownNotScheduled, CooldownExpired) => NoCooldown,
            (CooldownScheduled, Emit) => CooldownScheduled,
            (CooldownScheduled, CooldownExpired) => NoCooldown,
        }
    }
}

enum Transition {
    CooldownExpired,
    Emit,
}

#[allow(dead_code)]
impl Debouncer {
    pub fn new(cooldown_period: Duration) -> Debouncer {
        Debouncer {
            cooldown_period,
            cooldown_state: Arc::new(Mutex::new(DebouncerState::NoCooldown)),
        }
    }

    /// Updates the state according to the transition, and returns the state before the transition.
    /// The entire transition is atomic.
    fn accept_transition(&self, transition: Transition) -> DebouncerState {
        let mut lock = self.cooldown_state.lock().unwrap();
        let previous_state = *lock;
        let new_state = previous_state.accept(transition);
        *lock = new_state;
        previous_state
    }

    fn emit_message<A, M>(&self, ctx: &ActorContext<A>)
    where
        A: Actor + Handler<M>,
        M: Default + std::fmt::Debug + Send + Sync + 'static,
    {
        let _ = ctx.mailbox().send_message_with_high_priority(M::default());
    }

    fn schedule_post_cooldown_callback<A, M>(&self, ctx: &ActorContext<A>)
    where
        A: Actor + Handler<M>,
        M: Default + std::fmt::Debug + Send + Sync + 'static,
    {
        let ctx_clone = ctx.clone();
        let self_clone = self.clone();
        let callback = move || {
            let previous_state = self_clone.accept_transition(Transition::CooldownExpired);
            if previous_state == DebouncerState::CooldownScheduled {
                self_clone.self_send_with_cooldown(&ctx_clone);
            }
        };
        ctx.spawn_ctx()
            .schedule_event(callback, self.cooldown_period);
    }

    pub fn self_send_with_cooldown<M>(&self, ctx: &ActorContext<impl Handler<M>>)
    where M: Default + std::fmt::Debug + Send + Sync + 'static {
        let cooldown_state = self.accept_transition(Transition::Emit);
        match cooldown_state {
            DebouncerState::NoCooldown => {
                self.emit_message(ctx);
                self.schedule_post_cooldown_callback(ctx);
            }
            DebouncerState::CooldownNotScheduled | DebouncerState::CooldownScheduled => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use async_trait::async_trait;
    use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Universe};

    use crate::debouncer::Debouncer;

    struct DebouncingActor {
        count: usize,
        debouncer: Debouncer,
    }

    impl DebouncingActor {
        pub fn new(cooldown_duration: Duration) -> DebouncingActor {
            DebouncingActor {
                count: 0,
                debouncer: Debouncer::new(cooldown_duration),
            }
        }
    }

    #[derive(Debug, Default)]
    struct Increment;

    #[derive(Debug)]
    struct DebouncedIncrement;

    #[async_trait]
    impl Actor for DebouncingActor {
        type ObservableState = usize;

        fn observable_state(&self) -> Self::ObservableState {
            self.count
        }
    }

    #[async_trait]
    impl Handler<Increment> for DebouncingActor {
        type Reply = ();

        async fn handle(
            &mut self,
            _message: Increment,
            _ctx: &ActorContext<Self>,
        ) -> Result<Self::Reply, ActorExitStatus> {
            self.count += 1;
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<DebouncedIncrement> for DebouncingActor {
        type Reply = ();

        async fn handle(
            &mut self,
            _message: DebouncedIncrement,
            ctx: &ActorContext<Self>,
        ) -> Result<Self::Reply, ActorExitStatus> {
            self.debouncer.self_send_with_cooldown::<Increment>(ctx);
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_debouncer() {
        let universe = Universe::default();
        let cooldown_period = Duration::from_millis(1_000);
        let debouncer = DebouncingActor::new(cooldown_period);
        let (debouncer_mailbox, debouncer_handle) = universe.spawn_builder().spawn(debouncer);
        {
            let count = *debouncer_handle.process_pending_and_observe().await;
            assert_eq!(count, 0);
        }
        {
            let _ = debouncer_mailbox.ask(DebouncedIncrement).await;
            let count = *debouncer_handle.process_pending_and_observe().await;
            assert_eq!(count, 1);
        }
        for _ in 0..10 {
            let _ = debouncer_mailbox.ask(DebouncedIncrement).await;
            let count = *debouncer_handle.process_pending_and_observe().await;
            assert_eq!(count, 1);
        }
        {
            universe.sleep(cooldown_period.mul_f32(1.2f32)).await;
            let count = *debouncer_handle.process_pending_and_observe().await;
            assert_eq!(count, 2);
        }
        {
            let _ = debouncer_mailbox.ask(DebouncedIncrement).await;
            let count = *debouncer_handle.process_pending_and_observe().await;
            assert_eq!(count, 2);
        }
        {
            universe.sleep(cooldown_period * 2).await;
            let count = *debouncer_handle.process_pending_and_observe().await;
            assert_eq!(count, 3);
        }
        universe.assert_quit().await;
    }
}
