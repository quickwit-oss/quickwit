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

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;

use quickwit_actors::{Actor, ActorContext, DeferableReplyHandler, Handler};

#[derive(Clone)]
pub struct Debouncer {
    cool_down_period: Duration,
    cooldown_state: Arc<AtomicU8>,
}

impl Debouncer {
    pub fn new(cool_down_period: Duration) -> Debouncer {
        Debouncer {
            cool_down_period,
            cooldown_state: Arc::new(AtomicU8::new(NO_COOLDOWN)),
        }
    }
}

const NO_COOLDOWN: u8 = 0u8;

// We are possibly within the cooldown period, and no event has been scheduled right after
// the cooldown yet.
const COOLDOWN_NOT_SCHEDULED: u8 = 1u8;

// We are within the cooldown period, and an event has been scheduled right after
// the cooldown.
const COOLDOWN_SCHEDULED: u8 = 2u8;

impl Debouncer {
    pub fn self_send_with_cooldown<A, M>(&self, ctx: &ActorContext<A>)
    where
        A: Actor + Handler<M> + DeferableReplyHandler<M>,
        M: Default + std::fmt::Debug + Send + Sync + 'static,
    {
        let cooldown_state = self.cooldown_state.load(Ordering::SeqCst);
        if cooldown_state != NO_COOLDOWN {
            self.cooldown_state
                .store(COOLDOWN_SCHEDULED, Ordering::SeqCst);
            return;
        }
        let ctx_clone = ctx.clone();
        let self_clone = self.clone();
        let callback = move || {
            let is_scheduled = self_clone.cooldown_state.load(Ordering::SeqCst);
            self_clone
                .cooldown_state
                .store(NO_COOLDOWN, Ordering::SeqCst);
            if is_scheduled == COOLDOWN_SCHEDULED {
                self_clone.self_send_with_cooldown(&ctx_clone);
            }
        };
        ctx.spawn_ctx()
            .schedule_event(callback, self.cool_down_period);
        let _ = ctx.mailbox().send_message_with_high_priority(M::default());
        self.cooldown_state
            .store(COOLDOWN_NOT_SCHEDULED, Ordering::SeqCst);
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
            self.debouncer.self_send_with_cooldown::<_, Increment>(ctx);
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
