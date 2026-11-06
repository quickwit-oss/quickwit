// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::task::{Context, Poll, Wake, Waker};

use arc_swap::ArcSwapOption;

use super::high_priority_state_machine::{AtomicState, Event, State};
use super::{NotifyGuard, PriorityGate};

/// Owned by the `HighPriorityFuture`. Wakers only hold a `Weak` reference, so
/// dropping the future drops the state, and the guard in `guard_slot` with it.
///
/// Ideally, the gate is closed exactly while the task is woken up but not
/// polled yet. This is best effort: `state_machine` and `guard_slot` are two
/// separate atomics, and we cannot observe when tokio starts running the task.
/// When a race makes us deviate from the ideal, we always close the gate less
/// than in the ideal world, never more: some wake-ups miss their boost, but the
/// gate is never kept closed while the actor runs.
///
/// The only exception is a window of a few instructions in a wake-up, between
/// storing the guard and re-checking the state (see `apply_event`).
///
/// Invariant: a guard in `guard_slot` means the task has been woken up and
/// will be polled again: either the waker stored it and was forwarded to tokio,
/// or a poll returned `Pending` after a wake-up received during that poll. In
/// both cases a poll follows and drops the guard, unless the future is dropped
/// first. See `high_priority_state_machine` for the races.
struct HighPriorityState {
    gate: PriorityGate,
    guard_slot: ArcSwapOption<NotifyGuard>,
    state_machine: AtomicState,
}

impl HighPriorityState {
    /// Applies `event` to the state machine, and closes or opens the gate
    /// accordingly:
    /// - `Wake` while waiting closes the gate. A wake-up during a poll is only recorded.
    /// - `PollStart` opens the gate: the task is running.
    /// - `PollPending` after a wake-up received during the poll closes the gate: tokio polls the
    ///   task again, and that poll deserves the boost too.
    fn apply_event(&self, event: Event) {
        let (previous_state, new_state) = self.state_machine.apply_event(event);
        match (event, previous_state, new_state) {
            (Event::Wake, State::Waiting, State::Woken) => {
                self.store_guard();
                // A poll may have started between our transition and the store above.
                // That poll already cleared the (still empty) slot, so our guard would
                // keep the gate closed while the actor runs. We re-check the state, and
                // clear the slot if a poll is in progress. If the poll starts after the
                // re-check, its `PollStart` comes after our store, so the poll clears
                // the slot itself.
                let current_state = self.state_machine.load();
                if matches!(current_state, State::Polling | State::PollingWoken) {
                    self.guard_slot.store(None);
                }
            }
            (Event::PollStart, _, _) => {
                self.guard_slot.store(None);
            }
            (Event::PollPending, _, State::Woken) => {
                // We are inside the poll: no other poll can start concurrently.
                self.store_guard();
            }
            _ => {}
        }
    }

    fn store_guard(&self) {
        self.guard_slot.store(Some(self.gate.acquire_guard()));
    }
}

/// Waker handed to the actor loop of a high priority actor.
struct HighPriorityWaker {
    // `Weak`: wakers can outlive the future, for instance in a channel.
    state: Weak<HighPriorityState>,
    task_waker: Waker,
}

impl Wake for HighPriorityWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        // The future holds the only strong reference to the state, so the upgrade
        // fails if and only if the future was dropped. The inner future, the only
        // user of this waker, was dropped with it: there is nothing to prioritize,
        // and nothing to wake up.
        //
        // If the upgrade succeeds while the future is being dropped, our temporary
        // `Arc` keeps the state alive until we return, and the guard is dropped
        // along with the state.
        let Some(state) = self.state.upgrade() else {
            return;
        };
        // The gate must be closed before the task can be scheduled.
        state.apply_event(Event::Wake);
        self.task_waker.wake_by_ref();
    }
}

/// Wraps the actor loop of a high priority actor.
///
/// Between the moment the actor is woken up and the moment it is polled, the gate of its
/// runtime is closed: low priority actors wait before handling their next message.
///
/// A wake-up received during a poll does not close the gate right away, so the
/// gate is not kept closed while the actor runs a long handler. Tokio polls the
/// task again after such a wake-up, so the guard is stored when the current poll
/// returns `Pending`.
///
/// Invariants:
/// - the guard stored by a wake-up is dropped by the next poll, or with the future.
/// - wakers kept after the future is dropped cannot upgrade their `Weak` reference, so they cannot
///   close the gate.
///
/// Precondition: the task polling this future must poll it again after being
/// woken up (no `select!` around it). Otherwise, the gate stays closed until
/// the future is dropped.
pub(super) struct HighPriorityFuture<F> {
    inner: Pin<Box<F>>,
    state: Arc<HighPriorityState>,
    // Rebuilt only if the task waker changes.
    waker_cache: Option<CachedWrappedWaker>,
}

/// Wrapped waker handed to the inner future, together with the task waker it
/// was built from.
struct CachedWrappedWaker {
    original_waker: Waker,
    wrapped_waker: Waker,
}

impl<F: Future> HighPriorityFuture<F> {
    pub(super) fn new(inner: F, gate: PriorityGate) -> Self {
        let state = HighPriorityState {
            gate,
            guard_slot: ArcSwapOption::empty(),
            state_machine: AtomicState::new(State::Waiting),
        };
        HighPriorityFuture {
            inner: Box::pin(inner),
            state: Arc::new(state),
            waker_cache: None,
        }
    }

    fn wrapped_waker(&mut self, task_waker: &Waker) -> Waker {
        if let Some(cached_wrapped_waker) = &self.waker_cache
            && cached_wrapped_waker.original_waker.will_wake(task_waker)
        {
            return cached_wrapped_waker.wrapped_waker.clone();
        }
        let high_priority_waker = HighPriorityWaker {
            state: Arc::downgrade(&self.state),
            task_waker: task_waker.clone(),
        };
        let wrapped_waker = Waker::from(Arc::new(high_priority_waker));
        self.waker_cache = Some(CachedWrappedWaker {
            original_waker: task_waker.clone(),
            wrapped_waker: wrapped_waker.clone(),
        });
        wrapped_waker
    }
}

impl<F: Future> Future for HighPriorityFuture<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        // `HighPriorityFuture` is `Unpin`: the inner future is boxed.
        self.state.apply_event(Event::PollStart);
        let wrapped_waker = self.wrapped_waker(cx.waker());
        let mut wrapped_cx = Context::from_waker(&wrapped_waker);
        let poll_result = self.inner.as_mut().poll(&mut wrapped_cx);
        if poll_result.is_pending() {
            self.state.apply_event(Event::PollPending);
        }
        poll_result
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;
    use crate::priority::tests::{FlagWaker, num_guards, poll_once, poll_with_waker};

    /// Future that stays pending until `is_ready` is set.
    ///
    /// It keeps the last waker it was polled with, so that tests can call it
    /// like a channel would.
    struct ManualFuture {
        is_ready: Arc<AtomicBool>,
        waker_opt: Arc<Mutex<Option<Waker>>>,
        // If true wake is called within the poll impl.
        wake_during_poll: bool,
    }

    struct ManualFutureHandle {
        is_ready: Arc<AtomicBool>,
        waker_opt: Arc<Mutex<Option<Waker>>>,
    }

    impl ManualFutureHandle {
        fn wake(&self) {
            let waker = self
                .waker_opt
                .lock()
                .unwrap()
                .clone()
                .expect("the future should have been polled");
            waker.wake();
        }

        fn set_ready(&self) {
            self.is_ready.store(true, Ordering::SeqCst);
        }
    }

    fn manual_future(wake_during_poll: bool) -> (ManualFuture, ManualFutureHandle) {
        let is_ready = Arc::new(AtomicBool::new(false));
        let waker_opt = Arc::new(Mutex::new(None));
        let manual_future = ManualFuture {
            is_ready: is_ready.clone(),
            waker_opt: waker_opt.clone(),
            wake_during_poll,
        };
        let handle = ManualFutureHandle {
            is_ready,
            waker_opt,
        };
        (manual_future, handle)
    }

    impl Future for ManualFuture {
        type Output = ();

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.is_ready.load(Ordering::SeqCst) {
                return Poll::Ready(());
            }
            *self.waker_opt.lock().unwrap() = Some(cx.waker().clone());
            if self.wake_during_poll {
                cx.waker().wake_by_ref();
            }
            Poll::Pending
        }
    }

    #[test]
    fn test_high_priority_future_wake_closes_gate_until_polled() {
        let gate = PriorityGate::new();
        let (inner, handle) = manual_future(false);
        let mut high_priority_future = HighPriorityFuture::new(inner, gate.clone());
        let flag_waker = Arc::new(FlagWaker::default());
        let task_waker = Waker::from(flag_waker.clone());
        assert!(poll_with_waker(&mut high_priority_future, &task_waker).is_pending());
        assert!(gate.is_open());
        assert!(!flag_waker.is_woken.load(Ordering::SeqCst));

        handle.wake();
        // The wake-up is forwarded to the task.
        assert!(flag_waker.is_woken.load(Ordering::SeqCst));
        assert_eq!(num_guards(&gate), 1);
        // A second wake-up before the poll does not count twice.
        handle.wake();
        assert_eq!(num_guards(&gate), 1);

        assert!(poll_once(&mut high_priority_future).is_pending());
        assert!(gate.is_open());

        handle.set_ready();
        handle.wake();
        assert_eq!(num_guards(&gate), 1);
        assert!(poll_once(&mut high_priority_future).is_ready());
        assert!(gate.is_open());
    }

    #[test]
    fn test_high_priority_future_drop_while_ready_reopens_gate() {
        let gate = PriorityGate::new();
        let (inner, handle) = manual_future(false);
        let mut high_priority_future = HighPriorityFuture::new(inner, gate.clone());
        assert!(poll_once(&mut high_priority_future).is_pending());
        handle.wake();
        assert_eq!(num_guards(&gate), 1);
        drop(high_priority_future);
        assert!(gate.is_open());
    }

    #[test]
    fn test_high_priority_future_wake_during_poll_stores_guard_on_pending() {
        let gate = PriorityGate::new();
        let (inner, handle) = manual_future(true);
        let mut high_priority_future = HighPriorityFuture::new(inner, gate.clone());
        // The inner future wakes itself up during the poll. Tokio polls the task
        // again, so the guard is stored when the poll returns `Pending`.
        assert!(poll_once(&mut high_priority_future).is_pending());
        assert_eq!(num_guards(&gate), 1);

        // The next poll drops the guard. The inner future completes without
        // waking itself up, so no new guard is stored.
        handle.set_ready();
        assert!(poll_once(&mut high_priority_future).is_ready());
        assert!(gate.is_open());

        drop(high_priority_future);
        // The channel may keep the waker after the future is dropped.
        handle.wake();
        assert!(gate.is_open());
    }
}
