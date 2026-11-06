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

//! Best effort scheduling priority between actors running on the same tokio runtime.
//!
//! Tokio has no notion of task priority, so priority is cooperative:
//! - the actor loop of a high priority actor is wrapped in a [`HighPriorityFuture`]. Between the
//!   moment the actor is woken up and the moment it is polled, the [`PriorityGate`] of its runtime
//!   is closed.
//! - the actor loop of a low priority actor is wrapped in a [`LowPriorityFuture`]: it is only
//!   polled while the gate is open. Since a poll may last for many messages, low priority actors
//!   also check the gate between messages.

#[cfg(test)]
mod end_to_end_tests;
mod high_priority_future;
mod high_priority_state_machine;
mod low_priority_future;

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, Weak};

use arc_swap::ArcSwapWeak;
use high_priority_future::HighPriorityFuture;
use low_priority_future::LowPriorityFuture;
use tokio::sync::Notify;

/// Scheduling priority of an actor.
///
/// Priority only matters between actors of the same universe that run on the
/// same tokio runtime (see `Actor::runtime_handle`).
///
/// When a high priority actor is woken up (new message, end of backpressure,
/// reply, timer...), the low priority actors of its runtime wait before
/// handling their next message, until the high priority actor has started
/// running.
///
/// This is best effort:
/// - a running poll is never interrupted. A low priority actor only steps aside between two
///   messages, or when one of its polls starts (for instance, after an `.await` in a handler).
/// - a wake-up received while the high priority actor is being polled is not prioritized.
///
/// For a high priority actor, `Actor::yield_after_each_message` returns `false` by
/// default. Overriding it to return `true` throws the boost away: the actor would
/// yield right after receiving its first message, and wait behind the other actors.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ActorPriority {
    #[default]
    Low,
    High,
}

/// Keeps the `PriorityGate` closed while it is alive.
///
/// All the guards of a gate are clones of the same `Arc`. Dropping the last
/// one opens the gate and wakes up the low priority actors waiting on it.
pub(crate) struct NotifyGuard {
    notify: Arc<Notify>,
    // Only false for a guard that was never installed in the gate.
    notify_on_drop: bool,
}

impl NotifyGuard {
    /// Drops a guard that was never installed in the gate, without waking up
    /// the waiters.
    ///
    /// Precondition: `uninstalled_guard` is the only reference to the guard.
    fn drop_uninstalled(uninstalled_guard: Arc<NotifyGuard>) {
        let mut guard = Arc::try_unwrap(uninstalled_guard)
            .ok()
            .expect("an uninstalled guard should have no other strong reference");
        guard.notify_on_drop = false;
    }
}

impl Drop for NotifyGuard {
    fn drop(&mut self) {
        if self.notify_on_drop {
            self.notify.notify_waiters();
        }
    }
}

/// Shared by the actors of a universe that run on the same tokio runtime.
///
/// The gate is closed while at least one high priority actor has been woken
/// up by a new message, but has not been polled yet. Such an actor holds an
/// `Arc<NotifyGuard>`.
///
/// Low priority actors wait for the gate to open without any timeout. This
/// cannot deadlock: a woken up high priority actor only waits for a free worker
/// thread of its runtime, never for a waiting low priority actor.
///
/// Precondition: the task running a `HighPriorityFuture` must poll it again
/// after each wake-up (no `select!` or timeout around it). The guard is only
/// dropped by the next poll, or with the future. If that poll never happens,
/// the gate stays closed and the low priority actors of the runtime wait forever.
#[derive(Clone)]
pub(crate) struct PriorityGate {
    // Weak reference to the shared guard. The gate is open when no strong
    // reference is left.
    guard: Arc<ArcSwapWeak<NotifyGuard>>,
    notify: Arc<Notify>,
}

impl PriorityGate {
    fn new() -> PriorityGate {
        PriorityGate {
            guard: Arc::new(ArcSwapWeak::new(Weak::new())),
            notify: Arc::new(Notify::new()),
        }
    }

    /// Runs `future` with the given priority on this gate:
    /// - `High`: while the future is woken up but not polled, the gate is closed.
    /// - `Low`: the future is only polled while the gate is open.
    ///
    /// A wake-up received while a high priority future is being polled is ignored, so a long
    /// poll never keeps the gate closed.
    ///
    /// A low priority future waits for the gate before each of its polls. A long running future
    /// is therefore paused at its `.await` points every time a high priority actor wakes up.
    pub(crate) async fn run_with_priority<F: Future>(
        &self,
        priority: ActorPriority,
        future: F,
    ) -> F::Output {
        match priority {
            ActorPriority::High => HighPriorityFuture::new(future, self.clone()).await,
            ActorPriority::Low => LowPriorityFuture::new(self, std::pin::pin!(future)).await,
        }
    }

    /// The strong count of the guard drops to 0 before `NotifyGuard::drop` calls
    /// `notify_waiters()`. Waiters must therefore register on `notify` before
    /// calling this method, so that they cannot miss the notification.
    pub fn is_open(&self) -> bool {
        self.guard.load().strong_count() == 0
    }

    /// Returns the shared guard, creating it if the gate is open.
    fn acquire_guard(&self) -> Arc<NotifyGuard> {
        let mut current_weak: Weak<NotifyGuard> = self.guard.load_full();
        if let Some(guard) = current_weak.upgrade() {
            return guard;
        }

        let new_guard_arc = Arc::new(NotifyGuard {
            notify: self.notify.clone(),
            notify_on_drop: true,
        });
        loop {
            let previous_weak = self
                .guard
                .compare_and_swap(&current_weak, Arc::downgrade(&new_guard_arc));
            if Weak::ptr_eq(&previous_weak, &current_weak) {
                return new_guard_arc;
            }
            if let Some(guard) = previous_weak.upgrade() {
                NotifyGuard::drop_uninstalled(new_guard_arc);
                return guard;
            }
            // The guard that beat us is already gone: retry from the current slot value.
            current_weak = Weak::clone(&previous_weak);
        }
    }
}

/// One `PriorityGate` per tokio runtime.
///
/// It lives in the `SpawnContext`, so it is shared by all the actors of a universe.
#[derive(Clone, Default)]
pub(crate) struct PriorityGates {
    gates: Arc<Mutex<HashMap<tokio::runtime::Id, PriorityGate>>>,
}

impl PriorityGates {
    pub(crate) fn gate_for_runtime(&self, runtime_handle: &tokio::runtime::Handle) -> PriorityGate {
        let mut gates = self.gates.lock().expect("lock should not be poisoned");
        gates
            .entry(runtime_handle.id())
            .or_insert_with(PriorityGate::new)
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::{Context, Poll, Wake, Waker};

    use super::*;

    pub(super) fn poll_with_waker<F: Future + Unpin>(
        future: &mut F,
        waker: &Waker,
    ) -> Poll<F::Output> {
        let mut cx = Context::from_waker(waker);
        Pin::new(future).poll(&mut cx)
    }

    pub(super) fn poll_once<F: Future + Unpin>(future: &mut F) -> Poll<F::Output> {
        poll_with_waker(future, Waker::noop())
    }

    pub(super) fn num_guards(gate: &PriorityGate) -> usize {
        gate.guard.load().strong_count()
    }

    #[derive(Default)]
    pub(super) struct FlagWaker {
        pub(super) is_woken: AtomicBool,
    }

    impl Wake for FlagWaker {
        fn wake(self: Arc<Self>) {
            self.is_woken.store(true, Ordering::SeqCst);
        }
    }

    #[test]
    fn test_acquire_guard_returns_shared_guard() {
        let gate = PriorityGate::new();
        let guard_1 = gate.acquire_guard();
        let guard_2 = gate.acquire_guard();
        assert!(Arc::ptr_eq(&guard_1, &guard_2));
        assert_eq!(num_guards(&gate), 2);
    }

    #[test]
    fn test_priority_gates_one_gate_per_runtime() {
        let runtime_1 = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let runtime_2 = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let priority_gates = PriorityGates::default();
        let gate_1 = priority_gates.gate_for_runtime(runtime_1.handle());
        let gate_1_again = priority_gates.gate_for_runtime(runtime_1.handle());
        let gate_2 = priority_gates.gate_for_runtime(runtime_2.handle());
        assert!(Arc::ptr_eq(&gate_1.notify, &gate_1_again.notify));
        assert!(!Arc::ptr_eq(&gate_1.notify, &gate_2.notify));
    }
}
