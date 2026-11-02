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
use std::task::{Context, Poll};

use tokio::sync::futures::OwnedNotified;

use super::PriorityGate;

/// Wraps a future of a low priority actor.
///
/// The inner future is only polled while no high priority actor of the same
/// runtime is waiting to run. When it wraps the receive future, the actor does
/// not even take the message out of its mailbox while the gate is closed.
///
/// Wrapping `std::future::ready(())` simply waits for the gate to open.
pub(super) struct LowPriorityFuture<'a, F> {
    gate: &'a PriorityGate,
    inner: F,
    // Created when a poll finds the gate closed, dropped when the gate opens.
    // When the gate is open, we do not allocate anything.
    notified_opt: Option<Pin<Box<OwnedNotified>>>,
}

impl<'a, F: Future + Unpin> LowPriorityFuture<'a, F> {
    pub(super) fn new(gate: &'a PriorityGate, inner: F) -> Self {
        LowPriorityFuture {
            gate,
            inner,
            notified_opt: None,
        }
    }

    /// Returns `Poll::Ready` if the gate is open. Otherwise, makes sure the
    /// task is woken up when the gate opens.
    fn poll_gate_open(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let gate = self.gate;
        if gate.is_open() {
            // Unregisters from the gate, if we were registered.
            self.notified_opt = None;
            return Poll::Ready(());
        }
        let notified: &mut Pin<Box<OwnedNotified>> = self
            .notified_opt
            .get_or_insert_with(|| Box::pin(gate.notify.clone().notified_owned()));
        // `notified` is always created BEFORE checking the gate: a `Notified`
        // receives every `notify_waiters()` call made after its creation, even
        // before it is polled. A `notify_waiters()` call between the check and
        // the poll is therefore not lost.
        loop {
            if gate.is_open() {
                self.notified_opt = None;
                return Poll::Ready(());
            }
            if notified.as_mut().poll(cx).is_pending() {
                return Poll::Pending;
            }
            // We were notified, but another high priority actor may have closed
            // the gate again in the meantime. Register again and re-check.
            notified.set(gate.notify.clone().notified_owned());
        }
    }
}

impl<F: Future + Unpin> Future for LowPriorityFuture<'_, F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        // `LowPriorityFuture` is `Unpin`: `F` is `Unpin` and the notified future is boxed.
        let this = &mut *self;
        if this.poll_gate_open(cx).is_pending() {
            return Poll::Pending;
        }
        Pin::new(&mut this.inner).poll(cx)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Waker;

    use super::*;
    use crate::priority::tests::{FlagWaker, poll_once, poll_with_waker};

    #[test]
    fn test_low_priority_future_resolves_when_gate_opens() {
        let gate = PriorityGate::new();
        let guard_1 = gate.acquire_guard();
        let mut low_priority_future = LowPriorityFuture::new(&gate, std::future::ready(()));
        let flag_waker = Arc::new(FlagWaker::default());
        let task_waker = Waker::from(flag_waker.clone());
        assert!(poll_with_waker(&mut low_priority_future, &task_waker).is_pending());

        let guard_2 = gate.acquire_guard();

        // Dropping a guard that is not the last one keeps the gate closed.
        drop(guard_1);
        assert!(!flag_waker.is_woken.load(Ordering::SeqCst));
        assert!(poll_with_waker(&mut low_priority_future, &task_waker).is_pending());

        drop(guard_2);
        assert!(flag_waker.is_woken.load(Ordering::SeqCst));
        assert!(poll_with_waker(&mut low_priority_future, &task_waker).is_ready());
    }

    #[test]
    fn test_low_priority_future_polls_inner_only_when_gate_open() {
        let gate = PriorityGate::new();
        let num_inner_polls = AtomicUsize::new(0);
        let inner = std::future::poll_fn(|_cx| {
            let num_polls = num_inner_polls.fetch_add(1, Ordering::SeqCst) + 1;
            if num_polls < 2 {
                Poll::Pending
            } else {
                Poll::Ready(num_polls)
            }
        });
        let mut low_priority_future = LowPriorityFuture::new(&gate, inner);
        assert!(poll_once(&mut low_priority_future).is_pending());
        assert_eq!(num_inner_polls.load(Ordering::SeqCst), 1);

        let guard = gate.acquire_guard();
        assert!(poll_once(&mut low_priority_future).is_pending());
        assert_eq!(num_inner_polls.load(Ordering::SeqCst), 1);

        drop(guard);
        assert_eq!(poll_once(&mut low_priority_future), Poll::Ready(2));
    }
}
