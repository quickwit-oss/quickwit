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

//! State machine of a `HighPriorityFuture`.
//!
//! It tracks whether the future has been woken up since its last poll, so that
//! the priority guard covers exactly the time during which the task is woken up
//! but not polled:
//! - a wake-up while waiting stores the guard right away (`Waiting` -> `Woken`).
//! - a wake-up during a poll is only recorded (`Polling` -> `PollingWoken`). Tokio polls the task
//!   again after a wake-up received during a poll, so the guard is stored when the current poll
//!   returns `Pending` (`PollingWoken` -> `Woken`). It is never stored while the actor is running.
//! - the start of a poll always drops the guard.
//!
//! Races between a wake-up and a poll. Both are rare, and neither can leave
//! the gate closed for good:
//!
//! 1. Missed boost. Tokio marks the task as running (`transition_to_running`) before calling our
//!    `poll`. Consider:
//!    - the state is `Waiting`;
//!    - tokio starts polling the task;
//!    - a wake-up arrives before our `poll` applies `PollStart`. It moves `Waiting` -> `Woken` and
//!      stores a guard;
//!    - our `poll` applies `PollStart`, moving `Woken` -> `Polling`, and drops the guard.
//!
//!    Tokio saw the wake-up while the task was running, so it polls the task again. But
//!    our state looks like an ordinary poll triggered by that wake-up: nothing records
//!    that a second poll is due, and it runs without the boost. We cannot detect this
//!    case, because we cannot observe tokio's `transition_to_running`.
//!
//! 2. Gate closed during a poll. Consider:
//!    - a wake-up moves `Waiting` -> `Woken`, but has not stored its guard yet;
//!    - a poll starts, moves `Woken` -> `Polling`, and drops the (still empty) guard slot;
//!    - the wake-up stores its guard.
//!
//!    Without care, the guard would keep the gate closed for the rest of the current
//!    poll, while the actor is running. To avoid this, the wake-up re-checks the state
//!    after storing its guard, and clears the slot if a poll is in progress (see
//!    `HighPriorityState::apply_event`). If the guard is cleared, the
//!    boost of a concurrent wake-up may be lost.

use std::sync::atomic::{AtomicU8, Ordering};

use tracing::warn;

/// State of a `HighPriorityFuture`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(super) enum State {
    /// Pending, not woken up since the last poll.
    Waiting = 0,
    /// Woken up while waiting. A guard is stored.
    Woken = 1,
    /// Being polled, not woken up yet.
    Polling = 2,
    /// Woken up during the current poll. The guard is stored when the poll returns
    /// `Pending`.
    PollingWoken = 3,
}

impl From<u8> for State {
    fn from(state: u8) -> Self {
        match state {
            0 => State::Waiting,
            1 => State::Woken,
            2 => State::Polling,
            3 => State::PollingWoken,
            _ => unreachable!("only `State` values are stored in `AtomicState`"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Event {
    /// The waker was called.
    Wake,
    /// A poll starts.
    PollStart,
    /// The poll of the inner future returned `Pending`.
    PollPending,
}

/// Pure transition function.
///
/// Combinations that cannot happen (for instance, a poll starting while already
/// polling) are reported in debug builds, and keep the state unchanged.
fn transition(state: State, event: Event) -> State {
    match (state, event) {
        (State::Waiting, Event::Wake) => State::Woken,
        (State::Polling, Event::Wake) => State::PollingWoken,
        (State::Woken | State::PollingWoken, Event::Wake) => state,
        (State::Waiting | State::Woken, Event::PollStart) => State::Polling,
        (State::Polling, Event::PollPending) => State::Waiting,
        (State::PollingWoken, Event::PollPending) => State::Woken,
        (State::Polling | State::PollingWoken, Event::PollStart)
        | (State::Waiting | State::Woken, Event::PollPending) => {
            debug_assert!(false, "unexpected event {event:?} in state {state:?}");
            warn!("unexpected event {event:?} in state {state:?}");
            state
        }
    }
}

/// `State` stored in an `AtomicU8`.
pub(super) struct AtomicState(AtomicU8);

impl AtomicState {
    pub(super) fn new(state: State) -> Self {
        AtomicState(AtomicU8::new(state as u8))
    }

    pub(super) fn load(&self) -> State {
        self.0.load(Ordering::SeqCst).into()
    }

    /// Applies `event` atomically: computes the transition from the current state,
    /// and retries until the compare-and-swap succeeds.
    ///
    /// Returns `(previous_state, new_state)`.
    ///
    /// `Relaxed` is enough:
    /// - the state is a single atomic location, only updated by compare-and-swap. Its modification
    ///   order is total whatever the ordering, so each event applies on top of the latest state.
    /// - the state does not publish any other memory. The guard lives in an `ArcSwapOption`, which
    ///   synchronizes itself, and we never infer its content from the state.
    /// - the happens-before edge between a wake-up and the next poll comes from tokio: the waker
    ///   calls the task waker after its transition.
    pub(super) fn apply_event(&self, event: Event) -> (State, State) {
        let mut current_state: State = self.0.load(Ordering::Relaxed).into();
        loop {
            let new_state = transition(current_state, event);
            match self.0.compare_exchange_weak(
                current_state as u8,
                new_state as u8,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return (current_state, new_state),
                Err(observed_state) => current_state = observed_state.into(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transition_table() {
        let transitions: [(State, Event, State); 8] = [
            (State::Waiting, Event::Wake, State::Woken),
            (State::Woken, Event::Wake, State::Woken),
            (State::Polling, Event::Wake, State::PollingWoken),
            (State::PollingWoken, Event::Wake, State::PollingWoken),
            (State::Waiting, Event::PollStart, State::Polling),
            (State::Woken, Event::PollStart, State::Polling),
            (State::Polling, Event::PollPending, State::Waiting),
            (State::PollingWoken, Event::PollPending, State::Woken),
        ];
        for (state, event, expected_state) in transitions {
            assert_eq!(
                transition(state, event),
                expected_state,
                "transition({state:?}, {event:?})"
            );
        }
    }

    #[test]
    fn test_apply_event_returns_previous_and_new_state() {
        let atomic_state = AtomicState::new(State::Waiting);
        assert_eq!(
            atomic_state.apply_event(Event::Wake),
            (State::Waiting, State::Woken)
        );
        assert_eq!(
            atomic_state.apply_event(Event::Wake),
            (State::Woken, State::Woken)
        );
    }
}
