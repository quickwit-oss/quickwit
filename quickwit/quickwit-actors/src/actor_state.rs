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

use std::sync::atomic::{AtomicU32, Ordering};

#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActorState {
    /// Running means that the actor consumes and processes both low priority messages (regular
    /// message) and high priority message commands.
    Running = 0,
    /// Pause means that the actor only consumes and processes high priority messages. Typically
    /// commands as well as scheduled messages.
    Paused = 1,
    /// Success means that the actor exited and cannot return to any other states.
    Success = 2,
    /// Failure means that the actor exited with a failure or panicked.
    Failure = 3,
}

impl From<u32> for ActorState {
    fn from(actor_state_u32: u32) -> Self {
        match actor_state_u32 {
            0 => ActorState::Running,
            1 => ActorState::Paused,
            2 => ActorState::Success,
            3 => ActorState::Failure,
            _ => {
                panic!(
                    "Found forbidden u32 value for ActorState `{actor_state_u32}`. This should \
                     never happen."
                );
            }
        }
    }
}

impl From<ActorState> for AtomicState {
    fn from(state: ActorState) -> Self {
        AtomicState(AtomicU32::from(state as u32))
    }
}

impl ActorState {
    pub fn is_running(&self) -> bool {
        *self == ActorState::Running
    }

    pub fn is_exit(&self) -> bool {
        match self {
            ActorState::Running | ActorState::Paused => false,
            ActorState::Success | ActorState::Failure => true,
        }
    }
}

pub(crate) struct AtomicState(AtomicU32);

impl Default for AtomicState {
    fn default() -> Self {
        AtomicState(AtomicU32::new(ActorState::Running as u32))
    }
}

impl AtomicState {
    pub(crate) fn pause(&self) {
        let _ = self
            .0
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |state| {
                if ActorState::from(state).is_running() {
                    return Some(ActorState::Paused as u32);
                }
                None
            });
    }

    pub(crate) fn resume(&self) {
        let _ = self.0.compare_exchange(
            ActorState::Paused as u32,
            ActorState::Running as u32,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
    }

    pub(crate) fn exit(&self, success: bool) {
        let new_state = if success {
            ActorState::Success
        } else {
            ActorState::Failure
        };
        self.0.fetch_max(new_state as u32, Ordering::Release);
    }

    pub fn get_state(&self) -> ActorState {
        ActorState::from(self.0.load(Ordering::Acquire))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    enum Operation {
        Pause,
        Resume,
        ExitSuccess,
        ExitFailure,
    }

    impl Operation {
        fn apply(&self, state: &AtomicState) {
            match self {
                Operation::Pause => {
                    state.pause();
                }
                Operation::Resume => state.resume(),
                Operation::ExitSuccess => state.exit(true),
                Operation::ExitFailure => state.exit(false),
            }
        }
    }

    #[track_caller]
    fn test_transition(from_state: ActorState, op: Operation, expected_state: ActorState) {
        let state = AtomicState::from(from_state);
        op.apply(&state);
        assert_eq!(state.get_state(), expected_state);
    }

    #[test]
    fn test_atomic_state_from_running() {
        test_transition(ActorState::Running, Operation::Pause, ActorState::Paused);
        test_transition(ActorState::Running, Operation::Resume, ActorState::Running);
        test_transition(
            ActorState::Running,
            Operation::ExitSuccess,
            ActorState::Success,
        );
        test_transition(ActorState::Paused, Operation::Pause, ActorState::Paused);
        test_transition(ActorState::Paused, Operation::Resume, ActorState::Running);
        test_transition(
            ActorState::Paused,
            Operation::ExitSuccess,
            ActorState::Success,
        );
        test_transition(
            ActorState::Success,
            Operation::ExitFailure,
            ActorState::Failure,
        );

        test_transition(ActorState::Success, Operation::Pause, ActorState::Success);
        test_transition(ActorState::Success, Operation::Resume, ActorState::Success);
        test_transition(
            ActorState::Success,
            Operation::ExitSuccess,
            ActorState::Success,
        );

        test_transition(ActorState::Failure, Operation::Pause, ActorState::Failure);
        test_transition(ActorState::Failure, Operation::Resume, ActorState::Failure);
        test_transition(
            ActorState::Failure,
            Operation::ExitSuccess,
            ActorState::Failure,
        );
        test_transition(
            ActorState::Failure,
            Operation::ExitFailure,
            ActorState::Failure,
        );
    }
}
