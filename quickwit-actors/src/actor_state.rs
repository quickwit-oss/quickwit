// Copyright (C) 2021 Quickwit, Inc.
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
#[derive(Clone, Debug, Copy, Eq, PartialEq)]
pub enum ActorState {
    /// Processing implies that the actor has some message(s) to process.
    Processing = 0,
    /// Idle means that the actor is currently waiting for messages.
    Idle = 1,
    /// Pause means that the actor processes no message but can process commands.
    Paused = 2,
    /// Exit means that the actor exited and cannot return to any other states.
    Exit = 3,
}

impl From<u32> for ActorState {
    fn from(actor_state_u32: u32) -> Self {
        match actor_state_u32 {
            0 => ActorState::Processing,
            1 => ActorState::Idle,
            2 => ActorState::Paused,
            3 => ActorState::Exit,
            _ => {
                panic!(
                    "Found forbidden u32 value for ActorState `{}`. This should never happen.",
                    actor_state_u32
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
        *self == ActorState::Idle || *self == ActorState::Processing
    }
}

pub struct AtomicState(AtomicU32);

impl Default for AtomicState {
    fn default() -> Self {
        AtomicState(AtomicU32::new(ActorState::Processing as u32))
    }
}

impl AtomicState {
    pub fn process(&self) {
        let _ = self.0.compare_exchange(
            ActorState::Idle as u32,
            ActorState::Processing as u32,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
    }

    pub fn idle(&self) {
        let _ = self.0.compare_exchange(
            ActorState::Processing as u32,
            ActorState::Idle as u32,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
    }

    pub fn pause(&self) {
        let _ = self
            .0
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |state| {
                if ActorState::from(state).is_running() {
                    return Some(ActorState::Paused as u32);
                }
                None
            });
    }

    pub fn resume(&self) {
        let _ = self.0.compare_exchange(
            ActorState::Paused as u32,
            ActorState::Processing as u32,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
    }

    pub fn exit(&self) {
        self.0.store(ActorState::Exit as u32, Ordering::Release);
    }

    pub fn get_state(&self) -> ActorState {
        ActorState::from(self.0.load(Ordering::Acquire))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    enum Operation {
        Process,
        Idle,
        Pause,
        Resume,
        Quit,
    }

    impl Operation {
        fn apply(&self, state: &AtomicState) {
            match self {
                Operation::Process => {
                    state.process();
                }
                Operation::Idle => {
                    state.idle();
                }
                Operation::Pause => {
                    state.pause();
                }
                Operation::Resume => state.resume(),
                Operation::Quit => state.exit(),
            }
        }
    }

    fn test_transition(from_state: ActorState, op: Operation, expected_state: ActorState) {
        let state = AtomicState::from(from_state);
        op.apply(&state);
        assert_eq!(state.get_state(), expected_state);
    }

    #[test]
    fn test_atomic_state_from_running() {
        test_transition(ActorState::Idle, Operation::Process, ActorState::Processing);
        test_transition(ActorState::Processing, Operation::Idle, ActorState::Idle);
        test_transition(ActorState::Processing, Operation::Pause, ActorState::Paused);
        test_transition(ActorState::Idle, Operation::Pause, ActorState::Paused);
        test_transition(
            ActorState::Processing,
            Operation::Resume,
            ActorState::Processing,
        );
        test_transition(ActorState::Processing, Operation::Quit, ActorState::Exit);

        test_transition(ActorState::Paused, Operation::Pause, ActorState::Paused);
        test_transition(
            ActorState::Paused,
            Operation::Resume,
            ActorState::Processing,
        );
        test_transition(ActorState::Paused, Operation::Quit, ActorState::Exit);

        test_transition(ActorState::Exit, Operation::Process, ActorState::Exit);
        test_transition(ActorState::Exit, Operation::Idle, ActorState::Exit);
        test_transition(ActorState::Exit, Operation::Pause, ActorState::Exit);
        test_transition(ActorState::Exit, Operation::Resume, ActorState::Exit);
        test_transition(ActorState::Exit, Operation::Quit, ActorState::Exit);
    }
}
