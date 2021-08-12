// Quickwit
//  Copyright (C) 2021 Quickwit Inc.
//
//  Quickwit is offered under the AGPL v3.0 and as commercial software.
//  For commercial licensing, contact us at hello@quickwit.io.
//
//  AGPL:
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Affero General Public License as
//  published by the Free Software Foundation, either version 3 of the
//  License, or (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Affero General Public License for more details.
//
//  You should have received a copy of the GNU Affero General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;

#[repr(u32)]
#[derive(Clone, Debug, Copy, Eq, PartialEq)]
pub enum ActorState {
    Running = 0,
    Paused = 1,
    Terminated = 2,
}

impl From<u32> for ActorState {
    fn from(actor_state_u32: u32) -> Self {
        match actor_state_u32 {
            0 => ActorState::Running,
            1 => ActorState::Paused,
            2 => ActorState::Terminated,
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

pub struct AtomicState(AtomicU32);

impl Default for AtomicState {
    fn default() -> Self {
        AtomicState(AtomicU32::new(ActorState::Running as u32))
    }
}

impl AtomicState {
    pub fn pause(&self) {
        let _ = self.0.compare_exchange(
            ActorState::Running as u32,
            ActorState::Paused as u32,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
    }

    pub fn resume(&self) {
        let _ = self.0.compare_exchange(
            ActorState::Paused as u32,
            ActorState::Running as u32,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
    }

    pub fn terminate(&self) {
        self.0
            .store(ActorState::Terminated as u32, Ordering::Release);
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
        Terminate,
    }

    impl Operation {
        fn apply(&self, state: &AtomicState) {
            match self {
                Operation::Pause => {
                    state.pause();
                }
                Operation::Resume => state.resume(),
                Operation::Terminate => state.terminate(),
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
        test_transition(ActorState::Running, Operation::Pause, ActorState::Paused);
        test_transition(ActorState::Running, Operation::Resume, ActorState::Running);
        test_transition(
            ActorState::Running,
            Operation::Terminate,
            ActorState::Terminated,
        );

        test_transition(ActorState::Paused, Operation::Pause, ActorState::Paused);
        test_transition(ActorState::Paused, Operation::Resume, ActorState::Running);
        test_transition(
            ActorState::Paused,
            Operation::Terminate,
            ActorState::Terminated,
        );

        test_transition(
            ActorState::Terminated,
            Operation::Pause,
            ActorState::Terminated,
        );
        test_transition(
            ActorState::Terminated,
            Operation::Resume,
            ActorState::Terminated,
        );
        test_transition(
            ActorState::Terminated,
            Operation::Terminate,
            ActorState::Terminated,
        );
    }
}
