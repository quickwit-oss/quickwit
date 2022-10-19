// Copyright (C) 2022 Quickwit, Inc.
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

use std::sync::{atomic::{AtomicU32, Ordering}, Arc};

use crate::progress::{ProgressState, Progress};

#[derive(Clone, Default)]
pub struct ActorState {
    // TODO make it a single Arc
    pub progress: Arc<Progress>,
    pub state_id: Arc<AtomicState>,
}


impl ActorState {

    pub fn record_progress(&self) {
        self.progress.record_progress();
    }

    pub fn registered_activity_since_last_call(&self) -> bool {
        self.progress.registered_activity_since_last_call()
    }

    pub fn protect_zone(&self) -> ProtectedZoneGuard {
        loop {
            let previous_state: ProgressState = self.progress.0.load(Ordering::SeqCst).into();
            let new_state = match previous_state {
                ProgressState::NoUpdate | ProgressState::Updated => ProgressState::ProtectedZone(0),
                ProgressState::ProtectedZone(level) => ProgressState::ProtectedZone(level + 1),
            };
            if self
                .progress
                .0
                .compare_exchange(
                    previous_state.into(),
                    new_state.into(),
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
            {
                return ProtectedZoneGuard(self.clone());
            }
        }
    }
}


pub struct ProtectedZoneGuard(ActorState);

impl Drop for ProtectedZoneGuard {
    fn drop(&mut self) {
        let previous_state: ProgressState = self.0.progress.0.fetch_sub(1, Ordering::SeqCst).into();
        assert!(matches!(previous_state, ProgressState::ProtectedZone(_)));
    }
}

#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActorStateId {
    /// Processing implies that the actor has some message(s) (this includes commands) to process.
    Processing = 0,
    /// Idle means that the actor is currently waiting for messages.
    Idle = 1,
    /// Pause means that the actor processes no message but can process commands.
    Paused = 2,
    /// Success means that the actor exited and cannot return to any other states.
    Success = 3,
    /// Failure means that the actor exited with a failure or panicked.
    Failure = 4,
}

impl From<u32> for ActorStateId {
    fn from(actor_state_u32: u32) -> Self {
        match actor_state_u32 {
            0 => ActorStateId::Processing,
            1 => ActorStateId::Idle,
            2 => ActorStateId::Paused,
            3 => ActorStateId::Success,
            4 => ActorStateId::Failure,
            _ => {
                panic!(
                    "Found forbidden u32 value for ActorState `{}`. This should never happen.",
                    actor_state_u32
                );
            }
        }
    }
}

impl From<ActorStateId> for AtomicState {
    fn from(state: ActorStateId) -> Self {
        AtomicState(AtomicU32::from(state as u32))
    }
}

impl ActorStateId {
    pub fn is_running(&self) -> bool {
        *self == ActorStateId::Idle || *self == ActorStateId::Processing
    }

    pub fn is_exit(&self) -> bool {
        match self {
            ActorStateId::Processing | ActorStateId::Idle | ActorStateId::Paused => false,
            ActorStateId::Success | ActorStateId::Failure => true,
        }
    }
}

pub struct AtomicState(AtomicU32);

impl Default for AtomicState {
    fn default() -> Self {
        AtomicState(AtomicU32::new(ActorStateId::Processing as u32))
    }
}

impl AtomicState {
    pub fn process(&self) {
        let _ = self.0.compare_exchange(
            ActorStateId::Idle as u32,
            ActorStateId::Processing as u32,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
    }

    pub fn idle(&self) {
        let _ = self.0.compare_exchange(
            ActorStateId::Processing as u32,
            ActorStateId::Idle as u32,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
    }

    pub fn pause(&self) {
        let _ = self
            .0
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |state| {
                if ActorStateId::from(state).is_running() {
                    return Some(ActorStateId::Paused as u32);
                }
                None
            });
    }

    pub fn resume(&self) {
        let _ = self.0.compare_exchange(
            ActorStateId::Paused as u32,
            ActorStateId::Processing as u32,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
    }

    pub(crate) fn exit(&self, success: bool) {
        let new_state = if success {
            ActorStateId::Success
        } else {
            ActorStateId::Failure
        };
        self.0.fetch_max(new_state as u32, Ordering::Release);
    }

    pub fn get_state(&self) -> ActorStateId {
        ActorStateId::from(self.0.load(Ordering::Acquire))
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
        ExitSuccess,
        ExitFailure,
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
                Operation::ExitSuccess => state.exit(true),
                Operation::ExitFailure => state.exit(false),
            }
        }
    }

    #[track_caller]
    fn test_transition(from_state: ActorStateId, op: Operation, expected_state: ActorStateId) {
        let state = AtomicState::from(from_state);
        op.apply(&state);
        assert_eq!(state.get_state(), expected_state);
    }

    #[test]
    fn test_atomic_state_from_running() {
        test_transition(ActorStateId::Idle, Operation::Process, ActorStateId::Processing);
        test_transition(ActorStateId::Processing, Operation::Idle, ActorStateId::Idle);
        test_transition(ActorStateId::Processing, Operation::Pause, ActorStateId::Paused);
        test_transition(ActorStateId::Idle, Operation::Pause, ActorStateId::Paused);
        test_transition(
            ActorStateId::Processing,
            Operation::Resume,
            ActorStateId::Processing,
        );
        test_transition(
            ActorStateId::Processing,
            Operation::ExitSuccess,
            ActorStateId::Success,
        );
        test_transition(ActorStateId::Paused, Operation::Pause, ActorStateId::Paused);
        test_transition(
            ActorStateId::Paused,
            Operation::Resume,
            ActorStateId::Processing,
        );
        test_transition(
            ActorStateId::Paused,
            Operation::ExitSuccess,
            ActorStateId::Success,
        );
        test_transition(
            ActorStateId::Success,
            Operation::ExitFailure,
            ActorStateId::Failure,
        );

        test_transition(ActorStateId::Success, Operation::Process, ActorStateId::Success);
        test_transition(ActorStateId::Success, Operation::Idle, ActorStateId::Success);
        test_transition(ActorStateId::Success, Operation::Pause, ActorStateId::Success);
        test_transition(ActorStateId::Success, Operation::Resume, ActorStateId::Success);
        test_transition(
            ActorStateId::Success,
            Operation::ExitSuccess,
            ActorStateId::Success,
        );

        test_transition(ActorStateId::Failure, Operation::Process, ActorStateId::Failure);
        test_transition(ActorStateId::Failure, Operation::Idle, ActorStateId::Failure);
        test_transition(ActorStateId::Failure, Operation::Pause, ActorStateId::Failure);
        test_transition(ActorStateId::Failure, Operation::Resume, ActorStateId::Failure);
        test_transition(
            ActorStateId::Failure,
            Operation::ExitSuccess,
            ActorStateId::Failure,
        );
        test_transition(
            ActorStateId::Failure,
            Operation::ExitFailure,
            ActorStateId::Failure,
        );
    }

    #[test]
    fn test_progress_protect_zone() {
        let actor_state = ActorState::default();
        assert!(actor_state.registered_activity_since_last_call());
        actor_state.record_progress();
        assert!(actor_state.registered_activity_since_last_call());
        {
            let _protect_guard = actor_state.protect_zone();
            assert!(actor_state.registered_activity_since_last_call());
            assert!(actor_state.registered_activity_since_last_call());
        }
        assert!(actor_state.registered_activity_since_last_call());
        assert!(!actor_state.registered_activity_since_last_call());
    }

    #[test]
    fn test_progress_several_protect_zone() {
        let actor_state = ActorState::default();
        assert!(actor_state.registered_activity_since_last_call());
        actor_state.record_progress();
        assert!(actor_state.registered_activity_since_last_call());
        let first_protect_guard = actor_state.protect_zone();
        let second_protect_guard = actor_state.protect_zone();
        assert!(actor_state.registered_activity_since_last_call());
        assert!(actor_state.registered_activity_since_last_call());
        std::mem::drop(first_protect_guard);
        assert!(actor_state.registered_activity_since_last_call());
        assert!(actor_state.registered_activity_since_last_call());
        std::mem::drop(second_protect_guard);
        assert!(actor_state.registered_activity_since_last_call());
        assert!(!actor_state.registered_activity_since_last_call());
    }
}
