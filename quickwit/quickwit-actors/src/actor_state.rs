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

use std::panic::Location;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use crate::progress::Progress;

#[derive(Clone, Copy, Debug, Default)]
pub enum ProtectLocation {
    #[default]
    Idle,
    Waiting(&'static Location<'static>),
    Backpressure,
}

#[derive(Default)]
struct LockedState {
    root_state: ActorStateId,
    location_stack: Vec<ProtectLocation>,
}

#[derive(Clone, Default)]
pub struct ActorState {
    // TODO make it a single Arc
    pub progress: Arc<Progress>,
    pub state_id: Arc<AtomicState>,
    pub locked_state: Arc<Mutex<LockedState>>,
}

impl ActorState {
    pub fn record_progress(&self) {
        self.progress.record_progress();
    }

    pub fn registered_activity_since_last_call(&self) -> bool {
        self.progress.registered_activity_since_last_call()
    }

    /// This function returns a guard that prevents any supervisor from identifying the
    /// actor as dead.
    /// The protection ends when the `ProtectZoneGuard` is dropped.
    ///
    /// In an ideal world, you should never need to call this function.
    /// It is only useful in some corner cases, like calling a long blocking
    /// from an external library that you trust.
    #[track_caller]
    pub fn protect_zone(&self) -> ProtectedZoneGuard {
        let location = core::panic::Location::caller();
        let protect_location = ProtectLocation::Waiting(location);
        self.protect_zone_with_location(protect_location)
    }

    fn pop_stack(&self) {
        let mut locked_state = self.locked_state.lock().unwrap();
        locked_state.location_stack.pop();
        if locked_state.location_stack.is_empty() {
            self.state_id.set_state(locked_state.root_state);
        }
    }

    pub fn protect_zone_with_location(&self, location: ProtectLocation) -> ProtectedZoneGuard {
        let mut locked_state = self.locked_state.lock().unwrap();
        locked_state.location_stack.push(location);
        self.progress.protect();
        match location {
            ProtectLocation::Idle => {
                self.state_id.set_state(ActorStateId::Idle);
            }
            ProtectLocation::Waiting(_) => {
                self.state_id.set_state(ActorStateId::Waiting);
            }
            ProtectLocation::Backpressure => {
                self.state_id.set_state(ActorStateId::BackPressure);
            }
        };
        ProtectedZoneGuard {
            actor_state: self.clone(),
        }
    }
}

pub struct ProtectedZoneGuard {
    actor_state: ActorState,
}

impl Drop for ProtectedZoneGuard {
    fn drop(&mut self) {
        self.actor_state.pop_stack();
    }
}

#[repr(u32)]
#[derive(Clone, Copy, Debug, Default)]
pub enum ActorStateId {
    /// Processing implies that the actor has some message(s) (this includes commands) to process.
    #[default]
    Processing = 0,
    Idle = 1,
    /// Pause means that the actor processes no message but can process commands.
    Paused = 2,
    /// Success means that the actor exited and cannot return to any other states.
    Success = 3,
    /// Failure means that the actor exited with a failure or panicked.
    Failure = 4,
    /// Protected
    Waiting = 5,
    BackPressure = 6,
}

impl From<u32> for ActorStateId {
    fn from(actor_state_u32: u32) -> Self {
        match actor_state_u32 {
            0 => ActorStateId::Processing,
            1 => ActorStateId::Idle,
            2 => ActorStateId::Paused,
            3 => ActorStateId::Success,
            4 => ActorStateId::Failure,
            5 => ActorStateId::Waiting,
            6 => ActorStateId::BackPressure,
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
        match self {
            ActorStateId::Processing
            | ActorStateId::Waiting
            | ActorStateId::Idle
            | ActorStateId::BackPressure => true,
            ActorStateId::Paused | ActorStateId::Success | ActorStateId::Failure => false,
        }
    }

    pub fn is_exit(&self) -> bool {
        match self {
            ActorStateId::Processing
            | ActorStateId::Paused
            | ActorStateId::Idle
            | ActorStateId::Waiting
            | ActorStateId::BackPressure => false,
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
    pub fn set_state(&self, state_id: ActorStateId) {
        self.0.store(state_id as u32, Ordering::SeqCst);
    }

    pub fn swap_state(&self, state_id: ActorStateId) -> ActorStateId {
        self.0.swap(state_id as u32, Ordering::SeqCst).into()
    }

    pub fn process(&self) {
        let _ = self.0.compare_exchange(
            ActorStateId::Waiting as u32,
            ActorStateId::Processing as u32,
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

// #[cfg(test)]
// mod tests {
// use super::*;
//
// enum Operation {
// Process,
// Pause,
// Resume,
// ExitSuccess,
// ExitFailure,
// }
//
// impl Operation {
// fn apply(&self, state: &AtomicState) {
// match self {
// Operation::Process => {
// state.process();
// }
// Operation::Pause => {
// state.pause();
// }
// Operation::Resume => state.resume(),
// Operation::ExitSuccess => state.exit(true),
// Operation::ExitFailure => state.exit(false),
// }
// }
// }
//
// #[track_caller]
// fn test_transition(from_state: ActorStateId, op: Operation, expected_state: ActorStateId) {
// let state = AtomicState::from(from_state);
// op.apply(&state);
// assert!(matches!(state.get_state(), expected_state));
// }
//
// #[test]
// fn test_atomic_state_from_running() {
// test_transition(
// ActorStateId::Idle,
// Operation::Process,
// ActorStateId::Processing,
// );
// test_transition(
// ActorStateId::Processing,
// Operation::Idle,
// ActorStateId::Idle,
// );
// test_transition(
// ActorStateId::Processing,
// Operation::Pause,
// ActorStateId::Paused,
// );
// test_transition(ActorStateId::Idle, Operation::Pause, ActorStateId::Paused);
// test_transition(
// ActorStateId::Processing,
// Operation::Resume,
// ActorStateId::Processing,
// );
// test_transition(
// ActorStateId::Processing,
// Operation::ExitSuccess,
// ActorStateId::Success,
// );
// test_transition(ActorStateId::Paused, Operation::Pause, ActorStateId::Paused);
// test_transition(
// ActorStateId::Paused,
// Operation::Resume,
// ActorStateId::Processing,
// );
// test_transition(
// ActorStateId::Paused,
// Operation::ExitSuccess,
// ActorStateId::Success,
// );
// test_transition(
// ActorStateId::Success,
// Operation::ExitFailure,
// ActorStateId::Failure,
// );
//
// test_transition(
// ActorStateId::Success,
// Operation::Process,
// ActorStateId::Success,
// );
// test_transition(
// ActorStateId::Success,
// Operation::Idle,
// ActorStateId::Success,
// );
// test_transition(
// ActorStateId::Success,
// Operation::Pause,
// ActorStateId::Success,
// );
// test_transition(
// ActorStateId::Success,
// Operation::Resume,
// ActorStateId::Success,
// );
// test_transition(
// ActorStateId::Success,
// Operation::ExitSuccess,
// ActorStateId::Success,
// );
//
// test_transition(
// ActorStateId::Failure,
// Operation::Process,
// ActorStateId::Failure,
// );
// test_transition(
// ActorStateId::Failure,
// Operation::Idle,
// ActorStateId::Failure,
// );
// test_transition(
// ActorStateId::Failure,
// Operation::Pause,
// ActorStateId::Failure,
// );
// test_transition(
// ActorStateId::Failure,
// Operation::Resume,
// ActorStateId::Failure,
// );
// test_transition(
// ActorStateId::Failure,
// Operation::ExitSuccess,
// ActorStateId::Failure,
// );
// test_transition(
// ActorStateId::Failure,
// Operation::ExitFailure,
// ActorStateId::Failure,
// );
// }
//
// #[test]
// fn test_progress_protect_zone() {
// let actor_state = ActorState::default();
// assert!(actor_state.registered_activity_since_last_call());
// actor_state.record_progress();
// assert!(actor_state.registered_activity_since_last_call());
// {
// let _protect_guard = actor_state.protect_zone();
// assert!(actor_state.registered_activity_since_last_call());
// assert!(actor_state.registered_activity_since_last_call());
// }
// assert!(actor_state.registered_activity_since_last_call());
// assert!(!actor_state.registered_activity_since_last_call());
// }
//
// #[test]
// fn test_progress_several_protect_zone() {
// let actor_state = ActorState::default();
// assert!(actor_state.registered_activity_since_last_call());
// actor_state.record_progress();
// assert!(actor_state.registered_activity_since_last_call());
// let first_protect_guard = actor_state.protect_zone();
// let second_protect_guard = actor_state.protect_zone();
// assert!(actor_state.registered_activity_since_last_call());
// assert!(actor_state.registered_activity_since_last_call());
// std::mem::drop(first_protect_guard);
// assert!(actor_state.registered_activity_since_last_call());
// assert!(actor_state.registered_activity_since_last_call());
// std::mem::drop(second_protect_guard);
// assert!(actor_state.registered_activity_since_last_call());
// assert!(!actor_state.registered_activity_since_last_call());
// }
// }
//
