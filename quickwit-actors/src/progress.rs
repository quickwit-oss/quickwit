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
use std::sync::Arc;

/// Progress makes it possible to register some progress.
/// It is used in lieu of healthcheck.
///
/// If no progress is observed until the next heartbeat, the actor will be killed.
#[derive(Clone)]
pub struct Progress(Arc<AtomicU32>);

#[derive(Clone, Debug, Copy, PartialEq, Eq)]
enum ProgressState {
    // No update recorded since the last call to .check_for_update()
    NoUpdate,
    // An update was recorded since the last call to .check_for_update()
    Updated,
    // The actor is in the protected zone.
    //
    // The protected zone should seldom be used. It is useful
    // when calling an external library that is blocking for instance.
    //
    // Another use case is blocking when sending a message to another actor
    // with a saturated message bus.
    // The failure detection is then considered to be the problem of
    // the downstream actor.
    //
    // As long as the actor is in the protected zone, healthchecking won't apply
    // to it.
    //
    // The value inside starts at 0.
    ProtectedZone(u32),
}

#[allow(clippy::from_over_into)]
impl Into<u32> for ProgressState {
    fn into(self) -> u32 {
        match self {
            ProgressState::NoUpdate => 0,
            ProgressState::Updated => 1,
            ProgressState::ProtectedZone(level) => 2 + level,
        }
    }
}

impl From<u32> for ProgressState {
    fn from(level: u32) -> Self {
        match level {
            0 => ProgressState::NoUpdate,
            1 => ProgressState::Updated,
            level => ProgressState::ProtectedZone(level - 2),
        }
    }
}

impl Default for Progress {
    fn default() -> Progress {
        Progress(Arc::new(AtomicU32::new(ProgressState::Updated.into())))
    }
}

impl Progress {
    pub fn record_progress(&self) {
        self.0
            .fetch_max(ProgressState::Updated.into(), Ordering::Relaxed);
    }

    pub fn protect_zone(&self) -> ProtectedZoneGuard {
        loop {
            let previous_state: ProgressState = self.0.load(Ordering::Relaxed).into();
            let new_state = match previous_state {
                ProgressState::NoUpdate | ProgressState::Updated => ProgressState::ProtectedZone(0),
                ProgressState::ProtectedZone(level) => ProgressState::ProtectedZone(level + 1),
            };
            if self
                .0
                .compare_exchange(
                    previous_state.into(),
                    new_state.into(),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                return ProtectedZoneGuard(self.0.clone());
            }
        }
    }

    /// This method mutates the state as follows and returns true if
    /// the object was in the protected zone or had change registered.
    /// - Updated -> NoUpdate, returns true
    /// - NoUpdate -> NoUpdate, returns false
    /// - ProtectedZone -> ProtectedZone, returns true
    pub fn registered_activity_since_last_call(&self) -> bool {
        let previous_state: ProgressState = self
            .0
            .compare_exchange(
                ProgressState::Updated.into(),
                ProgressState::NoUpdate.into(),
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .unwrap_or_else(|previous_value| previous_value)
            .into();
        previous_state != ProgressState::NoUpdate
    }
}

pub struct ProtectedZoneGuard(Arc<AtomicU32>);

impl Drop for ProtectedZoneGuard {
    fn drop(&mut self) {
        let previous_state: ProgressState = self.0.fetch_sub(1, Ordering::Relaxed).into();
        assert!(matches!(previous_state, ProgressState::ProtectedZone(_)));
    }
}

#[cfg(test)]
mod tests {
    use super::Progress;

    #[test]
    fn test_progress() {
        let progress = Progress::default();
        assert!(progress.registered_activity_since_last_call());
        progress.record_progress();
        assert!(progress.registered_activity_since_last_call());
        assert!(!progress.registered_activity_since_last_call());
    }

    #[test]
    fn test_progress_protect_zone() {
        let progress = Progress::default();
        assert!(progress.registered_activity_since_last_call());
        progress.record_progress();
        assert!(progress.registered_activity_since_last_call());
        {
            let _protect_guard = progress.protect_zone();
            assert!(progress.registered_activity_since_last_call());
            assert!(progress.registered_activity_since_last_call());
        }
        assert!(progress.registered_activity_since_last_call());
        assert!(!progress.registered_activity_since_last_call());
    }

    #[test]
    fn test_progress_several_protect_zone() {
        let progress = Progress::default();
        assert!(progress.registered_activity_since_last_call());
        progress.record_progress();
        assert!(progress.registered_activity_since_last_call());
        let first_protect_guard = progress.protect_zone();
        let second_protect_guard = progress.protect_zone();
        assert!(progress.registered_activity_since_last_call());
        assert!(progress.registered_activity_since_last_call());
        std::mem::drop(first_protect_guard);
        assert!(progress.registered_activity_since_last_call());
        assert!(progress.registered_activity_since_last_call());
        std::mem::drop(second_protect_guard);
        assert!(progress.registered_activity_since_last_call());
        assert!(!progress.registered_activity_since_last_call());
    }
}
