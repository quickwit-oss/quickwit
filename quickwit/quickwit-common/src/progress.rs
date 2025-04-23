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

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use futures::Future;

/// Progress makes it possible to register some progress.
/// It is used in lieu of healthcheck.
///
/// If no progress is observed until the next heartbeat, the actor will be killed.
#[derive(Clone)]
pub struct Progress(Arc<AtomicU32>);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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

    /// Executes a future in a protected zone.
    pub async fn protect_future<Fut, T>(&self, future: Fut) -> T
    where Fut: Future<Output = T> {
        let _guard = self.protect_zone();
        future.await
    }

    pub fn protect_zone(&self) -> ProtectedZoneGuard {
        loop {
            let previous_state: ProgressState = self.0.load(Ordering::SeqCst).into();
            let new_state = match previous_state {
                ProgressState::NoUpdate | ProgressState::Updated => ProgressState::ProtectedZone(0),
                ProgressState::ProtectedZone(level) => ProgressState::ProtectedZone(level + 1),
            };
            if self
                .0
                .compare_exchange(
                    previous_state.into(),
                    new_state.into(),
                    Ordering::SeqCst,
                    Ordering::SeqCst,
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
        let previous_state: ProgressState = self.0.fetch_sub(1, Ordering::SeqCst).into();
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
