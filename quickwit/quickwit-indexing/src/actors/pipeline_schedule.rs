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

use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::LazyLock;
use std::time::Duration;

use tokio::time::Instant;

/// We allow ourselves to adjust the sleep time or commit timeout by at most `NUDGE_TOLERANCE`
/// in order to steer a pipeline to its phase.
pub(super) const NUDGE_TOLERANCE: Duration = Duration::from_secs(5);

// All pipelines in the process must measure their phase from the same origin, including respawns.
pub(super) static ORIGIN_OF_TIME: LazyLock<Instant> = LazyLock::new(Instant::now);

#[derive(Clone, Copy)]
pub(super) struct PipelineSchedule {
    target_phase: Duration,
    commit_timeout: Duration,
}

impl PipelineSchedule {
    pub fn new(phase_id: &(impl Hash + ?Sized), commit_timeout: Duration) -> Self {
        assert!(commit_timeout.as_millis() > 0);
        let mut hasher = DefaultHasher::new();
        phase_id.hash(&mut hasher);
        let target_phase_millis = u128::from(hasher.finish()) % commit_timeout.as_millis();
        Self::new_with_phase(
            Duration::from_millis(target_phase_millis as u64),
            commit_timeout,
        )
    }

    pub(super) fn new_with_phase(target_phase: Duration, commit_timeout: Duration) -> Self {
        // Force the initialization of the origin of time.
        let _t0 = *ORIGIN_OF_TIME;
        Self {
            target_phase,
            commit_timeout,
        }
    }

    pub fn commit_timeout(&self) -> Duration {
        self.commit_timeout
    }

    pub fn target_phase(&self) -> Duration {
        self.target_phase
    }

    pub fn current_phase(&self, now: Instant) -> Duration {
        let t0 = *ORIGIN_OF_TIME;
        let current_phase_millis = (now - t0).as_millis() % self.commit_timeout.as_millis();
        Duration::from_millis(current_phase_millis as u64)
    }

    pub fn nudged_commit_timeout(&self, now: Instant) -> Duration {
        let commit_timeout_millis = self.commit_timeout.as_millis() as i128;
        let current_phase_millis = self.current_phase(now).as_millis() as i128;
        let delta_phase = current_phase_millis - self.target_phase.as_millis() as i128;
        // Take the shortest correction across the period boundary, then bound it to five seconds.
        let half_period_millis = commit_timeout_millis / 2;
        let delta_phase = (delta_phase + half_period_millis).rem_euclid(commit_timeout_millis)
            - half_period_millis;
        let nudge_tolerance_millis = NUDGE_TOLERANCE.as_millis() as i128;
        let nudge_millis = delta_phase.clamp(-nudge_tolerance_millis, nudge_tolerance_millis);
        if nudge_millis >= 0 {
            self.commit_timeout - Duration::from_millis(nudge_millis as u64)
        } else {
            self.commit_timeout + Duration::from_millis((-nudge_millis) as u64)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nudged_commit_timeout() {
        let t0 = *ORIGIN_OF_TIME;
        let commit_timeout = Duration::from_secs(30);
        for (target_phase_secs, elapsed_secs, expected_timeout_secs) in [
            (10, 10, 30),
            (10, 12, 28),
            (10, 8, 32),
            (0, 29, 31),
            (29, 0, 29),
            (10, 16, 25),
            (10, 4, 35),
            (10, 24, 25),
            (10, 25, 35),
            (10, 42, 28),
            (10, 38, 32),
        ] {
            let schedule = PipelineSchedule::new_with_phase(
                Duration::from_secs(target_phase_secs),
                commit_timeout,
            );
            assert_eq!(
                schedule.nudged_commit_timeout(t0 + Duration::from_secs(elapsed_secs)),
                Duration::from_secs(expected_timeout_secs),
                "target phase {target_phase_secs}s, elapsed time {elapsed_secs}s"
            );
        }
    }
}
