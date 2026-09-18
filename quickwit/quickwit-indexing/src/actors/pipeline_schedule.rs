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

pub(super) const NUDGE_TOLERANCE: Duration = Duration::from_secs(5);

// All pipelines in the process must measure their phase from the same origin, including respawns.
pub(super) static ORIGIN_OF_TIME: LazyLock<Instant> = LazyLock::new(Instant::now);

#[derive(Clone, Copy)]
pub(super) struct PipelineSchedule {
    origin: Instant,
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
            *ORIGIN_OF_TIME,
        )
    }

    pub(super) fn new_with_phase(
        target_phase: Duration,
        commit_timeout: Duration,
        origin: Instant,
    ) -> Self {
        assert!(commit_timeout.as_millis() > 0);
        Self {
            origin,
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

    pub fn initial_sleep_duration(&self, now: Instant) -> Duration {
        let commit_timeout_millis = self.commit_timeout.as_millis();
        let current_phase_millis = (now - self.origin).as_millis() % commit_timeout_millis;
        let initial_sleep_millis = (commit_timeout_millis + self.target_phase.as_millis()
            - current_phase_millis)
            % commit_timeout_millis;
        if initial_sleep_millis + 2 * NUDGE_TOLERANCE.as_millis() > commit_timeout_millis {
            return Duration::ZERO;
        }
        Duration::from_millis(initial_sleep_millis as u64)
    }

    pub fn nudged_commit_timeout(&self, now: Instant) -> Duration {
        let commit_timeout_millis = self.commit_timeout.as_millis() as i128;
        let current_phase_millis = (now - self.origin).as_millis() as i128 % commit_timeout_millis;
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
    fn test_initial_sleep_time() {
        let origin = Instant::now();
        let commit_timeout = Duration::from_secs(30);
        for target_phase_secs in [0, 1, 2, 5, 10, 15, 20, 25, 29, 30, 1_000] {
            let target_phase = Duration::from_secs(target_phase_secs);
            let schedule = PipelineSchedule::new_with_phase(target_phase, commit_timeout, origin);
            for start_time_secs in [0, 1, 2, 5, 10, 15, 20, 25, 29, 30, 1_000] {
                let start_time = Duration::from_secs(start_time_secs);
                let sleep_duration = schedule.initial_sleep_duration(origin + start_time);
                let phase_offset_millis = ((start_time + sleep_duration).as_millis() as i64
                    - target_phase.as_millis() as i64)
                    .rem_euclid(commit_timeout.as_millis() as i64);

                assert!(sleep_duration < commit_timeout);
                if sleep_duration.is_zero() {
                    assert!(
                        phase_offset_millis < 2 * NUDGE_TOLERANCE.as_millis() as i64,
                        "target phase {target_phase_secs}s, start time {start_time_secs}s"
                    );
                } else {
                    assert_eq!(
                        phase_offset_millis, 0,
                        "target phase {target_phase_secs}s, start time {start_time_secs}s"
                    );
                }
            }
        }
    }

    #[test]
    fn test_nudged_commit_timeout() {
        let origin = Instant::now();
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
                origin,
            );
            assert_eq!(
                schedule.nudged_commit_timeout(origin + Duration::from_secs(elapsed_secs)),
                Duration::from_secs(expected_timeout_secs),
                "target phase {target_phase_secs}s, elapsed time {elapsed_secs}s"
            );
        }
    }
}
