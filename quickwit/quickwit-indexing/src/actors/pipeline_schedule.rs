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

/// Maximum adjustment of the commit timeout used to steer a pipeline toward its target phase.
pub(super) const MAX_PIPELINE_TIMEOUT_NUDGE: Duration = Duration::from_secs(5);

/// All pipelines in the process must measure their phase from the same time.
pub(super) static PHASE_ORIGIN: LazyLock<Instant> = LazyLock::new(Instant::now);

/// Steers a pipeline toward an ideal cycle with a period of `commit_timeout`.
///
/// A period in this ideal cycle should, for some k,
/// - start at `PHASE_ORIGIN + k * commit_timeout + target_phase`
/// - end at `PHASE_ORIGIN + (k+1) * commit_timeout + target_phase`.
///
/// `target_phase` is computed using a hash over the pipeline id, and meant to follow a uniform
/// distribution over the interval [0, commit_timeout).
#[derive(Clone, Copy)]
pub(super) struct PipelineSchedule {
    target_phase: Duration,
    period: Duration,
}

impl PipelineSchedule {
    pub fn new(phase_seed: &(impl Hash + ?Sized), commit_timeout: Duration) -> Self {
        let mut hasher = DefaultHasher::new();
        phase_seed.hash(&mut hasher);
        Self::new_with_phase(Duration::from_millis(hasher.finish()), commit_timeout)
    }

    pub(super) fn new_with_phase(target_phase: Duration, commit_timeout: Duration) -> Self {
        assert!(commit_timeout.as_millis() > 0);
        // Initialize the origin now so that it precedes any `now` passed to this schedule.
        LazyLock::force(&PHASE_ORIGIN);
        Self {
            target_phase: Duration::from_millis(
                (target_phase.as_millis() % commit_timeout.as_millis()) as u64,
            ),
            period: commit_timeout,
        }
    }

    pub fn period(&self) -> Duration {
        self.period
    }

    #[cfg(test)]
    pub fn target_phase(&self) -> Duration {
        self.target_phase
    }

    // Actual phase for this pipeline in the current cycle
    pub fn current_phase(&self, now: Instant) -> Duration {
        Duration::from_millis(((now - *PHASE_ORIGIN).as_millis() % self.period.as_millis()) as u64)
    }

    pub fn time_until_target_phase(&self, now: Instant) -> Duration {
        let phase = self.current_phase(now);
        if phase <= self.target_phase {
            // target phase is in the future
            self.target_phase - phase
        } else {
            // next cycle
            self.period + self.target_phase - phase
        }
    }

    pub fn nudged_commit_timeout(&self, now: Instant) -> Duration {
        let time_until_target_phase = self.time_until_target_phase(now);
        let time_since_target_phase = self.period - time_until_target_phase;

        // Take the shortest correction across the period boundary, then bound it by
        // `MAX_PIPELINE_TIMEOUT_NUDGE`.
        if time_until_target_phase <= time_since_target_phase {
            self.period + time_until_target_phase.min(MAX_PIPELINE_TIMEOUT_NUDGE)
        } else {
            self.period - time_since_target_phase.min(MAX_PIPELINE_TIMEOUT_NUDGE)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nudged_commit_timeout() {
        let commit_timeout = Duration::from_secs(30);
        for (case, target_phase_secs, elapsed_secs, expected_timeout_secs) in [
            ("on target phase", 10, 10, 30),
            ("after target phase", 10, 12, 28),
            ("before target phase", 10, 8, 32),
            ("far after target phase, nudge is bounded", 10, 16, 25),
            ("exactly half a period away, nudge goes long", 10, 25, 35),
            ("after target phase, in a later period", 10, 42, 28),
        ] {
            let schedule = PipelineSchedule::new_with_phase(
                Duration::from_secs(target_phase_secs),
                commit_timeout,
            );
            assert_eq!(
                schedule.nudged_commit_timeout(*PHASE_ORIGIN + Duration::from_secs(elapsed_secs)),
                Duration::from_secs(expected_timeout_secs),
                "{case}"
            );
        }
    }
}
