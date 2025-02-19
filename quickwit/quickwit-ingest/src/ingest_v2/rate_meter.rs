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

use quickwit_common::tower::ConstantRate;
use tokio::time::Instant;

/// A naive rate meter that tracks how much work was performed during a period of time defined by
/// two successive calls to `harvest`.
#[derive(Debug, Clone)]
pub(super) struct RateMeter {
    total_work: u64,
    harvested_at: Instant,
}

impl Default for RateMeter {
    fn default() -> Self {
        Self {
            total_work: 0,
            harvested_at: Instant::now(),
        }
    }
}

impl RateMeter {
    /// Increments the amount of work performed since the last call to `harvest`.
    pub fn update(&mut self, work: u64) {
        self.total_work += work;
    }

    /// Returns the average work rate since the last call to this method and resets the internal
    /// state.
    pub fn harvest(&mut self) -> ConstantRate {
        let now = Instant::now();
        let elapsed = now.duration_since(self.harvested_at);
        let rate = ConstantRate::new(self.total_work, elapsed);
        self.total_work = 0;
        self.harvested_at = now;
        rate
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use quickwit_common::tower::Rate;

    use super::*;

    #[tokio::test]
    async fn test_rate_meter() {
        tokio::time::pause();
        let mut rate_meter = RateMeter::default();

        let rate = rate_meter.harvest();
        assert_eq!(rate.work(), 0);
        assert!(rate.period().is_zero());

        tokio::time::advance(Duration::from_millis(100)).await;

        let rate = rate_meter.harvest();
        assert_eq!(rate.work(), 0);
        assert_eq!(rate.period(), Duration::from_millis(100));

        rate_meter.update(1);
        tokio::time::advance(Duration::from_millis(100)).await;

        let rate = rate_meter.harvest();
        assert_eq!(rate.work(), 1);
        assert_eq!(rate.period(), Duration::from_millis(100));
    }
}
