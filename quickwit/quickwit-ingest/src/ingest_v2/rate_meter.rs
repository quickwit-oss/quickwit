// Copyright (C) 2023 Quickwit, Inc.
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

use std::time::Instant;

use quickwit_common::tower::ConstantRate;

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
        self.harvest_inner(Instant::now())
    }

    fn harvest_inner(&mut self, now: Instant) -> ConstantRate {
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

    #[test]
    fn test_rate_meter() {
        let mut rate_meter = RateMeter::default();
        assert_eq!(rate_meter.total_work, 0);

        let now = Instant::now();
        rate_meter.harvested_at = now;

        let rate = rate_meter.harvest_inner(now + Duration::from_millis(100));
        assert_eq!(rate.work(), 0);
        assert_eq!(rate.period(), Duration::from_millis(100));

        rate_meter.update(1);
        let rate = rate_meter.harvest_inner(now + Duration::from_millis(200));
        assert_eq!(rate.work(), 1);
        assert_eq!(rate.period(), Duration::from_millis(100));
    }
}
