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

use std::time::Duration;

use quickwit_common::tower::{ConstantRate, Rate};
use quickwit_ingest::MemoryCapacity;

#[derive(Clone)]
pub(crate) struct RateModulator<R> {
    rate_estimator: R,
    memory_capacity: MemoryCapacity,
    min_rate: ConstantRate,
}

impl<R> RateModulator<R>
where R: Rate
{
    /// Creates a new [`RateModulator`] instance.
    ///
    /// # Panics
    ///
    /// Panics if `rate_estimator` and `min_rate` have different periods.
    pub fn new(rate_estimator: R, memory_capacity: MemoryCapacity, min_rate: ConstantRate) -> Self {
        assert_eq!(
            rate_estimator.period(),
            min_rate.period(),
            "Rate estimator and min rate periods must be equal."
        );

        Self {
            rate_estimator,
            memory_capacity,
            min_rate,
        }
    }
}

impl<R> Rate for RateModulator<R>
where R: Rate
{
    fn work(&self) -> u64 {
        let memory_usage_ratio = self.memory_capacity.usage_ratio();
        let work = self.rate_estimator.work().max(self.min_rate.work());

        if memory_usage_ratio < 0.25 {
            work * 2
        } else if memory_usage_ratio > 0.99 {
            work / 32
        } else if memory_usage_ratio > 0.98 {
            work / 16
        } else if memory_usage_ratio > 0.95 {
            work / 8
        } else if memory_usage_ratio > 0.90 {
            work / 4
        } else if memory_usage_ratio > 0.80 {
            work / 2
        } else if memory_usage_ratio > 0.70 {
            work * 2 / 3
        } else {
            work
        }
    }

    fn period(&self) -> Duration {
        self.rate_estimator.period()
    }
}
