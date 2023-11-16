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

use bytesize::ByteSize;

pub trait Rate: Clone {
    /// Returns the amount of work per time period.
    fn work(&self) -> u64;

    /// Returns the amount of work in bytes per time period.
    fn work_bytes(&self) -> ByteSize {
        ByteSize(self.work())
    }

    /// Returns the duration of a time period.
    fn period(&self) -> Duration;
}

/// A rate of unit of work per time period.
#[derive(Debug, Copy, Clone)]
pub struct ConstantRate {
    work: u64,
    period: Duration,
}

impl ConstantRate {
    /// Creates a new constant rate.
    ///
    /// # Panics
    ///
    /// This function panics if `period` is 0.
    pub fn new(work: u64, period: Duration) -> Self {
        assert!(!period.is_zero());

        Self { work, period }
    }

    pub fn bytes_per_period(bytes: ByteSize, period: Duration) -> Self {
        let work = bytes.as_u64();
        Self::new(work, period)
    }

    pub fn bytes_per_sec(bytes: ByteSize) -> Self {
        Self::bytes_per_period(bytes, Duration::from_secs(1))
    }

    /// Changes the scale of the rate, i.e. the duration of the time period, while keeping the rate
    /// constant.
    ///
    /// # Panics
    ///
    /// This function panics if `new_period` is 0.
    pub fn rescale(&self, new_period: Duration) -> Self {
        assert!(!new_period.is_zero());

        let new_work = self.work() as u128 * new_period.as_nanos() / self.period().as_nanos();
        Self::new(new_work as u64, new_period)
    }
}

impl Rate for ConstantRate {
    fn work(&self) -> u64 {
        self.work
    }

    fn period(&self) -> Duration {
        self.period
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rescale() {
        let rate = ConstantRate::bytes_per_period(ByteSize::mib(5), Duration::from_secs(5));
        let rescaled_rate = rate.rescale(Duration::from_secs(1));
        assert_eq!(rescaled_rate.work_bytes(), ByteSize::mib(1));
        assert_eq!(rescaled_rate.period(), Duration::from_secs(1));
    }
}
