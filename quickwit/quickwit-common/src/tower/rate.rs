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
    /// This function panics if `period` is 0 while work is != 0.
    pub const fn new(work: u64, period: Duration) -> Self {
        assert!(!period.is_zero() || work == 0u64);
        Self { work, period }
    }

    pub const fn bytes_per_period(bytes: ByteSize, period: Duration) -> Self {
        let work = bytes.as_u64();
        Self::new(work, period)
    }

    pub const fn bytes_per_sec(bytes: ByteSize) -> Self {
        Self::bytes_per_period(bytes, Duration::from_secs(1))
    }

    /// Changes the scale of the rate, i.e. the duration of the time period, while keeping the rate
    /// constant.
    ///
    /// # Panics
    ///
    /// This function panics if `new_period` is 0.
    pub fn rescale(&self, new_period: Duration) -> Self {
        if self.work == 0u64 {
            return Self::new(0u64, new_period);
        }
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
    #[should_panic]
    fn test_rescale_zero_duration_panics() {
        ConstantRate::bytes_per_period(ByteSize::b(1), Duration::default());
    }

    #[test]
    fn test_rescale_zero_duration_accepted_if_no_work() {
        let rate = ConstantRate::bytes_per_period(ByteSize::b(0), Duration::default());
        let rescaled_rate = rate.rescale(Duration::from_secs(1));
        assert_eq!(rescaled_rate.work_bytes(), ByteSize::b(0));
        assert_eq!(rescaled_rate.period(), Duration::from_secs(1));
    }

    #[test]
    fn test_rescale() {
        let rate = ConstantRate::bytes_per_period(ByteSize::mib(5), Duration::from_secs(5));
        let rescaled_rate = rate.rescale(Duration::from_secs(1));
        assert_eq!(rescaled_rate.work_bytes(), ByteSize::mib(1));
        assert_eq!(rescaled_rate.period(), Duration::from_secs(1));
    }
}
