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

use std::ops::Add;
use std::time::Duration;

use bytesize::ByteSize;
use governor::clock::{Clock, DefaultClock, Reference};
use governor::nanos::Nanos;

use crate::tower::{ConstantRate, Rate};

#[derive(Debug, Clone, Copy)]
pub struct RateLimiterSettings {
    // After a long period of inactivity, the rate limiter can accumulate some "credits"
    // up to what we call a `burst_limit`.
    //
    // Until these credits are expired, the rate limiter may exceed temporarily its rate limit.
    pub burst_limit: u64,
    pub rate_limit: ConstantRate,
    // The refill period has an effect on the resolution at which the
    // rate limiting is enforced.
    //
    // `Instant::now()` is guaranteed to be called at most once per refill_period.
    pub refill_period: Duration,
}

#[cfg(any(test, feature = "testsuite"))]
impl Default for RateLimiterSettings {
    fn default() -> Self {
        // 10 MB burst limit.
        let burst_limit = ByteSize::mb(10).as_u64();
        // 5 MB/s rate limit.
        let rate_limit = ConstantRate::bytes_per_sec(ByteSize::mb(5));
        // Refill every 100ms.
        let refill_period = Duration::from_millis(100);

        Self {
            burst_limit,
            rate_limit,
            refill_period,
        }
    }
}

/// A bursty token-based rate limiter.
#[derive(Debug, Clone)]
pub struct RateLimiter<C: Clock = DefaultClock> {
    // Maximum number of permits that can be accumulated.
    max_capacity: u64,
    // Number of permits available.
    available_permits: u64,
    refill_amount: u64,
    refill_period: Duration,
    refill_period_nanos: u64,
    refill_at: C::Instant,
    clock: C,
}

#[cfg(any(test, feature = "testsuite"))]
impl Default for RateLimiter<DefaultClock> {
    fn default() -> Self {
        Self::from_settings(RateLimiterSettings::default())
    }
}

impl RateLimiter<DefaultClock> {
    /// Creates a new rate limiter from the given settings using the default clock.
    pub fn from_settings(settings: RateLimiterSettings) -> Self {
        Self::from_settings_with_clock(settings, DefaultClock::default())
    }
}

impl<C: Clock> RateLimiter<C> {
    /// Creates a new rate limiter from the given settings with a custom clock.
    pub fn from_settings_with_clock(settings: RateLimiterSettings, clock: C) -> Self {
        let max_capacity = settings.burst_limit;
        let refill_period = settings.refill_period;
        let rate_limit = settings.rate_limit.rescale(refill_period);
        let now = clock.now();

        Self {
            max_capacity,
            available_permits: max_capacity,
            refill_amount: rate_limit.work(),
            refill_period,
            refill_period_nanos: refill_period.as_nanos() as u64,
            refill_at: now.add(Nanos::from(refill_period)),
            clock,
        }
    }

    /// Returns the number of permits available.
    pub fn available_permits(&mut self) -> u64 {
        self.refill(self.clock.now());
        self.available_permits
    }

    /// Acquires some permits from the rate limiter. Returns whether the permits were acquired.
    pub fn acquire(&mut self, num_permits: u64) -> bool {
        self.refill(self.clock.now());
        self.acquire_inner(num_permits)
    }

    /// Acquires some permits expressed in bytes from the rate limiter. Returns whether the permits
    /// were acquired.
    pub fn acquire_bytes(&mut self, bytes: ByteSize) -> bool {
        self.acquire(bytes.as_u64())
    }

    /// Drains all the permits from the rate limiter, effectively disabling all the operations
    /// guarded by the rate limiter for one refill period.
    pub fn drain(&mut self) {
        self.available_permits = 0;
        self.refill_at = self.clock.now().add(Nanos::from(self.refill_period));
    }

    /// Gives back some unused permits to the rate limiter.
    pub fn release(&mut self, num_permits: u64) {
        self.available_permits = self.max_capacity.min(self.available_permits + num_permits);
    }

    fn acquire_inner(&mut self, num_permits: u64) -> bool {
        if self.available_permits >= num_permits {
            self.available_permits -= num_permits;
            true
        } else {
            false
        }
    }

    fn refill(&mut self, now: C::Instant) {
        if now.lt(&self.refill_at) {
            return;
        }
        let elapsed_nanos = now.duration_since(self.refill_at).as_u64();
        // More than one refill period may have elapsed so we need to take that into account.
        let refill =
            self.refill_amount + self.refill_amount * elapsed_nanos / self.refill_period_nanos;
        self.available_permits = self.max_capacity.min(self.available_permits + refill);
        self.refill_at = now.add(Nanos::from(self.refill_period));
    }
}

#[cfg(test)]
mod tests {
    use governor::clock::FakeRelativeClock;

    use super::*;

    #[test]
    fn test_rate_limiter_acquire() {
        let settings = RateLimiterSettings {
            burst_limit: ByteSize::mb(2).as_u64(),
            rate_limit: ConstantRate::bytes_per_sec(ByteSize::mb(1)),
            refill_period: Duration::from_millis(100),
        };
        let clock = FakeRelativeClock::default();
        let mut rate_limiter = RateLimiter::from_settings_with_clock(settings, clock.clone());
        assert_eq!(rate_limiter.max_capacity, ByteSize::mb(2).as_u64());
        assert_eq!(rate_limiter.available_permits, ByteSize::mb(2).as_u64());
        assert_eq!(rate_limiter.refill_amount, ByteSize::kb(100).as_u64());
        assert_eq!(rate_limiter.refill_period, Duration::from_millis(100));

        assert!(rate_limiter.acquire_bytes(ByteSize::mb(1)));
        assert!(rate_limiter.acquire_bytes(ByteSize::mb(1)));
        assert!(!rate_limiter.acquire_bytes(ByteSize::kb(1)));

        clock.advance(Duration::from_millis(100));

        assert!(rate_limiter.acquire_bytes(ByteSize::kb(100)));
        assert!(!rate_limiter.acquire_bytes(ByteSize::kb(20)));

        clock.advance(Duration::from_millis(250));

        assert!(rate_limiter.acquire_bytes(ByteSize::kb(125)));
        assert!(rate_limiter.acquire_bytes(ByteSize::kb(125)));
        assert!(!rate_limiter.acquire_bytes(ByteSize::kb(20)));
    }

    #[test]
    fn test_rate_limiter_drain() {
        let settings = RateLimiterSettings {
            burst_limit: ByteSize::mb(2).as_u64(),
            rate_limit: ConstantRate::bytes_per_sec(ByteSize::mb(1)),
            refill_period: Duration::from_millis(100),
        };
        let clock = FakeRelativeClock::default();
        let mut rate_limiter = RateLimiter::from_settings_with_clock(settings, clock.clone());
        rate_limiter.drain();
        assert_eq!(rate_limiter.available_permits, 0);

        clock.advance(Duration::from_millis(50));
        rate_limiter.refill(clock.now());
        assert_eq!(rate_limiter.available_permits, 0);

        clock.advance(Duration::from_millis(50));
        rate_limiter.refill(clock.now());
        assert!(rate_limiter.available_permits >= ByteSize::kb(100).as_u64());
    }

    #[test]
    fn test_rate_limiter_release() {
        let settings = RateLimiterSettings {
            burst_limit: 1,
            rate_limit: ConstantRate::bytes_per_sec(ByteSize::mb(1)),
            refill_period: Duration::from_millis(100),
        };
        let mut rate_limiter = RateLimiter::from_settings(settings);
        rate_limiter.acquire(1);
        assert_eq!(rate_limiter.available_permits, 0);

        rate_limiter.release(1);
        assert_eq!(rate_limiter.available_permits, 1);

        rate_limiter.release(1);
        assert_eq!(rate_limiter.available_permits, 1);
    }

    #[test]
    fn test_rate_limiter_refill() {
        let settings = RateLimiterSettings {
            burst_limit: ByteSize::mb(2).as_u64(),
            rate_limit: ConstantRate::bytes_per_sec(ByteSize::mb(1)),
            refill_period: Duration::from_millis(100),
        };
        let clock = FakeRelativeClock::default();
        let mut rate_limiter = RateLimiter::from_settings_with_clock(settings, clock.clone());

        rate_limiter.available_permits = 0;
        assert_eq!(rate_limiter.available_permits, 0);

        rate_limiter.available_permits = 0;
        clock.advance(Duration::from_millis(100));
        rate_limiter.refill(clock.now());
        assert_eq!(rate_limiter.available_permits, ByteSize::kb(100).as_u64());

        rate_limiter.available_permits = 0;
        clock.advance(Duration::from_millis(110));
        rate_limiter.refill(clock.now());
        assert_eq!(rate_limiter.available_permits, ByteSize::kb(110).as_u64());

        rate_limiter.available_permits = 0;
        clock.advance(Duration::from_millis(210));
        rate_limiter.refill(clock.now());
        assert_eq!(rate_limiter.available_permits, ByteSize::kb(210).as_u64());
    }

    #[test]
    fn test_rate_limiter_available_permits() {
        let settings = RateLimiterSettings {
            burst_limit: ByteSize::mb(2).as_u64(),
            rate_limit: ConstantRate::bytes_per_sec(ByteSize::mb(1)),
            refill_period: Duration::from_millis(100),
        };
        let clock = FakeRelativeClock::default();
        let mut rate_limiter = RateLimiter::from_settings_with_clock(settings, clock.clone());

        rate_limiter.available_permits = 0;
        clock.advance(Duration::from_millis(100));
        assert_eq!(rate_limiter.available_permits(), ByteSize::kb(100).as_u64());
    }
}
