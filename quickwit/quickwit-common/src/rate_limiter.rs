// Copyright (C) 2024 Quickwit, Inc.
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

use std::time::{Duration, Instant};

use bytesize::ByteSize;

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
pub struct RateLimiter {
    // Maximum number of permits that can be accumulated.
    max_capacity: u64,
    // Number of permits available.
    available_permits: u64,
    refill_amount: u64,
    refill_period: Duration,
    refill_period_micros: u64,
    refill_at: Instant,
}

impl RateLimiter {
    /// Creates a new rate limiter from the given settings.
    pub fn from_settings(settings: RateLimiterSettings) -> Self {
        let max_capacity = settings.burst_limit;
        let refill_period = settings.refill_period;
        let rate_limit = settings.rate_limit.rescale(refill_period);
        let now = Instant::now();

        Self {
            max_capacity,
            available_permits: max_capacity,
            refill_amount: rate_limit.work(),
            refill_period,
            refill_period_micros: refill_period.as_micros() as u64,
            refill_at: now + refill_period,
        }
    }

    /// Returns the number of permits available.
    pub fn available_permits(&self) -> u64 {
        self.available_permits
    }

    /// Acquires some permits from the rate limiter. Returns whether the permits were acquired.
    pub fn acquire(&mut self, num_permits: u64) -> bool {
        if self.acquire_inner(num_permits) {
            true
        } else {
            self.refill(Instant::now());
            self.acquire_inner(num_permits)
        }
    }

    /// Acquires some permits from the rate limiter.
    /// If the permits are not available, returns the duration to wait before trying again.
    pub fn acquire_with_duration(&mut self, num_permits: u64) -> Result<(), Duration> {
        if self.acquire_inner(num_permits) {
            return Ok(());
        }
        self.refill(Instant::now());
        if self.acquire_inner(num_permits) {
            return Ok(());
        }
        let missing = num_permits - self.available_permits;
        let wait = Duration::from_micros(missing * self.refill_period_micros / self.refill_amount);
        Err(wait)
    }

    pub fn acquire_bytes(&mut self, bytes: ByteSize) -> bool {
        self.acquire(bytes.as_u64())
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

    fn refill(&mut self, now: Instant) {
        if now < self.refill_at {
            return;
        }
        let elapsed = (now - self.refill_at).as_micros() as u64;
        // More than one refill period may have elapsed so we need to take that into account.
        let refill = self.refill_amount + self.refill_amount * elapsed / self.refill_period_micros;
        self.available_permits = self.max_capacity.min(self.available_permits + refill);
        self.refill_at = now + self.refill_period;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limiter_acquire() {
        let settings = RateLimiterSettings {
            burst_limit: ByteSize::mb(2).as_u64(),
            rate_limit: ConstantRate::bytes_per_sec(ByteSize::mb(1)),
            refill_period: Duration::from_millis(100),
        };
        let mut rate_limiter = RateLimiter::from_settings(settings);
        assert_eq!(rate_limiter.max_capacity, ByteSize::mb(2).as_u64());
        assert_eq!(rate_limiter.available_permits, ByteSize::mb(2).as_u64());
        assert_eq!(rate_limiter.refill_amount, ByteSize::kb(100).as_u64());
        assert_eq!(rate_limiter.refill_period, Duration::from_millis(100));

        assert!(rate_limiter.acquire_bytes(ByteSize::mb(1)));
        assert!(rate_limiter.acquire_bytes(ByteSize::mb(1)));
        assert!(!rate_limiter.acquire_bytes(ByteSize::kb(1)));

        std::thread::sleep(Duration::from_millis(100));

        assert!(rate_limiter.acquire_bytes(ByteSize::kb(100)));
        assert!(!rate_limiter.acquire_bytes(ByteSize::kb(20)));

        std::thread::sleep(Duration::from_millis(250));

        assert!(rate_limiter.acquire_bytes(ByteSize::kb(125)));
        assert!(rate_limiter.acquire_bytes(ByteSize::kb(125)));
        assert!(!rate_limiter.acquire_bytes(ByteSize::kb(20)));
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
        let mut rate_limiter = RateLimiter::from_settings(settings);

        rate_limiter.available_permits = 0;
        let now = Instant::now();
        rate_limiter.refill(now);
        assert_eq!(rate_limiter.available_permits, 0);

        rate_limiter.available_permits = 0;
        let now = now + Duration::from_millis(100);
        rate_limiter.refill(now);
        assert_eq!(rate_limiter.available_permits, ByteSize::kb(100).as_u64());

        rate_limiter.available_permits = 0;
        let now = now + Duration::from_millis(110);
        rate_limiter.refill(now);
        assert_eq!(rate_limiter.available_permits, ByteSize::kb(110).as_u64());

        rate_limiter.available_permits = 0;
        let now = now + Duration::from_millis(210);
        rate_limiter.refill(now);
        assert_eq!(rate_limiter.available_permits, ByteSize::kb(210).as_u64());
    }
}
