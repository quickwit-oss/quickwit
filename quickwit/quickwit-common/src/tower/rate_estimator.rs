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

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::{ConstantRate, Rate};

pub trait RateEstimator: Rate {
    fn update(&mut self, started_at: Instant, ended_at: Instant, work: u64);
}

/// Simple moving average rate estimator. Tracks the average rate of work over a sliding time
/// window.
#[derive(Debug, Clone)]
pub struct SmaRateEstimator {
    inner: Arc<InnerSmaRateEstimator>,
}

#[derive(Debug)]
struct InnerSmaRateEstimator {
    anchor: Instant,
    buckets: Box<[Bucket]>,
    bucket_period_secs: u64,
    bucket_period_millis: u64,
    num_buckets: u64,
    period: Duration,
    period_millis: u64,
}

impl SmaRateEstimator {
    /// Creates a new simple moving average rate estimator.
    ///
    /// This rate estimator is bucket-based and outputs the average rate of work over the previous
    /// closed `n-1` buckets.
    ///
    /// # Panics
    ///
    /// This function panics if `bucket_period` is < 1s  or `period` is < 1ms.
    pub fn new(num_buckets: NonZeroUsize, bucket_period: Duration, period: Duration) -> Self {
        assert!(bucket_period.as_secs() > 0);
        assert!(period.as_millis() > 0);

        let mut buckets = Vec::with_capacity(num_buckets.get());
        for _ in 0..num_buckets.get() {
            buckets.push(Bucket::default());
        }
        let inner = InnerSmaRateEstimator {
            anchor: Instant::now(),
            buckets: buckets.into_boxed_slice(),
            bucket_period_secs: bucket_period.as_secs(),
            bucket_period_millis: bucket_period.as_millis() as u64,
            num_buckets: num_buckets.get() as u64,
            period,
            period_millis: period.as_millis() as u64,
        };
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Seeds the rate estimator with an initial rate.
    pub fn with_initial_rate(self, initial_rate: ConstantRate) -> Self {
        let initial_work = initial_rate.work() * self.inner.bucket_period_millis
            / initial_rate.period().as_millis() as u64;
        for bucket_ord in 0..self.inner.num_buckets {
            self.inner.buckets[bucket_ord as usize].increment_work(initial_work, 0);
        }
        self
    }
}

impl Rate for SmaRateEstimator {
    /// Returns the estimated amount of work performed during a `period`.
    ///
    /// This estimation is computed by summing the amount of work performed tracked in the previous
    /// `n-1` buckets and dividing it by the duration of the `n-1` periods.
    fn work(&self) -> u64 {
        let now = Instant::now();
        let elapsed = now.duration_since(self.inner.anchor).as_secs();
        let current_bucket_ord = elapsed / self.inner.bucket_period_secs;
        let current_bucket_idx = (current_bucket_ord % self.inner.num_buckets) as usize;
        let cumulative_work: u64 = self
            .inner
            .buckets
            .iter()
            .enumerate()
            .filter(|(bucket_idx, _)| *bucket_idx != current_bucket_idx)
            .map(|(_, bucket)| bucket.work())
            .sum();
        let num_bucket = self.inner.num_buckets - 1;
        cumulative_work * self.inner.period_millis / self.inner.bucket_period_millis / num_bucket
    }

    fn period(&self) -> Duration {
        self.inner.period
    }
}

impl RateEstimator for SmaRateEstimator {
    fn update(&mut self, _started_at: Instant, ended_at: Instant, work: u64) {
        let elapsed = ended_at.duration_since(self.inner.anchor).as_secs();
        let num_buckets = self.inner.num_buckets;
        let bucket_ord = elapsed / self.inner.bucket_period_secs;
        let bucket_idx = bucket_ord % num_buckets;
        let bucket_color = ((bucket_ord / num_buckets) & 1) << 63;
        let bucket = &self.inner.buckets[bucket_idx as usize];
        bucket.increment_work(work, bucket_color);
    }
}

/// Rate estimator bucket. The 63 least significant bits of the atomic integer store the amount of
/// work, while the most significant bit is used to indicate whether the bucket needs to be reset.
/// The reset bit is also called the "color" of a bucket in an attempt to make the code more
/// readable. After each complete pass over the buckets, the color is flipped. The color `0`
/// corresponds to the even passes, while the color `1` corresponds to the odd passes.
#[derive(Debug, Default)]
struct Bucket {
    bits: AtomicU64,
}

impl Bucket {
    const COLOR_MASK: u64 = 1 << 63;

    const WORK_MASK: u64 = u64::MAX - Self::COLOR_MASK;

    fn work(&self) -> u64 {
        self.bits.load(Ordering::Relaxed) & Self::WORK_MASK
    }

    fn increment_work(&self, work: u64, expected_bucket_color: u64) {
        let current_bits = self.bits.fetch_add(work, Ordering::Relaxed) + work;
        let current_bucket_color = current_bits & Self::COLOR_MASK;

        // If the current bucket color is not the expected one, we need to flip its color and reset
        // the amount of work.
        if current_bucket_color != expected_bucket_color {
            let mut expected_bits = current_bits;
            let new_color = !current_bits & Self::COLOR_MASK;
            let new_bits = new_color | work;

            while let Err(current_bits) = self.bits.compare_exchange(
                expected_bits,
                new_bits,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                if current_bits & Self::COLOR_MASK == new_color {
                    // Some thread managed to successfully flip the color. We're good.
                    self.bits.fetch_add(work, Ordering::Relaxed);
                    break;
                } else {
                    // We keep trying.
                    expected_bits = current_bits;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Barrier;
    use std::thread;

    use super::*;

    #[test]
    fn test_bucket() {
        const RED: u64 = 0 << 63;
        const BLACK: u64 = 1 << 63;

        let bucket = Bucket::default();
        assert_eq!(bucket.work(), 0);

        // First pass, the bucket is red.
        bucket.increment_work(1, RED);
        assert_eq!(bucket.work(), 1);

        bucket.increment_work(2, RED);
        assert_eq!(bucket.work(), 3);

        // Second pass, the bucket is now black.
        bucket.increment_work(5, BLACK);
        assert_eq!(bucket.work(), 5);

        bucket.increment_work(7, BLACK);
        assert_eq!(bucket.work(), 12);

        // Third pass, the bucket is red again.
        bucket.increment_work(9, RED);
        assert_eq!(bucket.work(), 9);

        bucket.increment_work(11, RED);
        assert_eq!(bucket.work(), 20);

        for num_threads in [1, 2, 3, 5, 10, 20] {
            let barrier = Arc::new(Barrier::new(num_threads));
            let bucket = Arc::new(Bucket::default());
            let mut cumulative_work = 0;
            let mut handles = Vec::with_capacity(num_threads);

            for i in 0..num_threads {
                let barrier = barrier.clone();
                let bucket = bucket.clone();
                cumulative_work += i as u64;

                handles.push(thread::spawn(move || {
                    barrier.wait();
                    // First time we increment the work in this second pass. All the threads will
                    // attempt to flip the bucket's color. Only one should succeed.
                    bucket.increment_work(i as u64, BLACK);
                }));
            }
            for handle in handles {
                handle.join().unwrap();
            }
            assert_eq!(bucket.work(), cumulative_work);
        }
    }

    #[test]
    fn test_sma_rate_estimator() {
        let num_buckets = NonZeroUsize::new(3).unwrap();
        let bucket_period = Duration::from_secs(1);
        let period = Duration::from_millis(100);

        let mut estimator = SmaRateEstimator::new(num_buckets, bucket_period, period);
        assert_eq!(estimator.work(), 0);
        assert_eq!(estimator.period(), Duration::from_millis(100));

        let anchor = estimator.inner.anchor;

        let started_at = anchor;
        let ended_at = started_at + Duration::from_millis(0);
        estimator.update(started_at, ended_at, 100);
        assert_eq!(estimator.inner.buckets[0].work(), 100);

        let ended_at = started_at + Duration::from_millis(999);
        estimator.update(started_at, ended_at, 200);
        assert_eq!(estimator.inner.buckets[0].work(), 300);

        assert_eq!(estimator.work(), 0);

        let ended_at = started_at + Duration::from_millis(1_000);
        estimator.update(started_at, ended_at, 300);
        assert_eq!(estimator.inner.buckets[1].work(), 300);

        let ended_at = started_at + Duration::from_millis(1_999);
        estimator.update(started_at, ended_at, 600);
        assert_eq!(estimator.inner.buckets[1].work(), 900);

        assert_eq!(estimator.work(), 45);

        let ended_at = started_at + Duration::from_millis(2_000);
        estimator.update(started_at, ended_at, 800);
        assert_eq!(estimator.inner.buckets[2].work(), 800);

        let ended_at = started_at + Duration::from_millis(2_999);
        estimator.update(started_at, ended_at, 1_000);
        assert_eq!(estimator.inner.buckets[2].work(), 1_800);

        assert_eq!(estimator.work(), 135);

        let ended_at = started_at + Duration::from_millis(3_000);
        estimator.update(started_at, ended_at, 500);
        assert_eq!(estimator.inner.buckets[0].work(), 500);
    }
}
