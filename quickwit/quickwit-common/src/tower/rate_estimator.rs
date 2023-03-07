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
        for bucket_ord in 0..num_buckets.get() {
            buckets.push(Bucket::new(bucket_ord as u64, 0));
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
        for i in 0..self.inner.num_buckets {
            self.inner.buckets[i as usize].update(initial_work);
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
        let cumulated_work: u64 = self
            .inner
            .buckets
            .iter()
            .enumerate()
            .filter(|(bucket_idx, _)| *bucket_idx != current_bucket_idx)
            .map(|(_, bucket)| bucket.cumulated_work())
            .sum();
        let num_bucket = self.inner.num_buckets - 1;
        cumulated_work * self.inner.period_millis / self.inner.bucket_period_millis / num_bucket
    }

    fn period(&self) -> Duration {
        self.inner.period
    }
}

impl RateEstimator for SmaRateEstimator {
    fn update(&mut self, _started_at: Instant, ended_at: Instant, work: u64) {
        let elapsed = ended_at.duration_since(self.inner.anchor).as_secs();
        let bucket_ord = elapsed / self.inner.bucket_period_secs;
        let bucket_idx = (bucket_ord % self.inner.num_buckets) as usize;

        let current_bucket = &self.inner.buckets[bucket_idx];
        let current_bucket_ord = current_bucket.bucket_ord();

        if current_bucket_ord > bucket_ord {
            return;
        }
        if current_bucket_ord != bucket_ord {
            current_bucket.reset(current_bucket_ord, bucket_ord);
        }
        current_bucket.update(work);
    }
}

#[derive(Debug, Default)]
struct Bucket {
    bucket_ord: AtomicU64,
    cumulated_work: AtomicU64,
}

impl Bucket {
    fn new(bucket_ord: u64, cumulated_work: u64) -> Self {
        Self {
            bucket_ord: AtomicU64::new(bucket_ord),
            cumulated_work: AtomicU64::new(cumulated_work),
        }
    }

    fn bucket_ord(&self) -> u64 {
        self.bucket_ord.load(Ordering::Acquire)
    }

    fn cumulated_work(&self) -> u64 {
        self.cumulated_work.load(Ordering::Relaxed)
    }

    fn update(&self, work: u64) {
        self.cumulated_work.fetch_add(work, Ordering::Relaxed);
    }

    fn reset(&self, current_bucket_ord: u64, new_bucket_ord: u64) {
        if self
            .bucket_ord
            .compare_exchange(
                current_bucket_ord,
                new_bucket_ord,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            self.cumulated_work.store(0, Ordering::Release);
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
        let bucket = Bucket::new(0, 0);
        assert_eq!(bucket.bucket_ord(), 0);
        assert_eq!(bucket.cumulated_work(), 0);

        bucket.update(10);
        assert_eq!(bucket.cumulated_work(), 10);

        let mut handles = Vec::with_capacity(10);
        let barrier = Arc::new(Barrier::new(10));
        let bucket = Arc::new(bucket);

        for _ in 0..10 {
            let barrier = barrier.clone();
            let bucket = bucket.clone();

            handles.push(thread::spawn(move || {
                barrier.wait();
                bucket.reset(0, 1);
                bucket.update(1);
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }
        assert_eq!(bucket.bucket_ord(), 1);
        assert_eq!(bucket.cumulated_work(), 10);
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
        assert_eq!(estimator.inner.buckets[0].cumulated_work(), 100);

        let ended_at = started_at + Duration::from_millis(999);
        estimator.update(started_at, ended_at, 200);
        assert_eq!(estimator.inner.buckets[0].cumulated_work(), 300);

        assert_eq!(estimator.work(), 0);

        let ended_at = started_at + Duration::from_millis(1_000);
        estimator.update(started_at, ended_at, 300);
        assert_eq!(estimator.inner.buckets[1].cumulated_work(), 300);

        let ended_at = started_at + Duration::from_millis(1_999);
        estimator.update(started_at, ended_at, 600);
        assert_eq!(estimator.inner.buckets[1].cumulated_work(), 900);

        assert_eq!(estimator.work(), 45);

        let ended_at = started_at + Duration::from_millis(2_000);
        estimator.update(started_at, ended_at, 800);
        assert_eq!(estimator.inner.buckets[2].cumulated_work(), 800);

        let ended_at = started_at + Duration::from_millis(2_999);
        estimator.update(started_at, ended_at, 1_000);
        assert_eq!(estimator.inner.buckets[2].cumulated_work(), 1_800);

        assert_eq!(estimator.work(), 135);

        let ended_at = started_at + Duration::from_millis(3_000);
        estimator.update(started_at, ended_at, 500);
        assert_eq!(estimator.inner.buckets[0].cumulated_work(), 500);
    }
}
