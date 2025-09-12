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

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use super::Rate;

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
    bucket_period_millis: u64,
    period_millis: u64,
    num_buckets: u64,
}

impl SmaRateEstimator {
    /// Creates a new simple moving average rate estimator.
    ///
    /// The rate returned is the rate measured over the last `n - 1` buckets. The
    /// ongoing bucket is not taken in account.
    /// In other words, we are returning a rolling average that spans over a period
    /// of `num_buckets * bucket_period`.
    ///
    /// The `period` argument is just a `scaling unit`. A period of 1s means that the
    /// the number returned by `work` is expressed in `bytes / second`.
    ///
    /// This rate estimator is bucket-based and outputs the average rate of work over the previous
    /// closed `n-1` buckets.
    ///
    /// # Panics
    ///
    /// This function panics if `bucket_period` is < 1s  or `period` is < 1ms.
    pub fn new(num_buckets: NonZeroUsize, bucket_period: Duration, period: Duration) -> Self {
        assert!(bucket_period.as_millis() >= 100);
        assert!(period.as_millis() > 0);

        let mut buckets = Vec::with_capacity(num_buckets.get());
        for _ in 0..num_buckets.get() {
            buckets.push(Bucket::default());
        }
        let inner = InnerSmaRateEstimator {
            anchor: Instant::now(),
            buckets: buckets.into_boxed_slice(),
            bucket_period_millis: bucket_period.as_millis() as u64,
            num_buckets: num_buckets.get() as u64,
            period_millis: period.as_millis() as u64,
        };
        Self {
            inner: Arc::new(inner),
        }
    }

    fn work_in_bucket(&self, bucket_ord: u64) -> u64 {
        self.inner.buckets[bucket_ord as usize % self.inner.buckets.len()]
            .work_for_bucket(bucket_ord)
    }

    fn work_at(&self, now: Instant) -> u64 {
        let elapsed_ms: u64 = now.duration_since(self.inner.anchor).as_millis() as u64;
        let current_bucket_ord = elapsed_ms / self.inner.bucket_period_millis;
        let num_buckets = self.inner.num_buckets - 1u64;
        let bucket_range = current_bucket_ord.saturating_sub(num_buckets)..current_bucket_ord;
        let cumulative_work: u64 = bucket_range
            .map(|bucket_ord| self.work_in_bucket(bucket_ord))
            .sum();
        (cumulative_work * self.inner.period_millis)
            / (self.inner.bucket_period_millis * num_buckets)
    }
}

impl Rate for SmaRateEstimator {
    /// Returns the estimated amount of work performed during a `period`.
    ///
    /// This estimation is computed by summing the amount of work performed tracked in the previous
    /// `n-1` buckets and dividing it by the duration of the `n-1` periods.
    fn work(&self) -> u64 {
        self.work_at(Instant::now())
    }

    fn period(&self) -> Duration {
        Duration::from_millis(self.inner.period_millis)
    }
}

#[inline]
fn compute_bucket_ord_hash(bucket_ord: u64) -> u8 {
    // We pick 241 because it is the highest prime number below 256
    // that can be computed easily.
    //
    // The fact that it is prime makes it so that it is complemented by the
    // bucket id for any value of num_buckets (well except multiples of 241)
    // thanks to the chinese theorem.
    (bucket_ord % 241) as u8
}

impl RateEstimator for SmaRateEstimator {
    fn update(&mut self, _started_at: Instant, ended_at: Instant, work: u64) {
        let elapsed = ended_at.duration_since(self.inner.anchor).as_millis() as u64;
        let num_buckets = self.inner.num_buckets;
        let bucket_ord = elapsed / self.inner.bucket_period_millis;
        let bucket = &self.inner.buckets[(bucket_ord % num_buckets) as usize];
        bucket.increment_work(work, bucket_ord);
    }
}

/// Rate estimator bucket. The 56 least significant bits of the atomic integer store the amount of
/// work, while the most significant 8 bits are encoding a well-thought hash of the bucket ord.
///
/// The hash is used to ensure that we know exactly when to reset the bucket's work.
#[derive(Debug, Default)]
struct Bucket {
    // This atomic is actually encoding two things:
    // - low bits [0..56): the amount of work recorded in the bucket.
    // - high bits [56..64): the bucket ord, or rather its last 8 bits.
    bits: AtomicU64,
}

const WORK_MASK: u64 = (1u64 << 56) - 1;

struct BucketVal {
    work: u64,
    bucket_ord_hash: u8,
}

impl From<u64> for BucketVal {
    #[inline]
    fn from(bucket_bits: u64) -> BucketVal {
        BucketVal {
            work: bucket_bits & WORK_MASK,
            bucket_ord_hash: (bucket_bits >> 56) as u8,
        }
    }
}

impl From<BucketVal> for u64 {
    #[inline]
    fn from(value: BucketVal) -> Self {
        (value.bucket_ord_hash as u64) << 56 | value.work
    }
}

impl Bucket {
    fn work_for_bucket(&self, bucket_ord: u64) -> u64 {
        let bucket_val = BucketVal::from(self.bits.load(Ordering::Relaxed));
        if bucket_val.bucket_ord_hash == compute_bucket_ord_hash(bucket_ord) {
            bucket_val.work
        } else {
            0
        }
    }

    fn increment_work(&self, work: u64, bucket_ord: u64) {
        let expected_bucket_ord_hash: u8 = compute_bucket_ord_hash(bucket_ord);
        let current_bits = self.bits.fetch_add(work, Ordering::Relaxed) + work;
        let bucket_val = BucketVal::from(current_bits);

        // This is not the bucket we targeted, we need to retry and update the bucket with the new
        // bucket_ord and a reset value.
        if bucket_val.bucket_ord_hash != expected_bucket_ord_hash {
            let mut expected_bits = current_bits;
            let new_bits: u64 = BucketVal {
                work,
                bucket_ord_hash: expected_bucket_ord_hash,
            }
            .into();

            while let Err(current_bits) = self.bits.compare_exchange(
                expected_bits,
                new_bits,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                if BucketVal::from(current_bits).bucket_ord_hash == expected_bucket_ord_hash {
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
        let bucket = Bucket::default();
        assert_eq!(bucket.work_for_bucket(0u64), 0);

        // First pass, the bucket is red.
        bucket.increment_work(1, 0u64);
        assert_eq!(bucket.work_for_bucket(0u64), 1);
        assert_eq!(bucket.work_for_bucket(1u64), 0);

        bucket.increment_work(2, 0u64);
        assert_eq!(bucket.work_for_bucket(0u64), 3);

        // Second pass, the bucket is now black.
        bucket.increment_work(5, 1u64);
        assert_eq!(bucket.work_for_bucket(1u64), 5);
        assert_eq!(bucket.work_for_bucket(0u64), 0);

        bucket.increment_work(7, 1u64);
        assert_eq!(bucket.work_for_bucket(1u64), 12);

        // Third pass, the bucket is red again.
        bucket.increment_work(9, 2u64);
        assert_eq!(bucket.work_for_bucket(2u64), 9);

        bucket.increment_work(11, 2u64);
        assert_eq!(bucket.work_for_bucket(2u64), 20);

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
                    bucket.increment_work(i as u64, 3u64);
                }));
            }
            for handle in handles {
                handle.join().unwrap();
            }
            assert_eq!(bucket.work_for_bucket(3u64), cumulative_work);
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
        assert_eq!(estimator.inner.buckets[0].work_for_bucket(0), 100);

        let ended_at = started_at + Duration::from_millis(999);
        estimator.update(started_at, ended_at, 200);
        assert_eq!(estimator.inner.buckets[0].work_for_bucket(0), 300);

        assert_eq!(estimator.work_at(anchor), 0);

        let ended_at = started_at + Duration::from_millis(1_000);
        estimator.update(started_at, ended_at, 300);
        assert_eq!(estimator.inner.buckets[1].work_for_bucket(1), 300);

        let ended_at = started_at + Duration::from_millis(1_999);
        estimator.update(started_at, ended_at, 600);
        assert_eq!(estimator.inner.buckets[1].work_for_bucket(1), 900);

        assert_eq!(
            estimator.work_at(anchor + Duration::from_secs(2)),
            (300 + 900) / 20
        );

        let ended_at = started_at + Duration::from_millis(2_000);
        estimator.update(started_at, ended_at, 800);
        assert_eq!(estimator.inner.buckets[2].work_for_bucket(2), 800);

        let ended_at = started_at + Duration::from_millis(2_999);
        estimator.update(started_at, ended_at, 1_000);
        assert_eq!(estimator.inner.buckets[2].work_for_bucket(2), 1_800);

        assert_eq!(estimator.work_at(anchor + Duration::from_secs(3)), 135);

        let ended_at = started_at + Duration::from_millis(3_000);
        estimator.update(started_at, ended_at, 500);
        assert_eq!(estimator.inner.buckets[0].work_for_bucket(0), 0);
        assert_eq!(estimator.inner.buckets[0].work_for_bucket(3), 500);
    }

    #[test]
    fn test_sma_rate_skipped_bucket() {
        let num_buckets = NonZeroUsize::new(10).unwrap();
        let bucket_period = Duration::from_secs(1);
        let period = Duration::from_secs(1);

        let mut estimator = SmaRateEstimator::new(num_buckets, bucket_period, period);

        assert_eq!(estimator.work(), 0);

        let anchor = estimator.inner.anchor;

        // We fill all of the bucket with 100 work.
        for i in 0..10 {
            let ended_at = anchor + Duration::from_secs(1) * i;
            estimator.update(ended_at, ended_at, 100);
        }

        assert_eq!(estimator.work_at(anchor + Duration::from_secs(10)), 100);

        // Now let's assume there isn't any work ongoing for 4s.
        // Over the last 9 seconds, we have received 500 works
        //
        // After the reset, we should have the following buckets:
        // We expect a mean of 44 work/s.
        // |0, 0, 0, 0, 0, 100*, 100, 100, 100, 100|
        //
        // Since the current bucket (idx = 5) is not taken into account, this leads
        // to an average of 400 / 9 = 44 work units.
        assert_eq!(estimator.work_at(anchor + Duration::from_secs(15)), 44);
    }
}
