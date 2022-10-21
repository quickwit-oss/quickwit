// Copyright (C) 2022 Quickwit, Inc.
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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Weak;
use std::time::Duration;
use std::time::Instant;
pub struct Throttle {
    num_units_consumed: AtomicU64,
    // Should wait is a atomic bool that we read with a relaxed ordering.
    // On our happy path (no cooldown required), we don't pick any lock nor have any memory barrier.
    should_wait: AtomicBool,
    // We use RwLock as a synchronization primitive.
    sync_rw_lock: parking_lot::RwLock<()>, //< we rely on parking_lot because the guard are `Send`
    async_rw_lock: tokio::sync::RwLock<()>,
}

impl Throttle {
    /// Records that we are about to consume `num_units`.
    /// A throttling task may have identified that some throttling is needed.
    ///
    /// In this case, this function will be block for as long is required by the throttling
    /// logic.
    ///
    /// When no throttling happens -our happy path-, this call is as light as can be.
    pub fn consume(&self, num_units: u64) {
        let should_wait = self.should_wait.load(Ordering::Relaxed);
        if should_wait {
            let _ = self.sync_rw_lock.read();
        }
        self.num_units_consumed
            .fetch_add(num_units, Ordering::Relaxed);
    }

    // Async version of consume
    pub async fn consume_async(&self, num_units: u64) {
        let should_wait = self.should_wait.load(Ordering::Relaxed);
        if should_wait {
            let _ = self.async_rw_lock.read().await;
        }
        self.num_units_consumed
            .fetch_add(num_units, Ordering::Relaxed);
    }
}

async fn apply_cooldown(throttle: &Throttle, cool_down_duration: Duration) {
    let _async_lock = throttle.async_rw_lock.write().await;
    let _sync_lock = throttle.sync_rw_lock.write();
    throttle.should_wait.store(true, Ordering::Relaxed);
    tokio::time::sleep(cool_down_duration).await;
    throttle.should_wait.store(false, Ordering::Relaxed);
}

async fn throttling_control_loop(
    throttle_weak: Weak<Throttle>,
    units_per_secs: u64,
    throttling_reactivity: Duration,
) {
    let mut window_start = Instant::now();

    let mut credit: i64 = 0i64;
    let credit_cap: i64 =
        throttling_reactivity.as_micros() as i64 * units_per_secs as i64 / 1_000_000;

    loop {
        if let Some(throttle) = throttle_weak.upgrade() {
            let elapsed = window_start.elapsed();
            window_start = Instant::now();

            let units_consumed = throttle.num_units_consumed.swap(0, Ordering::Relaxed);

            credit += (elapsed.as_micros() as u64 * units_per_secs) as i64 / 1_000_000;
            credit -= units_consumed as i64;
            credit = credit.min(credit_cap);

            if credit < 0 {
                // Cooldown. Everyone stops long enough to make the credit
                let cool_down_micros = (-credit as u64) * 1_000_000 / units_per_secs;
                let cool_down_duration =
                    Duration::from_micros(cool_down_micros as u64) + throttling_reactivity;
                apply_cooldown(&throttle, cool_down_duration).await;
            } else {
                // Let everyone unhinged for 1 full second.
                tokio::time::sleep(throttling_reactivity).await;
            }
        } else {
            break;
        }
    }
}

/// Creates a new throttle.
///
/// A throttle can be cloned to share a given "budget" between several objects/threads.
///
/// * units_per_secs: Number of units per second allowed. This is our maximum target throughput.
/// * throttling_reactivity (1s is a good default value):
///     - Measures at which frequency the throttler will stop resume tasks.
///     - What is the "window" on which throttling is enforced.
pub fn create_throttle(units_per_sec: u64, throttling_reactivity: Duration) -> Arc<Throttle> {
    assert!(units_per_sec > 0);
    let throttle = Arc::new(Throttle {
        num_units_consumed: Default::default(),
        should_wait: Default::default(),
        sync_rw_lock: Default::default(),
        async_rw_lock: Default::default(),
    });
    let throttle_weak = Arc::downgrade(&throttle);
    tokio::task::spawn(throttling_control_loop(
        throttle_weak,
        units_per_sec,
        throttling_reactivity,
    ));
    throttle
}

/// Returns a `throttle` object that will actually never enforce any throttling.
pub fn no_throttling() -> Arc<Throttle> {
    // In case you were wondering, apparently fetch_add guarantees wrapping behavior for fetch_add.
    Arc::new(Throttle {
        num_units_consumed: Default::default(),
        should_wait: Default::default(),
        sync_rw_lock: Default::default(),
        async_rw_lock: Default::default(),
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use std::time::Instant;

    use super::create_throttle;

    #[tokio::test]
    async fn test_sync_throttling() {
        let throttler = create_throttle(1_000, Duration::from_millis(10));
        let now = Instant::now();
        // We try to consume 400 at a pace of 4_000 / secs.
        // Because we are throttled, it will actually take around 400ms.
        let join_handle = tokio::task::spawn_blocking(move || {
            let start = Instant::now();
            for _ in 0..100 {
                std::thread::sleep(Duration::from_millis(1));
                throttler.consume(4);
            }
            start.elapsed()
        });
        let elapsed = join_handle.await.unwrap();
        assert!(elapsed.as_millis() > 350);
        assert!(elapsed.as_millis() < 450);
    }

    #[tokio::test]
    async fn test_async_throttling() {
        let throttler = create_throttle(1_000, Duration::from_millis(20));
        let now = Instant::now();
        // We try to consume 400 at a pace of 4_000 / secs.
        // Because we are throttled, it will actually take around 400ms.
        for _ in 0..100 {
            tokio::time::sleep(Duration::from_millis(1)).await;
            throttler.consume_async(4).await;
        }
        let elapsed = now.elapsed();
        assert!(elapsed.as_millis() > 350);
        assert!(elapsed.as_millis() < 450)
    }

    #[tokio::test]
    async fn test_credit_do_not_accumulate_forever() {
        let throttler = create_throttle(1_000, Duration::from_millis(10));
        let now = Instant::now();
        // We try to consume intermittently.
        //
        // - 100 at a pace of 4000/secs
        // - no consumption for 100ms
        // ... repeated 5 times.
        //
        // The point of this test is too see if extended (= much larger than `reactivity`)
        // lack of call to consume accumulate credits.
        //
        // We are hoping here to see throttling happen, and hope the result will take around
        // (100 + 100) * 5 = 1sec
        for _ in 0..5 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            for _ in 0..25 {
                tokio::time::sleep(Duration::from_millis(1)).await;
                throttler.consume_async(4).await;
            }
        }
        let elapsed = now.elapsed();
        assert!(elapsed.as_millis() > 900);
        assert!(elapsed.as_millis() < 1100)
    }

    #[tokio::test]
    async fn test_credit_do_accumulate() {
        let throttler = create_throttle(100_000, Duration::from_millis(100));
        let now = Instant::now();
        // We try to consume intermittently.
        //
        // - 100 at a pace of 4000/secs
        // - no consumption for 100ms
        // ... repeated 5 times.
        //
        // The point of this test is too see if extended (= much larger than `reactivity`)
        // lack of call to consume accumulate credits.
        //
        // We are hoping here to see throttling happen, and hope the result will take around
        // [100ms + 25ms ] * 5 = 625ms
        for _ in 0..5 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            for _ in 0..5 {
                tokio::time::sleep(Duration::from_millis(5)).await;
                throttler.consume_async(20).await;
            }
        }
        let elapsed = now.elapsed();
        assert!(elapsed.as_millis() > 550);
        assert!(elapsed.as_millis() < 720)
    }
}
