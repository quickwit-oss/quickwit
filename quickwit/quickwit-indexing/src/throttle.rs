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

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
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
    pub fn consume(&self, num_units: u64) {
        let should_wait= self.should_wait.load(Ordering::Relaxed);
        if should_wait {
            let _ = self.sync_rw_lock.read();
        }
        self.num_units_consumed.fetch_add(num_units, Ordering::Relaxed);
    }

    // Async version of consume
    pub async fn consume_async(&self, num_units: u64) {
        let should_wait= self.should_wait.load(Ordering::Relaxed);
        if should_wait {
            let _ = self.async_rw_lock.read();
        }
        self.num_units_consumed.fetch_add(num_units, Ordering::Relaxed);
    }
}

async fn apply_cooldown(throttle: &Throttle, cool_down_duration: Duration) {
    let _async_lock = throttle.async_rw_lock.write().await;
    let _sync_lock = throttle.sync_rw_lock.write();
    throttle.should_wait.store(true, Ordering::Relaxed);
    tokio::time::sleep(cool_down_duration).await;
    throttle.should_wait.store(false, Ordering::Relaxed);
}

pub fn create_throttle(units_per_secs: u64, throttling_interval: Duration) -> Arc<Throttle> {
    assert!(units_per_secs > 0);
    let throttle = Arc::new(Throttle {
        num_units_consumed: Default::default(),
        should_wait: Default::default(),
        sync_rw_lock: Default::default(),
        async_rw_lock: Default::default(),
    });
    let throttle_weak = Arc::downgrade(&throttle);
    tokio::task::spawn(async move {
        let mut window_start = Instant::now();
        let mut last_cooldown = Duration::default();
        let mut interval = tokio::time::interval(throttling_interval);
        loop {
            interval.tick().await;
            if let Some(throttle) = throttle_weak.upgrade() {
                let elapsed = window_start.elapsed();
                window_start = Instant::now();
                let units_consumed = throttle.num_units_consumed.swap(0, Ordering::Relaxed);
                let unhinged_units_per_secs =
                    (units_consumed * 1_000_000) / (elapsed.as_micros().checked_sub(last_cooldown.as_micros()).unwrap_or(1).max(1) as u64);
                if unhinged_units_per_secs > units_per_secs {
                    let cool_down_duration = throttling_interval.mul_f32(1.0f32 - (units_per_secs as f32) / (unhinged_units_per_secs as f32));
                    apply_cooldown(&throttle, cool_down_duration).await;
                    last_cooldown = cool_down_duration;
                } else {
                    // No need for any cool down
                    last_cooldown = Duration::default();
                }
            } else {
                break;
            }
        }
    });
    throttle
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
        // We try to consume 500 at a pace of 5_000 / secs.
        // Because we are throttled, it will actually take more than 500ms.
        let join_handle = tokio::task::spawn_blocking(move || {
            for _ in 0..1_000 {
                std::thread::sleep(Duration::from_millis(2));
                throttler.consume(5);
            }
            let elapsed = now.elapsed();
            elapsed

        });
        let elapsed = join_handle.await.unwrap();
        dbg!(elapsed);
        assert!(elapsed.as_millis() > 4_000);
        assert!(elapsed.as_millis() < 6_000);
    }

    #[tokio::test]
    async fn test_async_throttling() {
        let throttler = create_throttle(1_000, Duration::from_millis(20));
        let now = Instant::now();
        // We try to consume 500 at a pace of 5_000 / secs.
        // Because we are throttled, it will actually take more than 500ms.
        for _ in 0..100 {
            tokio::time::sleep(Duration::from_millis(5)).await;
            throttler.consume_async(50).await;
        }
        let elapsed = now.elapsed();
        dbg!(elapsed);
        assert!(elapsed.as_millis() > 400);
        assert!(elapsed.as_millis() < 600)
    }

}
