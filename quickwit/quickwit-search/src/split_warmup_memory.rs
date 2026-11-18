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

//! Memory reserved by the warmup of a single split search.

use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use quickwit_directories::{MemoryReservation, MemoryReserver, OutOfMemoryError};

use crate::metrics::LEAF_SEARCH_SPLIT_WARMUP_OUT_OF_MEMORY_SPLIT_BUDGET_TOTAL;
use crate::warmup_memory::WarmupMemoryGuard;

/// Counts the split search as out of memory and logs a warning. Called once per split search.
fn record_out_of_memory(
    reserved_num_bytes: u64,
    requested_num_bytes: u64,
    split_max_num_bytes: u64,
) {
    LEAF_SEARCH_SPLIT_WARMUP_OUT_OF_MEMORY_SPLIT_BUDGET_TOTAL.inc();
    quickwit_common::rate_limited_warn!(
        limit_per_min = 10,
        reserved_num_bytes,
        requested_num_bytes,
        split_max_num_bytes,
        "split search ran out of warmup memory: {OutOfMemoryError}"
    );
}

/// Warmup memory of a single split search.
///
/// Shared (`Arc<SplitWarmupMemory>`) between the split's `SearchPermit` and its `CachingDirectory`,
/// which reserves memory before each cache miss. The reservation is freed when the last of them
/// is dropped, so it lives as long as the cache that holds the warmup data.
///
/// The accounting itself is done by the [`WarmupMemoryGuard`]: this adds the out of memory metric
/// and the [`MemoryReserver`] interface.
pub(crate) struct SplitWarmupMemory {
    reservation: WarmupMemoryGuard,
    /// Set the first time a reservation fails.
    /// The point is to count each split search only once in the out of memory metric.
    out_of_memory: AtomicBool,
}

impl SplitWarmupMemory {
    pub fn new(reservation: WarmupMemoryGuard) -> Self {
        SplitWarmupMemory {
            reservation,
            out_of_memory: AtomicBool::new(false),
        }
    }

    /// Bytes currently reserved.
    #[cfg(test)]
    pub fn reserved_num_bytes(&self) -> u64 {
        self.reservation.num_bytes()
    }

    /// Whether a reservation failed, because of a warmup memory limit.
    #[cfg(test)]
    pub fn has_run_out_of_memory(&self) -> bool {
        self.out_of_memory.load(Ordering::Relaxed)
    }

    /// Grows the reservation to at least `num_bytes`, without using them. Waits until the memory
    /// is available. Used to reserve the memory of a split search before its warmup.
    pub async fn reserve_capacity(&self, num_bytes: u64) -> Result<(), OutOfMemoryError> {
        self.reservation
            .reserve_capacity(num_bytes)
            .await
            .inspect_err(|_| self.record_out_of_memory(num_bytes))
    }

    /// Shrinks the reservation to `max_num_bytes`, but never below the memory in use. Does nothing
    /// if the reservation is already smaller.
    pub fn shrink_reservation(&self, max_num_bytes: u64) {
        self.reservation.shrink_reservation(max_num_bytes);
    }

    /// Shrinks the reservation to the memory in use.
    pub fn release_unused_memory(&self) {
        self.shrink_reservation(0);
    }

    /// Counts the split search as out of memory, only the first time.
    fn record_out_of_memory(&self, requested: u64) {
        if self.out_of_memory.swap(true, Ordering::Relaxed) {
            return;
        }
        record_out_of_memory(
            self.reservation.num_bytes(),
            requested,
            self.reservation.max_num_bytes(),
        );
    }
}

#[async_trait]
impl MemoryReserver for SplitWarmupMemory {
    /// Reserves memory for `num_bytes` of warmup data, growing the reservation if needed.
    ///
    /// Waits until the memory is available. This cannot deadlock: see `GlobalWarmupMemory`.
    /// Fails right away if the reservation would go over the per-split limit.
    ///
    /// Cancel-safe: if the future is dropped while waiting, nothing is reserved.
    async fn reserve_memory(
        &self,
        num_bytes: u64,
    ) -> Result<MemoryReservation<'_>, OutOfMemoryError> {
        self.reservation
            .reserve(num_bytes)
            .await
            .inspect_err(|_| self.record_out_of_memory(num_bytes))?;
        Ok(MemoryReservation::new(self, num_bytes))
    }

    /// Gives back memory reserved for a read that failed. The reservation itself is not shrunk:
    /// `release_unused_memory` does it after the warmup.
    ///
    /// Does not panic, because it is called from `Drop`, possibly while a panic unwinds. Releasing
    /// more than the memory in use breaks the contract of [`MemoryReserver::release_memory`]: it
    /// fails a debug assertion, and saturates at 0 in release builds.
    fn release_memory(&self, num_bytes: u64) {
        self.reservation.release(num_bytes);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use bytesize::ByteSize;
    use futures::FutureExt;

    use super::*;
    use crate::warmup_memory::GlobalWarmupMemory;

    const MB: u64 = 1_000_000;

    /// A warmup memory where a single split search can use the whole limit.
    fn global_warmup_memory(limit_mb: u64) -> GlobalWarmupMemory {
        global_warmup_memory_with_split_max(limit_mb, limit_mb)
    }

    fn global_warmup_memory_with_split_max(limit_mb: u64, split_max_mb: u64) -> GlobalWarmupMemory {
        GlobalWarmupMemory::new(ByteSize::mb(limit_mb), ByteSize::mb(split_max_mb))
    }

    /// Creates the warmup memory of a new split search, with an initial reservation that must not
    /// wait.
    fn split_warmup_memory(
        global: &GlobalWarmupMemory,
        initial_reservation_num_bytes: u64,
    ) -> SplitWarmupMemory {
        let split_memory = SplitWarmupMemory::new(global.new_guard());
        split_memory
            .reserve_capacity(initial_reservation_num_bytes)
            .now_or_never()
            .expect("the initial reservation should not wait")
            .unwrap();
        split_memory
    }

    /// Reserves `num_bytes` for a read. Returns `None` if the request would wait: it is then
    /// cancelled.
    fn reserve_now(
        split_memory: &SplitWarmupMemory,
        num_bytes: u64,
    ) -> Option<Result<MemoryReservation<'_>, OutOfMemoryError>> {
        split_memory.reserve_memory(num_bytes).now_or_never()
    }

    #[test]
    fn test_reserve_memory_within_reservation() {
        let global = global_warmup_memory(100);
        let split_memory = split_warmup_memory(&global, 10 * MB);
        let _reservation_1 = reserve_now(&split_memory, 4 * MB).unwrap().unwrap();
        let _reservation_2 = reserve_now(&split_memory, 6 * MB).unwrap().unwrap();
        assert_eq!(split_memory.reserved_num_bytes(), 10 * MB);
        assert_eq!(global.allocated(), 10 * MB);
    }

    #[test]
    fn test_reserve_memory_grows_exponentially() {
        let global = global_warmup_memory(100);
        let split_memory = split_warmup_memory(&global, 10 * MB);
        // 11MB does not fit in 10MB: the reservation grows to 1.7 x 10MB.
        let _reservation_1 = reserve_now(&split_memory, 11 * MB).unwrap().unwrap();
        assert_eq!(split_memory.reserved_num_bytes(), 17 * MB);
        // 11MB + 6MB = 17MB still fits.
        let _reservation_2 = reserve_now(&split_memory, 6 * MB).unwrap().unwrap();
        assert_eq!(split_memory.reserved_num_bytes(), 17 * MB);
        // 18MB does not fit: the reservation grows to 1.7 x 17MB.
        let _reservation_3 = reserve_now(&split_memory, MB).unwrap().unwrap();
        assert_eq!(split_memory.reserved_num_bytes(), 28_900_000);
        assert_eq!(global.allocated(), 28_900_000);
    }

    #[test]
    fn test_reserve_memory_falls_back_to_exact_need() {
        let global = global_warmup_memory_with_split_max(100, 85);
        let _oldest_split_memory = split_warmup_memory(&global, 0);
        // Not the oldest split search: limited to 100MB - 85MB = 15MB.
        let split_memory = split_warmup_memory(&global, 10 * MB);
        // 1.7 x 10MB = 17MB would go over the limit, but the 12MB needed fit.
        let _reservation = reserve_now(&split_memory, 12 * MB).unwrap().unwrap();
        assert_eq!(split_memory.reserved_num_bytes(), 12 * MB);
    }

    #[tokio::test]
    async fn test_reserve_memory_waits_when_budget_is_full() {
        let global = global_warmup_memory_with_split_max(100, 50);
        let _oldest_split_memory = split_warmup_memory(&global, 10 * MB);
        // Not the oldest split search: limited to 100MB - 50MB = 50MB, with the 10MB of the
        // oldest split search.
        let split_memory = split_warmup_memory(&global, 10 * MB);
        let mut reserve_fut = Box::pin(split_memory.reserve_memory(41 * MB));
        assert!(futures::poll!(&mut reserve_fut).is_pending());
        // Waiting is not running out of memory, and changes neither the reservation nor the memory
        // in use.
        assert!(!split_memory.has_run_out_of_memory());
        assert_eq!(split_memory.reserved_num_bytes(), 10 * MB);
    }

    #[test]
    fn test_reserve_memory_over_split_limit_fails() {
        let global = global_warmup_memory_with_split_max(100, 20);
        let split_memory = split_warmup_memory(&global, 10 * MB);
        // The global warmup memory has room left, but the per-split limit is 20MB.
        assert_eq!(
            reserve_now(&split_memory, 21 * MB).unwrap().err(),
            Some(OutOfMemoryError)
        );
        assert!(split_memory.has_run_out_of_memory());
        // Neither the reservation nor the global memory changed.
        assert_eq!(split_memory.reserved_num_bytes(), 10 * MB);
        assert_eq!(global.allocated(), 10 * MB);
    }

    #[test]
    fn test_reserve_memory_up_to_split_limit() {
        let global = global_warmup_memory_with_split_max(100, 20);
        let split_memory = split_warmup_memory(&global, 10 * MB);
        let _ = reserve_now(&split_memory, 20 * MB).unwrap().unwrap();
        assert_eq!(split_memory.reserved_num_bytes(), 20 * MB);
        assert!(!split_memory.has_run_out_of_memory());
    }

    #[test]
    fn test_reserve_memory_exponential_growth_clamped_to_split_limit() {
        let global = global_warmup_memory_with_split_max(100, 15);
        let split_memory = split_warmup_memory(&global, 10 * MB);
        // 11MB does not fit in 10MB. 1.7 x 10MB = 17MB is clamped to the 15MB per-split limit.
        let _ = reserve_now(&split_memory, 11 * MB).unwrap().unwrap();
        assert_eq!(split_memory.reserved_num_bytes(), 15 * MB);
        assert_eq!(global.allocated(), 15 * MB);
    }

    #[test]
    fn test_shrink_reservation_never_below_memory_in_use() {
        let global = global_warmup_memory(100);
        let split_memory = split_warmup_memory(&global, 10 * MB);
        let _reservation = reserve_now(&split_memory, 4 * MB).unwrap().unwrap();
        split_memory.shrink_reservation(MB);
        assert_eq!(split_memory.reserved_num_bytes(), 4 * MB);
        // Shrinking never grows the reservation.
        split_memory.shrink_reservation(20 * MB);
        assert_eq!(split_memory.reserved_num_bytes(), 4 * MB);
        assert_eq!(global.allocated(), 4 * MB);
    }

    #[test]
    fn test_release_memory_of_failed_read() {
        let global = global_warmup_memory(100);
        let split_memory = split_warmup_memory(&global, 10 * MB);
        let _successful_read_reservation = reserve_now(&split_memory, 4 * MB).unwrap().unwrap();
        let failed_read_reservation = reserve_now(&split_memory, 3 * MB).unwrap().unwrap();
        // The second read failed: dropping its reservation releases its memory.
        drop(failed_read_reservation);
        // The reservation is kept until the warmup ends...
        assert_eq!(split_memory.reserved_num_bytes(), 10 * MB);
        // ...but only the memory of the successful read is still in use.
        split_memory.release_unused_memory();
        assert_eq!(split_memory.reserved_num_bytes(), 4 * MB);
        assert_eq!(global.allocated(), 4 * MB);
    }

    #[test]
    #[should_panic(expected = "cannot release more warmup memory than reserved")]
    fn test_release_memory_above_memory_in_use_panics() {
        let global = global_warmup_memory(100);
        let split_memory = split_warmup_memory(&global, 10 * MB);
        let _reservation = reserve_now(&split_memory, MB).unwrap().unwrap();
        split_memory.release_memory(2 * MB);
    }

    #[test]
    fn test_release_unused_memory() {
        let global = global_warmup_memory(100);
        let split_memory = split_warmup_memory(&global, 10 * MB);
        let _reservation = reserve_now(&split_memory, 11 * MB).unwrap().unwrap();
        assert_eq!(split_memory.reserved_num_bytes(), 17 * MB);
        split_memory.release_unused_memory();
        assert_eq!(split_memory.reserved_num_bytes(), 11 * MB);
        assert_eq!(global.allocated(), 11 * MB);
    }

    #[test]
    fn test_drop_frees_reservation() {
        let global = global_warmup_memory(100);
        let split_memory = split_warmup_memory(&global, 10 * MB);
        // The reservation grows to 17MB. Releasing the memory in use does not shrink it.
        let reservation = reserve_now(&split_memory, 11 * MB).unwrap().unwrap();
        drop(reservation);
        drop(split_memory);
        assert_eq!(global.allocated(), 0);
    }

    #[test]
    fn test_concurrent_reserve_memory() {
        const NUM_THREADS: usize = 8;
        const NUM_RESERVATIONS_PER_THREAD: usize = 10;
        let global = global_warmup_memory(1_000);
        let split_memory = split_warmup_memory(&global, 10 * MB);
        std::thread::scope(|scope| {
            let mut join_handles = Vec::with_capacity(NUM_THREADS);
            for _ in 0..NUM_THREADS {
                let split_memory = &split_memory;
                join_handles.push(scope.spawn(move || {
                    let mut reservations = Vec::with_capacity(NUM_RESERVATIONS_PER_THREAD);
                    for _ in 0..NUM_RESERVATIONS_PER_THREAD {
                        reservations.push(reserve_now(split_memory, MB).unwrap().unwrap());
                    }
                    reservations
                }));
            }
            // The reservations are kept alive, so all 80MB stay in use.
            let reservations: Vec<_> = join_handles
                .into_iter()
                .flat_map(|join_handle| join_handle.join().unwrap())
                .collect();
            assert_eq!(
                reservations.len(),
                NUM_THREADS * NUM_RESERVATIONS_PER_THREAD
            );
            // All 80MB are in use, so the reservation cannot go below that.
            split_memory.release_unused_memory();
            assert_eq!(split_memory.reserved_num_bytes(), 80 * MB);
        });
    }

    #[tokio::test]
    async fn test_reserve_memory_waits_until_oldest_split_frees_memory() {
        let global = global_warmup_memory_with_split_max(100, 50);
        let oldest_split_memory = split_warmup_memory(&global, 10 * MB);
        let split_memory = split_warmup_memory(&global, 10 * MB);
        let oldest_reservation = reserve_now(&oldest_split_memory, 40 * MB).unwrap().unwrap();
        // 40MB + 10MB fill the 100MB - 50MB the non-oldest split searches can use.
        let mut reserve_fut = Box::pin(split_memory.reserve_memory(11 * MB));
        assert!(futures::poll!(&mut reserve_fut).is_pending());
        assert!(!split_memory.has_run_out_of_memory());
        // The oldest split search completes: the other one becomes the oldest one.
        drop(oldest_reservation);
        drop(oldest_split_memory);
        let _reservation = tokio::time::timeout(Duration::from_secs(5), reserve_fut)
            .await
            .expect("the reservation should not wait once memory is freed")
            .unwrap();
        assert!(split_memory.reserved_num_bytes() >= 11 * MB);
    }

    #[tokio::test]
    async fn test_reserve_memory_never_waits_for_oldest_split() {
        let global = global_warmup_memory_with_split_max(100, 50);
        let oldest_split_memory = split_warmup_memory(&global, 10 * MB);
        let split_memory = split_warmup_memory(&global, 10 * MB);
        let _reservation = reserve_now(&split_memory, 40 * MB).unwrap().unwrap();
        // The non-oldest split searches use all they can, but the oldest one can still grow to
        // the per-split limit.
        let _oldest_reservation = oldest_split_memory
            .reserve_memory(50 * MB)
            .now_or_never()
            .expect("the oldest split search should never wait")
            .unwrap();
        assert_eq!(global.allocated(), 90 * MB);
    }

    #[tokio::test]
    async fn test_reserve_memory_over_split_limit_fails_right_away() {
        let global = global_warmup_memory_with_split_max(100, 20);
        let _oldest_split_memory = split_warmup_memory(&global, 10 * MB);
        let split_memory = split_warmup_memory(&global, 10 * MB);
        let reserve_res = split_memory
            .reserve_memory(21 * MB)
            .now_or_never()
            .expect("going over the per-split limit should fail without waiting");
        assert_eq!(reserve_res.err(), Some(OutOfMemoryError));
        assert!(split_memory.has_run_out_of_memory());
    }

    #[tokio::test]
    async fn test_dropped_reserve_memory_future_reserves_nothing() {
        let global = global_warmup_memory_with_split_max(100, 50);
        let oldest_split_memory = split_warmup_memory(&global, 50 * MB);
        let split_memory = split_warmup_memory(&global, 0);
        let mut reserve_fut = Box::pin(split_memory.reserve_memory(MB));
        assert!(futures::poll!(&mut reserve_fut).is_pending());
        drop(reserve_fut);
        assert_eq!(split_memory.reserved_num_bytes(), 0);
        assert_eq!(global.allocated(), 50 * MB);
        drop(oldest_split_memory);
        assert_eq!(global.allocated(), 0);
    }

    /// Several split searches fill the warmup memory, then all ask for more. With a plain
    /// semaphore, they would wait for each other forever.
    #[tokio::test]
    async fn test_reserve_memory_does_not_deadlock() {
        const NUM_SPLITS: usize = 4;
        const NUM_READS_PER_SPLIT: usize = 8;
        let global = global_warmup_memory_with_split_max(100, 40);
        let mut join_handles = Vec::with_capacity(NUM_SPLITS);
        for _ in 0..NUM_SPLITS {
            let split_memory = Arc::new(split_warmup_memory(&global, 10 * MB));
            join_handles.push(tokio::spawn(async move {
                // The reservations are kept until the end, like the data of a warmup cache.
                let mut reservations = Vec::with_capacity(NUM_READS_PER_SPLIT);
                for _ in 0..NUM_READS_PER_SPLIT {
                    reservations.push(split_memory.reserve_memory(5 * MB).await.unwrap());
                    tokio::task::yield_now().await;
                }
            }));
        }
        let all_splits_fut = futures::future::join_all(join_handles);
        let join_results = tokio::time::timeout(Duration::from_secs(10), all_splits_fut)
            .await
            .expect("split searches should not deadlock");
        for join_result in join_results {
            join_result.unwrap();
        }
        assert_eq!(global.allocated(), 0);
    }
}
