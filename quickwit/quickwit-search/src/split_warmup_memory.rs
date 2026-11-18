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

use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;
use quickwit_directories::{MemoryReservation, MemoryReserver, OutOfMemoryError};
use tokio::sync::Notify;

use crate::metrics::LEAF_SEARCH_SPLIT_WARMUP_OUT_OF_MEMORY_TOTAL;
use crate::warmup_memory::WarmupMemoryGuard;

/// Growth factor of a split's memory reservation, as `GROWTH_NUMERATOR / GROWTH_DENOMINATOR`.
///
/// Growing exponentially reduces the number of allocations in the global warmup memory, at the
/// cost of reserving up to 70% more memory than needed during the warmup.
const GROWTH_NUMERATOR: u64 = 17;
const GROWTH_DENOMINATOR: u64 = 10;

/// Warmup memory of a single split search.
///
/// Shared (`Arc<SplitWarmupMemory>`) between the split's `SearchPermit` and its `CachingDirectory`,
/// which reserves memory before each cache miss. The reservation is freed when the last of them
/// is dropped, so it lives as long as the cache that holds the warmup data.
pub(crate) struct SplitWarmupMemory {
    /// Only locked for some arithmetic, never across an `.await`.
    state: Mutex<SplitWarmupMemoryState>,
    /// Notified each time warmup memory is freed. Kept outside of `state`, so that
    /// `reserve_memory` can wait on it without holding the lock.
    memory_freed_notify: Arc<Notify>,
}

struct SplitWarmupMemoryState {
    /// Bytes reserved for data that entered the split's warmup cache.
    in_use: u64,
    /// Memory reserved in the global warmup memory.
    ///
    /// Invariant: `in_use <= reservation.num_bytes()`.
    reservation: WarmupMemoryGuard,
    /// Set the first time the reservation fails to grow.
    /// The point is to count each split search only once in the out of memory metric.
    out_of_memory: bool,
}

impl SplitWarmupMemory {
    pub fn new(reservation: WarmupMemoryGuard) -> Self {
        let memory_freed_notify = reservation.memory_freed_notify();
        let state = SplitWarmupMemoryState {
            in_use: 0,
            reservation,
            out_of_memory: false,
        };
        SplitWarmupMemory {
            state: Mutex::new(state),
            memory_freed_notify,
        }
    }

    /// Bytes currently reserved.
    #[cfg(test)]
    pub fn reserved_num_bytes(&self) -> u64 {
        self.lock_state().reservation.num_bytes()
    }

    /// Whether the reservation failed to grow, because of the per-split warmup memory limit.
    #[cfg(test)]
    pub fn has_run_out_of_memory(&self) -> bool {
        self.lock_state().out_of_memory
    }

    /// Shrinks the reservation to `max_num_bytes`, but never below the memory in use. Does nothing
    /// if the reservation is already smaller.
    pub fn shrink_reservation(&self, max_num_bytes: u64) {
        let mut state = self.lock_state();
        let new_num_bytes = max_num_bytes.max(state.in_use);
        if new_num_bytes >= state.reservation.num_bytes() {
            return;
        }
        state.reservation.shrink_to(new_num_bytes);
    }

    /// Shrinks the reservation to the memory in use.
    pub fn release_unused_memory(&self) {
        self.shrink_reservation(0);
    }

    fn lock_state(&self) -> MutexGuard<'_, SplitWarmupMemoryState> {
        self.state
            .lock()
            .expect("split warmup memory lock should not be poisoned")
    }
}

#[async_trait]
impl MemoryReserver for SplitWarmupMemory {
    /// Reserves memory for `num_bytes` of warmup data, growing the reservation if needed.
    ///
    /// Waits while the global warmup memory is full. This cannot deadlock: the oldest split search
    /// never waits (see `GlobalWarmupMemory`), and the other ones wait for it to free its memory.
    /// Fails right away if the reservation would go over the per-split limit.
    ///
    /// Cancel-safe: if the future is dropped while waiting, nothing is reserved.
    async fn reserve_memory(
        &self,
        num_bytes: u64,
    ) -> Result<MemoryReservation<'_>, OutOfMemoryError> {
        loop {
            // Registered before trying, so that memory freed between the attempt and the `.await`
            // still wakes us up.
            let memory_freed = self.memory_freed_notify.notified();
            let mut memory_freed = std::pin::pin!(memory_freed);
            memory_freed.as_mut().enable();
            {
                let mut state = self.lock_state();
                let needed_num_bytes = state.in_use + num_bytes;
                if needed_num_bytes > state.reservation.max_num_bytes() {
                    // Waiting cannot help: fails right away.
                    state.record_out_of_memory(needed_num_bytes);
                    return Err(OutOfMemoryError);
                }
                if state.grow_reservation(needed_num_bytes) {
                    state.in_use = needed_num_bytes;
                    return Ok(MemoryReservation::new(self, num_bytes));
                }
                // Not enough memory left in the global warmup memory: waits for memory to be
                // freed, then tries again.
            }
            memory_freed.await;
        }
    }

    /// Gives back memory reserved for a read that failed. The reservation itself is not shrunk:
    /// `release_unused_memory` does it after the warmup.
    ///
    /// Does not panic, because it is called from `Drop`, possibly while a panic unwinds. Releasing
    /// more than the memory in use breaks the contract of [`MemoryReserver::release_memory`]: it
    /// fails a debug assertion, and saturates at 0 in release builds.
    fn release_memory(&self, num_bytes: u64) {
        // Unlike `lock_state`, does not panic on a poisoned lock.
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        debug_assert!(
            num_bytes <= state.in_use,
            "cannot release more warmup memory than reserved"
        );
        state.in_use = state.in_use.saturating_sub(num_bytes);
    }
}

impl SplitWarmupMemoryState {
    /// Counts the split search as out of memory and logs a warning, only the first time.
    fn record_out_of_memory(&mut self, needed_num_bytes: u64) {
        if self.out_of_memory {
            return;
        }
        self.out_of_memory = true;
        LEAF_SEARCH_SPLIT_WARMUP_OUT_OF_MEMORY_TOTAL.inc();
        quickwit_common::rate_limited_warn!(
            limit_per_min = 10,
            allocated_num_bytes = self.reservation.num_bytes(),
            needed_num_bytes,
            split_max_num_bytes = self.reservation.max_num_bytes(),
            "split search ran out of warmup memory"
        );
    }

    /// Grows the reservation so that it holds at least `needed_num_bytes`.
    ///
    /// Tries to grow by `GROWTH_NUMERATOR / GROWTH_DENOMINATOR` first, within the per-split limit,
    /// and falls back to the exact need if that does not fit in the global warmup memory. So the
    /// exponential growth never makes a split fail.
    ///
    /// Returns false if the global warmup memory does not have enough memory left right now. The
    /// reservation is then left unchanged.
    ///
    /// Contract: `needed_num_bytes <= reservation.max_num_bytes()`.
    fn grow_reservation(&mut self, needed_num_bytes: u64) -> bool {
        let allocated_num_bytes = self.reservation.num_bytes();
        if needed_num_bytes <= allocated_num_bytes {
            return true;
        }
        let exponential_num_bytes = allocated_num_bytes
            .saturating_mul(GROWTH_NUMERATOR)
            .div_ceil(GROWTH_DENOMINATOR)
            .min(self.reservation.max_num_bytes());
        if exponential_num_bytes > needed_num_bytes
            && self
                .reservation
                .try_grow(exponential_num_bytes - allocated_num_bytes)
        {
            return true;
        }
        self.reservation
            .try_grow(needed_num_bytes - allocated_num_bytes)
    }
}

#[cfg(test)]
mod tests {
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
        GlobalWarmupMemory::with_limits_and_free_callback(
            ByteSize::mb(limit_mb),
            ByteSize::mb(split_max_mb),
            || {},
        )
    }

    /// Reserves memory, expecting `reserve_memory` not to wait.
    fn reserve_now(
        split_memory: &SplitWarmupMemory,
        num_bytes: u64,
    ) -> Result<MemoryReservation<'_>, OutOfMemoryError> {
        split_memory
            .reserve_memory(num_bytes)
            .now_or_never()
            .expect("reserve_memory should not wait")
    }

    fn split_warmup_memory(
        global: &GlobalWarmupMemory,
        initial_reservation_num_bytes: u64,
    ) -> SplitWarmupMemory {
        let reservation = global.try_allocate(initial_reservation_num_bytes).unwrap();
        SplitWarmupMemory::new(reservation)
    }

    #[test]
    fn test_reserve_memory_within_reservation() {
        let global = global_warmup_memory(100);
        let split_memory = split_warmup_memory(&global, 10 * MB);
        let _reservation_1 = reserve_now(&split_memory, 4 * MB).unwrap();
        let _reservation_2 = reserve_now(&split_memory, 6 * MB).unwrap();
        assert_eq!(split_memory.reserved_num_bytes(), 10 * MB);
        assert_eq!(global.allocated(), 10 * MB);
    }

    #[test]
    fn test_reserve_memory_grows_exponentially() {
        let global = global_warmup_memory(100);
        let split_memory = split_warmup_memory(&global, 10 * MB);
        // 11MB does not fit in 10MB: the reservation grows to 1.7 x 10MB.
        let _reservation_1 = reserve_now(&split_memory, 11 * MB).unwrap();
        assert_eq!(split_memory.reserved_num_bytes(), 17 * MB);
        // 11MB + 6MB = 17MB still fits.
        let _reservation_2 = reserve_now(&split_memory, 6 * MB).unwrap();
        assert_eq!(split_memory.reserved_num_bytes(), 17 * MB);
        // 18MB does not fit: the reservation grows to 1.7 x 17MB.
        let _reservation_3 = reserve_now(&split_memory, MB).unwrap();
        assert_eq!(split_memory.reserved_num_bytes(), 28_900_000);
        assert_eq!(global.allocated(), 28_900_000);
    }

    #[test]
    fn test_reserve_memory_falls_back_to_exact_need() {
        let global = global_warmup_memory_with_split_max(100, 85);
        let _oldest_reservation = global.try_allocate(0).unwrap();
        // Not the oldest split search: limited to 100MB - 85MB = 15MB.
        let split_memory = split_warmup_memory(&global, 10 * MB);
        // 1.7 x 10MB = 17MB would go over the limit, but the 12MB needed fit.
        let _reservation = reserve_now(&split_memory, 12 * MB).unwrap();
        assert_eq!(split_memory.reserved_num_bytes(), 12 * MB);
    }

    #[tokio::test]
    async fn test_reserve_memory_over_global_limit_waits() {
        let global = global_warmup_memory_with_split_max(100, 50);
        let _oldest_reservation = global.try_allocate(10 * MB).unwrap();
        // Not the oldest split search: limited to 100MB - 50MB = 50MB, with the 10MB of the
        // oldest split search.
        let split_memory = split_warmup_memory(&global, 10 * MB);
        let mut reserve_fut = Box::pin(split_memory.reserve_memory(41 * MB));
        assert!(futures::poll!(&mut reserve_fut).is_pending());
        drop(reserve_fut);
        // Waiting is not running out of memory.
        assert!(!split_memory.has_run_out_of_memory());
        // A pending reservation changes neither the reservation nor the memory in use.
        assert_eq!(split_memory.reserved_num_bytes(), 10 * MB);
        let _reservation = reserve_now(&split_memory, 40 * MB).unwrap();
        assert_eq!(split_memory.reserved_num_bytes(), 40 * MB);
    }

    #[test]
    fn test_reserve_memory_up_to_split_limit() {
        let global = global_warmup_memory_with_split_max(100, 20);
        let split_memory = split_warmup_memory(&global, 10 * MB);
        let _ = reserve_now(&split_memory, 20 * MB).unwrap();
        assert_eq!(split_memory.reserved_num_bytes(), 20 * MB);
        assert!(!split_memory.has_run_out_of_memory());
    }

    #[test]
    fn test_reserve_memory_exponential_growth_clamped_to_split_limit() {
        let global = global_warmup_memory_with_split_max(100, 15);
        let split_memory = split_warmup_memory(&global, 10 * MB);
        // 11MB does not fit in 10MB. 1.7 x 10MB = 17MB is clamped to the 15MB per-split limit.
        let _ = reserve_now(&split_memory, 11 * MB).unwrap();
        assert_eq!(split_memory.reserved_num_bytes(), 15 * MB);
        assert_eq!(global.allocated(), 15 * MB);
    }

    #[test]
    fn test_shrink_reservation_never_below_memory_in_use() {
        let global = global_warmup_memory(100);
        let split_memory = split_warmup_memory(&global, 10 * MB);
        let _reservation = reserve_now(&split_memory, 4 * MB).unwrap();
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
        let _successful_read_reservation = reserve_now(&split_memory, 4 * MB).unwrap();
        let failed_read_reservation = reserve_now(&split_memory, 3 * MB).unwrap();
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
        let _reservation = reserve_now(&split_memory, MB).unwrap();
        split_memory.release_memory(2 * MB);
    }

    #[test]
    fn test_release_unused_memory() {
        let global = global_warmup_memory(100);
        let split_memory = split_warmup_memory(&global, 10 * MB);
        let _reservation = reserve_now(&split_memory, 11 * MB).unwrap();
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
        let reservation = reserve_now(&split_memory, 11 * MB).unwrap();
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
                        reservations.push(reserve_now(split_memory, MB).unwrap());
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
        let oldest_reservation = reserve_now(&oldest_split_memory, 40 * MB).unwrap();
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
        let _reservation = reserve_now(&split_memory, 40 * MB).unwrap();
        // The non-oldest split searches use all they can, but the oldest one can still grow to
        // the per-split limit.
        let _oldest_reservation = oldest_split_memory
            .reserve_memory(50 * MB)
            .now_or_never()
            .expect("the oldest split search should never wait")
            .unwrap();
        assert_eq!(global.allocated(), 90 * MB);
    }

    #[test]
    fn test_reserve_memory_over_split_limit_fails_right_away() {
        let global = global_warmup_memory_with_split_max(100, 20);
        let _oldest_split_memory = split_warmup_memory(&global, 10 * MB);
        // Not the oldest split search: going over the per-split limit fails without waiting.
        let split_memory = split_warmup_memory(&global, 10 * MB);
        assert_eq!(
            reserve_now(&split_memory, 21 * MB).err(),
            Some(OutOfMemoryError)
        );
        assert!(split_memory.has_run_out_of_memory());
        // Neither the reservation nor the global memory changed.
        assert_eq!(split_memory.reserved_num_bytes(), 10 * MB);
        assert_eq!(global.allocated(), 20 * MB);
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
