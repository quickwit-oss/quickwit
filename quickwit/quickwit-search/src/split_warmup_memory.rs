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

use std::sync::{Mutex, MutexGuard};

use async_trait::async_trait;
use quickwit_directories::{MemoryReservation, MemoryReserver, OutOfMemoryError};

use crate::metrics::LEAF_SEARCH_SPLIT_WARMUP_OUT_OF_MEMORY_TOTAL;
use crate::warmup_memory::WarmupMemoryGuard;

/// Counts the split search as out of memory and logs a warning. Called once per split search.
fn record_out_of_memory(allocated_num_bytes: u64, needed_num_bytes: u64, split_max_num_bytes: u64) {
    LEAF_SEARCH_SPLIT_WARMUP_OUT_OF_MEMORY_TOTAL.inc();
    quickwit_common::rate_limited_warn!(
        limit_per_min = 10,
        allocated_num_bytes,
        needed_num_bytes,
        split_max_num_bytes,
        "split search ran out of warmup memory: {OutOfMemoryError}"
    );
}

/// Growth factor of a split's memory reservation, as `GROWTH_NUMERATOR / GROWTH_DENOMINATOR`.
///
/// Growing exponentially reduces the number of allocations in the global warmup memory, at the
/// cost of reserving up to 70% more memory than needed during the warmup.
const GROWTH_NUMERATOR: u64 = 17;
const GROWTH_DENOMINATOR: u64 = 10;

/// Warmup memory of a single split search.
///
/// Shared (`Arc<SplitWarmupMemory>`) between the split's `SearchPermit` and its `CachingDirectory`,
/// which reserves memory before each async cache miss. The reservation is freed when the last of
/// them is dropped, so it lives as long as the cache that holds the warmup data.
pub(crate) struct SplitWarmupMemory {
    /// Memory reserved in the global warmup memory.
    reservation: WarmupMemoryGuard,
    /// The reservation never grows above this. Never changes.
    ///
    /// The initial reservation is expected to be lower or equal: the permit size is clamped to it.
    max_num_bytes: u64,
    /// Only locked for integer arithmetic, never across an `.await`.
    state: Mutex<SplitWarmupMemoryState>,
}

struct SplitWarmupMemoryState {
    /// Bytes reserved for data that entered (or is being read into) the split's warmup cache.
    ///
    /// Invariant: `in_use <= reservation.num_bytes()`.
    in_use: u64,
    /// Set the first time a reservation exceeds `max_num_bytes`.
    /// The point is to count each split search only once in the out of memory metric.
    out_of_memory: bool,
}

impl SplitWarmupMemory {
    /// `max_num_bytes` is the per-split limit: the reservation never grows above it, even if the
    /// global warmup memory has room left or this split is the oldest active split search.
    pub fn new(reservation: WarmupMemoryGuard, max_num_bytes: u64) -> Self {
        debug_assert!(reservation.num_bytes() <= max_num_bytes);
        let state = SplitWarmupMemoryState {
            in_use: 0,
            out_of_memory: false,
        };
        SplitWarmupMemory {
            reservation,
            max_num_bytes,
            state: Mutex::new(state),
        }
    }

    /// Bytes currently reserved.
    #[cfg(test)]
    pub fn reserved_num_bytes(&self) -> u64 {
        self.reservation.num_bytes()
    }

    /// Whether the reservation failed to grow, because of the per-split warmup memory limit.
    #[cfg(test)]
    pub fn has_run_out_of_memory(&self) -> bool {
        self.lock_state().out_of_memory
    }

    /// Shrinks the reservation to `max_num_bytes`, but never below the memory in use. Does nothing
    /// if the reservation is already smaller.
    pub fn shrink_reservation(&self, max_num_bytes: u64) {
        let state = self.lock_state();
        let new_num_bytes = max_num_bytes.max(state.in_use);
        if new_num_bytes >= self.reservation.num_bytes() {
            return;
        }
        self.reservation.shrink_to(new_num_bytes);
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
    /// Waits until enough global warmup memory is freed, unless this split search is the oldest
    /// active one (which allocates immediately without waiting to prevent deadlocks).
    ///
    /// Fails right away if `num_bytes` would push this split over its per-split limit.
    async fn reserve_memory(
        &self,
        num_bytes: u64,
    ) -> Result<MemoryReservation<'_>, OutOfMemoryError> {
        loop {
            let (allocated_num_bytes, needed_num_bytes) = {
                let mut state = self.lock_state();
                let needed_num_bytes = state.in_use.saturating_add(num_bytes);
                let allocated_num_bytes = self.reservation.num_bytes();
                if needed_num_bytes > self.max_num_bytes {
                    if !state.out_of_memory {
                        state.out_of_memory = true;
                        record_out_of_memory(
                            allocated_num_bytes,
                            needed_num_bytes,
                            self.max_num_bytes,
                        );
                    }
                    return Err(OutOfMemoryError);
                }
                if needed_num_bytes <= allocated_num_bytes {
                    state.in_use = needed_num_bytes;
                    return Ok(MemoryReservation::new(self, num_bytes));
                }
                (allocated_num_bytes, needed_num_bytes)
            };

            let desired_num_bytes = allocated_num_bytes
                .saturating_mul(GROWTH_NUMERATOR)
                .div_ceil(GROWTH_DENOMINATOR)
                .min(self.max_num_bytes)
                .max(needed_num_bytes);

            self.reservation
                .grow_or_wait(needed_num_bytes, desired_num_bytes)
                .await;
        }
    }

    /// Gives back memory reserved for a read that failed or overlapped cached bytes. The
    /// reservation itself is not shrunk: `release_unused_memory` does it after the warmup.
    ///
    /// Does not panic, because it is called from `Drop`, possibly while a panic unwinds. Releasing
    /// more than the memory in use breaks the contract of [`MemoryReserver::release_memory`]: it
    /// fails a debug assertion, and saturates at 0 in release builds.
    fn release_memory(&self, num_bytes: u64) {
        if num_bytes == 0 {
            return;
        }
        // Unlike `lock_state`, does not panic on a poisoned lock.
        {
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
        // Wake any sibling read on this split that may now fit within `reservation`.
        self.reservation.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytesize::ByteSize;
    use tokio::task::JoinSet;

    use super::*;
    use crate::warmup_memory::GlobalWarmupMemory;

    const MB: u64 = 1_000_000;

    fn split_warmup_memory(
        global: &GlobalWarmupMemory,
        initial_reservation_num_bytes: u64,
    ) -> SplitWarmupMemory {
        let reservation = global.try_allocate(initial_reservation_num_bytes).unwrap();
        SplitWarmupMemory::new(reservation, u64::MAX)
    }

    #[tokio::test]
    async fn test_reserve_memory_within_reservation() {
        let global = GlobalWarmupMemory::with_limit_and_free_callback(ByteSize::mb(100), || {});
        let split_memory = split_warmup_memory(&global, 10 * MB);
        let _reservation_1 = split_memory.reserve_memory(4 * MB).await.unwrap();
        let _reservation_2 = split_memory.reserve_memory(6 * MB).await.unwrap();
        assert_eq!(split_memory.reserved_num_bytes(), 10 * MB);
        assert_eq!(global.allocated(), 10 * MB);
    }

    #[tokio::test]
    async fn test_reserve_memory_grows_exponentially() {
        let global = GlobalWarmupMemory::with_limit_and_free_callback(ByteSize::mb(100), || {});
        let split_memory = split_warmup_memory(&global, 10 * MB);
        // 11MB does not fit in 10MB: the reservation grows to 1.7 x 10MB.
        let _reservation_1 = split_memory.reserve_memory(11 * MB).await.unwrap();
        assert_eq!(split_memory.reserved_num_bytes(), 17 * MB);
        // 11MB + 6MB = 17MB still fits.
        let _reservation_2 = split_memory.reserve_memory(6 * MB).await.unwrap();
        assert_eq!(split_memory.reserved_num_bytes(), 17 * MB);
        // 18MB does not fit: the reservation grows to 1.7 x 17MB.
        let _reservation_3 = split_memory.reserve_memory(MB).await.unwrap();
        assert_eq!(split_memory.reserved_num_bytes(), 28_900_000);
        assert_eq!(global.allocated(), 28_900_000);
    }

    #[tokio::test]
    async fn test_reserve_memory_falls_back_to_exact_need() {
        let global = GlobalWarmupMemory::with_limit_and_free_callback(ByteSize::mb(15), || {});
        let split_memory = split_warmup_memory(&global, 10 * MB);
        // 1.7 x 10MB = 17MB would go over the limit, but the 12MB needed fit.
        let _reservation = split_memory.reserve_memory(12 * MB).await.unwrap();
        assert_eq!(split_memory.reserved_num_bytes(), 12 * MB);
    }

    #[tokio::test]
    async fn test_reserve_memory_waits_for_global_memory_while_oldest_progresses() {
        let global = GlobalWarmupMemory::with_limit_and_free_callback(ByteSize::mb(100), || {});
        let oldest_split = Arc::new(split_warmup_memory(&global, 40 * MB));
        let younger_split = Arc::new(split_warmup_memory(&global, 40 * MB));

        // Younger split wants 70MB total (needs +30MB, only 20MB left in global budget): it waits.
        let younger_split_clone = younger_split.clone();
        let younger_handle = tokio::spawn(async move {
            let _reservation = younger_split_clone.reserve_memory(70 * MB).await.unwrap();
            younger_split_clone.reserved_num_bytes()
        });
        tokio::task::yield_now().await;
        assert!(!younger_handle.is_finished());
        assert_eq!(younger_split.reserved_num_bytes(), 40 * MB);

        // Oldest split also wants 70MB total (40MB + 30MB + younger's 40MB = 110MB > 100MB):
        // because it is the oldest active split, it allocates immediately without waiting!
        let oldest_reservation = oldest_split.reserve_memory(70 * MB).await.unwrap();
        assert_eq!(oldest_split.reserved_num_bytes(), 70 * MB);
        assert_eq!(global.allocated(), 110 * MB);
        assert!(!younger_handle.is_finished());

        // Once the oldest split finishes and drops its memory, the younger split unblocks.
        drop(oldest_reservation);
        drop(oldest_split);
        let younger_reserved = younger_handle.await.unwrap();
        assert_eq!(younger_reserved, 70 * MB);
        drop(younger_split);
        assert_eq!(global.allocated(), 0);
    }

    #[tokio::test]
    async fn test_reserve_memory_over_split_limit_fails_even_for_oldest() {
        let global = GlobalWarmupMemory::with_limit_and_free_callback(ByteSize::mb(100), || {});
        let reservation = global.try_allocate(10 * MB).unwrap();
        let split_memory = SplitWarmupMemory::new(reservation, 20 * MB);
        // Even as the oldest split with global room left, exceeding the per-split limit fails.
        assert_eq!(
            split_memory.reserve_memory(21 * MB).await.err(),
            Some(OutOfMemoryError)
        );
        assert!(split_memory.has_run_out_of_memory());
        // Neither the reservation nor the global memory changed.
        assert_eq!(split_memory.reserved_num_bytes(), 10 * MB);
        assert_eq!(global.allocated(), 10 * MB);
    }

    #[tokio::test]
    async fn test_reserve_memory_up_to_split_limit() {
        let global = GlobalWarmupMemory::with_limit_and_free_callback(ByteSize::mb(100), || {});
        let reservation = global.try_allocate(10 * MB).unwrap();
        let split_memory = SplitWarmupMemory::new(reservation, 20 * MB);
        let _reservation = split_memory.reserve_memory(20 * MB).await.unwrap();
        assert_eq!(split_memory.reserved_num_bytes(), 20 * MB);
        assert!(!split_memory.has_run_out_of_memory());
    }

    #[tokio::test]
    async fn test_reserve_memory_exponential_growth_clamped_to_split_limit() {
        let global = GlobalWarmupMemory::with_limit_and_free_callback(ByteSize::mb(100), || {});
        let reservation = global.try_allocate(10 * MB).unwrap();
        let split_memory = SplitWarmupMemory::new(reservation, 15 * MB);
        // 11MB does not fit in 10MB. 1.7 x 10MB = 17MB is clamped to the 15MB per-split limit.
        let _reservation = split_memory.reserve_memory(11 * MB).await.unwrap();
        assert_eq!(split_memory.reserved_num_bytes(), 15 * MB);
        assert_eq!(global.allocated(), 15 * MB);
    }

    #[tokio::test]
    async fn test_shrink_reservation_never_below_memory_in_use() {
        let global = GlobalWarmupMemory::with_limit_and_free_callback(ByteSize::mb(100), || {});
        let split_memory = split_warmup_memory(&global, 10 * MB);
        let _reservation = split_memory.reserve_memory(4 * MB).await.unwrap();
        split_memory.shrink_reservation(MB);
        assert_eq!(split_memory.reserved_num_bytes(), 4 * MB);
        // Shrinking never grows the reservation.
        split_memory.shrink_reservation(20 * MB);
        assert_eq!(split_memory.reserved_num_bytes(), 4 * MB);
        assert_eq!(global.allocated(), 4 * MB);
    }

    #[tokio::test]
    async fn test_release_memory_of_failed_read_wakes_sibling_read() {
        let global = GlobalWarmupMemory::with_limit_and_free_callback(ByteSize::mb(100), || {});
        let _oldest_blocker = global.try_allocate(90 * MB).unwrap();
        let split_memory = Arc::new(split_warmup_memory(&global, 10 * MB));

        let _successful_read_reservation = split_memory.reserve_memory(4 * MB).await.unwrap();
        let failed_read_reservation = split_memory.reserve_memory(5 * MB).await.unwrap();

        // Global budget is full (90 + 10 = 100MB) and 9MB of 10MB are in use on this split:
        // a sibling 4MB read must wait.
        let split_memory_clone = split_memory.clone();
        let sibling_handle = tokio::spawn(async move {
            let _sibling = split_memory_clone.reserve_memory(4 * MB).await.unwrap();
        });
        tokio::task::yield_now().await;
        assert!(!sibling_handle.is_finished());

        // Dropping the failed 5MB read frees `in_use` down to 4MB, so 4MB + 4MB <= 10MB fits
        // inside this split's existing reservation without needing global memory.
        drop(failed_read_reservation);
        sibling_handle.await.unwrap();

        // Sibling reservation was dropped on task exit, so only the 4MB read remains in use.
        split_memory.release_unused_memory();
        assert_eq!(split_memory.reserved_num_bytes(), 4 * MB);
        assert_eq!(global.allocated(), 94 * MB);
    }

    #[tokio::test]
    #[should_panic(expected = "cannot release more warmup memory than reserved")]
    async fn test_release_memory_above_memory_in_use_panics() {
        let global = GlobalWarmupMemory::with_limit_and_free_callback(ByteSize::mb(100), || {});
        let split_memory = split_warmup_memory(&global, 10 * MB);
        let _reservation = split_memory.reserve_memory(MB).await.unwrap();
        split_memory.release_memory(2 * MB);
    }

    #[tokio::test]
    async fn test_release_unused_memory() {
        let global = GlobalWarmupMemory::with_limit_and_free_callback(ByteSize::mb(100), || {});
        let split_memory = split_warmup_memory(&global, 10 * MB);
        let _reservation = split_memory.reserve_memory(11 * MB).await.unwrap();
        assert_eq!(split_memory.reserved_num_bytes(), 17 * MB);
        split_memory.release_unused_memory();
        assert_eq!(split_memory.reserved_num_bytes(), 11 * MB);
        assert_eq!(global.allocated(), 11 * MB);
    }

    #[tokio::test]
    async fn test_drop_frees_reservation() {
        let global = GlobalWarmupMemory::with_limit_and_free_callback(ByteSize::mb(100), || {});
        let split_memory = split_warmup_memory(&global, 10 * MB);
        // The reservation grows to 17MB. Releasing the memory in use does not shrink it.
        let reservation = split_memory.reserve_memory(11 * MB).await.unwrap();
        drop(reservation);
        drop(split_memory);
        assert_eq!(global.allocated(), 0);
    }

    #[tokio::test]
    async fn test_concurrent_reserve_memory() {
        const NUM_TASKS: usize = 8;
        const NUM_RESERVATIONS_PER_TASK: usize = 10;
        let global = GlobalWarmupMemory::with_limit_and_free_callback(ByteSize::gb(1), || {});
        let split_memory = Arc::new(split_warmup_memory(&global, 10 * MB));
        let mut join_set = JoinSet::new();
        for _ in 0..NUM_TASKS {
            let split_memory = split_memory.clone();
            join_set.spawn(async move {
                for _ in 0..NUM_RESERVATIONS_PER_TASK {
                    let reservation = split_memory.reserve_memory(MB).await.unwrap();
                    // Keep the bytes counted in `in_use` as if added to the cache.
                    std::mem::forget(reservation);
                }
            });
        }
        while let Some(join_res) = join_set.join_next().await {
            join_res.unwrap();
        }
        // All 80MB are in use, so the reservation cannot go below that.
        split_memory.release_unused_memory();
        assert_eq!(split_memory.reserved_num_bytes(), 80 * MB);
    }

    #[tokio::test]
    async fn test_concurrent_splits_all_exceed_initial_budget_without_deadlock() {
        use std::path::Path;

        use quickwit_directories::CachingDirectory;
        use tantivy::Directory;
        use tantivy::directory::RamDirectory;

        const NUM_SPLITS: usize = 10;
        const INITIAL_PER_SPLIT: u64 = 5 * MB;
        const MAX_PER_SPLIT: u64 = 25 * MB;
        const GLOBAL_LIMIT: u64 = NUM_SPLITS as u64 * INITIAL_PER_SPLIT;
        const READ_SIZE: usize = 4_000_000;
        const READS_PER_SPLIT: usize = 5;

        let global =
            GlobalWarmupMemory::with_limit_and_free_callback(ByteSize(GLOBAL_LIMIT), || {});
        let file_path = Path::new("postings");
        let ram_directory = RamDirectory::default();
        ram_directory
            .atomic_write(file_path, &vec![7u8; READ_SIZE * READS_PER_SPLIT])
            .unwrap();
        let underlying: Arc<dyn Directory> = Arc::new(ram_directory);

        // Allocate all 10 initial guards up front so 100% of `GLOBAL_LIMIT` is already reserved
        // before any split starts warming up: without oldest-split progress this would deadlock.
        let mut split_memories = Vec::with_capacity(NUM_SPLITS);
        for _ in 0..NUM_SPLITS {
            let guard = global.try_allocate(INITIAL_PER_SPLIT).unwrap();
            split_memories.push(Arc::new(SplitWarmupMemory::new(guard, MAX_PER_SPLIT)));
        }
        assert_eq!(global.allocated(), GLOBAL_LIMIT);

        let mut join_set = JoinSet::new();
        for split_memory in split_memories {
            let underlying_clone = underlying.clone();
            let global_clone = global.clone();
            join_set.spawn(async move {
                let caching_directory = CachingDirectory::new_unbounded(underlying_clone)
                    .with_memory_reserver(split_memory.clone());
                let file_handle = caching_directory
                    .get_file_handle(Path::new("postings"))
                    .unwrap();
                let mut read_tasks = JoinSet::new();
                for read_idx in 0..READS_PER_SPLIT {
                    let file_handle_clone = file_handle.clone();
                    let range = (read_idx * READ_SIZE)..((read_idx + 1) * READ_SIZE);
                    read_tasks.spawn(async move {
                        let bytes = file_handle_clone.read_bytes_async(range).await.unwrap();
                        assert_eq!(bytes.len(), READ_SIZE);
                    });
                }
                while let Some(res) = read_tasks.join_next().await {
                    res.unwrap();
                    assert!(global_clone.allocated() <= GLOBAL_LIMIT + MAX_PER_SPLIT);
                }
                split_memory.release_unused_memory();
                tokio::task::yield_now().await;
            });
        }
        while let Some(res) = join_set.join_next().await {
            res.unwrap();
        }
        assert_eq!(global.allocated(), 0);
    }
}
