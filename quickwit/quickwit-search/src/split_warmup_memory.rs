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

use quickwit_directories::{MemoryReserver, OutOfMemoryError};

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
    /// Only locked for some arithmetic and one CAS, never across an `.await`.
    state: Mutex<SplitWarmupMemoryState>,
}

struct SplitWarmupMemoryState {
    /// Bytes reserved for data that entered the split's warmup cache.
    in_use: u64,
    /// Memory reserved in the global warmup memory.
    ///
    /// Invariant: `in_use <= reservation.num_bytes()`.
    reservation: WarmupMemoryGuard,
    /// The reservation never grows above this. Never changes.
    ///
    /// The initial reservation is expected to be lower or equal: the permit size is clamped to it.
    max_num_bytes: u64,
    /// Set the first time the reservation fails to grow.
    /// The point is to avoid double counting in LEAF_SEARCH_SPLIT_WARMUP_OUT_OF_MEMORY_TOTAL.
    out_of_memory: bool,
}

impl SplitWarmupMemory {
    /// `max_num_bytes` is the per-split limit: the reservation never grows above it, even if the
    /// global warmup memory has room left.
    pub fn new(reservation: WarmupMemoryGuard, max_num_bytes: u64) -> Self {
        let state = SplitWarmupMemoryState {
            in_use: 0,
            reservation,
            max_num_bytes,
            out_of_memory: false,
        };
        SplitWarmupMemory {
            state: Mutex::new(state),
        }
    }

    /// Bytes currently reserved.
    #[cfg(test)]
    pub fn reserved_num_bytes(&self) -> u64 {
        self.lock_state().reservation.num_bytes()
    }

    /// Whether the reservation failed to grow, because of the warmup memory limit.
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

impl MemoryReserver for SplitWarmupMemory {
    /// Reserves memory for `num_bytes` of warmup data, growing the reservation if needed.
    ///
    /// Fails if the reservation cannot grow without going over the per-split limit or the global
    /// warmup memory limit.
    fn try_reserve_memory(&self, num_bytes: u64) -> Result<(), OutOfMemoryError> {
        let mut state = self.lock_state();
        let needed_num_bytes = state.in_use.saturating_add(num_bytes);
        state.grow_reservation(needed_num_bytes)?;
        state.in_use = needed_num_bytes;
        Ok(())
    }
}

impl SplitWarmupMemoryState {
    /// Grows the reservation so that it holds at least `needed_num_bytes`.
    ///
    /// On failure, the split search is counted as out of memory, only the first time.
    fn grow_reservation(&mut self, needed_num_bytes: u64) -> Result<(), OutOfMemoryError> {
        let allocated_num_bytes = self.reservation.num_bytes();
        if needed_num_bytes <= allocated_num_bytes {
            return Ok(());
        }
        let grow_res = self.try_grow_reservation(allocated_num_bytes, needed_num_bytes);
        if grow_res.is_err() && !self.out_of_memory {
            self.out_of_memory = true;
            LEAF_SEARCH_SPLIT_WARMUP_OUT_OF_MEMORY_TOTAL.inc();
        }
        grow_res
    }

    /// Fails right away if `needed_num_bytes` is above the per-split limit.
    ///
    /// Otherwise, tries to grow by `GROWTH_NUMERATOR / GROWTH_DENOMINATOR` first, within the
    /// per-split limit, and falls back to the exact need if that does not fit in the global warmup
    /// memory. So the exponential growth never makes a split fail.
    fn try_grow_reservation(
        &mut self,
        allocated_num_bytes: u64,
        needed_num_bytes: u64,
    ) -> Result<(), OutOfMemoryError> {
        if needed_num_bytes > self.max_num_bytes {
            return Err(OutOfMemoryError);
        }
        let exponential_num_bytes = allocated_num_bytes
            .saturating_mul(GROWTH_NUMERATOR)
            .div_ceil(GROWTH_DENOMINATOR)
            .min(self.max_num_bytes);
        if exponential_num_bytes > needed_num_bytes
            && self
                .reservation
                .try_grow(exponential_num_bytes - allocated_num_bytes)
                .is_ok()
        {
            return Ok(());
        }
        self.reservation
            .try_grow(needed_num_bytes - allocated_num_bytes)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytesize::ByteSize;

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

    #[test]
    fn test_try_reserve_memory_within_reservation() {
        let global = GlobalWarmupMemory::with_limit_and_free_callback(ByteSize::mb(100), || {});
        let split_memory = split_warmup_memory(&global, 10 * MB);
        split_memory.try_reserve_memory(4 * MB).unwrap();
        split_memory.try_reserve_memory(6 * MB).unwrap();
        assert_eq!(split_memory.reserved_num_bytes(), 10 * MB);
        assert_eq!(global.allocated(), 10 * MB);
    }

    #[test]
    fn test_try_reserve_memory_grows_exponentially() {
        let global = GlobalWarmupMemory::with_limit_and_free_callback(ByteSize::mb(100), || {});
        let split_memory = split_warmup_memory(&global, 10 * MB);
        // 11MB does not fit in 10MB: the reservation grows to 1.7 x 10MB.
        split_memory.try_reserve_memory(11 * MB).unwrap();
        assert_eq!(split_memory.reserved_num_bytes(), 17 * MB);
        // 11MB + 6MB = 17MB still fits.
        split_memory.try_reserve_memory(6 * MB).unwrap();
        assert_eq!(split_memory.reserved_num_bytes(), 17 * MB);
        // 18MB does not fit: the reservation grows to 1.7 x 17MB.
        split_memory.try_reserve_memory(MB).unwrap();
        assert_eq!(split_memory.reserved_num_bytes(), 28_900_000);
        assert_eq!(global.allocated(), 28_900_000);
    }

    #[test]
    fn test_try_reserve_memory_falls_back_to_exact_need() {
        let global = GlobalWarmupMemory::with_limit_and_free_callback(ByteSize::mb(15), || {});
        let split_memory = split_warmup_memory(&global, 10 * MB);
        // 1.7 x 10MB = 17MB would go over the limit, but the 12MB needed fit.
        split_memory.try_reserve_memory(12 * MB).unwrap();
        assert_eq!(split_memory.reserved_num_bytes(), 12 * MB);
    }

    #[test]
    fn test_try_reserve_memory_out_of_memory() {
        let global = GlobalWarmupMemory::with_limit_and_free_callback(ByteSize::mb(100), || {});
        let split_memory = split_warmup_memory(&global, 10 * MB);
        let _other_reservation = global.try_allocate(10 * MB).unwrap();
        // 91MB + 10MB for the other reservation would go over the limit.
        assert_eq!(
            split_memory.try_reserve_memory(91 * MB).err(),
            Some(OutOfMemoryError)
        );
        assert!(split_memory.has_run_out_of_memory());
        // A failed reservation changes neither the reservation nor the memory in use.
        assert_eq!(split_memory.reserved_num_bytes(), 10 * MB);
        split_memory.try_reserve_memory(90 * MB).unwrap();
        assert_eq!(split_memory.reserved_num_bytes(), 90 * MB);
    }

    #[test]
    fn test_try_reserve_memory_over_split_limit_fails() {
        let global = GlobalWarmupMemory::with_limit_and_free_callback(ByteSize::mb(100), || {});
        let reservation = global.try_allocate(10 * MB).unwrap();
        let split_memory = SplitWarmupMemory::new(reservation, 20 * MB);
        // The global warmup memory has room left, but the per-split limit is 20MB.
        assert_eq!(
            split_memory.try_reserve_memory(21 * MB).err(),
            Some(OutOfMemoryError)
        );
        assert!(split_memory.has_run_out_of_memory());
        // Neither the reservation nor the global memory changed.
        assert_eq!(split_memory.reserved_num_bytes(), 10 * MB);
        assert_eq!(global.allocated(), 10 * MB);
    }

    #[test]
    fn test_try_reserve_memory_up_to_split_limit() {
        let global = GlobalWarmupMemory::with_limit_and_free_callback(ByteSize::mb(100), || {});
        let reservation = global.try_allocate(10 * MB).unwrap();
        let split_memory = SplitWarmupMemory::new(reservation, 20 * MB);
        split_memory.try_reserve_memory(20 * MB).unwrap();
        assert_eq!(split_memory.reserved_num_bytes(), 20 * MB);
        assert!(!split_memory.has_run_out_of_memory());
    }

    #[test]
    fn test_try_reserve_memory_exponential_growth_clamped_to_split_limit() {
        let global = GlobalWarmupMemory::with_limit_and_free_callback(ByteSize::mb(100), || {});
        let reservation = global.try_allocate(10 * MB).unwrap();
        let split_memory = SplitWarmupMemory::new(reservation, 15 * MB);
        // 11MB does not fit in 10MB. 1.7 x 10MB = 17MB is clamped to the 15MB per-split limit.
        split_memory.try_reserve_memory(11 * MB).unwrap();
        assert_eq!(split_memory.reserved_num_bytes(), 15 * MB);
        assert_eq!(global.allocated(), 15 * MB);
    }

    #[test]
    fn test_shrink_reservation_never_below_memory_in_use() {
        let global = GlobalWarmupMemory::with_limit_and_free_callback(ByteSize::mb(100), || {});
        let split_memory = split_warmup_memory(&global, 10 * MB);
        split_memory.try_reserve_memory(4 * MB).unwrap();
        split_memory.shrink_reservation(MB);
        assert_eq!(split_memory.reserved_num_bytes(), 4 * MB);
        // Shrinking never grows the reservation.
        split_memory.shrink_reservation(20 * MB);
        assert_eq!(split_memory.reserved_num_bytes(), 4 * MB);
        assert_eq!(global.allocated(), 4 * MB);
    }

    #[test]
    fn test_release_unused_memory() {
        let global = GlobalWarmupMemory::with_limit_and_free_callback(ByteSize::mb(100), || {});
        let split_memory = split_warmup_memory(&global, 10 * MB);
        split_memory.try_reserve_memory(11 * MB).unwrap();
        assert_eq!(split_memory.reserved_num_bytes(), 17 * MB);
        split_memory.release_unused_memory();
        assert_eq!(split_memory.reserved_num_bytes(), 11 * MB);
        assert_eq!(global.allocated(), 11 * MB);
    }

    #[test]
    fn test_drop_frees_reservation() {
        let global = GlobalWarmupMemory::with_limit_and_free_callback(ByteSize::mb(100), || {});
        let split_memory = split_warmup_memory(&global, 10 * MB);
        split_memory.try_reserve_memory(11 * MB).unwrap();
        drop(split_memory);
        assert_eq!(global.allocated(), 0);
    }

    #[test]
    fn test_concurrent_try_reserve_memory() {
        const NUM_THREADS: usize = 8;
        const NUM_RESERVATIONS_PER_THREAD: usize = 10;
        let global = GlobalWarmupMemory::with_limit_and_free_callback(ByteSize::gb(1), || {});
        let split_memory = Arc::new(split_warmup_memory(&global, 10 * MB));
        std::thread::scope(|scope| {
            for _ in 0..NUM_THREADS {
                let split_memory = split_memory.clone();
                scope.spawn(move || {
                    for _ in 0..NUM_RESERVATIONS_PER_THREAD {
                        split_memory.try_reserve_memory(MB).unwrap();
                    }
                });
            }
        });
        // All 80MB are in use, so the reservation cannot go below that.
        split_memory.release_unused_memory();
        assert_eq!(split_memory.reserved_num_bytes(), 80 * MB);
    }
}
