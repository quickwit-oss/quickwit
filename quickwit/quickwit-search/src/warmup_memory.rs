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

//! Node-wide accounting of the memory reserved by split search warmups.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytesize::ByteSize;
use quickwit_directories::OutOfMemoryError;

use crate::metrics::LEAF_SEARCH_WARMUP_ONGOING_NUM_BYTES;

/// Called after memory is freed.
type OnMemoryFreed = Box<dyn Fn() + Send + Sync>;

/// Memory reserved by split search warmups on this node.
///
/// It is shared by the search permit actor and by every [`WarmupMemoryGuard`].
///
/// Invariant: `allocated <= limit`.
#[derive(Clone)]
pub(crate) struct GlobalWarmupMemory {
    inner: Arc<GlobalWarmupMemoryInner>,
}

struct GlobalWarmupMemoryInner {
    allocated: AtomicU64,
    limit: u64,
    on_memory_freed: OnMemoryFreed,
}

impl GlobalWarmupMemory {
    /// Creates a warmup memory that never allocates more than `limit`.
    ///
    /// `on_memory_freed` is called each time memory is freed, after the memory is freed. The
    /// search permit actor uses it to try granting pending permits again. It is called from the
    /// thread that frees the memory, possibly while a lock is held, so it must be quick and must
    /// not block.
    pub fn with_limit_and_free_callback(
        limit: ByteSize,
        on_memory_freed: impl Fn() + Send + Sync + 'static,
    ) -> Self {
        let inner = GlobalWarmupMemoryInner {
            allocated: AtomicU64::new(0),
            limit: limit.as_u64(),
            on_memory_freed: Box::new(on_memory_freed),
        };
        GlobalWarmupMemory {
            inner: Arc::new(inner),
        }
    }

    /// Number of bytes currently allocated.
    #[cfg(test)]
    pub fn allocated(&self) -> u64 {
        self.inner.allocated.load(Ordering::Relaxed)
    }

    /// Allocates `num_bytes`, or fails if the total would go over the limit.
    ///
    /// The memory is freed when the returned guard is dropped.
    pub fn try_allocate(&self, num_bytes: u64) -> Result<WarmupMemoryGuard, OutOfMemoryError> {
        self.try_add(num_bytes)?;
        Ok(WarmupMemoryGuard {
            global: self.clone(),
            num_bytes,
        })
    }

    /// Adds `num_bytes` to the allocated memory, or fails if the total would go over the limit.
    fn try_add(&self, num_bytes: u64) -> Result<(), OutOfMemoryError> {
        let limit = self.inner.limit;
        // The check and the addition must happen atomically, hence the CAS loop.
        let previous_allocated = self
            .inner
            .allocated
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |allocated| {
                let new_allocated = allocated.checked_add(num_bytes)?;
                if new_allocated > limit {
                    return None;
                }
                Some(new_allocated)
            })
            .map_err(|_| OutOfMemoryError)?;
        LEAF_SEARCH_WARMUP_ONGOING_NUM_BYTES.set((previous_allocated + num_bytes) as f64);
        Ok(())
    }

    /// Frees `num_bytes`, then calls the `on_memory_freed` callback.
    ///
    /// No CAS loop is needed: freeing memory can never break the limit invariant.
    ///
    /// Only called by [`WarmupMemoryGuard`], with bytes the guard holds, so it never frees more
    /// than allocated.
    fn deallocate(&self, num_bytes: u64) {
        if num_bytes == 0 {
            return;
        }
        let previous_allocated = self.inner.allocated.fetch_sub(num_bytes, Ordering::Relaxed);
        LEAF_SEARCH_WARMUP_ONGOING_NUM_BYTES.set((previous_allocated - num_bytes) as f64);
        // Called after the memory is freed, so that the callback sees it as available.
        (self.inner.on_memory_freed)();
    }
}

/// Memory allocated in a [`GlobalWarmupMemory`]. Freed on drop.
///
/// Shrinking or dropping a guard calls the `on_memory_freed` callback of the global warmup memory.
pub(crate) struct WarmupMemoryGuard {
    global: GlobalWarmupMemory,
    num_bytes: u64,
}

impl WarmupMemoryGuard {
    /// Number of bytes held by this guard.
    pub fn num_bytes(&self) -> u64 {
        self.num_bytes
    }

    /// Frees memory so that this guard only holds `new_num_bytes`.
    ///
    /// # Panics
    ///
    /// Panics if `new_num_bytes` is larger than the current size.
    pub fn shrink_to(&mut self, new_num_bytes: u64) {
        assert!(
            new_num_bytes <= self.num_bytes,
            "shrink_to cannot grow a warmup memory guard"
        );
        self.global.deallocate(self.num_bytes - new_num_bytes);
        self.num_bytes = new_num_bytes;
    }

    /// Adds `additional_num_bytes` to this guard, or fails if the total would go over the limit.
    ///
    /// On failure, the guard is left unchanged.
    pub fn try_grow(&mut self, additional_num_bytes: u64) -> Result<(), OutOfMemoryError> {
        self.global.try_add(additional_num_bytes)?;
        self.num_bytes += additional_num_bytes;
        Ok(())
    }
}

impl Drop for WarmupMemoryGuard {
    fn drop(&mut self) {
        self.global.deallocate(self.num_bytes);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, OnceLock};

    use super::*;

    #[test]
    fn test_try_allocate_within_limit() {
        let global = GlobalWarmupMemory::with_limit_and_free_callback(ByteSize(100), || {});
        let guard_1 = global.try_allocate(60).unwrap();
        let guard_2 = global.try_allocate(40).unwrap();
        assert_eq!(guard_1.num_bytes(), 60);
        assert_eq!(guard_2.num_bytes(), 40);
        assert_eq!(global.allocated(), 100);
    }

    #[test]
    fn test_try_allocate_over_limit_fails() {
        let global = GlobalWarmupMemory::with_limit_and_free_callback(ByteSize(100), || {});
        let _guard = global.try_allocate(60).unwrap();
        assert_eq!(global.try_allocate(41).err(), Some(OutOfMemoryError));
        // A failed allocation does not change the allocated memory.
        assert_eq!(global.allocated(), 60);
        assert_eq!(global.try_allocate(u64::MAX).err(), Some(OutOfMemoryError));
        assert_eq!(global.allocated(), 60);
    }

    #[test]
    fn test_guard_drop_frees_memory() {
        let global = GlobalWarmupMemory::with_limit_and_free_callback(ByteSize(100), || {});
        let guard = global.try_allocate(100).unwrap();
        assert!(global.try_allocate(1).is_err());
        drop(guard);
        assert_eq!(global.allocated(), 0);
        let _guard = global.try_allocate(100).unwrap();
    }

    #[test]
    fn test_guard_shrink_to() {
        let global = GlobalWarmupMemory::with_limit_and_free_callback(ByteSize(100), || {});
        let mut guard = global.try_allocate(100).unwrap();
        guard.shrink_to(30);
        assert_eq!(guard.num_bytes(), 30);
        assert_eq!(global.allocated(), 30);
        guard.shrink_to(0);
        assert_eq!(global.allocated(), 0);
        drop(guard);
        assert_eq!(global.allocated(), 0);
    }

    #[test]
    #[should_panic(expected = "shrink_to cannot grow")]
    fn test_guard_shrink_to_larger_size_panics() {
        let global = GlobalWarmupMemory::with_limit_and_free_callback(ByteSize(100), || {});
        let mut guard = global.try_allocate(10).unwrap();
        guard.shrink_to(11);
    }

    #[test]
    fn test_guard_try_grow() {
        let global = GlobalWarmupMemory::with_limit_and_free_callback(ByteSize(100), || {});
        let mut guard = global.try_allocate(60).unwrap();
        guard.try_grow(30).unwrap();
        assert_eq!(guard.num_bytes(), 90);
        assert_eq!(global.allocated(), 90);
        // A failed grow leaves the guard and the global memory unchanged.
        assert_eq!(guard.try_grow(11).err(), Some(OutOfMemoryError));
        assert_eq!(guard.num_bytes(), 90);
        assert_eq!(global.allocated(), 90);
        drop(guard);
        assert_eq!(global.allocated(), 0);
    }

    /// Returns a warmup memory whose callback records the allocated memory each time it is called.
    fn global_warmup_memory_recording_callbacks(
        limit: ByteSize,
    ) -> (GlobalWarmupMemory, Arc<Mutex<Vec<u64>>>) {
        let allocated_on_callbacks: Arc<Mutex<Vec<u64>>> = Arc::default();
        let global_cell: Arc<OnceLock<GlobalWarmupMemory>> = Arc::default();
        let global = GlobalWarmupMemory::with_limit_and_free_callback(limit, {
            let allocated_on_callbacks = allocated_on_callbacks.clone();
            let global_cell = global_cell.clone();
            move || {
                let allocated = global_cell.get().unwrap().allocated();
                allocated_on_callbacks.lock().unwrap().push(allocated);
            }
        });
        // This creates a reference cycle, which is fine in a test.
        global_cell.set(global.clone()).ok().unwrap();
        (global, allocated_on_callbacks)
    }

    #[test]
    fn test_callback_on_memory_freed() {
        let (global, allocated_on_callbacks) =
            global_warmup_memory_recording_callbacks(ByteSize(100));
        let mut guard = global.try_allocate(100).unwrap();
        let failed_guard_res = global.try_allocate(1);
        assert!(failed_guard_res.is_err());
        guard.try_grow(0).unwrap();
        assert!(allocated_on_callbacks.lock().unwrap().is_empty());

        // The callback is called after the memory is freed.
        guard.shrink_to(30);
        assert_eq!(*allocated_on_callbacks.lock().unwrap(), vec![30]);
        drop(guard);
        assert_eq!(*allocated_on_callbacks.lock().unwrap(), vec![30, 0]);
    }

    #[test]
    fn test_callback_not_called_when_no_memory_is_freed() {
        let (global, allocated_on_callbacks) =
            global_warmup_memory_recording_callbacks(ByteSize(100));
        let mut guard = global.try_allocate(30).unwrap();
        guard.shrink_to(30);
        guard.shrink_to(0);
        drop(guard);
        let empty_guard = global.try_allocate(0).unwrap();
        drop(empty_guard);
        // Only `shrink_to(0)` freed memory.
        assert_eq!(*allocated_on_callbacks.lock().unwrap(), vec![0]);
    }

    #[test]
    fn test_concurrent_try_allocate_never_exceeds_limit() {
        const NUM_THREADS: usize = 8;
        const NUM_ITERATIONS: usize = 10_000;
        const LIMIT: u64 = 100;
        let global = GlobalWarmupMemory::with_limit_and_free_callback(ByteSize(LIMIT), || {});
        std::thread::scope(|scope| {
            for _ in 0..NUM_THREADS {
                let global = global.clone();
                scope.spawn(move || {
                    for i in 0..NUM_ITERATIONS {
                        let num_bytes = (i as u64 % 30) + 1;
                        let Ok(guard) = global.try_allocate(num_bytes) else {
                            continue;
                        };
                        assert!(global.allocated() <= LIMIT);
                        drop(guard);
                    }
                });
            }
        });
        assert_eq!(global.allocated(), 0);
    }
}
