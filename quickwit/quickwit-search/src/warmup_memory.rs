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
//!
//! # Why split searches can wait for memory without deadlocking
//!
//! Each split search holds one [`WarmupMemoryGuard`], and grows it during its warmup. If split
//! searches could simply wait for memory, they could all hold some memory and wait for more,
//! forever. To prevent that, the oldest split search (the one whose guard was allocated first)
//! never has to wait:
//!
//! - a guard never holds more than `split_max_allocation`: `try_grow` enforces it, and callers
//!   clamp the sizes they pass to `try_allocate`;
//! - the oldest split search can allocate up to `limit`;
//! - the other split searches can only allocate up to `limit - split_max_allocation`.
//!
//! So the memory held by the other split searches is at most `limit - split_max_allocation`, and
//! the oldest split search can always grow to `split_max_allocation`. It completes, drops its
//! guard, and the next split search becomes the oldest one.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, MutexGuard};

use bytesize::ByteSize;
use tokio::sync::Notify;

use crate::metrics::{LEAF_SEARCH_WARMUP_BUDGET_NUM_BYTES, LEAF_SEARCH_WARMUP_ONGOING_NUM_BYTES};

/// Called after memory is freed.
type OnMemoryFreed = Box<dyn Fn() + Send + Sync>;

/// Memory reserved by split search warmups on this node.
///
/// It is shared by the search permit actor and by every [`WarmupMemoryGuard`].
///
/// Invariants:
/// - `allocated <= limit`;
/// - each guard holds at most `split_max_allocation`, as long as callers respect the contract of
///   [`GlobalWarmupMemory::try_allocate`];
/// - the guards other than the oldest one hold at most `limit - split_max_allocation` in total.
#[derive(Clone)]
pub(crate) struct GlobalWarmupMemory {
    inner: Arc<GlobalWarmupMemoryInner>,
}

struct GlobalWarmupMemoryInner {
    /// Only locked for some arithmetic, never across an `.await`.
    state: Mutex<GlobalWarmupMemoryState>,
    limit: u64,
    split_max_allocation: u64,
    on_memory_freed: OnMemoryFreed,
    /// Notified each time memory is freed or a guard is dropped, so that waiting split searches
    /// try again.
    memory_freed_notify: Arc<Notify>,
}

struct GlobalWarmupMemoryState {
    allocated: u64,
    /// Ids of the live guards. The smallest one is the oldest split search.
    live_guard_ids: BTreeSet<u64>,
    next_guard_id: u64,
}

impl GlobalWarmupMemory {
    /// Creates a warmup memory that never allocates more than `limit`. A single guard never holds
    /// more than `split_max_allocation`, as long as callers respect the contract of `try_allocate`.
    ///
    /// `on_memory_freed` is called each time memory is freed or a guard is dropped, after the
    /// memory is freed. The search permit actor uses it to try granting pending permits again. It
    /// is called from the thread that frees the memory, possibly while a lock is held, so it must
    /// be quick and must not block.
    ///
    /// # Panics
    ///
    /// Panics if `split_max_allocation` is larger than `limit`.
    pub fn with_limits_and_free_callback(
        limit: ByteSize,
        split_max_allocation: ByteSize,
        on_memory_freed: impl Fn() + Send + Sync + 'static,
    ) -> Self {
        assert!(
            split_max_allocation <= limit,
            "split_max_allocation ({split_max_allocation}) must be lower or equal to the limit \
             ({limit})"
        );
        let state = GlobalWarmupMemoryState {
            allocated: 0,
            live_guard_ids: BTreeSet::new(),
            next_guard_id: 0,
        };
        let inner = GlobalWarmupMemoryInner {
            state: Mutex::new(state),
            limit: limit.as_u64(),
            split_max_allocation: split_max_allocation.as_u64(),
            on_memory_freed: Box::new(on_memory_freed),
            memory_freed_notify: Arc::new(Notify::new()),
        };
        LEAF_SEARCH_WARMUP_BUDGET_NUM_BYTES.set(inner.limit as f64);
        GlobalWarmupMemory {
            inner: Arc::new(inner),
        }
    }

    /// Number of bytes currently allocated.
    #[cfg(test)]
    pub fn allocated(&self) -> u64 {
        self.lock_state().allocated
    }

    /// Allocates `num_bytes` for a new split search. Returns `None` if there is not enough memory
    /// left right now.
    ///
    /// The new guard is the youngest one. If there is no other live guard, it is the oldest one
    /// and can use the whole limit. The memory is freed when the guard is dropped.
    ///
    /// Contract: `num_bytes <= split_max_allocation`. Callers clamp the permit sizes they request
    /// to it. Otherwise, the deadlock freedom still holds, but the split search holds more than
    /// the per-split limit, and fails as soon as it needs more memory.
    pub fn try_allocate(&self, num_bytes: u64) -> Option<WarmupMemoryGuard> {
        let mut state = self.lock_state();
        let limit = if state.live_guard_ids.is_empty() {
            self.inner.limit
        } else {
            self.non_oldest_limit()
        };
        if !state.try_add(num_bytes, limit) {
            return None;
        }
        let guard_id = state.next_guard_id;
        state.next_guard_id += 1;
        state.live_guard_ids.insert(guard_id);
        Some(WarmupMemoryGuard {
            global: self.clone(),
            guard_id,
            num_bytes,
        })
    }

    /// Memory the guards other than the oldest one can allocate in total.
    fn non_oldest_limit(&self) -> u64 {
        self.inner.limit - self.inner.split_max_allocation
    }

    fn lock_state(&self) -> MutexGuard<'_, GlobalWarmupMemoryState> {
        self.inner
            .state
            .lock()
            .expect("global warmup memory lock should not be poisoned")
    }

    /// Wakes up the permit actor and the split searches waiting for memory.
    ///
    /// Must be called after the state lock is released.
    fn notify_memory_freed(&self) {
        self.inner.memory_freed_notify.notify_waiters();
        (self.inner.on_memory_freed)();
    }
}

impl GlobalWarmupMemoryState {
    /// Adds `num_bytes` to the allocated memory. Returns false, and allocates nothing, if the total
    /// would go over `limit`.
    fn try_add(&mut self, num_bytes: u64, limit: u64) -> bool {
        let new_allocated = self.allocated + num_bytes;
        if new_allocated > limit {
            return false;
        }
        self.allocated = new_allocated;
        LEAF_SEARCH_WARMUP_ONGOING_NUM_BYTES.set(new_allocated as f64);
        true
    }

    /// Frees `num_bytes`. Never frees more than allocated: only called by [`WarmupMemoryGuard`],
    /// with bytes the guard holds.
    fn sub(&mut self, num_bytes: u64) {
        self.allocated -= num_bytes;
        LEAF_SEARCH_WARMUP_ONGOING_NUM_BYTES.set(self.allocated as f64);
    }

    fn is_oldest(&self, guard_id: u64) -> bool {
        self.live_guard_ids.first() == Some(&guard_id)
    }
}

/// Memory allocated in a [`GlobalWarmupMemory`] for one split search. Freed on drop.
///
/// Shrinking or dropping a guard calls the `on_memory_freed` callback of the global warmup memory.
pub(crate) struct WarmupMemoryGuard {
    global: GlobalWarmupMemory,
    /// Increases with the allocation order: the smallest live id is the oldest split search.
    guard_id: u64,
    num_bytes: u64,
}

impl WarmupMemoryGuard {
    /// Number of bytes held by this guard.
    pub fn num_bytes(&self) -> u64 {
        self.num_bytes
    }

    /// Maximum number of bytes this guard can hold.
    pub fn max_num_bytes(&self) -> u64 {
        self.global.inner.split_max_allocation
    }

    /// Whether this guard is the oldest live guard.
    ///
    /// Once a guard is the oldest one, it stays the oldest one until it is dropped.
    #[cfg(test)]
    pub(crate) fn is_oldest(&self) -> bool {
        self.global.lock_state().is_oldest(self.guard_id)
    }

    /// Notified each time memory is freed or a guard is dropped. Waiting on it is how a split
    /// search waits for memory.
    pub fn memory_freed_notify(&self) -> Arc<Notify> {
        self.global.inner.memory_freed_notify.clone()
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
        let freed_num_bytes = self.num_bytes - new_num_bytes;
        if freed_num_bytes == 0 {
            return;
        }
        self.global.lock_state().sub(freed_num_bytes);
        self.num_bytes = new_num_bytes;
        self.global.notify_memory_freed();
    }

    /// Adds `additional_num_bytes` to this guard. Returns false if there is not enough memory left
    /// right now. This never happens to the oldest guard.
    ///
    /// On failure, the guard is left unchanged.
    ///
    /// # Panics
    ///
    /// Panics if the guard would hold more than `max_num_bytes`. Callers check the per-split limit
    /// before growing the guard.
    #[must_use]
    pub fn try_grow(&mut self, additional_num_bytes: u64) -> bool {
        let new_num_bytes = self.num_bytes + additional_num_bytes;
        assert!(
            new_num_bytes <= self.max_num_bytes(),
            "warmup memory guard ({new_num_bytes}) over split_max_allocation ({})",
            self.max_num_bytes()
        );
        let mut state = self.global.lock_state();
        let limit = if state.is_oldest(self.guard_id) {
            self.global.inner.limit
        } else {
            self.global.non_oldest_limit()
        };
        if !state.try_add(additional_num_bytes, limit) {
            return false;
        }
        self.num_bytes = new_num_bytes;
        true
    }
}

impl Drop for WarmupMemoryGuard {
    fn drop(&mut self) {
        {
            let mut state = self.global.lock_state();
            state.sub(self.num_bytes);
            state.live_guard_ids.remove(&self.guard_id);
        }
        // Even if the guard held no memory, the oldest guard may have changed, which lets the
        // other guards allocate more.
        self.global.notify_memory_freed();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, OnceLock};

    use super::*;

    /// A warmup memory where a single guard can use the whole limit.
    fn global_warmup_memory(limit: ByteSize) -> GlobalWarmupMemory {
        GlobalWarmupMemory::with_limits_and_free_callback(limit, limit, || {})
    }

    #[test]
    fn test_try_allocate_within_limit() {
        let global =
            GlobalWarmupMemory::with_limits_and_free_callback(ByteSize(100), ByteSize(60), || {});
        // The first guard is the oldest one: it can use the whole limit.
        let mut guard_1 = global.try_allocate(10).unwrap();
        // The other guards share `limit - split_max_allocation`.
        let guard_2 = global.try_allocate(30).unwrap();
        assert!(guard_1.try_grow(50));
        assert_eq!(guard_1.num_bytes(), 60);
        assert_eq!(guard_2.num_bytes(), 30);
        assert_eq!(global.allocated(), 90);
    }

    #[test]
    fn test_try_allocate_over_limit_fails() {
        let global = global_warmup_memory(ByteSize(100));
        let _guard = global.try_allocate(60).unwrap();
        assert!(global.try_allocate(41).is_none());
        // A failed allocation does not change the allocated memory.
        assert_eq!(global.allocated(), 60);
    }

    #[test]
    fn test_only_oldest_guard_can_use_reserve() {
        let global =
            GlobalWarmupMemory::with_limits_and_free_callback(ByteSize(100), ByteSize(40), || {});
        let mut oldest_guard = global.try_allocate(10).unwrap();
        let other_guard = global.try_allocate(40).unwrap();
        assert!(oldest_guard.is_oldest());
        assert!(!other_guard.is_oldest());
        // The non-oldest guards are limited to 100 - 40 = 60 bytes, including the memory of the
        // oldest guard.
        assert!(global.try_allocate(11).is_none());
        // The oldest guard can always grow to the split max allocation.
        assert!(oldest_guard.try_grow(30));
        assert_eq!(global.allocated(), 80);
        // Dropping the oldest guard promotes the next one.
        drop(oldest_guard);
        assert!(other_guard.is_oldest());
        assert_eq!(global.allocated(), 40);
    }

    #[test]
    fn test_non_oldest_guard_cannot_grow_into_reserve() {
        let global =
            GlobalWarmupMemory::with_limits_and_free_callback(ByteSize(100), ByteSize(40), || {});
        let oldest_guard = global.try_allocate(30).unwrap();
        let mut other_guard = global.try_allocate(20).unwrap();
        // 30 + 20 + 11 > 100 - 40.
        assert!(!other_guard.try_grow(11));
        assert_eq!(other_guard.num_bytes(), 20);
        assert!(other_guard.try_grow(10));
        // Once the oldest guard is dropped, the other guard can use the whole limit.
        drop(oldest_guard);
        assert!(other_guard.try_grow(10));
        assert_eq!(global.allocated(), 40);
    }

    #[test]
    fn test_guard_drop_frees_memory() {
        let global = global_warmup_memory(ByteSize(100));
        let guard = global.try_allocate(100).unwrap();
        assert!(global.try_allocate(1).is_none());
        drop(guard);
        assert_eq!(global.allocated(), 0);
        let _guard = global.try_allocate(100).unwrap();
    }

    #[test]
    fn test_guard_shrink_to() {
        let global = global_warmup_memory(ByteSize(100));
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
        let global = global_warmup_memory(ByteSize(100));
        let mut guard = global.try_allocate(10).unwrap();
        guard.shrink_to(11);
    }

    #[test]
    fn test_guard_try_grow() {
        let global =
            GlobalWarmupMemory::with_limits_and_free_callback(ByteSize(100), ByteSize(50), || {});
        let _oldest_guard = global.try_allocate(10).unwrap();
        let mut guard = global.try_allocate(20).unwrap();
        assert!(guard.try_grow(20));
        assert_eq!(guard.num_bytes(), 40);
        assert_eq!(global.allocated(), 50);
        // 10 + 40 + 1 > 100 - 50. A failed grow leaves the guard and the global memory unchanged.
        assert!(!guard.try_grow(1));
        assert_eq!(guard.num_bytes(), 40);
        assert_eq!(global.allocated(), 50);
        drop(guard);
        assert_eq!(global.allocated(), 10);
    }

    #[test]
    #[should_panic(expected = "over split_max_allocation")]
    fn test_guard_try_grow_over_split_max_allocation_panics() {
        let global =
            GlobalWarmupMemory::with_limits_and_free_callback(ByteSize(100), ByteSize(40), || {});
        let mut guard = global.try_allocate(40).unwrap();
        let _ = guard.try_grow(1);
    }

    /// Returns a warmup memory whose callback records the allocated memory each time it is called.
    fn global_warmup_memory_recording_callbacks(
        limit: ByteSize,
    ) -> (GlobalWarmupMemory, Arc<Mutex<Vec<u64>>>) {
        let allocated_on_callbacks: Arc<Mutex<Vec<u64>>> = Arc::default();
        let global_cell: Arc<OnceLock<GlobalWarmupMemory>> = Arc::default();
        let global = GlobalWarmupMemory::with_limits_and_free_callback(limit, limit, {
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
        assert!(global.try_allocate(1).is_none());
        assert!(guard.try_grow(0));
        assert!(allocated_on_callbacks.lock().unwrap().is_empty());

        // The callback is called after the memory is freed.
        guard.shrink_to(30);
        assert_eq!(*allocated_on_callbacks.lock().unwrap(), vec![30]);
        drop(guard);
        assert_eq!(*allocated_on_callbacks.lock().unwrap(), vec![30, 0]);
    }

    #[test]
    fn test_callback_not_called_when_shrink_frees_nothing() {
        let (global, allocated_on_callbacks) =
            global_warmup_memory_recording_callbacks(ByteSize(100));
        let mut guard = global.try_allocate(30).unwrap();
        guard.shrink_to(30);
        assert!(allocated_on_callbacks.lock().unwrap().is_empty());
        guard.shrink_to(0);
        assert_eq!(*allocated_on_callbacks.lock().unwrap(), vec![0]);
    }

    #[test]
    fn test_callback_called_when_empty_guard_dropped() {
        let (global, allocated_on_callbacks) =
            global_warmup_memory_recording_callbacks(ByteSize(100));
        let empty_guard = global.try_allocate(0).unwrap();
        // No memory is freed, but the oldest guard changes.
        drop(empty_guard);
        assert_eq!(*allocated_on_callbacks.lock().unwrap(), vec![0]);
    }

    #[tokio::test]
    async fn test_memory_freed_notify() {
        let global = global_warmup_memory(ByteSize(100));
        let mut guard = global.try_allocate(100).unwrap();
        let memory_freed_notify = guard.memory_freed_notify();
        let memory_freed = memory_freed_notify.notified();
        tokio::pin!(memory_freed);
        memory_freed.as_mut().enable();
        guard.shrink_to(50);
        tokio::time::timeout(std::time::Duration::from_secs(1), memory_freed)
            .await
            .expect("shrinking a guard should notify the waiters");
    }

    #[test]
    fn test_concurrent_try_allocate_never_exceeds_limit() {
        const NUM_THREADS: usize = 8;
        const NUM_ITERATIONS: usize = 10_000;
        const LIMIT: u64 = 100;
        const SPLIT_MAX_ALLOCATION: u64 = 30;
        let global = GlobalWarmupMemory::with_limits_and_free_callback(
            ByteSize(LIMIT),
            ByteSize(SPLIT_MAX_ALLOCATION),
            || {},
        );
        std::thread::scope(|scope| {
            for _ in 0..NUM_THREADS {
                let global = global.clone();
                scope.spawn(move || {
                    for i in 0..NUM_ITERATIONS {
                        let num_bytes = (i as u64 % SPLIT_MAX_ALLOCATION) + 1;
                        let Some(mut guard) = global.try_allocate(num_bytes) else {
                            continue;
                        };
                        let _ = guard.try_grow(SPLIT_MAX_ALLOCATION - num_bytes);
                        assert!(global.allocated() <= LIMIT);
                        drop(guard);
                    }
                });
            }
        });
        assert_eq!(global.allocated(), 0);
    }
}
