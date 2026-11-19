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

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use bytesize::ByteSize;
use tokio::sync::Notify;

use crate::metrics::LEAF_SEARCH_WARMUP_ONGOING_NUM_BYTES;

/// Called after memory is freed.
type OnMemoryFreed = Box<dyn Fn() + Send + Sync>;

/// Memory reserved by split search warmups on this node.
///
/// It is shared by the search permit actor and by every [`WarmupMemoryGuard`].
///
/// Each [`WarmupMemoryGuard`] receives a monotonically increasing `task_id` on creation.
/// Non-oldest guards wait when growing would exceed `limit`, while the oldest live guard
/// (`active_tasks.first()`) always allocates immediately without waiting so that at least one
/// split search task is guaranteed to progress and eventually release its memory.
///
/// Hidden contract / invariant:
/// Non-oldest guards and [`GlobalWarmupMemory::try_allocate`] only allocate when
/// `allocated + additional <= limit`. Therefore, at every instant, the sum of bytes held by all
/// guards other than the oldest live guard is `<= limit`. Total `allocated` can temporarily
/// exceed `limit` by at most the oldest guard's growth, and returns to `<= limit` as soon as the
/// oldest guard is dropped.
#[derive(Clone)]
pub(crate) struct GlobalWarmupMemory {
    inner: Arc<GlobalWarmupMemoryInner>,
}

struct GlobalWarmupMemoryInner {
    limit: u64,
    state: Mutex<GlobalWarmupMemoryState>,
    notify: Notify,
    on_memory_freed: OnMemoryFreed,
}

struct GlobalWarmupMemoryState {
    allocated: u64,
    next_task_id: u64,
    /// Monotonic `task_id`s of all live [`WarmupMemoryGuard`]s.
    /// `active_tasks.first()` is the oldest live split search task.
    active_tasks: BTreeSet<u64>,
}

fn fits_in_limit(allocated: u64, additional_num_bytes: u64, limit: u64) -> bool {
    let Some(new_allocated) = allocated.checked_add(additional_num_bytes) else {
        return false;
    };
    new_allocated <= limit
}

impl GlobalWarmupMemory {
    /// Creates a warmup memory with budget `limit`.
    ///
    /// `on_memory_freed` is called each time memory is freed, after the memory is freed. The
    /// search permit actor uses it to try granting pending permits again. It is called from the
    /// thread that frees the memory, so it must be quick and must not block.
    pub fn with_limit_and_free_callback(
        limit: ByteSize,
        on_memory_freed: impl Fn() + Send + Sync + 'static,
    ) -> Self {
        let state = GlobalWarmupMemoryState {
            allocated: 0,
            next_task_id: 0,
            active_tasks: BTreeSet::new(),
        };
        let inner = GlobalWarmupMemoryInner {
            limit: limit.as_u64(),
            state: Mutex::new(state),
            notify: Notify::new(),
            on_memory_freed: Box::new(on_memory_freed),
        };
        GlobalWarmupMemory {
            inner: Arc::new(inner),
        }
    }

    /// Maximum shared memory limit, excluding the headroom reserved for the oldest split search.
    pub fn limit(&self) -> u64 {
        self.inner.limit
    }

    /// Number of bytes currently allocated.
    #[cfg(test)]
    pub fn allocated(&self) -> u64 {
        self.lock_state().allocated
    }

    /// Allocates `num_bytes` for a new split search task, or returns `None` if the total would go
    /// over the limit.
    ///
    /// Assigns a monotonically increasing task ID to the returned guard so that the oldest live
    /// split search task can always be identified.
    ///
    /// The memory is freed when the returned guard is dropped.
    pub fn try_allocate(&self, num_bytes: u64) -> Option<WarmupMemoryGuard> {
        let mut state = self.lock_state();
        if !fits_in_limit(state.allocated, num_bytes, self.inner.limit) {
            return None;
        }
        let task_id = state.next_task_id;
        state.next_task_id += 1;
        state.allocated += num_bytes;
        state.active_tasks.insert(task_id);
        LEAF_SEARCH_WARMUP_ONGOING_NUM_BYTES.set(state.allocated as f64);
        Some(WarmupMemoryGuard {
            global: self.clone(),
            task_id,
            num_bytes: AtomicU64::new(num_bytes),
        })
    }

    fn lock_state(&self) -> MutexGuard<'_, GlobalWarmupMemoryState> {
        self.inner
            .state
            .lock()
            .expect("global warmup memory lock should not be poisoned")
    }
}

/// Memory allocated in a [`GlobalWarmupMemory`] for a single split search task. Freed on drop.
///
/// Shrinking or dropping a guard wakes tasks waiting for memory and calls the `on_memory_freed`
/// callback of the global warmup memory. Dropping the oldest guard also wakes waiting tasks even
/// when the guard held 0 bytes, because a new guard has become the oldest.
pub(crate) struct WarmupMemoryGuard {
    global: GlobalWarmupMemory,
    task_id: u64,
    /// Mutated only while holding `global.inner.state`.
    num_bytes: AtomicU64,
}

impl WarmupMemoryGuard {
    /// Number of bytes held by this guard.
    pub fn num_bytes(&self) -> u64 {
        self.num_bytes.load(Ordering::Acquire)
    }

    /// Frees memory so that this guard only holds `new_num_bytes`.
    ///
    /// # Panics
    ///
    /// Panics if `new_num_bytes` is larger than the current size.
    pub fn shrink_to(&self, new_num_bytes: u64) {
        {
            let mut state = self.global.lock_state();
            let current_num_bytes = self.num_bytes.load(Ordering::Relaxed);
            assert!(
                new_num_bytes <= current_num_bytes,
                "shrink_to cannot grow a warmup memory guard"
            );
            let freed_num_bytes = current_num_bytes - new_num_bytes;
            if freed_num_bytes == 0 {
                return;
            }
            self.num_bytes.store(new_num_bytes, Ordering::Release);
            state.allocated -= freed_num_bytes;
            LEAF_SEARCH_WARMUP_ONGOING_NUM_BYTES.set(state.allocated as f64);
        }
        self.global.inner.notify.notify_waiters();
        (self.global.inner.on_memory_freed)();
    }

    /// Tries to grow this guard to `desired_num_bytes` (or at least `min_num_bytes`).
    ///
    /// - If `desired_num_bytes` fits in the global limit, grows to `desired_num_bytes`.
    /// - Else if `min_num_bytes` fits in the global limit, grows to `min_num_bytes`.
    /// - Else if this guard belongs to the oldest live split search task, grows to `min_num_bytes`
    ///   immediately without waiting (even if `allocated` exceeds `limit`).
    /// - Otherwise, waits for one notification that memory was freed or that the oldest guard
    ///   changed, then returns so the caller can re-check its current need.
    pub async fn grow_or_wait(&self, min_num_bytes: u64, desired_num_bytes: u64) {
        debug_assert!(min_num_bytes <= desired_num_bytes);
        let limit = self.global.inner.limit;
        let notified = self.global.inner.notify.notified();
        tokio::pin!(notified);
        {
            let mut state = self.global.lock_state();
            let current_num_bytes = self.num_bytes.load(Ordering::Relaxed);
            if current_num_bytes >= min_num_bytes {
                return;
            }
            let min_additional = min_num_bytes - current_num_bytes;
            let desired_additional = desired_num_bytes - current_num_bytes;
            let is_oldest = state.active_tasks.first().copied() == Some(self.task_id);

            let granted_additional = if fits_in_limit(state.allocated, desired_additional, limit) {
                Some(desired_additional)
            } else if is_oldest || fits_in_limit(state.allocated, min_additional, limit) {
                Some(min_additional)
            } else {
                None
            };

            if let Some(additional_num_bytes) = granted_additional {
                state.allocated = state
                    .allocated
                    .checked_add(additional_num_bytes)
                    .expect("warmup memory allocation overflow");
                self.num_bytes
                    .store(current_num_bytes + additional_num_bytes, Ordering::Release);
                LEAF_SEARCH_WARMUP_ONGOING_NUM_BYTES.set(state.allocated as f64);
                return;
            }

            // Hidden contract: `enable()` must run while `state` is still locked so a concurrent
            // `notify_waiters()` after unlocking `state` cannot be missed.
            notified.as_mut().enable();
        }
        notified.await;
    }

    /// Wakes tasks waiting in [`WarmupMemoryGuard::grow_or_wait`].
    pub fn notify_waiters(&self) {
        self.global.inner.notify.notify_waiters();
    }
}

impl Drop for WarmupMemoryGuard {
    fn drop(&mut self) {
        let (freed_num_bytes, was_oldest) = {
            let mut state = self
                .global
                .inner
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let was_oldest = state.active_tasks.first().copied() == Some(self.task_id);
            let removed = state.active_tasks.remove(&self.task_id);
            debug_assert!(
                removed,
                "dropped warmup memory guard must be in active_tasks"
            );
            let freed_num_bytes = self.num_bytes.load(Ordering::Relaxed);
            if freed_num_bytes > 0 {
                state.allocated -= freed_num_bytes;
                LEAF_SEARCH_WARMUP_ONGOING_NUM_BYTES.set(state.allocated as f64);
            }
            (freed_num_bytes, was_oldest)
        };
        if freed_num_bytes > 0 || was_oldest {
            self.global.inner.notify.notify_waiters();
        }
        if freed_num_bytes > 0 {
            (self.global.inner.on_memory_freed)();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, OnceLock};

    use super::*;

    async fn grow_to(guard: &WarmupMemoryGuard, min_num_bytes: u64, desired_num_bytes: u64) {
        while guard.num_bytes() < min_num_bytes {
            guard.grow_or_wait(min_num_bytes, desired_num_bytes).await;
        }
    }

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
        assert!(global.try_allocate(41).is_none());
        // A failed allocation does not change the allocated memory.
        assert_eq!(global.allocated(), 60);
        assert!(global.try_allocate(u64::MAX).is_none());
        assert_eq!(global.allocated(), 60);
    }

    #[test]
    fn test_guard_drop_frees_memory() {
        let global = GlobalWarmupMemory::with_limit_and_free_callback(ByteSize(100), || {});
        let guard = global.try_allocate(100).unwrap();
        assert!(global.try_allocate(1).is_none());
        drop(guard);
        assert_eq!(global.allocated(), 0);
        let _guard = global.try_allocate(100).unwrap();
    }

    #[test]
    fn test_guard_shrink_to() {
        let global = GlobalWarmupMemory::with_limit_and_free_callback(ByteSize(100), || {});
        let guard = global.try_allocate(100).unwrap();
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
        let guard = global.try_allocate(10).unwrap();
        guard.shrink_to(11);
    }

    #[tokio::test]
    async fn test_guard_grow_within_limit_and_oldest_above_limit() {
        let global = GlobalWarmupMemory::with_limit_and_free_callback(ByteSize(100), || {});
        let oldest_guard = global.try_allocate(60).unwrap();
        grow_to(&oldest_guard, 80, 90).await;
        assert_eq!(oldest_guard.num_bytes(), 90);
        assert_eq!(global.allocated(), 90);

        // Only 10 bytes left under the limit: desired (120) does not fit, min (95) fits.
        grow_to(&oldest_guard, 95, 120).await;
        assert_eq!(oldest_guard.num_bytes(), 95);
        assert_eq!(global.allocated(), 95);

        // Oldest guard can grow above the limit without waiting, by exact min need.
        grow_to(&oldest_guard, 120, 150).await;
        assert_eq!(oldest_guard.num_bytes(), 120);
        assert_eq!(global.allocated(), 120);
        // New permits cannot be allocated while above the limit.
        assert!(global.try_allocate(1).is_none());

        drop(oldest_guard);
        assert_eq!(global.allocated(), 0);
    }

    #[tokio::test]
    async fn test_non_oldest_guard_waits_until_memory_freed() {
        let global = GlobalWarmupMemory::with_limit_and_free_callback(ByteSize(100), || {});
        let oldest_guard = global.try_allocate(60).unwrap();
        let younger_guard = Arc::new(global.try_allocate(30).unwrap());

        let younger_guard_clone = younger_guard.clone();
        let grow_handle = tokio::spawn(async move {
            grow_to(&younger_guard_clone, 50, 50).await;
        });
        tokio::task::yield_now().await;
        assert!(!grow_handle.is_finished());
        assert_eq!(younger_guard.num_bytes(), 30);

        // Shrinking the oldest guard frees enough room (40 + 50 <= 100).
        oldest_guard.shrink_to(40);
        grow_handle.await.unwrap();
        assert_eq!(younger_guard.num_bytes(), 50);
        assert_eq!(global.allocated(), 90);
    }

    #[tokio::test]
    async fn test_non_oldest_guard_wakes_when_zero_byte_oldest_guard_drops() {
        let global = GlobalWarmupMemory::with_limit_and_free_callback(ByteSize(100), || {});
        let oldest_guard = global.try_allocate(10).unwrap();
        let middle_guard = Arc::new(global.try_allocate(50).unwrap());
        let youngest_guard = global.try_allocate(40).unwrap();

        // Oldest guard shrinks to 0 after warmup, then youngest guard takes the freed 10 bytes.
        oldest_guard.shrink_to(0);
        grow_to(&youngest_guard, 50, 50).await;
        assert_eq!(global.allocated(), 100);

        // Middle guard wants +20 bytes (total 120 > 100) while `oldest_guard` (holding 0 bytes)
        // is still alive.
        let middle_guard_clone = middle_guard.clone();
        let grow_handle = tokio::spawn(async move {
            grow_to(&middle_guard_clone, 70, 70).await;
        });
        tokio::task::yield_now().await;
        assert!(!grow_handle.is_finished());

        // Dropping the 0-byte oldest guard frees 0 bytes, but promotes `middle_guard` to oldest.
        drop(oldest_guard);
        grow_handle.await.unwrap();
        assert_eq!(middle_guard.num_bytes(), 70);
        assert_eq!(global.allocated(), 120);
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

    #[tokio::test]
    async fn test_callback_on_memory_freed() {
        let (global, allocated_on_callbacks) =
            global_warmup_memory_recording_callbacks(ByteSize(100));
        let guard = global.try_allocate(100).unwrap();
        assert!(global.try_allocate(1).is_none());
        grow_to(&guard, 100, 100).await;
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
        let guard = global.try_allocate(30).unwrap();
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
                        let Some(guard) = global.try_allocate(num_bytes) else {
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
