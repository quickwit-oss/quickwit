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
//! Each split search owns one [`WarmupMemoryGuard`]. Guards get increasing ids, in the order in
//! which split searches are granted a permit. All the memory requests of split searches, including
//! their initial reservation, go through a single queue, served by guard id, then in arrival
//! order. So older split searches are served first, and a request never passes an older one.
//!
//! # Why split searches can wait for memory without deadlocking
//!
//! If split searches could simply wait for memory, they could all hold some memory and wait for
//! more, forever. To prevent that, the oldest split search (the live guard with the smallest id)
//! never has to wait:
//!
//! - a guard never holds more than `split_max_allocation`;
//! - the oldest guard can grow up to `limit`;
//! - the other guards can only grow up to `limit - split_max_allocation`.
//!
//! So the other guards hold at most `limit - split_max_allocation`, and the requests of the oldest
//! guard always fit. Its requests are at the front of the queue, since it has the smallest id. It
//! completes, its guard is dropped, and the next guard becomes the oldest one.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bytesize::ByteSize;
use quickwit_directories::OutOfMemoryError;
use tokio::sync::oneshot;
use tracing::error;

use crate::metrics::LEAF_SEARCH_WARMUP_ONGOING_NUM_BYTES;

/// Growth factor of a reservation, as `GROWTH_NUMERATOR / GROWTH_DENOMINATOR`.
///
/// Growing exponentially reduces the number of requests that have to wait, at the cost of
/// reserving up to 70% more memory than needed during the warmup.
const GROWTH_NUMERATOR: u64 = 17;
const GROWTH_DENOMINATOR: u64 = 10;

/// Identifies the split search that owns a [`WarmupMemoryGuard`].
///
/// Increases with the creation order of the guards, which is the order in which permits are
/// granted: the smallest live id is the oldest split search.
type SplitSearchId = u64;

/// Memory reserved by split search warmups on this node.
///
/// Shared by the search permit actor, which creates the guards, and by every
/// [`WarmupMemoryGuard`].
#[derive(Clone)]
pub(crate) struct GlobalWarmupMemory {
    /// Only locked for some bookkeeping, never across an `.await`.
    state: Arc<Mutex<GlobalWarmupMemoryState>>,
}

/// Invariants:
/// - `allocated` is the sum of `held` over all guards, and `allocated <= limit`;
/// - for each guard, `in_use <= held <= split_max_allocation`;
/// - the guards other than the oldest one hold at most `limit - split_max_allocation` in total.
struct GlobalWarmupMemoryState {
    limit: u64,
    split_max_allocation: u64,
    allocated: u64,
    /// Live guards. The first key is the oldest guard.
    guards: BTreeMap<SplitSearchId, GuardAccount>,
    /// Requests waiting for memory, served by guard id, then in arrival order.
    ///
    /// May contain cancelled requests: they are removed when `serve_waiters` meets them.
    waiters: BTreeMap<(SplitSearchId, u64), Waiter>,
    next_guard_id: SplitSearchId,
    next_waiter_seq: u64,
}

#[derive(Default)]
struct GuardAccount {
    /// Memory reserved by the split search.
    held: u64,
    /// Bytes of the data that entered the split's warmup cache.
    in_use: u64,
    /// Sum of the `in_use_delta` of this guard's waiters, including cancelled ones that are not
    /// removed yet.
    ///
    /// Counted in the per-split limit check, so that `in_use` can never go over the limit once
    /// they are served. Counting cancelled waiters only makes the check stricter.
    waiting: u64,
}

#[derive(Clone, Copy)]
struct MemoryRequest {
    /// Added to `in_use` once served. 0 for the initial reservation.
    in_use_delta: u64,
    /// `held` is grown to at least this once served. 0 for reads.
    min_held: u64,
}

impl MemoryRequest {
    /// `held` must reach this to serve the request.
    fn needed_held(&self, in_use: u64) -> u64 {
        self.min_held.max(in_use + self.in_use_delta)
    }
}

struct Waiter {
    request: MemoryRequest,
    served_tx: oneshot::Sender<()>,
}

impl GlobalWarmupMemory {
    /// Creates a warmup memory that never allocates more than `limit`, and never more than
    /// `split_max_allocation` for a single guard.
    ///
    /// # Panics
    ///
    /// Panics if `split_max_allocation` is larger than `limit`.
    pub fn new(limit: ByteSize, split_max_allocation: ByteSize) -> Self {
        assert!(
            split_max_allocation <= limit,
            "split_max_allocation ({split_max_allocation}) must be lower or equal to the limit \
             ({limit})"
        );
        let state = GlobalWarmupMemoryState {
            limit: limit.as_u64(),
            split_max_allocation: split_max_allocation.as_u64(),
            allocated: 0,
            guards: BTreeMap::new(),
            waiters: BTreeMap::new(),
            next_guard_id: 0,
            next_waiter_seq: 0,
        };
        GlobalWarmupMemory {
            state: Arc::new(Mutex::new(state)),
        }
    }

    /// Number of bytes currently allocated.
    #[cfg(test)]
    pub fn allocated(&self) -> u64 {
        self.lock_state().allocated
    }

    /// Creates a guard that holds no memory yet.
    ///
    /// The guard is younger than all the live guards: its requests are served after theirs.
    pub fn new_guard(&self) -> WarmupMemoryGuard {
        let mut state = self.lock_state();
        let guard_id = state.next_guard_id;
        state.next_guard_id += 1;
        state.guards.insert(guard_id, GuardAccount::default());
        WarmupMemoryGuard {
            global: self.clone(),
            guard_id,
        }
    }

    /// Locks the state, even if a panic poisoned the lock.
    ///
    /// The state is also locked from `Drop`, possibly while a panic unwinds, so this must not
    /// panic.
    fn lock_state(&self) -> MutexGuard<'_, GlobalWarmupMemoryState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl GlobalWarmupMemoryState {
    fn account(&self, guard_id: SplitSearchId) -> &GuardAccount {
        self.guards
            .get(&guard_id)
            .expect("a live guard should have an account")
    }

    fn account_mut(&mut self, guard_id: SplitSearchId) -> &mut GuardAccount {
        self.guards
            .get_mut(&guard_id)
            .expect("a live guard should have an account")
    }

    fn is_oldest(&self, guard_id: SplitSearchId) -> bool {
        self.guards.first_key_value().map(|(id, _)| *id) == Some(guard_id)
    }

    fn set_allocated(&mut self, allocated: u64) {
        self.allocated = allocated;
        LEAF_SEARCH_WARMUP_ONGOING_NUM_BYTES.set(allocated as f64);
    }

    /// Checks the per-split limit, then serves the request right away if it fits in the memory
    /// the guard already holds. Returns whether the request was served.
    fn check_and_serve_from_reservation(
        &mut self,
        guard_id: SplitSearchId,
        request: MemoryRequest,
    ) -> Result<bool, OutOfMemoryError> {
        let split_max_allocation = self.split_max_allocation;
        let account = self.account_mut(guard_id);
        let max_needed_held = request
            .min_held
            .max(account.in_use + account.waiting + request.in_use_delta);
        if max_needed_held > split_max_allocation {
            return Err(OutOfMemoryError);
        }
        if request.needed_held(account.in_use) > account.held {
            return Ok(false);
        }
        account.in_use += request.in_use_delta;
        Ok(true)
    }

    /// Serves the request by growing the guard's reservation, if that fits in the limit of the
    /// guard. Returns whether the request was served.
    ///
    /// Grows by `GROWTH_NUMERATOR / GROWTH_DENOMINATOR` if that fits, and by the exact need
    /// otherwise. So the exponential growth never makes a request wait.
    ///
    /// Contract: the request passed the per-split limit check.
    fn try_serve_by_growing(&mut self, guard_id: SplitSearchId, request: MemoryRequest) -> bool {
        let limit = if self.is_oldest(guard_id) {
            self.limit
        } else {
            self.limit - self.split_max_allocation
        };
        let split_max_allocation = self.split_max_allocation;
        let allocated = self.allocated;
        let account = self.account(guard_id);
        let needed_held = request.needed_held(account.in_use);
        debug_assert!(needed_held <= split_max_allocation);
        let exact_growth = needed_held.saturating_sub(account.held);
        let exponential_growth = account
            .held
            .saturating_mul(GROWTH_NUMERATOR)
            .div_ceil(GROWTH_DENOMINATOR)
            .min(split_max_allocation)
            .saturating_sub(account.held);
        let growth = if exact_growth == 0 {
            0
        } else if exponential_growth > exact_growth && allocated + exponential_growth <= limit {
            exponential_growth
        } else if allocated + exact_growth <= limit {
            exact_growth
        } else {
            return false;
        };
        let account = self.account_mut(guard_id);
        account.held += growth;
        account.in_use += request.in_use_delta;
        self.set_allocated(allocated + growth);
        true
    }

    /// Adds a waiter for the request, then serves the waiters. The returned receiver gets a
    /// value once the request is served, possibly already.
    fn enqueue(
        &mut self,
        guard_id: SplitSearchId,
        request: MemoryRequest,
    ) -> oneshot::Receiver<()> {
        let (served_tx, served_rx) = oneshot::channel();
        let waiter_seq = self.next_waiter_seq;
        self.next_waiter_seq += 1;
        self.account_mut(guard_id).waiting += request.in_use_delta;
        let waiter = Waiter { request, served_tx };
        self.waiters.insert((guard_id, waiter_seq), waiter);
        self.serve_waiters();
        served_rx
    }

    /// Serves the waiters in order, until one does not fit. Called after each change that can
    /// let a waiter be served: a new waiter, freed memory, a dropped guard.
    fn serve_waiters(&mut self) {
        while let Some((&waiter_key, waiter)) = self.waiters.first_key_value() {
            let (guard_id, _waiter_seq) = waiter_key;
            let request = waiter.request;
            if waiter.served_tx.is_closed() {
                // The request was cancelled.
                self.waiters.remove(&waiter_key);
                self.account_mut(guard_id).waiting -= request.in_use_delta;
                continue;
            }
            if !self.try_serve_by_growing(guard_id, request) {
                if self.is_oldest(guard_id) {
                    error!(
                        guard_id,
                        "the oldest split search could not get warmup memory: this is a bug"
                    );
                }
                // Strict order: a request never passes an older one.
                return;
            }
            let waiter = self
                .waiters
                .remove(&waiter_key)
                .expect("the waiter should still be in the queue");
            let account = self.account_mut(guard_id);
            account.waiting -= request.in_use_delta;
            if waiter.served_tx.send(()).is_err() {
                // The request was cancelled after the `is_closed` check: nobody will use these
                // bytes. The memory stays in the reservation, and is freed by the next shrink.
                account.in_use -= request.in_use_delta;
            }
        }
    }

    /// Shrinks the reservation of the guard to `max_num_bytes`, but never below its memory in use.
    fn shrink_reservation(&mut self, guard_id: SplitSearchId, max_num_bytes: u64) {
        let account = self.account_mut(guard_id);
        let new_held = max_num_bytes.max(account.in_use);
        if new_held >= account.held {
            return;
        }
        let freed_num_bytes = account.held - new_held;
        account.held = new_held;
        self.set_allocated(self.allocated - freed_num_bytes);
        self.serve_waiters();
    }

    fn remove_guard(&mut self, guard_id: SplitSearchId) {
        let account = self
            .guards
            .remove(&guard_id)
            .expect("a live guard should have an account");
        // A guard cannot be dropped while one of its requests is waiting: the remaining waiters
        // are cancelled ones.
        self.waiters
            .retain(|&(waiter_guard_id, _), _| waiter_guard_id != guard_id);
        self.set_allocated(self.allocated - account.held);
        // The oldest guard may have changed: its waiters can now use the whole limit.
        self.serve_waiters();
    }
}

/// Memory reserved in a [`GlobalWarmupMemory`] for one split search. Freed on drop.
///
/// The bytes are accounted in the global warmup memory: this is just a handle.
pub(crate) struct WarmupMemoryGuard {
    global: GlobalWarmupMemory,
    /// Increases with the creation order: the smallest live id is the oldest split search.
    guard_id: SplitSearchId,
}

impl WarmupMemoryGuard {
    /// Number of bytes reserved by this guard.
    pub fn num_bytes(&self) -> u64 {
        self.global.lock_state().account(self.guard_id).held
    }

    /// Number of bytes in use, out of the reserved ones.
    #[cfg(test)]
    pub fn in_use_num_bytes(&self) -> u64 {
        self.global.lock_state().account(self.guard_id).in_use
    }

    /// Maximum number of bytes this guard can reserve.
    pub fn max_num_bytes(&self) -> u64 {
        self.global.lock_state().split_max_allocation
    }

    /// Reserves `num_bytes` for data entering the warmup cache. Waits until the memory is
    /// available.
    ///
    /// Fails right away with `OutOfMemoryError` if the guard would go over the per-split limit.
    ///
    /// Cancel-safe: if the future is dropped, nothing stays in use.
    pub async fn reserve(&self, num_bytes: u64) -> Result<(), OutOfMemoryError> {
        let request = MemoryRequest {
            in_use_delta: num_bytes,
            min_held: 0,
        };
        self.request(request).await
    }

    /// Grows the reservation to at least `num_bytes`, without using them. Waits until the memory
    /// is available.
    ///
    /// Fails right away with `OutOfMemoryError` if `num_bytes` is over the per-split limit.
    pub async fn reserve_capacity(&self, num_bytes: u64) -> Result<(), OutOfMemoryError> {
        let request = MemoryRequest {
            in_use_delta: 0,
            min_held: num_bytes,
        };
        self.request(request).await
    }

    /// Gives back `num_bytes` reserved by `reserve` that did not enter the cache. The reservation
    /// itself is not shrunk.
    ///
    /// Does not panic, because it is called from `Drop`. Releasing more than the memory in use
    /// fails a debug assertion, and saturates at 0 in release builds.
    pub fn release(&self, num_bytes: u64) {
        let mut state = self.global.lock_state();
        let account = state.account_mut(self.guard_id);
        debug_assert!(
            num_bytes <= account.in_use,
            "cannot release more warmup memory than reserved"
        );
        account.in_use = account.in_use.saturating_sub(num_bytes);
    }

    /// Shrinks the reservation to `max_num_bytes`, but never below the memory in use. Does nothing
    /// if the reservation is already smaller.
    pub fn shrink_reservation(&self, max_num_bytes: u64) {
        self.global
            .lock_state()
            .shrink_reservation(self.guard_id, max_num_bytes);
    }

    async fn request(&self, request: MemoryRequest) -> Result<(), OutOfMemoryError> {
        let served_rx = {
            let mut state = self.global.lock_state();
            if state.check_and_serve_from_reservation(self.guard_id, request)? {
                return Ok(());
            }
            state.enqueue(self.guard_id, request)
        };
        let mut pending_request = PendingRequest {
            guard: self,
            served_rx,
            in_use_delta: request.in_use_delta,
            is_done: false,
        };
        let served_res = (&mut pending_request.served_rx).await;
        pending_request.is_done = true;
        served_res.expect(
            "a waiter is only removed without being served when its guard is dropped, which \
             cannot happen while this future borrows the guard",
        );
        Ok(())
    }
}

impl Drop for WarmupMemoryGuard {
    fn drop(&mut self) {
        self.global.lock_state().remove_guard(self.guard_id);
    }
}

/// Undoes a waiting request if its future is dropped.
struct PendingRequest<'a> {
    guard: &'a WarmupMemoryGuard,
    served_rx: oneshot::Receiver<()>,
    in_use_delta: u64,
    /// Set once `served_rx` resolved. It must not be polled after that.
    is_done: bool,
}

impl Drop for PendingRequest<'_> {
    fn drop(&mut self) {
        if self.is_done {
            return;
        }
        // Closed first, so that the request cannot be served between the check and the drop.
        // The waiter is then removed the next time the waiters are served.
        self.served_rx.close();
        if self.served_rx.try_recv().is_ok() {
            // Served before it was closed: the bytes are in use, but no read will use them.
            self.guard.release(self.in_use_delta);
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::FutureExt;

    use super::*;

    fn global_warmup_memory(limit: u64, split_max_allocation: u64) -> GlobalWarmupMemory {
        GlobalWarmupMemory::new(ByteSize(limit), ByteSize(split_max_allocation))
    }

    /// Creates a guard and reserves `num_bytes` for it, which must not wait.
    fn guard_with_capacity(global: &GlobalWarmupMemory, num_bytes: u64) -> WarmupMemoryGuard {
        let guard = global.new_guard();
        guard
            .reserve_capacity(num_bytes)
            .now_or_never()
            .expect("the reservation should not wait")
            .unwrap();
        guard
    }

    /// Reserves `num_bytes` for a read. Returns `None` if the request would wait: it is then
    /// cancelled.
    fn reserve_now(
        guard: &WarmupMemoryGuard,
        num_bytes: u64,
    ) -> Option<Result<(), OutOfMemoryError>> {
        guard.reserve(num_bytes).now_or_never()
    }

    #[test]
    #[should_panic(expected = "must be lower or equal to the limit")]
    fn test_split_max_allocation_above_limit_panics() {
        global_warmup_memory(100, 101);
    }

    #[test]
    fn test_reserve_grows_reservation() {
        let global = global_warmup_memory(100, 100);
        let guard = guard_with_capacity(&global, 10);
        // Fits in the reservation.
        reserve_now(&guard, 10).unwrap().unwrap();
        assert_eq!(guard.num_bytes(), 10);
        // 11 does not fit in 10: the reservation grows to 1.7 x 10.
        reserve_now(&guard, 1).unwrap().unwrap();
        assert_eq!(guard.num_bytes(), 17);
        assert_eq!(guard.in_use_num_bytes(), 11);
        assert_eq!(global.allocated(), 17);
    }

    #[test]
    fn test_reserve_over_split_max_allocation_fails() {
        let global = global_warmup_memory(100, 40);
        let guard = guard_with_capacity(&global, 10);
        assert_eq!(reserve_now(&guard, 41), Some(Err(OutOfMemoryError)));
        assert_eq!(guard.num_bytes(), 10);
        assert_eq!(guard.in_use_num_bytes(), 0);
    }

    #[test]
    fn test_only_oldest_guard_can_use_reserve() {
        let global = global_warmup_memory(100, 40);
        let oldest_guard = guard_with_capacity(&global, 10);
        let other_guard = guard_with_capacity(&global, 40);
        // The other guards are limited to 100 - 40 = 60 bytes, including the oldest guard's.
        let new_guard = global.new_guard();
        assert!(reserve_now(&new_guard, 11).is_none());
        // The oldest guard can always grow to the split max allocation.
        reserve_now(&oldest_guard, 40).unwrap().unwrap();
        assert_eq!(global.allocated(), 80);
        // Dropping the oldest guard frees its memory, and the next guard becomes the oldest.
        drop(oldest_guard);
        assert_eq!(global.allocated(), 40);
        reserve_now(&new_guard, 20).unwrap().unwrap();
        assert!(reserve_now(&new_guard, 1).is_none());
        reserve_now(&other_guard, 40).unwrap().unwrap();
        assert_eq!(global.allocated(), 60);
    }

    #[test]
    fn test_shrink_reservation_never_below_memory_in_use() {
        let global = global_warmup_memory(100, 100);
        let guard = guard_with_capacity(&global, 50);
        reserve_now(&guard, 20).unwrap().unwrap();
        guard.shrink_reservation(10);
        assert_eq!(guard.num_bytes(), 20);
        // Shrinking never grows the reservation.
        guard.shrink_reservation(30);
        assert_eq!(guard.num_bytes(), 20);
        guard.release(20);
        guard.shrink_reservation(0);
        assert_eq!(global.allocated(), 0);
    }

    #[test]
    fn test_guard_drop_frees_memory() {
        let global = global_warmup_memory(100, 100);
        let guard = guard_with_capacity(&global, 100);
        reserve_now(&guard, 30).unwrap().unwrap();
        drop(guard);
        assert_eq!(global.allocated(), 0);
    }

    #[tokio::test]
    async fn test_waiters_served_by_guard_age() {
        // The guards other than the oldest one can use 50 bytes.
        let global = global_warmup_memory(100, 50);
        let _oldest_guard = guard_with_capacity(&global, 10);
        let full_guard = guard_with_capacity(&global, 40);
        let older_guard = global.new_guard();
        let younger_guard = global.new_guard();
        // The younger guard asks first.
        let mut younger_fut = Box::pin(younger_guard.reserve_capacity(5));
        assert!(futures::poll!(&mut younger_fut).is_pending());
        let mut older_fut = Box::pin(older_guard.reserve_capacity(5));
        assert!(futures::poll!(&mut older_fut).is_pending());
        // Only 5 bytes are freed: they go to the older guard.
        full_guard.shrink_reservation(35);
        assert!(futures::poll!(&mut older_fut).is_ready());
        assert!(futures::poll!(&mut younger_fut).is_pending());
        full_guard.shrink_reservation(30);
        assert!(futures::poll!(&mut younger_fut).is_ready());
    }

    #[tokio::test]
    async fn test_younger_request_does_not_pass_older_one() {
        let global = global_warmup_memory(100, 50);
        let _oldest_guard = guard_with_capacity(&global, 10);
        let full_guard = guard_with_capacity(&global, 40);
        let older_guard = global.new_guard();
        let younger_guard = global.new_guard();
        let mut older_fut = Box::pin(older_guard.reserve_capacity(20));
        assert!(futures::poll!(&mut older_fut).is_pending());
        full_guard.shrink_reservation(35);
        // 5 bytes are free: they would be enough for the younger guard, but the older one asked
        // first.
        let mut younger_fut = Box::pin(younger_guard.reserve_capacity(1));
        assert!(futures::poll!(&mut younger_fut).is_pending());
        assert!(reserve_now(&younger_guard, 1).is_none());
        full_guard.shrink_reservation(0);
        assert!(futures::poll!(&mut older_fut).is_ready());
        assert!(futures::poll!(&mut younger_fut).is_ready());
    }

    #[tokio::test]
    async fn test_oldest_guard_never_waits() {
        let global = global_warmup_memory(100, 50);
        let oldest_guard = guard_with_capacity(&global, 0);
        let _full_guard = guard_with_capacity(&global, 50);
        oldest_guard
            .reserve(50)
            .now_or_never()
            .expect("the oldest guard should never wait")
            .unwrap();
        assert_eq!(global.allocated(), 100);
    }

    #[tokio::test]
    async fn test_dropping_oldest_guard_serves_next_guard() {
        let global = global_warmup_memory(100, 50);
        let oldest_guard = guard_with_capacity(&global, 10);
        let next_guard = guard_with_capacity(&global, 40);
        let mut next_fut = Box::pin(next_guard.reserve(50));
        assert!(futures::poll!(&mut next_fut).is_pending());
        // The next guard becomes the oldest one: it can use the whole limit.
        drop(oldest_guard);
        assert!(futures::poll!(&mut next_fut).is_ready());
        assert_eq!(next_guard.in_use_num_bytes(), 50);
    }

    #[tokio::test]
    async fn test_cancelled_waiter_is_removed() {
        let global = global_warmup_memory(100, 50);
        let _oldest_guard = guard_with_capacity(&global, 10);
        let full_guard = guard_with_capacity(&global, 40);
        let cancelled_guard = global.new_guard();
        let other_guard = global.new_guard();
        let mut cancelled_fut = Box::pin(cancelled_guard.reserve(20));
        assert!(futures::poll!(&mut cancelled_fut).is_pending());
        full_guard.shrink_reservation(35);
        drop(cancelled_fut);
        // The cancelled request does not block the next one anymore.
        other_guard
            .reserve_capacity(5)
            .now_or_never()
            .expect("the cancelled request should be removed")
            .unwrap();
        assert_eq!(cancelled_guard.num_bytes(), 0);
        // The cancelled request does not count in the per-split limit anymore: 50 bytes do not go
        // over it, they only have to wait.
        assert!(reserve_now(&cancelled_guard, 50).is_none());
    }

    #[tokio::test]
    async fn test_request_cancelled_after_being_served_releases_memory() {
        let global = global_warmup_memory(100, 50);
        let _oldest_guard = guard_with_capacity(&global, 10);
        let full_guard = guard_with_capacity(&global, 40);
        let guard = global.new_guard();
        let mut reserve_fut = Box::pin(guard.reserve(10));
        assert!(futures::poll!(&mut reserve_fut).is_pending());
        // Served, but the future is dropped before it is polled again.
        full_guard.shrink_reservation(0);
        assert_eq!(guard.in_use_num_bytes(), 10);
        drop(reserve_fut);
        assert_eq!(guard.in_use_num_bytes(), 0);
        guard.shrink_reservation(0);
        assert_eq!(global.allocated(), 10);
    }

    #[tokio::test]
    async fn test_per_split_limit_counts_waiting_requests() {
        let global = global_warmup_memory(100, 50);
        let _oldest_guard = guard_with_capacity(&global, 10);
        let _full_guard = guard_with_capacity(&global, 40);
        let guard = global.new_guard();
        let mut reserve_fut = Box::pin(guard.reserve(30));
        assert!(futures::poll!(&mut reserve_fut).is_pending());
        assert_eq!(
            guard.reserve(21).now_or_never().unwrap().err(),
            Some(OutOfMemoryError)
        );
    }

    #[test]
    fn test_concurrent_reserve_never_exceeds_limit() {
        const NUM_THREADS: usize = 8;
        const NUM_ITERATIONS: usize = 10_000;
        const LIMIT: u64 = 100;
        const SPLIT_MAX_ALLOCATION: u64 = 30;
        let global = global_warmup_memory(LIMIT, SPLIT_MAX_ALLOCATION);
        std::thread::scope(|scope| {
            for _ in 0..NUM_THREADS {
                let global = global.clone();
                scope.spawn(move || {
                    for i in 0..NUM_ITERATIONS {
                        let num_bytes = (i as u64 % SPLIT_MAX_ALLOCATION) + 1;
                        let guard = global.new_guard();
                        let _ = reserve_now(&guard, num_bytes);
                        assert!(global.allocated() <= LIMIT);
                    }
                });
            }
        });
        assert_eq!(global.allocated(), 0);
    }
}
