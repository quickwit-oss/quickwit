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

mod state;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use async_trait::async_trait;
use bytesize::ByteSize;
use quickwit_directories::{MemoryReservation, MemoryReserver, OutOfMemoryError};
use tokio::sync::oneshot;

use self::state::{GlobalWarmupMemoryState, MemoryRequest, SplitSearchId, WarmupMemoryLimits};
use crate::metrics::LEAF_SEARCH_SPLIT_WARMUP_OUT_OF_MEMORY_SPLIT_BUDGET_TOTAL;

/// Memory reserved by split search warmups on this node.
///
/// Shared by the search permit actor, which creates the guards, and by every
/// [`WarmupMemoryGuard`].
#[derive(Clone)]
pub(crate) struct GlobalWarmupMemory {
    inner: Arc<GlobalWarmupMemoryInner>,
}

struct GlobalWarmupMemoryInner {
    /// Never changes, so it lives outside of the lock.
    limits: WarmupMemoryLimits,
    /// Only locked for some bookkeeping, never across an `.await`.
    state: Mutex<GlobalWarmupMemoryState>,
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
        let limits = WarmupMemoryLimits {
            limit: limit.as_u64(),
            split_max_allocation: split_max_allocation.as_u64(),
        };
        let inner = GlobalWarmupMemoryInner {
            limits,
            state: Mutex::new(GlobalWarmupMemoryState::new()),
        };
        GlobalWarmupMemory {
            inner: Arc::new(inner),
        }
    }

    /// Number of bytes currently allocated.
    #[cfg(test)]
    pub fn allocated(&self) -> u64 {
        self.lock_state().allocated()
    }

    /// Creates a guard that holds no memory yet.
    ///
    /// The guard is younger than all the live guards: its requests are served after theirs.
    pub fn new_guard(&self) -> WarmupMemoryGuard {
        let guard_id = self.lock_state().add_guard();
        WarmupMemoryGuard {
            global: self.clone(),
            guard_id,
            out_of_memory: AtomicBool::new(false),
        }
    }

    fn limits(&self) -> WarmupMemoryLimits {
        self.inner.limits
    }

    /// Locks the state, even if a panic poisoned the lock.
    ///
    /// The state is also locked from `Drop`, possibly while a panic unwinds, so this must not
    /// panic.
    fn lock_state(&self) -> MutexGuard<'_, GlobalWarmupMemoryState> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

/// Warmup memory of a single split search, reserved in a [`GlobalWarmupMemory`]. Freed on drop.
///
/// Shared (`Arc<WarmupMemoryGuard>`) between the split's `SearchPermit` and its
/// `CachingDirectory`, which reserves memory before each cache miss through the [`MemoryReserver`]
/// interface. The memory is freed when the last of them is dropped, so it lives as long as the
/// cache that holds the warmup data.
///
/// The bytes are accounted in the global warmup memory: this is just a handle.
pub(crate) struct WarmupMemoryGuard {
    global: GlobalWarmupMemory,
    /// Increases with the creation order: the smallest live id is the oldest split search.
    guard_id: SplitSearchId,
    /// Set the first time a request fails.
    /// The point is to count each split search only once in the out of memory metric.
    out_of_memory: AtomicBool,
}

impl WarmupMemoryGuard {
    /// Number of bytes reserved by this guard.
    #[cfg(test)]
    pub fn num_bytes(&self) -> u64 {
        self.global.lock_state().guard_account(self.guard_id).held
    }

    /// Number of bytes in use, out of the reserved ones.
    #[cfg(test)]
    pub fn in_use_num_bytes(&self) -> u64 {
        self.global.lock_state().guard_account(self.guard_id).in_use
    }

    /// Whether a request failed, because of the per-split limit.
    #[cfg(test)]
    pub fn has_run_out_of_memory(&self) -> bool {
        self.out_of_memory.load(Ordering::Relaxed)
    }

    /// Maximum number of bytes this guard can reserve.
    pub fn max_num_bytes(&self) -> u64 {
        self.global.limits().split_max_allocation
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
    /// is available. Used to reserve the memory of a split search before its warmup.
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
    /// itself is not shrunk: `release_unused_memory` does it after the warmup.
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
        self.global.lock_state().shrink_reservation(
            self.global.limits(),
            self.guard_id,
            max_num_bytes,
        );
    }

    /// Shrinks the reservation to the memory in use.
    pub fn release_unused_memory(&self) {
        self.shrink_reservation(0);
    }

    async fn request(&self, request: MemoryRequest) -> Result<(), OutOfMemoryError> {
        let limits = self.global.limits();
        let served_rx = {
            let mut state = self.global.lock_state();
            let served_res = state.check_and_serve_from_reservation(limits, self.guard_id, request);
            match served_res {
                Ok(true) => return Ok(()),
                Ok(false) => state.enqueue(limits, self.guard_id, request),
                Err(out_of_memory_error) => {
                    let reserved_num_bytes = state.guard_account(self.guard_id).held;
                    drop(state);
                    self.record_out_of_memory(reserved_num_bytes, request);
                    return Err(out_of_memory_error);
                }
            }
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

    /// Counts the split search as out of memory and logs a warning, only the first time.
    fn record_out_of_memory(&self, reserved_num_bytes: u64, request: MemoryRequest) {
        if self.out_of_memory.swap(true, Ordering::Relaxed) {
            return;
        }
        LEAF_SEARCH_SPLIT_WARMUP_OUT_OF_MEMORY_SPLIT_BUDGET_TOTAL.inc();
        let requested_num_bytes = request.in_use_delta.max(request.min_held);
        let split_max_num_bytes = self.max_num_bytes();
        quickwit_common::rate_limited_warn!(
            limit_per_min = 10,
            reserved_num_bytes,
            requested_num_bytes,
            split_max_num_bytes,
            "split search ran out of warmup memory: {OutOfMemoryError}"
        );
    }
}

#[async_trait]
impl MemoryReserver for WarmupMemoryGuard {
    /// Reserves memory for `num_bytes` of warmup data, growing the reservation if needed.
    ///
    /// Waits until the memory is available. This cannot deadlock: see the module documentation.
    /// Fails right away if the reservation would go over the per-split limit.
    ///
    /// Cancel-safe: if the future is dropped while waiting, nothing is reserved.
    async fn reserve_memory(
        &self,
        num_bytes: u64,
    ) -> Result<MemoryReservation<'_>, OutOfMemoryError> {
        self.reserve(num_bytes).await?;
        Ok(MemoryReservation::new(self, num_bytes))
    }

    fn release_memory(&self, num_bytes: u64) {
        self.release(num_bytes);
    }
}

impl Drop for WarmupMemoryGuard {
    fn drop(&mut self) {
        self.global
            .lock_state()
            .remove_guard(self.global.limits(), self.guard_id);
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
mod memory_reserver_tests;
#[cfg(test)]
mod queue_tests;
