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

//! The state of the warmup memory: the accounts of the split searches and the waiter queue.

use std::collections::BTreeMap;

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
pub(super) type SplitSearchId = u64;

#[derive(Clone, Copy)]
pub(super) struct WarmupMemoryLimits {
    pub(super) limit: u64,
    pub(super) split_max_allocation: u64,
}

impl WarmupMemoryLimits {
    /// Memory all the guards can hold, if the guard asking for more is the oldest one, and memory
    /// all the guards but the oldest can hold otherwise.
    pub(super) fn limit_for(&self, is_oldest: bool) -> u64 {
        if is_oldest {
            self.limit
        } else {
            self.limit - self.split_max_allocation
        }
    }
}

/// Invariants:
/// - `allocated` is the sum of `held` over all guards, and `allocated <= limit`;
/// - for each guard, `in_use <= held <= split_max_allocation`;
/// - the guards other than the oldest one hold at most `limit - split_max_allocation` in total.
pub(super) struct GlobalWarmupMemoryState {
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
pub(super) struct GuardAccount {
    /// Memory reserved by the split search.
    pub(super) held: u64,
    /// Bytes of the data that entered the split's warmup cache.
    pub(super) in_use: u64,
    /// Sum of the `in_use_delta` of this guard's waiters, including cancelled ones that are not
    /// removed yet.
    ///
    /// Counted in the per-split limit check, so that `in_use` can never go over the limit once
    /// they are served. Counting cancelled waiters only makes the check stricter.
    waiting: u64,
}

#[derive(Clone, Copy)]
pub(super) struct MemoryRequest {
    /// Added to `in_use` once served. 0 for the initial reservation.
    pub(super) in_use_delta: u64,
    /// `held` is grown to at least this once served. 0 for reads.
    pub(super) min_held: u64,
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

impl GlobalWarmupMemoryState {
    pub(super) fn new() -> Self {
        GlobalWarmupMemoryState {
            allocated: 0,
            guards: BTreeMap::new(),
            waiters: BTreeMap::new(),
            next_guard_id: 0,
            next_waiter_seq: 0,
        }
    }

    /// Number of bytes currently allocated.
    #[cfg(test)]
    pub(super) fn allocated(&self) -> u64 {
        self.allocated
    }

    /// Adds the account of a new guard, younger than all the live ones, and returns its id.
    pub(super) fn add_guard(&mut self) -> SplitSearchId {
        let guard_id = self.next_guard_id;
        self.next_guard_id += 1;
        self.guards.insert(guard_id, GuardAccount::default());
        guard_id
    }

    /// Account of a live guard.
    pub(super) fn guard_account(&self, guard_id: SplitSearchId) -> &GuardAccount {
        self.account(guard_id)
    }

    fn account(&self, guard_id: SplitSearchId) -> &GuardAccount {
        self.guards
            .get(&guard_id)
            .expect("a live guard should have an account")
    }

    pub(super) fn account_mut(&mut self, guard_id: SplitSearchId) -> &mut GuardAccount {
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
    pub(super) fn check_and_serve_from_reservation(
        &mut self,
        limits: WarmupMemoryLimits,
        guard_id: SplitSearchId,
        request: MemoryRequest,
    ) -> Result<bool, OutOfMemoryError> {
        let account = self.account_mut(guard_id);
        let max_needed_held = request
            .min_held
            .max(account.in_use + account.waiting + request.in_use_delta);
        if max_needed_held > limits.split_max_allocation {
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
    fn try_serve_by_growing(
        &mut self,
        limits: WarmupMemoryLimits,
        guard_id: SplitSearchId,
        request: MemoryRequest,
    ) -> bool {
        let limit = limits.limit_for(self.is_oldest(guard_id));
        let allocated = self.allocated;
        let account = self.account(guard_id);
        let needed_held = request.needed_held(account.in_use);
        debug_assert!(needed_held <= limits.split_max_allocation);
        let exact_growth = needed_held.saturating_sub(account.held);
        let exponential_growth = account
            .held
            .saturating_mul(GROWTH_NUMERATOR)
            .div_ceil(GROWTH_DENOMINATOR)
            .min(limits.split_max_allocation)
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
    pub(super) fn enqueue(
        &mut self,
        limits: WarmupMemoryLimits,
        guard_id: SplitSearchId,
        request: MemoryRequest,
    ) -> oneshot::Receiver<()> {
        let (served_tx, served_rx) = oneshot::channel();
        let waiter_seq = self.next_waiter_seq;
        self.next_waiter_seq += 1;
        self.account_mut(guard_id).waiting += request.in_use_delta;
        let waiter = Waiter { request, served_tx };
        self.waiters.insert((guard_id, waiter_seq), waiter);
        self.serve_waiters(limits);
        served_rx
    }

    /// Serves the waiters in order, until one does not fit. Called after each change that can
    /// let a waiter be served: a new waiter, freed memory, a dropped guard.
    fn serve_waiters(&mut self, limits: WarmupMemoryLimits) {
        while let Some((&waiter_key, waiter)) = self.waiters.first_key_value() {
            let (guard_id, _waiter_seq) = waiter_key;
            let request = waiter.request;
            if waiter.served_tx.is_closed() {
                // The request was cancelled.
                self.waiters.remove(&waiter_key);
                self.account_mut(guard_id).waiting -= request.in_use_delta;
                continue;
            }
            if !self.try_serve_by_growing(limits, guard_id, request) {
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
    pub(super) fn shrink_reservation(
        &mut self,
        limits: WarmupMemoryLimits,
        guard_id: SplitSearchId,
        max_num_bytes: u64,
    ) {
        let account = self.account_mut(guard_id);
        let new_held = max_num_bytes.max(account.in_use);
        if new_held >= account.held {
            return;
        }
        let freed_num_bytes = account.held - new_held;
        account.held = new_held;
        self.set_allocated(self.allocated - freed_num_bytes);
        self.serve_waiters(limits);
    }

    pub(super) fn remove_guard(&mut self, limits: WarmupMemoryLimits, guard_id: SplitSearchId) {
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
        self.serve_waiters(limits);
    }
}
