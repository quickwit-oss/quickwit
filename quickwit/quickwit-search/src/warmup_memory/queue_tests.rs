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

//! Tests of the waiter queue and of the accounting, through the guard API.

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
fn reserve_now(guard: &WarmupMemoryGuard, num_bytes: u64) -> Option<Result<(), OutOfMemoryError>> {
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
