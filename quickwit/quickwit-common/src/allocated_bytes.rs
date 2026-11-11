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

//! Live heap bytes requested through [`CountingAllocator`].
//!
//! Small changes are batched per thread to avoid contending on every allocation.
//! This measures requested sizes, excluding allocator overhead, retained pages,
//! stacks, memory mappings, and allocations that bypass the Rust global allocator.

use std::alloc::{GlobalAlloc, Layout};
use std::cell::Cell;
use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};

const BATCH_BYTES: usize = 64 * 1024;
const NUM_SHARDS: usize = 64;

// Separate writers' cache lines, including Apple's 128-byte cache lines.
#[repr(align(128))]
struct Shard(AtomicIsize);

static SHARDS: [Shard; NUM_SHARDS] = [const { Shard(AtomicIsize::new(0)) }; NUM_SHARDS];
static NEXT_SHARD: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static PENDING_BYTES: PendingBytes = PendingBytes {
        bytes: Cell::new(0),
        counter: &SHARDS[NEXT_SHARD.fetch_add(1, Ordering::Relaxed) % NUM_SHARDS].0,
    };
}

struct PendingBytes {
    bytes: Cell<isize>,
    counter: &'static AtomicIsize,
}

impl PendingBytes {
    fn flush(&self) {
        let bytes = self.bytes.replace(0);
        if bytes != 0 {
            self.counter.fetch_add(bytes, Ordering::Relaxed);
        }
    }
}

impl Drop for PendingBytes {
    fn drop(&mut self) {
        self.flush();
    }
}

#[inline]
fn record(bytes: isize) {
    let result = PENDING_BYTES.try_with(|pending| {
        if bytes.unsigned_abs() >= BATCH_BYTES {
            pending.counter.fetch_add(bytes, Ordering::Relaxed);
            return;
        }
        let bytes = pending.bytes.get() + bytes;
        pending.bytes.set(bytes);
        if bytes.unsigned_abs() >= BATCH_BYTES {
            pending.flush();
        }
    });
    // Other TLS destructors can still allocate after our pending delta has been flushed.
    if result.is_err() {
        SHARDS[0].0.fetch_add(bytes, Ordering::Relaxed);
    }
}

fn published_bytes() -> isize {
    // Cross-thread frees leave signed balances that can wrap over the process lifetime.
    SHARDS.iter().fold(0isize, |total, shard| {
        total.wrapping_add(shard.0.load(Ordering::Relaxed))
    })
}

/// Returns the approximate number of live heap bytes requested through [`CountingAllocator`].
///
/// Flushes the calling thread's pending delta. Other threads can each have less than 64 KiB
/// of unpublished allocations or frees, including while idle. Thread exit flushes their delta.
/// Sums 64 shards and clamps the total to zero. Shards are sampled sequentially, so concurrent
/// changes can cause additional under- or overcounting beyond the unpublished deltas.
///
/// Install [`CountingAllocator`] as the global allocator to account for the application's Rust
/// heap. Callers choose their own threshold and polling interval. This is not an RSS measurement
/// or a hard memory limit.
pub fn allocated_bytes() -> usize {
    let _ = PENDING_BYTES.try_with(PendingBytes::flush);
    published_bytes().max(0) as usize
}

/// Wraps an allocator and contributes live requested bytes to [`allocated_bytes()`].
///
/// All instances share the same counter. Allocations, zeroed allocations, and reallocations
/// are counted only on success. Accounting does not allocate, lock, log, or invoke callbacks.
/// Keep the wrapper installed for the entire lifetime of the allocations it tracks.
pub struct CountingAllocator<A>(pub A);

unsafe impl<A: GlobalAlloc> GlobalAlloc for CountingAllocator<A> {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { self.0.alloc(layout) };
        if !ptr.is_null() {
            record(layout.size() as isize);
        }
        ptr
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { self.0.alloc_zeroed(layout) };
        if !ptr.is_null() {
            record(layout.size() as isize);
        }
        ptr
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        record(-(layout.size() as isize));
        unsafe { self.0.dealloc(ptr, layout) };
    }

    #[inline]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { self.0.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            record(new_size as isize - layout.size() as isize);
        }
        new_ptr
    }
}

#[cfg(test)]
mod tests {
    use std::alloc::System;
    use std::ptr;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;

    use super::*;

    // These tests explicitly wrap System and share the process-wide counter.
    static TEST_LOCK: Mutex<()> = Mutex::new(());
    static ALLOCATOR: CountingAllocator<System> = CountingAllocator(System);

    #[test]
    fn test_alloc_zeroed_realloc_and_dealloc() {
        let _guard = TEST_LOCK.lock().unwrap();
        assert_eq!(allocated_bytes(), 0);
        unsafe {
            let layout = Layout::from_size_align(32, 16).unwrap();
            let ptr = ALLOCATOR.alloc(layout);
            assert!(!ptr.is_null());
            ptr.write_bytes(42, layout.size());
            assert_eq!(allocated_bytes(), 32);

            let zeroed = ALLOCATOR.alloc_zeroed(layout);
            assert!(!zeroed.is_null());
            assert!(
                std::slice::from_raw_parts(zeroed, 32)
                    .iter()
                    .all(|byte| *byte == 0)
            );
            assert_eq!(allocated_bytes(), 64);

            let grown = ALLOCATOR.realloc(ptr, layout, 128);
            assert!(!grown.is_null());
            assert_eq!(*grown, 42);
            assert_eq!(allocated_bytes(), 160);
            let shrunk = ALLOCATOR.realloc(grown, Layout::from_size_align(128, 16).unwrap(), 16);
            assert!(!shrunk.is_null());
            assert_eq!(*shrunk, 42);
            assert_eq!(allocated_bytes(), 48);

            ALLOCATOR.dealloc(shrunk, Layout::from_size_align(16, 16).unwrap());
            ALLOCATOR.dealloc(zeroed, layout);
        }
        assert_eq!(allocated_bytes(), 0);
    }

    struct FallibleAllocator(AtomicBool);

    unsafe impl GlobalAlloc for FallibleAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            if self.0.load(Ordering::Relaxed) {
                return ptr::null_mut();
            }
            unsafe { System.alloc(layout) }
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            unsafe { System.dealloc(ptr, layout) }
        }

        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            if self.0.load(Ordering::Relaxed) {
                return ptr::null_mut();
            }
            unsafe { System.realloc(ptr, layout, new_size) }
        }
    }

    #[test]
    fn test_failed_allocations_leave_count_unchanged() {
        let _guard = TEST_LOCK.lock().unwrap();
        assert_eq!(allocated_bytes(), 0);
        let allocator = CountingAllocator(FallibleAllocator(AtomicBool::new(false)));
        unsafe {
            let layout = Layout::from_size_align(32, 16).unwrap();
            let ptr = allocator.alloc(layout);
            assert!(!ptr.is_null());
            ptr.write(42);
            allocator.0.0.store(true, Ordering::Relaxed);
            assert!(allocator.alloc(layout).is_null());
            assert!(allocator.alloc_zeroed(layout).is_null());
            assert!(allocator.realloc(ptr, layout, 128).is_null());
            assert_eq!(allocated_bytes(), 32);
            assert_eq!(*ptr, 42);
            allocator.dealloc(ptr, layout);
        }
        assert_eq!(allocated_bytes(), 0);
    }

    #[test]
    fn test_batching_and_large_changes() {
        let _guard = TEST_LOCK.lock().unwrap();
        assert_eq!(allocated_bytes(), 0);
        let batch = BATCH_BYTES as isize;
        record(batch - 1);
        assert_eq!(published_bytes(), 0);
        record(1);
        assert_eq!(published_bytes(), batch);
        record(-(batch - 1));
        assert_eq!(published_bytes(), batch);
        record(-1);
        assert_eq!(published_bytes(), 0);

        record(1);
        record(batch * 2);
        assert_eq!(published_bytes(), batch * 2);
        assert_eq!(allocated_bytes(), BATCH_BYTES * 2 + 1);
        record(-batch * 2);
        record(-1);
        assert_eq!(allocated_bytes(), 0);
    }

    #[test]
    fn test_threads_cycle_through_separate_cache_lines() {
        let _guard = TEST_LOCK.lock().unwrap();
        assert_eq!(std::mem::size_of::<Shard>(), 128);
        let counters: Vec<_> = (0..=NUM_SHARDS)
            .map(|_| {
                std::thread::spawn(|| {
                    PENDING_BYTES.with(|pending| pending.counter as *const AtomicIsize as usize)
                })
                .join()
                .unwrap()
            })
            .collect();
        let unique: std::collections::HashSet<_> = counters[..NUM_SHARDS].iter().collect();
        assert_eq!(unique.len(), NUM_SHARDS);
        assert!(counters.iter().all(|address| address % 128 == 0));
        assert_eq!(counters[0], counters[NUM_SHARDS]);
    }

    #[test]
    fn test_shard_sum_wraps_before_clamping() {
        let _guard = TEST_LOCK.lock().unwrap();
        assert_eq!(allocated_bytes(), 0);
        let previous: Vec<_> = SHARDS
            .iter()
            .map(|shard| shard.0.swap(0, Ordering::Relaxed))
            .collect();
        SHARDS[0].0.store(isize::MAX, Ordering::Relaxed);
        SHARDS[1].0.store(42, Ordering::Relaxed);
        SHARDS[2].0.store(-isize::MAX, Ordering::Relaxed);
        assert_eq!(allocated_bytes(), 42);
        for (shard, value) in SHARDS.iter().zip(previous) {
            shard.0.store(value, Ordering::Relaxed);
        }
        assert_eq!(allocated_bytes(), 0);
    }

    #[test]
    fn test_cross_thread_free_before_alloc_is_published() {
        let _guard = TEST_LOCK.lock().unwrap();
        assert_eq!(allocated_bytes(), 0);
        let layout = Layout::from_size_align(32, 16).unwrap();
        let ptr = unsafe { ALLOCATOR.alloc(layout) };
        assert!(!ptr.is_null());
        let address = ptr as usize;
        std::thread::spawn(move || {
            unsafe { ALLOCATOR.dealloc(address as *mut u8, layout) };
            assert_eq!(allocated_bytes(), 0);
            assert_eq!(published_bytes(), -32);
        })
        .join()
        .unwrap();
        assert_eq!(allocated_bytes(), 0);
    }

    #[test]
    fn test_concurrent_allocations_flush_on_thread_exit() {
        let _guard = TEST_LOCK.lock().unwrap();
        assert_eq!(allocated_bytes(), 0);
        let layout = Layout::from_size_align(128, 16).unwrap();
        let threads: Vec<_> = (0..8)
            .map(|_| {
                std::thread::spawn(move || {
                    (0..1000)
                        .map(|_| {
                            let ptr = unsafe { ALLOCATOR.alloc(layout) };
                            assert!(!ptr.is_null());
                            ptr as usize
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        let addresses: Vec<_> = threads
            .into_iter()
            .flat_map(|thread| thread.join().unwrap())
            .collect();
        assert_eq!(allocated_bytes(), 8 * 1000 * 128);
        for address in addresses {
            unsafe { ALLOCATOR.dealloc(address as *mut u8, layout) };
        }
        assert_eq!(allocated_bytes(), 0);
    }

    #[test]
    fn test_allocations_after_counter_tls_is_destroyed() {
        struct LateFree(Cell<usize>);

        impl Drop for LateFree {
            fn drop(&mut self) {
                assert!(PENDING_BYTES.try_with(|_| ()).is_err());
                let layout = Layout::from_size_align(32, 16).unwrap();
                unsafe {
                    ALLOCATOR.dealloc(self.0.get() as *mut u8, layout);
                    let ptr = ALLOCATOR.alloc(layout);
                    assert!(!ptr.is_null());
                    ALLOCATOR.dealloc(ptr, layout);
                }
            }
        }

        thread_local! {
            static LATE_FREE: LateFree = const { LateFree(Cell::new(0)) };
        }

        let _guard = TEST_LOCK.lock().unwrap();
        assert_eq!(allocated_bytes(), 0);
        std::thread::spawn(|| {
            LATE_FREE.with(|late_free| {
                let ptr = unsafe { ALLOCATOR.alloc(Layout::from_size_align(32, 16).unwrap()) };
                assert!(!ptr.is_null());
                late_free.0.set(ptr as usize);
            });
        })
        .join()
        .unwrap();
        assert_eq!(allocated_bytes(), 0);
    }
}
