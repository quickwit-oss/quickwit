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

use std::alloc::{GlobalAlloc, Layout};
use std::hash::Hasher;
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use bytesize::ByteSize;
use tikv_jemallocator::Jemalloc;
use tracing::{error, info};

use crate::alloc_tracker::{self, AllocRecordingResponse};

const DEFAULT_MIN_ALLOC_BYTES_FOR_PROFILING: u64 = 64 * 1024;
const DEFAULT_REPORTING_INTERVAL_BYTES: u64 = 1024 * 1024 * 1024;

/// Atomics are used to communicate configurations between the start/stop
/// endpoints and the JemallocProfiled allocator wrapper.
///
/// The flags are padded to avoid false sharing of the CPU cache line between
/// threads. 128 bytes is the cache line size on x86_64 and arm64.
#[repr(align(128))]
struct Flags {
    /// The minimum allocation size that is recorded by the tracker.
    min_alloc_bytes_for_profiling: AtomicU64,
    /// Whether the profiling is started or not.
    enabled: AtomicBool,
    /// Padding to make sure we fill the cache line.
    _padding: [u8; 119], // 128 (align) - 8 (u64) - 1 (bool)
}

static FLAGS: Flags = Flags {
    min_alloc_bytes_for_profiling: AtomicU64::new(DEFAULT_MIN_ALLOC_BYTES_FOR_PROFILING),
    enabled: AtomicBool::new(false),
    _padding: [0; 119],
};

/// Starts measuring heap allocations and logs important leaks.
///
/// This function uses a wrapper around the global Jemalloc allocator to
/// instrument it.
///
/// Each time an allocation bigger than min_alloc_bytes_for_profiling is
/// performed, it is recorded in a map and the statistics for its call site are
/// updated. Tracking allocations is costly because it requires acquiring a
/// global mutex. Setting a reasonable value for min_alloc_bytes_for_profiling
/// is crucial. For instance for a search aggregation request, tracking every
/// allocations (min_alloc_bytes_for_profiling=1) is typically 100x slower than
/// using a minimum of 64kB.
///
/// During profiling, the statistics per call site are used to log when specific
/// thresholds are exceeded. For each call site, the allocated memory is logged
/// (with a backtrace) every time it exceeds the last logged allocated memory by
/// at least alloc_bytes_triggering_backtrace. This logging interval should
/// usually be set to a value of at least 500MB to limit the logging verbosity.
pub fn start_profiling(
    min_alloc_bytes_for_profiling: Option<u64>,
    alloc_bytes_triggering_backtrace: Option<u64>,
) {
    #[cfg(miri)]
    warn!(
        "heap profiling is not supported with Miri because in that case the `backtrace` crate \
         allocates"
    );

    // Call backtrace once to warmup symbolization allocations (~30MB)
    backtrace::trace(|frame| {
        backtrace::resolve_frame(frame, |_| {});
        true
    });

    let alloc_bytes_triggering_backtrace =
        alloc_bytes_triggering_backtrace.unwrap_or(DEFAULT_REPORTING_INTERVAL_BYTES);
    alloc_tracker::init(alloc_bytes_triggering_backtrace);

    let min_alloc_bytes_for_profiling =
        min_alloc_bytes_for_profiling.unwrap_or(DEFAULT_MIN_ALLOC_BYTES_FOR_PROFILING);

    // stdout() might allocate a buffer on first use. If the first allocation
    // tracked comes from stdout, it will trigger a deadlock. Logging here
    // guarantees that it doesn't happen.
    info!(
        min_alloc_for_profiling = %ByteSize(min_alloc_bytes_for_profiling),
        alloc_triggering_backtrace = %ByteSize(alloc_bytes_triggering_backtrace),
        "heap profiling running"
    );

    // Use strong ordering to make sure all threads see these changes in this order
    FLAGS
        .min_alloc_bytes_for_profiling
        .store(min_alloc_bytes_for_profiling, Ordering::SeqCst);
    FLAGS.enabled.store(true, Ordering::SeqCst);
}

/// Stops measuring heap allocations.
///
/// The allocation tracking tables and the symbol cache are not cleared.
pub fn stop_profiling() {
    // Use strong ordering to make sure all threads see these changes in this order
    let previously_enabled = FLAGS.enabled.swap(false, Ordering::SeqCst);
    FLAGS
        .min_alloc_bytes_for_profiling
        .store(DEFAULT_MIN_ALLOC_BYTES_FOR_PROFILING, Ordering::SeqCst);

    info!(previously_enabled, "heap profiling stopped");
}

/// Wraps the Jemalloc global allocator calls with tracking routines.
///
/// The tracking routines are called only when [ENABLED] is set to true (calling
/// [start_profiling()]), but we don't enforce any synchronization (we load it with
/// Ordering::Relaxed) because it's fine to miss or record extra allocation events.
///
/// It's important to ensure that no allocations are performed inside the allocator!
pub struct JemallocProfiled(pub Jemalloc);

unsafe impl GlobalAlloc for JemallocProfiled {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { self.0.alloc(layout) };
        if FLAGS.enabled.load(Ordering::Relaxed) {
            track_alloc_call(ptr, layout);
        }
        ptr
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { self.0.alloc_zeroed(layout) };
        if FLAGS.enabled.load(Ordering::Relaxed) {
            track_alloc_call(ptr, layout);
        }
        ptr
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if FLAGS.enabled.load(Ordering::Relaxed) {
            track_dealloc_call(ptr, layout);
        }
        unsafe { self.0.dealloc(ptr, layout) }
    }

    #[inline]
    unsafe fn realloc(&self, old_ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { self.0.realloc(old_ptr, layout, new_size) };
        if FLAGS.enabled.load(Ordering::Relaxed) {
            track_realloc_call(old_ptr, new_ptr, layout, new_size);
        }
        new_ptr
    }
}

/// Warning: stdout allocates a buffer on first use.
#[inline]
fn print_backtrace(callsite_hash: u64, stat: alloc_tracker::Statistic) {
    {
        let mut lock = std::io::stdout().lock();
        let _ = writeln!(
            &mut lock,
            "htrk callsite={} allocs={} size={}",
            callsite_hash, stat.count, stat.size
        );
        backtrace::trace(|frame| {
            backtrace::resolve_frame(frame, |symbol| {
                if let Some(symbole_name) = symbol.name() {
                    let _ = writeln!(&mut lock, "{}", symbole_name);
                } else {
                    let _ = writeln!(&mut lock, "symb failed");
                }
            });
            true
        });
    }
}

#[inline]
fn backtrace_hash() -> u64 {
    let mut hasher = fnv::FnvHasher::default();
    backtrace::trace(|frame| {
        hasher.write_usize(frame.ip() as usize);
        true
    });
    hasher.finish()
}

/// Warning: allocating inside this function can cause an error (abort, panic or even deadlock).
#[cold]
fn track_alloc_call(ptr: *mut u8, layout: Layout) {
    if layout.size() >= FLAGS.min_alloc_bytes_for_profiling.load(Ordering::Relaxed) as usize {
        let callsite_hash = backtrace_hash();
        let recording_response =
            alloc_tracker::record_allocation(callsite_hash, layout.size() as u64, ptr);

        match recording_response {
            AllocRecordingResponse::ThresholdExceeded(stat_for_trace) => {
                // warning: stdout might allocate a buffer on first use
                print_backtrace(callsite_hash, stat_for_trace);
            }
            AllocRecordingResponse::TrackerFull(table_name) => {
                // this message might be displayed multiple times but that's fine
                // warning: stdout might allocate a buffer on first use
                error!("heap profiling stopped, {table_name} full");
                FLAGS.enabled.store(false, Ordering::Relaxed);
            }
            AllocRecordingResponse::ThresholdNotExceeded => {}
            AllocRecordingResponse::NotStarted => {}
        }
    }
}

/// Warning: allocating inside this function can cause an error (abort, panic or even deadlock).
#[cold]
fn track_dealloc_call(ptr: *mut u8, layout: Layout) {
    if layout.size() >= FLAGS.min_alloc_bytes_for_profiling.load(Ordering::Relaxed) as usize {
        alloc_tracker::record_deallocation(ptr);
    }
}

#[cold]
fn track_realloc_call(
    _old_ptr: *mut u8,
    _new_pointer: *mut u8,
    _current_layout: Layout,
    _new_size: usize,
) {
    // TODO handle realloc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_size_of_flags() {
        assert_eq!(std::mem::size_of::<Flags>(), 128);
    }
}
