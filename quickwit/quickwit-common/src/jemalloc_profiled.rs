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
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use tikv_jemallocator::Jemalloc;
use tracing::{error, info};

use crate::alloc_tracker::{self, AllocRecordingResponse};

const DEFAULT_MIN_ALLOC_SIZE_FOR_PROFILING: usize = 256 * 1024;

// Atomics are used to communicate configurations between the start/stop
// endpoints and the JemallocProfiled allocator wrapper.

/// The minimum allocation size that is recorded by the tracker.
static MIN_ALLOC_SIZE_FOR_PROFILING: AtomicUsize =
    AtomicUsize::new(DEFAULT_MIN_ALLOC_SIZE_FOR_PROFILING);

/// Whether the profiling is started or not.
static ENABLED: AtomicBool = AtomicBool::new(false);

/// Starts measuring heap allocations and logs important leaks.
///
/// This function uses a wrapper around the global Jemalloc allocator to
/// instrument it. Each time an allocation bigger than
/// min_alloc_size_for_profiling is performed, it is recorded in a map and the
/// statistics for its call site are updated.
///
/// During profiling, the statistics per call site are used to log when specific
/// thresholds are exceeded. For each call site, the allocated memory is
/// logged (with a backtrace) every time it exceeds the last logged allocated
/// memory by at least alloc_size_triggering_backtrace.
pub fn start_profiling(
    min_alloc_size_for_profiling: Option<usize>,
    alloc_size_triggering_backtrace: Option<u64>,
) {
    // Call backtrace once to warmup symbolization allocations (~30MB)
    backtrace::trace(|frame| {
        backtrace::resolve_frame(frame, |_| {});
        true
    });

    alloc_tracker::init(alloc_size_triggering_backtrace);

    let min_alloc_size_for_profiling =
        min_alloc_size_for_profiling.unwrap_or(DEFAULT_MIN_ALLOC_SIZE_FOR_PROFILING);
    // Use strong ordering to make sure all threads see these changes in this order
    MIN_ALLOC_SIZE_FOR_PROFILING.store(min_alloc_size_for_profiling, Ordering::SeqCst);
    let previously_enabled = ENABLED.swap(true, Ordering::SeqCst);

    info!(
        min_alloc_size_for_profiling,
        alloc_size_triggering_backtrace, previously_enabled, "heap profiling running"
    );
}

/// Stops measuring heap allocations.
///
/// The allocation tracking tables and the symbol cache are not cleared.
pub fn stop_profiling() {
    // Use strong ordering to make sure all threads see these changes in this order
    let previously_enabled = ENABLED.swap(false, Ordering::SeqCst);
    MIN_ALLOC_SIZE_FOR_PROFILING.store(DEFAULT_MIN_ALLOC_SIZE_FOR_PROFILING, Ordering::SeqCst);

    info!(previously_enabled, "heap profiling stopped");
    // alloc_tracker::log_dump();
    // backtrace::clear_symbol_cache();
}

/// Wraps the Jemalloc global allocator calls with tracking routines.
///
/// The tracking routines are called only when [ENABLED] is set to true (calling
/// [start_profiling()]), but we don't enforce any synchronization (we load it with
/// Ordering::Relaxed) because it's fine to miss or record extra allocation events.
pub struct JemallocProfiled(pub Jemalloc);

unsafe impl GlobalAlloc for JemallocProfiled {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { self.0.alloc(layout) };
        if ENABLED.load(Ordering::Relaxed) {
            track_alloc_call(ptr, layout);
        }
        ptr
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { self.0.alloc_zeroed(layout) };
        if ENABLED.load(Ordering::Relaxed) {
            track_alloc_call(ptr, layout);
        }
        ptr
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if ENABLED.load(Ordering::Relaxed) {
            track_dealloc_call(ptr, layout);
        }
        unsafe { self.0.dealloc(ptr, layout) }
    }

    #[inline]
    unsafe fn realloc(&self, old_ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { self.0.realloc(old_ptr, layout, new_size) };
        if ENABLED.load(Ordering::Relaxed) {
            track_realloc_call(old_ptr, new_ptr, layout, new_size);
        }
        new_ptr
    }
}

#[inline]
fn print_backtrace(callsite_hash: u64, stat: alloc_tracker::Statistic) {
    {
        let mut lock = std::io::stdout().lock();
        let _ = writeln!(
            &mut lock,
            "htrk callsite={} allocs={} size={}MiB",
            callsite_hash,
            stat.count,
            stat.size / 1024 / 1024
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

#[cold]
fn track_alloc_call(ptr: *mut u8, layout: Layout) {
    if layout.size() > MIN_ALLOC_SIZE_FOR_PROFILING.load(Ordering::Relaxed) {
        let callsite_hash = backtrace_hash();
        let recording_response =
            alloc_tracker::record_allocation(callsite_hash, layout.size() as u64, ptr);

        match recording_response {
            AllocRecordingResponse::ThresholdExceeded(stat_for_trace) => {
                print_backtrace(callsite_hash, stat_for_trace);
                // Could we use tracing to caracterize the call site here?
                // tracing::info!(size = alloc_size_for_trace, "large alloc");
            }
            AllocRecordingResponse::TrackerFull(reason) => {
                // this message might be displayed multiple times but that's fine
                error!("{reason} full, profiling stopped");
                ENABLED.store(false, Ordering::Relaxed);
            }
            AllocRecordingResponse::ThresholdNotExceeded => {}
            AllocRecordingResponse::NotStarted => {}
        }
    }
}

#[cold]
fn track_dealloc_call(ptr: *mut u8, layout: Layout) {
    if layout.size() > MIN_ALLOC_SIZE_FOR_PROFILING.load(Ordering::Relaxed) {
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
