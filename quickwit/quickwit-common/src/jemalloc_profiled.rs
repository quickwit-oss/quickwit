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
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};

use serde::Serialize;
use tikv_jemallocator::Jemalloc;
use tracing::info;

pub const DEFAULT_MIN_ALLOC_SIZE_FOR_BACKTRACE: usize = 256 * 1024;

// commands
static MIN_ALLOC_SIZE_FOR_BACKTRACE: AtomicUsize =
    AtomicUsize::new(DEFAULT_MIN_ALLOC_SIZE_FOR_BACKTRACE);
static ENABLED: AtomicBool = AtomicBool::new(false);

// stats, actual allocations might be larger for alignment reasons
static ALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);
static ALLOC_ZEROED_COUNT: AtomicUsize = AtomicUsize::new(0);
static ALLOC_SIZE: AtomicUsize = AtomicUsize::new(0);
/// total size of allocations large enough to be printed
static ALLOC_PRINTED_SIZE: AtomicUsize = AtomicUsize::new(0);
static DEALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);
static DEALLOC_SIZE: AtomicUsize = AtomicUsize::new(0);
static REALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);
static REALLOC_GROW_SIZE: AtomicUsize = AtomicUsize::new(0);
static REALLOC_SHRINK_SIZE: AtomicUsize = AtomicUsize::new(0);
/// total size of reallocations large enough to be printed
static REALLOC_PRINTED_SIZE: AtomicI64 = AtomicI64::new(0);

#[derive(Debug, Serialize)]
pub struct JemallocProfile {
    pub alloc_count: usize,
    pub alloc_size: usize,
    pub alloc_printed_size: usize,
    pub alloc_zeroed_count: usize,
    pub dealloc_count: usize,
    pub dealloc_size: usize,
    pub realloc_count: usize,
    pub realloc_shrink_size: usize,
    pub realloc_grow_size: usize,
    pub realloc_printed_size: i64,
}

#[derive(Debug, thiserror::Error)]
#[error("profiling unavailable")]
pub struct Unavailable;

pub fn start_profiling(min_alloc_size_for_backtrace: usize) -> Result<(), Unavailable> {
    warmup_symbol_cache();
    configure_min_alloc_size_for_backtrace(min_alloc_size_for_backtrace);
    info!(min_alloc_size_for_backtrace, "start heap profiling");
    let profiling_previously_enabled = ENABLED.swap(true, Ordering::Acquire);
    if profiling_previously_enabled {
        info!("heap profiling already running");
        return Err(Unavailable);
    }
    ALLOC_COUNT.store(0, Ordering::SeqCst);
    ALLOC_SIZE.store(0, Ordering::SeqCst);
    ALLOC_ZEROED_COUNT.store(0, Ordering::SeqCst);
    DEALLOC_COUNT.store(0, Ordering::SeqCst);
    DEALLOC_SIZE.store(0, Ordering::SeqCst);
    REALLOC_COUNT.store(0, Ordering::SeqCst);
    REALLOC_GROW_SIZE.store(0, Ordering::SeqCst);
    REALLOC_SHRINK_SIZE.store(0, Ordering::SeqCst);
    Ok(())
}

pub fn stop_profiling() -> Result<JemallocProfile, Unavailable> {
    let profiling_previously_enabled = ENABLED.swap(false, Ordering::Release);
    if !profiling_previously_enabled {
        return Err(Unavailable);
    }
    info!("end heap profiling");
    backtrace::clear_symbol_cache();
    Ok(JemallocProfile {
        alloc_count: ALLOC_COUNT.load(Ordering::SeqCst),
        alloc_size: ALLOC_SIZE.load(Ordering::SeqCst),
        alloc_printed_size: ALLOC_PRINTED_SIZE.load(Ordering::SeqCst),
        alloc_zeroed_count: ALLOC_ZEROED_COUNT.load(Ordering::SeqCst),
        dealloc_count: DEALLOC_COUNT.load(Ordering::SeqCst),
        dealloc_size: DEALLOC_SIZE.load(Ordering::SeqCst),
        realloc_count: REALLOC_COUNT.load(Ordering::SeqCst),
        realloc_grow_size: REALLOC_GROW_SIZE.load(Ordering::SeqCst),
        realloc_shrink_size: REALLOC_SHRINK_SIZE.load(Ordering::SeqCst),
        realloc_printed_size: REALLOC_PRINTED_SIZE.load(Ordering::SeqCst),
    })
}

fn configure_min_alloc_size_for_backtrace(min_alloc_size_for_backtrace: usize) {
    MIN_ALLOC_SIZE_FOR_BACKTRACE.store(min_alloc_size_for_backtrace, Ordering::Relaxed);
}

/// Calls backtrace once to warmup symbolization allocations (~30MB)
fn warmup_symbol_cache() {
    backtrace::trace(|frame| {
        backtrace::resolve_frame(frame, |_| {});
        true
    });
}

/// Wraps Jemalloc global allocator calls with tracking routines.
///
/// The tracking routines are called only when [JEMALLOC_PROFILED_ENABLED]
/// is set to true, but we don't enforce any synchronization (we load it
/// with Ordering::Relaxed) because it's fine to miss or record extra
/// allocation events.
pub struct JemallocProfiled(pub Jemalloc);

unsafe impl GlobalAlloc for JemallocProfiled {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if ENABLED.load(Ordering::Relaxed) {
            track_alloc_call(layout);
        }
        self.0.alloc(layout)
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if ENABLED.load(Ordering::Relaxed) {
            track_alloc_zeroed_call(layout);
        }
        self.0.alloc_zeroed(layout)
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if ENABLED.load(Ordering::Relaxed) {
            track_dealloc_call(layout);
        }
        self.0.dealloc(ptr, layout)
    }

    #[inline]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if ENABLED.load(Ordering::Relaxed) {
            track_realloc_call(layout, new_size);
        }
        self.0.realloc(ptr, layout, new_size)
    }
}

#[cold]
fn track_alloc_call(layout: Layout) {
    if layout.size() > MIN_ALLOC_SIZE_FOR_BACKTRACE.load(Ordering::Relaxed) {
        ALLOC_PRINTED_SIZE.fetch_add(layout.size(), Ordering::Relaxed);
        dump_trace("alloc", layout.size());
    }
    ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
    ALLOC_SIZE.fetch_add(layout.size(), Ordering::Relaxed);
}

#[cold]
fn track_alloc_zeroed_call(layout: Layout) {
    ALLOC_ZEROED_COUNT.fetch_add(1, Ordering::Relaxed);
    ALLOC_SIZE.fetch_add(layout.size(), Ordering::Relaxed);
}

#[cold]
fn track_dealloc_call(layout: Layout) {
    DEALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
    DEALLOC_SIZE.fetch_add(layout.size(), Ordering::Relaxed);
}

#[cold]
fn track_realloc_call(current_layout: Layout, new_size: usize) {
    let alloc_delta = new_size as i64 - current_layout.size() as i64;
    if alloc_delta > MIN_ALLOC_SIZE_FOR_BACKTRACE.load(Ordering::Relaxed) as i64 {
        REALLOC_PRINTED_SIZE.fetch_add(alloc_delta, Ordering::Relaxed);
        dump_trace("realloc", alloc_delta as usize);
    }
    REALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
    if alloc_delta > 0 {
        REALLOC_GROW_SIZE.fetch_add(alloc_delta as usize, Ordering::Relaxed);
    } else {
        REALLOC_SHRINK_SIZE.fetch_add(-alloc_delta as usize, Ordering::Relaxed);
    }
}

/// Calling this function when allocating can trigger a recursive call (and
/// stack overflow) if [backtrace] is first called after the tracking starts.
/// Indeed, [backtrace] grows a pretty big allocation to cache all the symbols
/// (somewhere in backtrace-rs/symbolize/gimli/macho.rs).
fn dump_trace(kind: &'static str, alloc_size: usize) {
    let mut profiling_frames_skipped = false;
    let mut remaining_frames_to_inspect = 100;
    let mut logged = false;
    backtrace::trace(|frame| {
        if remaining_frames_to_inspect == 0 {
            println!("{},{},<max_frames_reached>", kind, alloc_size);
            logged = true;
            return false;
        } else {
            remaining_frames_to_inspect = -1;
        }

        if !profiling_frames_skipped {
            backtrace::resolve_frame(frame, |symbol| {
                if let Some(symbol_name) = symbol.name() {
                    if prefix_helper::is_prefix("quickwit_common::jemalloc_profiled", &symbol_name)
                    {
                        profiling_frames_skipped = true;
                    }
                }
            });
            return true;
        }
        let mut keep_going = true;
        backtrace::resolve_frame(frame, |symbol| {
            if let Some(symbol_name) = symbol.name() {
                for prefix in ["quickwit", "chitchat", "tantivy"].iter() {
                    if prefix_helper::is_prefix(prefix, &symbol_name) {
                        println!("{},{},{}", kind, alloc_size, symbol_name);
                        logged = true;
                        keep_going = false;
                        return;
                    }
                }
            }
        });
        keep_going
    });
    if !logged {
        println!("{},<no-prefix-matched>", kind);
    }
}

/// Helper that enables checking prefix matches on types implementing Display
/// (e.g SymbolName) without any extra allocation.
///
/// The matcher also discards the first character of the string if it is a '<'.
mod prefix_helper {
    use std::fmt::{Display, Write};

    pub fn is_prefix(prefix: &'static str, value: impl Display) -> bool {
        let mut comp_write = PrefixWriteMatcher::new(prefix);
        write!(&mut comp_write, "{}", value).unwrap();
        comp_write.matched()
    }

    struct PrefixWriteMatcher {
        prefix: &'static str,
        chars_matched: usize,
        failed: bool,
    }

    impl PrefixWriteMatcher {
        fn new(prefix: &'static str) -> Self {
            Self {
                prefix,
                chars_matched: 0,
                failed: false,
            }
        }

        fn matched(&self) -> bool {
            self.chars_matched == self.prefix.len()
        }
    }

    impl Write for PrefixWriteMatcher {
        fn write_str(&mut self, mut new_slice: &str) -> std::fmt::Result {
            if self.matched() || self.failed {
                return Ok(());
            }
            if new_slice.starts_with('<') && self.chars_matched == 0 {
                new_slice = &new_slice[1..];
            }
            let max_idx = (self.chars_matched + new_slice.len()).min(self.prefix.len());
            let matched_prefix_slice = &self.prefix[self.chars_matched..max_idx];
            if matched_prefix_slice != &new_slice[..matched_prefix_slice.len()] {
                self.failed = true;
            } else {
                self.chars_matched += matched_prefix_slice.len();
            }
            Ok(())
        }
    }

    #[cfg(test)]
    mod tests {
        use itertools::Itertools;

        use super::*;

        #[test]
        fn test_comparison_write() {
            assert!(!is_prefix("hello", ""));
            assert!(is_prefix("hello", "hello"));
            assert!(is_prefix("hello", "hello world"));
            assert!(!is_prefix("hello", "hell"));
            assert!(!is_prefix("hello", ["h", "e"].iter().format("")));
            assert!(is_prefix(
                "hello",
                ["h", "e", "l", "l", "o"].iter().format("")
            ));
            assert!(is_prefix(
                "hello",
                ["h", "e", "l", "l", "o", "!"].iter().format("")
            ));
            assert!(!is_prefix("hello", ["h", "i"].iter().format("")));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_profiling() {
        start_profiling(1024).unwrap();
        start_profiling(2048).unwrap_err();
        stop_profiling().unwrap();
        stop_profiling().unwrap_err();
    }
}
