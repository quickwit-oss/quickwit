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

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Mutex;

use once_cell::sync::Lazy;

const DEFAULT_REPORTING_INTERVAL: u64 = 1024 * 1024 * 1024;

static ALLOCATION_TRACKER: Lazy<Mutex<Allocations>> =
    Lazy::new(|| Mutex::new(Allocations::default()));

#[derive(Debug)]
struct Allocation {
    pub callsite_hash: u64,
    pub size: u64,
}

#[derive(Debug, Copy, Clone)]
pub struct Statistic {
    pub count: u64,
    pub size: u64,
    pub last_print: u64,
}

/// WARN:
/// - keys and values in these maps should not allocate!
/// - we assume HashMaps don't allocate if their capacity is not exceeded
#[derive(Debug)]
struct Allocations {
    memory_locations: HashMap<usize, Allocation>,
    callsite_statistics: HashMap<u64, Statistic>,
    is_started: bool,
    reporting_interval: u64,
}

impl Default for Allocations {
    fn default() -> Self {
        Self {
            memory_locations: HashMap::with_capacity(128 * 1024),
            callsite_statistics: HashMap::with_capacity(32 * 1024),
            is_started: false,
            reporting_interval: DEFAULT_REPORTING_INTERVAL,
        }
    }
}

// pub fn log_dump() {
//     tracing::info!(allocations=?ALLOCATION_TRACKER.lock().unwrap(), "dump");
// }

pub fn init(alloc_size_triggering_backtrace: Option<u64>) {
    let mut guard = ALLOCATION_TRACKER.lock().unwrap();
    guard.memory_locations.clear();
    guard.callsite_statistics.clear();
    guard.is_started = true;
    guard.reporting_interval =
        alloc_size_triggering_backtrace.unwrap_or(DEFAULT_REPORTING_INTERVAL);
}

pub enum AllocRecordingResponse {
    ThresholdExceeded(Statistic),
    ThresholdNotExceeded,
    TrackerFull(&'static str),
    NotStarted,
}

/// Records an allocation and occasionally reports the cumulated allocation size
/// for the provided callsite_hash.
///
/// Every time a the total allocated size with the same callsite_hash
/// exceeds the previous reported value by at least reporting_interval, that
/// allocated size is reported.
///
/// WARN: this function should not allocate!
pub fn record_allocation(callsite_hash: u64, size: u64, ptr: *mut u8) -> AllocRecordingResponse {
    let mut guard = ALLOCATION_TRACKER.lock().unwrap();
    if !guard.is_started {
        return AllocRecordingResponse::NotStarted;
    }
    if guard.memory_locations.capacity() == guard.memory_locations.len() {
        return AllocRecordingResponse::TrackerFull("memory_locations");
    }
    if guard.callsite_statistics.capacity() == guard.callsite_statistics.len() {
        return AllocRecordingResponse::TrackerFull("memory_locations");
    }
    guard.memory_locations.insert(
        ptr as usize,
        Allocation {
            callsite_hash,
            size,
        },
    );
    let reporting_interval = guard.reporting_interval;
    let entry = guard
        .callsite_statistics
        .entry(callsite_hash)
        .and_modify(|stat| {
            stat.count += 1;
            stat.size += size;
        })
        .or_insert(Statistic {
            count: 1,
            size,
            last_print: 0,
        });
    let new_threshold_exceeded = entry.size > (entry.last_print + reporting_interval);
    if new_threshold_exceeded {
        let reported_statistic = *entry;
        entry.last_print = entry.size;
        AllocRecordingResponse::ThresholdExceeded(reported_statistic)
    } else {
        AllocRecordingResponse::ThresholdNotExceeded
    }
}

/// WARN: this function should not allocate!
pub fn record_deallocation(ptr: *mut u8) {
    let mut guard = ALLOCATION_TRACKER.lock().unwrap();
    if !guard.is_started {
        return;
    }
    let Some(Allocation {
        size,
        callsite_hash,
        ..
    }) = guard.memory_locations.remove(&(ptr as usize))
    else {
        // this was allocated before the tracking started
        return;
    };
    if let Entry::Occupied(mut content) = guard.callsite_statistics.entry(callsite_hash) {
        content.get_mut().count -= 1;
        content.get_mut().size -= size;
        if content.get().count == 0 {
            content.remove();
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    #[serial_test::file_serial]
    fn test_record_allocation_and_deallocation() {
        init(Some(2000));
        let callsite_hash_1 = 777;

        let ptr_1 = 0x1 as *mut u8;
        let response = record_allocation(callsite_hash_1, 1500, ptr_1);
        assert!(matches!(
            response,
            AllocRecordingResponse::ThresholdNotExceeded
        ));

        let ptr_2 = 0x2 as *mut u8;
        let response = record_allocation(callsite_hash_1, 1500, ptr_2);
        let AllocRecordingResponse::ThresholdExceeded(statistic) = response else {
            panic!("Expected ThresholdExceeded response");
        };
        assert_eq!(statistic.count, 2);
        assert_eq!(statistic.size, 3000);
        assert_eq!(statistic.last_print, 0);

        record_deallocation(ptr_2);

        // the threshold was already crossed
        let ptr_3 = 0x3 as *mut u8;
        let response = record_allocation(callsite_hash_1, 1500, ptr_3);
        assert!(matches!(
            response,
            AllocRecordingResponse::ThresholdNotExceeded
        ));

        // this is a brand new call site with different statistics
        let callsite_hash_2 = 42;
        let ptr_3 = 0x3 as *mut u8;
        let response = record_allocation(callsite_hash_2, 1500, ptr_3);
        assert!(matches!(
            response,
            AllocRecordingResponse::ThresholdNotExceeded
        ));
    }

    #[test]
    #[serial_test::file_serial]
    fn test_tracker_full() {
        init(Some(1024 * 1024 * 1024));
        let memory_locations_capacity = ALLOCATION_TRACKER
            .lock()
            .unwrap()
            .memory_locations
            .capacity();

        for i in 0..memory_locations_capacity {
            let ptr = (i + 1) as *mut u8;
            let response = record_allocation(777, 10, ptr);
            assert!(matches!(
                response,
                AllocRecordingResponse::ThresholdNotExceeded
            ));
        }
        let response = record_allocation(777, 10, (memory_locations_capacity + 1) as *mut u8);
        assert!(matches!(
            response,
            AllocRecordingResponse::TrackerFull("memory_locations")
        ));
        // make sure that the map didn't grow
        let current_memory_locations_capacity = ALLOCATION_TRACKER
            .lock()
            .unwrap()
            .memory_locations
            .capacity();
        assert_eq!(current_memory_locations_capacity, memory_locations_capacity);
    }
}
