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

use bytesize::ByteSize;

#[derive(Debug)]
struct Allocation {
    pub callsite_hash: u64,
    pub size: ByteSize,
}

#[derive(Debug, Copy, Clone)]
pub struct AllocStat {
    pub count: u64,
    pub size: ByteSize,
    pub last_report: ByteSize,
}

#[derive(Debug)]
enum TrackerStatus {
    Started { reporting_interval: ByteSize },
    Stopped,
}

/// WARN:
/// - keys and values in these maps should not allocate!
/// - we assume HashMaps don't allocate if their capacity is not exceeded
#[derive(Debug)]
pub struct Allocations {
    memory_locations: HashMap<usize, Allocation>,
    max_tracked_memory_locations: usize,
    callsite_statistics: HashMap<u64, AllocStat>,
    max_tracked_callsites: usize,
    status: TrackerStatus,
}

impl Default for Allocations {
    fn default() -> Self {
        let max_tracked_memory_locations = 128 * 1024;
        let max_tracked_callsites = 32 * 1024;
        // TODO: We use a load factor of 0.5 to avoid resizing. There is no
        // strict guarantee with std::collections::HashMap that it's enough, but
        // it seems to be the case in practice (see test_tracker_full).
        Self {
            memory_locations: HashMap::with_capacity(2 * max_tracked_memory_locations),
            max_tracked_memory_locations,
            callsite_statistics: HashMap::with_capacity(2 * max_tracked_callsites),
            max_tracked_callsites,
            status: TrackerStatus::Stopped,
        }
    }
}

pub enum AllocRecordingResponse {
    ThresholdExceeded(AllocStat),
    ThresholdNotExceeded,
    TrackerFull(&'static str),
    NotStarted,
}

pub enum ReallocRecordingResponse {
    ThresholdExceeded {
        statistics: AllocStat,
        callsite_hash: u64,
    },
    ThresholdNotExceeded,
    NotStarted,
}

impl Allocations {
    pub fn init(&mut self, reporting_interval_bytes: u64) {
        self.memory_locations.clear();
        self.callsite_statistics.clear();
        self.status = TrackerStatus::Started {
            reporting_interval: ByteSize(reporting_interval_bytes),
        }
    }

    /// Records an allocation and occasionally reports the cumulated allocation
    /// size for the provided callsite_hash.
    ///
    /// Every time the total allocated size for a given callsite_hash exceeds
    /// the previous reported value by at least reporting_interval, the new total
    /// allocated size is reported.
    ///
    /// WARN: this function should not allocate!
    pub fn record_allocation(
        &mut self,
        callsite_hash: u64,
        size_bytes: u64,
        ptr: *mut u8,
    ) -> AllocRecordingResponse {
        let TrackerStatus::Started { reporting_interval } = self.status else {
            return AllocRecordingResponse::NotStarted;
        };
        if self.max_tracked_memory_locations == self.memory_locations.len() {
            return AllocRecordingResponse::TrackerFull("memory_locations");
        }
        if self.max_tracked_callsites == self.callsite_statistics.len() {
            return AllocRecordingResponse::TrackerFull("tracked_callsites");
        }
        self.memory_locations.insert(
            ptr as usize,
            Allocation {
                callsite_hash,
                size: ByteSize(size_bytes),
            },
        );
        let entry = self
            .callsite_statistics
            .entry(callsite_hash)
            .and_modify(|stat| {
                stat.count += 1;
                stat.size += size_bytes;
            })
            .or_insert(AllocStat {
                count: 1,
                size: ByteSize(size_bytes),
                last_report: ByteSize(0),
            });
        let new_threshold_exceeded = entry.size >= (entry.last_report + reporting_interval);
        if new_threshold_exceeded {
            let reported_statistic = *entry;
            entry.last_report = entry.size;
            AllocRecordingResponse::ThresholdExceeded(reported_statistic)
        } else {
            AllocRecordingResponse::ThresholdNotExceeded
        }
    }

    /// Updates the memory location and size of an existing allocation. Only
    /// update the statistics if the original allocation was recorded.
    ///
    /// WARN: this function should not allocate!
    pub fn record_reallocation(
        &mut self,
        new_size_bytes: u64,
        old_ptr: *mut u8,
        new_ptr: *mut u8,
    ) -> ReallocRecordingResponse {
        let TrackerStatus::Started { reporting_interval } = self.status else {
            return ReallocRecordingResponse::NotStarted;
        };
        let (callsite_hash, old_size_bytes) = if old_ptr != new_ptr {
            let Some(old_alloc) = self.memory_locations.remove(&(old_ptr as usize)) else {
                return ReallocRecordingResponse::ThresholdNotExceeded;
            };
            self.memory_locations.insert(
                new_ptr as usize,
                Allocation {
                    callsite_hash: old_alloc.callsite_hash,
                    size: ByteSize(new_size_bytes),
                },
            );
            (old_alloc.callsite_hash, old_alloc.size.0)
        } else {
            let Some(alloc) = self.memory_locations.get_mut(&(old_ptr as usize)) else {
                return ReallocRecordingResponse::ThresholdNotExceeded;
            };
            let old_size_bytes = alloc.size.0;
            alloc.size = ByteSize(new_size_bytes);
            (alloc.callsite_hash, old_size_bytes)
        };

        let delta = new_size_bytes as i64 - old_size_bytes as i64;

        let Some(current_stat) = self.callsite_statistics.get_mut(&callsite_hash) else {
            // tables are inconsistent, this should not happen
            return ReallocRecordingResponse::ThresholdNotExceeded;
        };
        current_stat.size = ByteSize((current_stat.size.0 as i64 + delta) as u64);
        let new_threshold_exceeded =
            current_stat.size >= (current_stat.last_report + reporting_interval);
        if new_threshold_exceeded {
            let reported_statistic = *current_stat;
            current_stat.last_report = current_stat.size;
            ReallocRecordingResponse::ThresholdExceeded {
                statistics: reported_statistic,
                callsite_hash,
            }
        } else {
            ReallocRecordingResponse::ThresholdNotExceeded
        }
    }

    /// WARN: this function should not allocate!
    pub fn record_deallocation(&mut self, ptr: *mut u8) {
        if let TrackerStatus::Stopped = self.status {
            return;
        }
        let Some(Allocation {
            size,
            callsite_hash,
            ..
        }) = self.memory_locations.remove(&(ptr as usize))
        else {
            // this was allocated before the tracking started
            return;
        };
        if let Entry::Occupied(mut content) = self.callsite_statistics.entry(callsite_hash) {
            let new_size_bytes = content.get().size.0.saturating_sub(size.0);
            let new_count = content.get().count.saturating_sub(1);
            content.get_mut().count = new_count;
            content.get_mut().size = ByteSize(new_size_bytes);
            if content.get().count == 0 {
                content.remove();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn as_ptr(i: usize) -> *mut u8 {
        i as *mut u8
    }

    #[test]
    fn test_record_allocation_and_deallocation() {
        let mut allocations = Allocations::default();
        allocations.init(2000);
        let callsite_hash_1 = 777;

        let ptr_1 = as_ptr(1);
        let response = allocations.record_allocation(callsite_hash_1, 1500, ptr_1);
        assert!(matches!(
            response,
            AllocRecordingResponse::ThresholdNotExceeded
        ));

        let ptr_2 = as_ptr(2);
        let response = allocations.record_allocation(callsite_hash_1, 1500, ptr_2);
        let AllocRecordingResponse::ThresholdExceeded(statistic) = response else {
            panic!("Expected ThresholdExceeded response");
        };
        assert_eq!(statistic.count, 2);
        assert_eq!(statistic.size, ByteSize(3000));
        assert_eq!(statistic.last_report, ByteSize(0));

        allocations.record_deallocation(ptr_2);

        // the threshold was already crossed
        let ptr_3 = as_ptr(3);
        let response = allocations.record_allocation(callsite_hash_1, 1500, ptr_3);
        assert!(matches!(
            response,
            AllocRecordingResponse::ThresholdNotExceeded
        ));

        // this is a brand new call site with different statistics
        let callsite_hash_2 = 42;
        let ptr_4 = as_ptr(4);
        let response = allocations.record_allocation(callsite_hash_2, 1500, ptr_4);
        assert!(matches!(
            response,
            AllocRecordingResponse::ThresholdNotExceeded
        ));
    }

    #[test]
    fn test_record_allocation_and_reallocation() {
        let mut allocations = Allocations::default();
        allocations.init(2000);
        let callsite_hash_1 = 777;

        let ptr_1 = as_ptr(1);
        let response = allocations.record_allocation(callsite_hash_1, 1500, ptr_1);
        assert!(matches!(
            response,
            AllocRecordingResponse::ThresholdNotExceeded
        ));

        let ptr_2 = as_ptr(2);
        let response = allocations.record_allocation(callsite_hash_1, 1500, ptr_2);
        let AllocRecordingResponse::ThresholdExceeded(statistic) = response else {
            panic!("Expected ThresholdExceeded response");
        };
        assert_eq!(statistic.count, 2);
        assert_eq!(statistic.size, ByteSize(3000));
        assert_eq!(statistic.last_report, ByteSize(0));

        // alloc grows a little bit
        let response = allocations.record_reallocation(2000, ptr_1, ptr_1);
        assert!(matches!(
            response,
            ReallocRecordingResponse::ThresholdNotExceeded
        ));

        // alloc grows a lot
        let response = allocations.record_reallocation(4000, ptr_1, ptr_1);
        let ReallocRecordingResponse::ThresholdExceeded {
            statistics,
            callsite_hash,
        } = response
        else {
            panic!("Expected ThresholdExceeded response");
        };
        assert_eq!(statistics.count, 2);
        assert_eq!(statistics.size, ByteSize(5500));
        assert_eq!(statistics.last_report, ByteSize(3000));
        assert_eq!(callsite_hash, callsite_hash_1);

        // alloc grows a little bit and moves
        let ptr_3 = as_ptr(3);
        let response = allocations.record_reallocation(4500, ptr_1, ptr_3);
        assert!(matches!(
            response,
            ReallocRecordingResponse::ThresholdNotExceeded
        ));

        // alloc grows a lot and moves
        let ptr_4 = as_ptr(4);
        let response = allocations.record_reallocation(6000, ptr_3, ptr_4);
        let ReallocRecordingResponse::ThresholdExceeded {
            statistics,
            callsite_hash,
        } = response
        else {
            panic!("Expected ThresholdExceeded response");
        };
        assert_eq!(statistics.count, 2);
        assert_eq!(statistics.size, ByteSize(7500));
        assert_eq!(statistics.last_report, ByteSize(5500));
        assert_eq!(callsite_hash, callsite_hash_1);

        // once an existing allocation moved, it's previous location can be re-allocated
        let response = allocations.record_allocation(callsite_hash_1, 2000, ptr_1);
        let AllocRecordingResponse::ThresholdExceeded(statistics) = response else {
            panic!("Expected ThresholdExceeded response");
        };
        assert_eq!(statistics.count, 3);
        assert_eq!(statistics.size, ByteSize(9500));
        assert_eq!(statistics.last_report, ByteSize(7500));
        assert_eq!(callsite_hash, callsite_hash_1);

        // reallocation is ignored on unknown allocation
        let ptr_404 = as_ptr(404);
        let response = allocations.record_reallocation(10000, ptr_404, ptr_404);
        assert!(matches!(
            response,
            ReallocRecordingResponse::ThresholdNotExceeded
        ));
    }

    #[test]
    fn test_tracker_full() {
        let mut allocations = Allocations::default();
        allocations.init(1024 * 1024 * 1024);
        let max_tracked_locations = allocations.max_tracked_memory_locations;

        // Track a first allocation. This one is not removed thoughout this test.
        let first_location_ptr = as_ptr(1);
        let response = allocations.record_allocation(777, 10, first_location_ptr);
        assert!(matches!(
            response,
            AllocRecordingResponse::ThresholdNotExceeded
        ));
        let ref_addr = allocations
            .memory_locations
            .get(&(first_location_ptr as usize))
            .unwrap() as *const Allocation;
        // Assert that no hashmap resize occurs by tracking the address
        // stability of the first value. Using HashMap::capacity() proved not to
        // be reliable (unclear spec).
        let assert_locations_map_didnt_move = |allocations: &Allocations, loc: &str| {
            assert_eq!(
                allocations
                    .memory_locations
                    .get(&(first_location_ptr as usize))
                    .unwrap() as *const Allocation,
                ref_addr,
                "{loc}",
            );
        };

        // fill the table
        let moving_ptr_range = (first_location_ptr as usize + 1)
            ..(first_location_ptr as usize + max_tracked_locations);
        for i in moving_ptr_range.clone() {
            let ptr = as_ptr(i);
            let response = allocations.record_allocation(777, 10, ptr);
            assert!(matches!(
                response,
                AllocRecordingResponse::ThresholdNotExceeded
            ));
            assert_locations_map_didnt_move(&allocations, "fill");
        }
        assert_eq!(allocations.memory_locations.len(), max_tracked_locations);

        // the table is full, no more allocation is tracked
        let response = allocations.record_allocation(777, 10, as_ptr(moving_ptr_range.end));
        assert!(matches!(
            response,
            AllocRecordingResponse::TrackerFull("memory_locations")
        ));
        assert_locations_map_didnt_move(&allocations, "full");

        // run a heavy insert/remove workload
        let last_location = 10 * max_tracked_locations;
        for i in moving_ptr_range.end..=last_location {
            let removed_ptr = as_ptr(i - 1);
            allocations.record_deallocation(removed_ptr);
            let inserted_ptr = as_ptr(i);
            let response = allocations.record_allocation(888, 10, inserted_ptr);
            assert!(matches!(
                response,
                AllocRecordingResponse::ThresholdNotExceeded
            ));
            assert_locations_map_didnt_move(&allocations, "reinsert");
        }

        // reallocations are fine because they don't create an entry in the map
        let response =
            allocations.record_reallocation(10, as_ptr(last_location), as_ptr(last_location + 1));
        assert!(matches!(
            response,
            ReallocRecordingResponse::ThresholdNotExceeded,
        ));
        assert_locations_map_didnt_move(&allocations, "realloc");
    }
}
