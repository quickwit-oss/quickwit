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

/// Tracks known metric names with per-entry TTL expiry timestamps.
///
/// Entries are inserted with a randomized TTL drawn uniformly from
/// [ttl_min_hours, ttl_max_hours]. Expired entries are only removed by
/// explicit calls to `prune_expired()` (eager pruning per D-08). Between
/// prune ticks, expired entries are still treated as "known" (D-09).
pub struct KnownMetrics {
    entries: HashMap<String, u64>,
    #[allow(dead_code)]
    ttl_min_secs: u64,
    #[allow(dead_code)]
    ttl_max_secs: u64,
}

impl KnownMetrics {
    /// Creates an empty known-metrics set with TTL bounds in hours.
    pub fn new(ttl_min_hours: u64, ttl_max_hours: u64) -> Self {
        Self {
            entries: HashMap::new(),
            ttl_min_secs: ttl_min_hours * 3600,
            ttl_max_secs: ttl_max_hours * 3600,
        }
    }

    /// Returns the number of known metrics.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns true if the known-metrics set is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}
