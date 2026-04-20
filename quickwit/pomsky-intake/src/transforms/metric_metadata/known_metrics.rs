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

use rand::Rng;

/// Returns the current time as Unix seconds since the epoch.
fn now_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is before Unix epoch")
        .as_secs()
}

/// Tracks known metric names with per-entry TTL expiry timestamps.
///
/// Entries are inserted with a randomized TTL drawn uniformly from
/// [ttl_min_hours, ttl_max_hours]. Expired entries are only removed by
/// explicit calls to `prune_expired()` (eager pruning per D-08). Between
/// prune ticks, expired entries are still treated as "known" (D-09).
pub struct KnownMetrics {
    entries: HashMap<String, u64>,
    ttl_min_secs: u64,
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

    /// Checks whether a metric name is in the known set.
    ///
    /// Per D-09: expired entries are still treated as "known" between prune
    /// ticks. This method does NOT check expiry timestamps.
    pub fn contains(&self, name: &str) -> bool {
        self.entries.contains_key(name)
    }

    /// Inserts a metric with a fresh randomized TTL.
    ///
    /// The expiry timestamp is computed as `now + uniform_random(ttl_min_secs, ttl_max_secs)`.
    pub fn insert(&mut self, name: String) {
        let ttl_secs = rand::rng().random_range(self.ttl_min_secs..=self.ttl_max_secs);
        let expiry_ts = now_unix_secs() + ttl_secs;
        self.entries.insert(name, expiry_ts);
    }

    /// Removes all entries whose expiry timestamp is in the past.
    ///
    /// Called during persist tick per D-08. Between prune calls, expired
    /// entries are still visible to `contains()` per D-09.
    pub fn prune_expired(&mut self) {
        let now = now_unix_secs();
        self.entries.retain(|_name, expiry| *expiry > now);
    }

    /// Returns an iterator over (name, expiry_ts) pairs for CSV serialization.
    pub fn iter(&self) -> impl Iterator<Item = (&str, u64)> {
        self.entries.iter().map(|(k, v)| (k.as_str(), *v))
    }

    /// Returns the number of known metrics.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns true if the known-metrics set is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Replaces the internal entries map with the provided data.
    ///
    /// Used by `build()` to load CSV data at startup.
    pub fn load_entries(&mut self, entries: HashMap<String, u64>) {
        self.entries = entries;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn test_new_creates_empty_map() {
        let known = KnownMetrics::new(12, 36);
        assert_eq!(known.len(), 0);
        assert!(known.is_empty());
    }

    #[test]
    fn test_insert_and_contains() {
        let mut known = KnownMetrics::new(12, 36);
        known.insert("cpu.user".to_string());
        assert!(known.contains("cpu.user"), "inserted metric should be known");
        assert!(
            !known.contains("mem.free"),
            "non-inserted metric should not be known"
        );
        assert_eq!(known.len(), 1);
    }

    #[test]
    fn test_multiple_inserts() {
        let mut known = KnownMetrics::new(12, 36);
        known.insert("a".to_string());
        known.insert("b".to_string());
        assert_eq!(known.len(), 2);

        let collected: HashMap<&str, u64> = known.iter().collect();
        assert!(collected.contains_key("a"), "expected 'a' in iter output");
        assert!(collected.contains_key("b"), "expected 'b' in iter output");
    }

    #[test]
    fn test_ttl_randomization_bounds() {
        let min_hours = 12u64;
        let max_hours = 36u64;
        let min_secs = min_hours * 3600;
        let max_secs = max_hours * 3600;
        let mut known = KnownMetrics::new(min_hours, max_hours);

        let before = now_unix_secs();
        for idx in 0..1000 {
            known.insert(format!("metric_{idx}"));
        }
        let after = now_unix_secs();

        for (name, expiry) in known.iter() {
            // Conservative bounds: use earliest possible `now` for lower bound
            // and latest possible `now` for upper bound to account for test
            // execution time.
            let ttl_lower = expiry.saturating_sub(after);
            let ttl_upper = expiry.saturating_sub(before);
            assert!(
                ttl_lower >= min_secs,
                "metric {name}: TTL lower bound {ttl_lower} < min {min_secs}"
            );
            assert!(
                ttl_upper <= max_secs,
                "metric {name}: TTL upper bound {ttl_upper} > max {max_secs}"
            );
        }
    }

    #[test]
    fn test_prune_expired_removes_old_entries() {
        let mut known = KnownMetrics::new(12, 36);
        let mut entries = HashMap::new();
        // Far past -- should be pruned
        entries.insert("old_metric".to_string(), 1u64);
        // Far future -- should survive
        let future_expiry = now_unix_secs() + 100_000;
        entries.insert("new_metric".to_string(), future_expiry);
        known.load_entries(entries);

        assert_eq!(known.len(), 2);
        known.prune_expired();
        assert_eq!(known.len(), 1, "only the future entry should survive");
        assert!(
            known.contains("new_metric"),
            "future entry should still be known"
        );
        assert!(
            !known.contains("old_metric"),
            "expired entry should be pruned"
        );
    }

    #[test]
    fn test_expired_entry_still_known_before_prune() {
        // D-09: expired entries are treated as "known" until prune_expired() sweeps them.
        let mut known = KnownMetrics::new(12, 36);
        let mut entries = HashMap::new();
        entries.insert("stale_metric".to_string(), 1u64); // expiry far in the past
        known.load_entries(entries);

        assert!(
            known.contains("stale_metric"),
            "expired-but-unpruned entry should still be known (D-09)"
        );

        known.prune_expired();
        assert!(
            !known.contains("stale_metric"),
            "entry should be gone after prune"
        );
    }

    #[test]
    fn test_load_entries() {
        let mut known = KnownMetrics::new(12, 36);
        let mut entries = HashMap::new();
        entries.insert("metric_a".to_string(), 1_700_000_000u64);
        entries.insert("metric_b".to_string(), 1_700_100_000u64);
        entries.insert("metric_c".to_string(), 1_700_200_000u64);
        known.load_entries(entries);

        assert_eq!(known.len(), 3);
        assert!(known.contains("metric_a"));
        assert!(known.contains("metric_b"));
        assert!(known.contains("metric_c"));
    }

    #[test]
    fn test_is_empty() {
        let mut known = KnownMetrics::new(12, 36);
        assert!(known.is_empty(), "new KnownMetrics should be empty");
        known.insert("x".to_string());
        assert!(!known.is_empty(), "after insert, should not be empty");
    }
}
