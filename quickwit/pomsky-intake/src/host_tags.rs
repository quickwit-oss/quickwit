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
use std::sync::{Arc, LazyLock};

use arc_swap::ArcSwap;
use bytesize::ByteSize;

use crate::unix_timestamp::UnixTimestamp;

/// A single host-tag entry: key-value pair like `("env", "prod")`.
pub type HostTag = (String, String);

/// A host's tags, optional numeric host ID, and expiry timestamp.
#[derive(Clone)]
pub struct HostTagsEntry {
    pub tags: Arc<[HostTag]>,
    /// Numeric host ID from HMS, populated when the metadata service returns
    /// one. Used to set the `host_id` fast field on span documents.
    pub host_id: Option<i64>,
    pub expires_at: UnixTimestamp,
}

impl HostTagsEntry {
    /// Returns `true` if this entry has expired.
    pub fn is_expired(&self, now: UnixTimestamp) -> bool {
        self.expires_at <= now
    }

    /// Iterates over the host's tags as `(&str, &str)` key-value pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.tags
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
    }
}

/// Immutable snapshot of the host→tags mapping.
pub type HostTagsMap = HashMap<String, HostTagsEntry>;

/// Lock-free, in-memory store mapping hostnames to their tag entries.
///
/// Uses [`ArcSwap`] so readers (Vector transforms on the hot path) never
/// block. Writers call [`store`] or [`merge`] to swap in a new snapshot
/// atomically after polling the metadata service.
pub struct HostTagsStore {
    inner: ArcSwap<HostTagsMap>,
}

static GLOBAL_STORE: LazyLock<Arc<HostTagsStore>> =
    LazyLock::new(|| Arc::new(HostTagsStore::default()));

impl Default for HostTagsStore {
    fn default() -> Self {
        Self {
            inner: ArcSwap::from_pointee(HashMap::new()),
        }
    }
}

impl HostTagsStore {
    /// Returns the global shared instance.
    pub fn global() -> Arc<HostTagsStore> {
        GLOBAL_STORE.clone()
    }

    /// Looks up the entry for a given hostname.
    pub fn lookup(&self, hostname: &str) -> Option<HostTagsEntry> {
        let snapshot = self.inner.load_full();
        snapshot.get(hostname).cloned()
    }

    /// Atomically replaces the entire map.
    pub fn store(&self, entries: HostTagsMap) {
        self.inner.store(Arc::new(entries));
    }

    /// Merges fresh entries into the current snapshot.
    ///
    /// Hosts not present in `fresh` are left unchanged.
    pub fn merge(&self, fresh: HostTagsMap) {
        let mut updated = (*self.inner.load_full()).clone();
        updated.extend(fresh);
        self.inner.store(Arc::new(updated));
    }

    /// Removes the given hosts from the map.
    ///
    /// No-op if `hosts` is empty.
    pub fn evict(&self, hosts: &[String]) {
        if hosts.is_empty() {
            return;
        }
        let mut updated = (*self.inner.load_full()).clone();
        for host in hosts {
            updated.remove(host);
        }
        self.inner.store(Arc::new(updated));
    }

    /// Returns the current map snapshot for cache persistence.
    pub fn snapshot(&self) -> Arc<HostTagsMap> {
        self.inner.load_full()
    }

    /// Returns the number of hosts in the current snapshot.
    pub fn len(&self) -> usize {
        self.inner.load().len()
    }

    /// Returns true if the current snapshot contains no hosts.
    pub fn is_empty(&self) -> bool {
        self.inner.load().is_empty()
    }

    /// Approximates the heap footprint of the current snapshot in bytes.
    ///
    /// Counts the `HashMap` bucket array, hostname string bytes, tag-list
    /// slice bytes, and every tag key/value string's bytes. Ignores
    /// small, fixed-size overheads (Arc headers, struct padding, etc.).
    pub fn memory_footprint(&self) -> ByteSize {
        let snapshot = self.inner.load();
        let mut bytes = snapshot.capacity()
            * (std::mem::size_of::<String>() + std::mem::size_of::<HostTagsEntry>());
        for (host, entry) in snapshot.iter() {
            bytes += host.capacity();
            bytes += entry.tags.len() * std::mem::size_of::<HostTag>();
            for (key, value) in entry.tags.iter() {
                bytes += key.capacity() + value.capacity();
            }
        }
        ByteSize(bytes as u64)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn entry(tags: Vec<HostTag>, expires_at: UnixTimestamp) -> HostTagsEntry {
        HostTagsEntry {
            tags: tags.into(),
            host_id: None,
            expires_at,
        }
    }

    fn fresh_entry(tags: Vec<HostTag>) -> HostTagsEntry {
        entry(tags, UnixTimestamp::now() + Duration::from_secs(3600))
    }

    #[test]
    fn test_lookup_missing_host_returns_none() {
        let store = HostTagsStore::default();
        assert!(store.lookup("unknown-host").is_none());
    }

    #[test]
    fn test_store_and_lookup() {
        let store = HostTagsStore::default();
        let mut entries = HashMap::new();
        entries.insert(
            "host-1".to_string(),
            fresh_entry(vec![
                ("env".to_string(), "prod".to_string()),
                ("region".to_string(), "us-east-1".to_string()),
            ]),
        );
        entries.insert(
            "host-2".to_string(),
            fresh_entry(vec![("env".to_string(), "staging".to_string())]),
        );
        store.store(entries);

        assert_eq!(store.len(), 2);

        let entry = store.lookup("host-1").expect("host-1 should exist");
        let tags: Vec<_> = entry.iter().collect();
        assert_eq!(tags.len(), 2);
        assert!(tags.contains(&("env", "prod")));
        assert!(tags.contains(&("region", "us-east-1")));

        let entry = store.lookup("host-2").expect("host-2 should exist");
        let tags: Vec<_> = entry.iter().collect();
        assert_eq!(tags, vec![("env", "staging")]);

        assert!(store.lookup("host-3").is_none());
    }

    #[test]
    fn test_lookup_returns_expiry() {
        let store = HostTagsStore::default();
        let mut entries = HashMap::new();
        entries.insert(
            "host-1".to_string(),
            entry(
                vec![("env".to_string(), "prod".to_string())],
                UnixTimestamp(9999),
            ),
        );
        store.store(entries);

        let entry = store.lookup("host-1").expect("host-1 should exist");
        assert!(!entry.is_expired(UnixTimestamp(1000)));
        assert!(entry.is_expired(UnixTimestamp(9999)));
        assert!(entry.is_expired(UnixTimestamp(10000)));
    }

    #[test]
    fn test_store_overwrites_previous_data() {
        let store = HostTagsStore::default();

        let mut first = HashMap::new();
        first.insert(
            "host-1".to_string(),
            fresh_entry(vec![("env".to_string(), "prod".to_string())]),
        );
        store.store(first);
        assert_eq!(store.len(), 1);

        let mut second = HashMap::new();
        second.insert(
            "host-2".to_string(),
            fresh_entry(vec![("env".to_string(), "staging".to_string())]),
        );
        store.store(second);
        assert_eq!(store.len(), 1);
        assert!(store.lookup("host-1").is_none());
        assert!(store.lookup("host-2").is_some());
    }

    #[test]
    fn test_merge_updates_subset_and_preserves_rest() {
        let store = HostTagsStore::default();

        let mut initial = HashMap::new();
        initial.insert(
            "host-1".to_string(),
            fresh_entry(vec![("env".to_string(), "prod".to_string())]),
        );
        initial.insert(
            "host-2".to_string(),
            fresh_entry(vec![("env".to_string(), "staging".to_string())]),
        );
        store.store(initial);
        assert_eq!(store.len(), 2);

        let mut fresh = HashMap::new();
        fresh.insert(
            "host-2".to_string(),
            fresh_entry(vec![("env".to_string(), "prod".to_string())]),
        );
        fresh.insert(
            "host-3".to_string(),
            fresh_entry(vec![("env".to_string(), "dev".to_string())]),
        );
        store.merge(fresh);

        assert_eq!(store.len(), 3);

        let host1 = store.lookup("host-1").expect("host-1");
        let tags: Vec<_> = host1.iter().collect();
        assert_eq!(tags, vec![("env", "prod")]);

        let host2 = store.lookup("host-2").expect("host-2");
        let tags: Vec<_> = host2.iter().collect();
        assert_eq!(tags, vec![("env", "prod")]);

        let host3 = store.lookup("host-3").expect("host-3");
        let tags: Vec<_> = host3.iter().collect();
        assert_eq!(tags, vec![("env", "dev")]);
    }

    #[test]
    fn test_merge_updates_expiry() {
        let store = HostTagsStore::default();
        let mut initial = HashMap::new();
        initial.insert(
            "host-1".to_string(),
            entry(
                vec![("env".to_string(), "prod".to_string())],
                UnixTimestamp(100),
            ),
        );
        store.store(initial);

        let mut fresh = HashMap::new();
        fresh.insert(
            "host-1".to_string(),
            entry(
                vec![("env".to_string(), "prod".to_string())],
                UnixTimestamp(9999),
            ),
        );
        store.merge(fresh);

        let entry = store.lookup("host-1").expect("host-1");
        assert!(!entry.is_expired(UnixTimestamp(9998)));
        assert!(entry.is_expired(UnixTimestamp(9999)));
    }

    #[test]
    fn test_evict_removes_hosts() {
        let store = HostTagsStore::default();
        let mut entries = HashMap::new();
        entries.insert(
            "host-1".to_string(),
            fresh_entry(vec![("env".to_string(), "prod".to_string())]),
        );
        entries.insert(
            "host-2".to_string(),
            fresh_entry(vec![("env".to_string(), "staging".to_string())]),
        );
        store.store(entries);
        assert_eq!(store.len(), 2);

        store.evict(&["host-1".to_string()]);

        assert_eq!(store.len(), 1);
        assert!(store.lookup("host-1").is_none());
        assert!(store.lookup("host-2").is_some());
    }

    #[test]
    fn test_empty_store() {
        let store = HostTagsStore::default();
        assert!(store.is_empty());
        assert_eq!(store.len(), 0);
    }
}
