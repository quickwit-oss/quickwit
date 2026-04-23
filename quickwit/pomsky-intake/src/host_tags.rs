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

/// A single host-tag entry: key-value pair like `("env", "prod")`.
pub type HostTag = (String, String);

/// Immutable snapshot of the host→tags mapping.
///
/// Values are `Arc<[HostTag]>` so that a [`HostTagsRef`] returned by
/// [`HostTagsStore::lookup`] can outlive the snapshot it was taken from:
/// refcounting keeps each host's tag list alive independently of the
/// surrounding map. It also makes [`HostTagsStore::merge`] cheap — the
/// map clone only bumps Arc refcounts, never copies tag data.
pub type HostTagsMap = HashMap<String, Arc<[HostTag]>>;

/// A reference to one host's tag list.
///
/// Holds a single `Arc<[HostTag]>` cloned out of the store — no hostname
/// copy and no reference into the surrounding snapshot.
pub struct HostTagsRef {
    tags: Arc<[HostTag]>,
}

impl HostTagsRef {
    /// Iterates over the host's tags as `(&str, &str)` key-value pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.tags
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
    }
}

/// Lock-free, in-memory store mapping hostnames to their tag sets.
///
/// Uses [`ArcSwap`] so readers (Vector transforms on the hot path) never
/// block. Writers call [`store`] to swap in a new snapshot atomically
/// after polling the metadata service.
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

    /// Looks up the tag set for a given hostname. Returns a [`HostTagsRef`]
    /// that yields `(&str, &str)` references without cloning tag data.
    pub fn lookup(&self, hostname: &str) -> Option<HostTagsRef> {
        let snapshot = self.inner.load_full();
        snapshot
            .get(hostname)
            .cloned()
            .map(|tags| HostTagsRef { tags })
    }

    /// Atomically replaces the entire tag map with a new snapshot.
    pub fn store(&self, entries: HostTagsMap) {
        self.inner.store(Arc::new(entries));
    }

    /// Merges freshly-fetched entries into the current snapshot.
    ///
    /// Loads the existing map, clones it, applies the updates, and swaps
    /// the result in. Hosts not present in `fresh` are left unchanged.
    pub fn merge(&self, fresh: HostTagsMap) {
        let current = self.inner.load_full();
        let mut merged = (*current).clone();
        for (host, tags) in fresh {
            merged.insert(host, tags);
        }
        self.inner.store(Arc::new(merged));
    }

    /// Returns a clone of the current snapshot for persistence.
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
    pub fn memory_footprint_bytes(&self) -> usize {
        let snapshot = self.inner.load();
        let mut bytes = snapshot.capacity()
            * (std::mem::size_of::<String>() + std::mem::size_of::<Arc<[HostTag]>>());
        for (host, tags) in snapshot.iter() {
            bytes += host.capacity();
            bytes += tags.len() * std::mem::size_of::<HostTag>();
            for (key, value) in tags.iter() {
                bytes += key.capacity() + value.capacity();
            }
        }
        bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            vec![
                ("env".to_string(), "prod".to_string()),
                ("region".to_string(), "us-east-1".to_string()),
            ]
            .into(),
        );
        entries.insert(
            "host-2".to_string(),
            vec![("env".to_string(), "staging".to_string())].into(),
        );
        store.store(entries);

        assert_eq!(store.len(), 2);

        let tags_ref = store.lookup("host-1").expect("host-1 should exist");
        let tags: Vec<_> = tags_ref.iter().collect();
        assert_eq!(tags.len(), 2);
        assert!(tags.contains(&("env", "prod")));
        assert!(tags.contains(&("region", "us-east-1")));

        let tags_ref = store.lookup("host-2").expect("host-2 should exist");
        let tags: Vec<_> = tags_ref.iter().collect();
        assert_eq!(tags, vec![("env", "staging")]);

        assert!(store.lookup("host-3").is_none());
    }

    #[test]
    fn test_store_overwrites_previous_data() {
        let store = HostTagsStore::default();

        let mut first = HashMap::new();
        first.insert(
            "host-1".to_string(),
            vec![("env".to_string(), "prod".to_string())].into(),
        );
        store.store(first);
        assert_eq!(store.len(), 1);

        let mut second = HashMap::new();
        second.insert(
            "host-2".to_string(),
            vec![("env".to_string(), "staging".to_string())].into(),
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
            vec![("env".to_string(), "prod".to_string())].into(),
        );
        initial.insert(
            "host-2".to_string(),
            vec![("env".to_string(), "staging".to_string())].into(),
        );
        store.store(initial);
        assert_eq!(store.len(), 2);

        // Merge updates host-2 and adds host-3, but host-1 is untouched.
        let mut fresh = HashMap::new();
        fresh.insert(
            "host-2".to_string(),
            vec![("env".to_string(), "prod".to_string())].into(),
        );
        fresh.insert(
            "host-3".to_string(),
            vec![("env".to_string(), "dev".to_string())].into(),
        );
        store.merge(fresh);

        assert_eq!(store.len(), 3);

        // host-1 unchanged.
        let host1 = store.lookup("host-1").expect("host-1");
        let tags: Vec<_> = host1.iter().collect();
        assert_eq!(tags, vec![("env", "prod")]);

        // host-2 updated.
        let host2 = store.lookup("host-2").expect("host-2");
        let tags: Vec<_> = host2.iter().collect();
        assert_eq!(tags, vec![("env", "prod")]);

        // host-3 added.
        let host3 = store.lookup("host-3").expect("host-3");
        let tags: Vec<_> = host3.iter().collect();
        assert_eq!(tags, vec![("env", "dev")]);
    }

    #[test]
    fn test_empty_store() {
        let store = HostTagsStore::default();
        assert!(store.is_empty());
        assert_eq!(store.len(), 0);
    }
}
