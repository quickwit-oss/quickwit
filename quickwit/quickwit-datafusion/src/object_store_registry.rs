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

//! Custom [`ObjectStoreRegistry`] that turns any URL reachable by the
//! node-local [`StorageResolver`] into a lazy [`QuickwitObjectStore`].
//!
//! ## Why this exists
//!
//! DataFusion looks up object stores by URL **synchronously** on its hot
//! path (`RuntimeEnv::object_store(&url)`). Quickwit's `StorageResolver`
//! is **async**. The two don't compose directly, which is what the
//! previous "pre-register everything at startup" hack tried to work
//! around.
//!
//! This registry resolves the tension by doing the sync/async split on
//! the correct axis:
//!
//! - `get_store` (sync) builds a [`QuickwitObjectStore`] wrapper on demand. Construction is cheap —
//!   just stashes the URI and a clone of the resolver.
//! - `QuickwitObjectStore`'s own methods are already async, so the actual
//!   `StorageResolver::resolve(&uri).await` runs the first time DataFusion asks for data — inside
//!   the `ObjectStore` method itself.
//!
//! End result: one `StorageResolver` per node, used per-request, no
//! global cache warm-up, no per-query listing. Matches the search-leaf
//! pattern in `quickwit-search/src/leaf.rs`.
//!
//! ## Delegation
//!
//! Any URL explicitly registered via `register_store` takes precedence —
//! the default registry is consulted first. This lets callers override a
//! specific URL with a custom store (e.g. tests wanting an in-memory
//! object store) without losing the lazy fallback for everything else.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, RwLock};

use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::execution::object_store::{DefaultObjectStoreRegistry, ObjectStoreRegistry};
use object_store::ObjectStore;
use quickwit_common::uri::Uri;
use quickwit_storage::StorageResolver;
use url::Url;

use crate::storage_bridge::QuickwitObjectStore;

/// `ObjectStoreRegistry` that constructs a lazy [`QuickwitObjectStore`] on
/// the first `get_store` call for any URL the node-local [`StorageResolver`]
/// can handle (`s3://`, `gs://`, `file://`, `ram://`, …).
///
/// Wrappers are cached per URL so subsequent lookups return the same
/// `Arc<dyn ObjectStore>` — and therefore share the one-shot resolved
/// `Storage` handle inside it.
pub struct QuickwitObjectStoreRegistry {
    /// Delegate for explicit `register_store` overrides. Checked first in
    /// `get_store` so a caller-supplied store always wins over our lazy
    /// fallback.
    default: DefaultObjectStoreRegistry,
    storage_resolver: StorageResolver,
    /// Lazy wrappers constructed by `get_store` on demand, keyed by
    /// `scheme://authority`. Plain `RwLock<HashMap>` is fine — contention
    /// is negligible because the write lock is only taken once per unique
    /// URL, and `get_store` itself is off the query hot path.
    lazy_stores: RwLock<HashMap<String, Arc<dyn ObjectStore>>>,
}

impl std::fmt::Debug for QuickwitObjectStoreRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuickwitObjectStoreRegistry")
            .field(
                "cached_stores",
                &self.lazy_stores.read().map(|s| s.len()).unwrap_or(0),
            )
            .finish_non_exhaustive()
    }
}

impl QuickwitObjectStoreRegistry {
    pub fn new(storage_resolver: StorageResolver) -> Self {
        Self {
            default: DefaultObjectStoreRegistry::new(),
            storage_resolver,
            lazy_stores: RwLock::new(HashMap::new()),
        }
    }

    /// Canonical cache key mirroring DataFusion's `DefaultObjectStoreRegistry`:
    /// `scheme://authority`. Preserves the authority so indexes in
    /// different buckets stay distinct; paths within an authority share
    /// one wrapper (and thus one resolved `Storage`).
    fn url_key(url: &Url) -> String {
        if let Some(host) = url.host_str() {
            format!("{}://{}", url.scheme(), host)
        } else {
            format!("{}://", url.scheme())
        }
    }
}

impl ObjectStoreRegistry for QuickwitObjectStoreRegistry {
    fn register_store(
        &self,
        url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        self.default.register_store(url, store)
    }

    fn get_store(&self, url: &Url) -> DFResult<Arc<dyn ObjectStore>> {
        // Explicit registration wins.
        if let Ok(store) = self.default.get_store(url) {
            return Ok(store);
        }
        let key = Self::url_key(url);
        // Fast path: cache hit under a read lock.
        if let Some(cached) = self
            .lazy_stores
            .read()
            .expect("object store cache lock poisoned")
            .get(&key)
        {
            return Ok(Arc::clone(cached));
        }
        let uri = Uri::from_str(&key).map_err(|err| {
            DataFusionError::External(Box::new(std::io::Error::other(format!(
                "failed to build Quickwit URI from `{key}`: {err}"
            ))))
        })?;
        let store: Arc<dyn ObjectStore> =
            Arc::new(QuickwitObjectStore::new(uri, self.storage_resolver.clone()));
        let mut write = self
            .lazy_stores
            .write()
            .expect("object store cache lock poisoned");
        // Double-check — another caller may have raced us to initialise.
        if let Some(existing) = write.get(&key) {
            return Ok(Arc::clone(existing));
        }
        write.insert(key, Arc::clone(&store));
        Ok(store)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Exercises the lazy fallback on a scheme DataFusion's default registry
    /// does not own. `ram://` is a `StorageResolver` scheme; the default
    /// `DefaultObjectStoreRegistry::get_store` misses, so we fall through
    /// to the lazy wrapper path. This proves the scheme routing works for a
    /// non-`file://` URL — the rest of the suite is `file://`-backed, so
    /// without this test the path-prefix + lazy-construct code would have
    /// no coverage for anything else.
    ///
    /// End-to-end I/O against `RamStorage` is deliberately not exercised
    /// here: its path semantics are idiosyncratic (per-prefix namespaces
    /// keyed by absolute paths), and an end-to-end I/O test would be
    /// testing `RamStorage` rather than this registry. The `file://` test
    /// suite covers actual reads.
    #[tokio::test]
    async fn lazy_fallback_constructs_wrapper_for_non_file_scheme() {
        let registry = QuickwitObjectStoreRegistry::new(StorageResolver::unconfigured());
        let url = Url::parse("ram://").expect("valid ram url");

        // Miss on the default registry → lazy construction + cache insert.
        let store_a = registry.get_store(&url).expect("lazy construct");

        // Hit on the cache → same Arc.
        let store_b = registry.get_store(&url).expect("cache hit");
        assert!(Arc::ptr_eq(&store_a, &store_b));
    }

    /// Different URLs produce distinct wrappers — no collision on the
    /// `scheme://authority` cache key.
    #[tokio::test]
    async fn distinct_urls_produce_distinct_stores() {
        let registry = QuickwitObjectStoreRegistry::new(StorageResolver::unconfigured());
        let ram = registry.get_store(&Url::parse("ram://").unwrap()).unwrap();
        let file = registry.get_store(&Url::parse("file://").unwrap()).unwrap();
        assert!(!Arc::ptr_eq(&ram, &file));
    }

    /// An explicit `register_store` wins over the lazy fallback, so tests
    /// or callers that want a specific `ObjectStore` at a given URL can
    /// still inject one.
    #[tokio::test]
    async fn explicit_registration_wins_over_lazy_fallback() {
        let registry = QuickwitObjectStoreRegistry::new(StorageResolver::unconfigured());
        let url = Url::parse("ram://explicit").expect("valid url");
        let explicit: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        registry.register_store(&url, Arc::clone(&explicit));

        let resolved = registry.get_store(&url).expect("explicit lookup");
        assert!(Arc::ptr_eq(&explicit, &resolved));
    }
}
