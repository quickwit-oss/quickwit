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

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};

use dashmap::DashMap;
use metrics::CounterFn;

use crate::MetricInfo;

/// Global deduplication map: cache-key hash → shared counter handle.
///
/// All threads that declare the same metric (same name + labels) share
/// a single `CounterInner` behind an `Arc`. The `DashMap` entry is
/// created once; subsequent lookups just `Arc::clone` the value.
static COUNTERS: LazyLock<DashMap<u64, Arc<CounterInner>>> = LazyLock::new(DashMap::new);

/// Looks up or creates a `Counter` in the global `COUNTERS` map.
///
/// `build` is called at most once per unique `hash` — it constructs
/// the static metadata, key, and recorder handle that live for the
/// lifetime of the process.
///
/// Prefer the `counter!` macro over calling this directly.
#[doc(hidden)]
pub fn __counter_get_or_register(
    hash: u64,
    build: impl FnOnce() -> (
        &'static MetricInfo,
        metrics::Key,
        metrics::Metadata<'static>,
    ),
) -> Counter {
    let inner = COUNTERS
        .entry(hash)
        .or_insert_with(|| {
            let (info, key, metadata) = build();
            // Register with the installed recorder (e.g. Prometheus).
            let inner =
                metrics::with_recorder(|recorder| recorder.register_counter(&key, &metadata));
            let counter_inner = CounterInner::new(hash, info, key, inner);
            Arc::new(counter_inner)
        })
        .value()
        .clone(); // Arc::clone — cheap reference count bump.
    Counter(inner)
}

/// Internal storage for a single counter metric.
///
/// Held behind an `Arc` so that all handles (`Counter` clones, thread-local
/// caches, parent extensions with matching labels) point to the same data.
struct CounterInner {
    /// Static metadata (name, subsystem, description).
    info: &'static MetricInfo,
    /// Full metric key: qualified name + all labels.
    key: metrics::Key,
    /// Recorder-provided counter handle for the actual recording backend.
    inner: metrics::Counter,
    /// Shadow atomic that mirrors every mutation so `get()` can read
    /// the current value without querying the recorder.
    shadow: AtomicU64,
    /// Pre-computed cache key used for DashMap lookups, thread-local
    /// comparisons, and the `Hash` / `Eq` impls on `Counter`.
    hash: u64,
}

impl CounterInner {
    fn new(
        hash: u64,
        info: &'static MetricInfo,
        key: metrics::Key,
        inner: metrics::Counter,
    ) -> Self {
        Self {
            info,
            key,
            inner,
            shadow: AtomicU64::new(0),
            hash,
        }
    }
}

/// A registered counter metric backed by [`metrics::Counter`].
///
/// Created via the `counter!` macro, either as a base declaration with
/// static labels or as a child that extends a parent's labels at runtime.
///
/// Counters are **monotonically increasing** — use [`increment`](Self::increment)
/// for deltas or [`absolute`](Self::absolute) to set a known total.
///
/// Every counter maintains an `AtomicU64` shadow so that [`get()`](Self::get)
/// can read the current value without querying the recorder. All clones
/// share the same shadow via `Arc<CounterInner>`.
#[derive(Clone)]
#[repr(transparent)]
pub struct Counter(Arc<CounterInner>);

impl std::fmt::Debug for Counter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Counter").field("key", &self.0.key).finish()
    }
}

/// Uses the pre-computed cache-key hash so counters can be stored in
/// `HashMap`/`HashSet` without re-hashing the key contents.
impl std::hash::Hash for Counter {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.get_hash().hash(state);
    }
}

/// Two counters are equal when they share the same cache-key hash,
/// i.e. they were declared with identical name + labels.
impl PartialEq for Counter {
    fn eq(&self, other: &Self) -> bool {
        self.get_hash() == other.get_hash()
    }
}

impl Eq for Counter {}

impl Counter {
    // NOTE: never call it directly, use the macro instead because it ensures the hash is
    // pre-computed correctly.
    #[doc(hidden)]
    pub fn __new(
        hash: u64,
        info: &'static MetricInfo,
        key: metrics::Key,
        inner: metrics::Counter,
    ) -> Self {
        Self(Arc::new(CounterInner::new(hash, info, key, inner)))
    }

    #[doc(hidden)]
    pub fn __info(&self) -> &'static MetricInfo {
        self.0.info
    }

    /// Returns the pre-computed cache-key hash for this counter.
    pub fn get_hash(&self) -> u64 {
        self.0.hash
    }

    /// Returns the [`metrics::Key`] (name + labels) this counter was
    /// registered with.
    pub fn key(&self) -> &metrics::Key {
        &self.0.key
    }

    /// Adds `value` to the counter and its shadow atomic.
    pub fn increment(&self, value: u64) {
        self.0.shadow.fetch_add(value, Ordering::Relaxed);
        self.0.inner.increment(value);
    }

    /// Sets the counter to an absolute `value`, useful for process-level
    /// totals that are already tracked externally.
    pub fn absolute(&self, value: u64) {
        self.0.shadow.store(value, Ordering::Relaxed);
        self.0.inner.absolute(value);
    }

    /// Returns the current counter value from the shadow atomic.
    pub fn get(&self) -> u64 {
        self.0.shadow.load(Ordering::Relaxed)
    }
}

/// Bridges `Counter` into the `metrics` recorder trait so it can be
/// used wherever a `CounterFn` is expected.
impl CounterFn for Counter {
    fn increment(&self, value: u64) {
        Self::increment(self, value);
    }

    fn absolute(&self, value: u64) {
        Self::absolute(self, value);
    }
}

/// Declares or extends a counter metric.
///
/// # Base declaration
///
/// Creates a new counter with a static name, description, subsystem, and
/// optional static labels. Every counter maintains a shadow `AtomicU64`
/// so [`get()`](Counter::get) always returns the current value.
///
/// ```ignore
/// let c = counter!(
///     name: "requests_total",
///     description: "Total number of HTTP requests",
///     subsystem: "http",
///     "env" => "prod",
/// );
/// c.increment(1);
/// assert_eq!(c.get(), 1);
/// ```
///
/// # Parent extension
///
/// Derives a child counter from an existing one, inheriting its name and
/// labels while appending additional (possibly dynamic) labels.
///
/// ```ignore
/// let child = counter!(parent: base, "region" => region);
/// child.increment(1);
/// ```
#[macro_export]
macro_rules! counter {
    // Base declaration: all-static name, labels, and key — zero allocations.
    (
        name: $name:literal,
        description: $description:literal,
        subsystem: $subsystem:tt
        $(, $label:literal => $value:literal)* $(,)?
    ) => {{
        $crate::__key_info_metadata!(
            kind: $crate::MetricKind::Counter,
            name: $name,
            description: $description,
            subsystem: $subsystem
            $(, $label => $value)*
        );
        $crate::__metric_declaration!(
            metric_type: $crate::Counter,
            register_fn: $crate::__counter_get_or_register,
            metric_info: &INFO
            $(, $label => $value)*
        )
    }};

    // Parent extension with inline dynamic labels.
    // Derives a child counter by inheriting the parent's name and labels,
    // appending new (possibly dynamic) key => value pairs.
    (
        parent: $parent:expr,
        $($label:literal => $value:expr),+ $(,)?
    ) => {
        $crate::__metric_extension!(
            metric_type: $crate::Counter,
            register_fn: $crate::__counter_get_or_register,
            parent: $parent,
            metric_info: $parent.__info(),
            // Seed with parent hash, fold in each (name, value) pair.
            hash: $crate::__key_hash!($parent.get_hash(), $(($label, $value)),+),
            label_count: $crate::__count!($($label)*),
            labels_iter: [$($crate::__metrics::Label::new($label, $value)),+].into_iter()
        )
    };

    // Parent extension via one or more pre-built Labels<N> bundles.
    // Composes hash, count, and label iterators across all labels via the
    // __bind_labels! tt-muncher — zero allocation on the hot path.
    (
        parent: $parent:expr,
        labels: $($labels:expr),+ $(,)?
    ) => {{
        $crate::__bind_labels!(
            metric_type: $crate::Counter,
            register_fn: $crate::__counter_get_or_register,
            parent: $parent,
            metric_info: $parent.__info(),
            hash: $parent.get_hash(),
            count: 0usize,
            iter: std::iter::empty::<$crate::__metrics::Label>(),
            $(next: $labels,)+
        )
    }};
}
