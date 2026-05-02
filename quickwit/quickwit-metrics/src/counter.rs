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
    /// Static metadata (name, subsystem, description, observable flag).
    info: &'static MetricInfo,
    /// Full metric key: qualified name + all labels.
    key: metrics::Key,
    /// Recorder-provided counter handle for the actual recording backend.
    inner: metrics::Counter,
    /// Shadow atomic for observable counters (`Some`), or `None` for
    /// fire-and-forget counters where `get()` returns `u64::MAX`.
    shadow: Option<AtomicU64>,
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
        let shadow = if info.observable {
            Some(AtomicU64::new(0))
        } else {
            None
        };
        Self {
            info,
            key,
            inner,
            shadow,
            hash,
        }
    }
}

/// A registered counter metric backed by [`metrics::Counter`].
///
/// Created via the [`counter!`] macro, either as a base declaration with
/// static labels or as a child that extends a parent's labels at runtime.
///
/// Counters are **monotonically increasing** — use [`increment`](Self::increment)
/// for deltas or [`absolute`](Self::absolute) to set a known total.
///
/// When declared with `observable: true`, the counter holds an
/// `AtomicU64` shadow inside its `Arc<CounterInner>`. All clones share
/// the same atomic, so `get()` is always consistent. Non-observable
/// counters store `None` and `get()` returns `u64::MAX`.
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

    /// Adds `value` to the counter.
    ///
    /// If observable, also bumps the shadow `AtomicU64` so that
    /// [`get()`](Self::get) reflects the update.
    pub fn increment(&self, value: u64) {
        if let Some(s) = self.0.shadow.as_ref() {
            s.fetch_add(value, Ordering::Relaxed);
        }
        self.0.inner.increment(value);
    }

    /// Sets the counter to an absolute `value`, useful for process-level
    /// totals that are already tracked externally.
    ///
    /// If observable, also stores into the shadow `AtomicU64`.
    pub fn absolute(&self, value: u64) {
        if let Some(s) = self.0.shadow.as_ref() {
            s.store(value, Ordering::Relaxed);
        }
        self.0.inner.absolute(value);
    }

    /// Returns the current shadow counter value.
    ///
    /// Observable counters return the accumulated value.
    /// Non-observable counters always return `u64::MAX`.
    pub fn get(&self) -> u64 {
        match self.0.shadow.as_ref() {
            Some(s) => s.load(Ordering::Relaxed),
            None => u64::MAX,
        }
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
/// optional static labels. By default counters are non-observable; add
/// `observable: true` to enable `get()` readback via a shadow `AtomicU64`.
///
/// ```ignore
/// let c = counter!(
///     name: "requests_total",
///     description: "Total number of HTTP requests",
///     subsystem: "http",
///     "env" => "prod",
/// );
/// c.increment(1);
/// ```
///
/// With `observable: true`, `get()` returns the current value:
///
/// ```ignore
/// let c = counter!(
///     name: "requests_total",
///     description: "Total number of HTTP requests",
///     subsystem: "http",
///     observable: true,
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
    // Base declaration without observable (defaults to false).
    // Convenience shorthand that delegates to the full base arm below.
    (
        name: $name:literal,
        description: $description:literal,
        subsystem: $subsystem:tt
        $(, $label:literal => $value:literal)* $(,)?
    ) => {{
        $crate::counter!(
            name: $name,
            description: $description,
            subsystem: $subsystem,
            observable: false
            $(, $label => $value)*
        )
    }};

    // Base declaration: all-static name, labels, and key — zero allocations.
    (
        name: $name:literal,
        description: $description:literal,
        subsystem: $subsystem:tt,
        observable: $observable:expr
        $(, $label:literal => $value:literal)* $(,)?
    ) => {{
        // Expand compile-time statics: KEY_NAME, INFO, KEY, LABELS, METADATA.
        $crate::__key_info_metadata!(
            kind: $crate::MetricKind::Counter,
            observable: $observable,
            name: $name,
            description: $description,
            subsystem: $subsystem
            $(, $label => $value)*
        );
        // Thread-local cache + global DashMap registration.
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

    // Parent extension via a pre-built LabelValues<N> bundle.
    // Same as the inline arm but hash and labels come from a LabelValues<N>.
    (
        parent: $parent:expr,
        labels: $labels:expr $(,)?
    ) => {{
        let label_values = $labels;
        $crate::__metric_extension!(
            metric_type: $crate::Counter,
            register_fn: $crate::__counter_get_or_register,
            parent: $parent,
            metric_info: $parent.__info(),
            hash: label_values.hash($parent.get_hash()),
            label_count: label_values.len(),
            labels_iter: label_values.to_labels()
        )
    }};
}
