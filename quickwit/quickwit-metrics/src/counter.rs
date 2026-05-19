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
/// # Recorder-binding invariant
///
/// The `metrics::Counter` handle returned by `register_counter` is
/// permanently bound to whichever [`metrics::Recorder`] is active at
/// first-access time. If this function is called before the production
/// recorder is installed (e.g. while the noop default is still active),
/// the cached handle will silently discard all subsequent increments.
///
/// **Callers must ensure that the global recorder is installed before
/// any metric is first accessed.** In Quickwit this is guaranteed by
/// `init_telemetry()` running before any `LazyLock<Counter>` is forced.
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
    let counter_arc = COUNTERS
        .entry(hash)
        .or_insert_with(|| {
            let (info, key, metadata) = build();
            let recorder_counter =
                metrics::with_recorder(|recorder| recorder.register_counter(&key, &metadata));
            Arc::new(CounterInner::new(hash, info, key, recorder_counter))
        })
        .value()
        .clone(); // Arc::clone — cheap reference count bump.
    Counter(counter_arc)
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

/// Two counters are equal when they point to the same `Arc` allocation,
/// i.e. they were produced by cloning the same handle. The global
/// `DashMap` guarantees that all call sites with identical name + labels
/// share one `Arc`, so identity equality implies semantic equality.
impl PartialEq for Counter {
    fn eq(&self, other: &Self) -> bool {
        Arc::as_ptr(&self.0) == Arc::as_ptr(&other.0)
    }
}

impl Eq for Counter {}

impl std::hash::Hash for Counter {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.0).hash(state);
    }
}

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

    #[doc(hidden)]
    pub fn __hash(&self) -> u64 {
        self.0.hash
    }

    /// Returns the [`metrics::Key`] (name + labels) this counter was
    /// registered with.
    pub fn key(&self) -> &metrics::Key {
        &self.0.key
    }

    /// Adds `value` to the counter and its shadow atomic.
    pub fn inc_by(&self, value: u64) {
        self.0.shadow.fetch_add(value, Ordering::Relaxed);
        self.0.inner.increment(value);
    }

    /// Increments the counter by 1 and its shadow atomic.
    pub fn inc(&self) {
        self.inc_by(1);
    }

    /// Bumps the counter to at least `value` (monotonic max).
    ///
    /// Useful for process-level totals that are already tracked externally:
    /// the stored value advances to `value` only when it is greater than
    /// the current one, preserving counter monotonicity.
    pub fn absolute(&self, value: u64) {
        self.0.shadow.fetch_max(value, Ordering::Relaxed);
        self.0.inner.absolute(value);
    }

    /// Returns the current counter value from the shadow atomic.
    pub fn get(&self) -> u64 {
        self.0.shadow.load(Ordering::Relaxed)
    }

    /// Returns a detached counter with a noop recorder and its own shadow atomic.
    pub fn local() -> Counter {
        // Static key from empty parts — clone is free (Cow::Borrowed).
        static KEY: metrics::Key = metrics::Key::from_static_parts("", &[]);
        static METADATA: metrics::Metadata<'static> =
            metrics::Metadata::new("local", metrics::Level::DEBUG, None);
        static INFO: MetricInfo = MetricInfo {
            key_name: "",
            description: "local counter",
            kind: crate::MetricKind::Counter,
            metadata: &METADATA,
            static_labels: &[],
        };
        // Hash is irrelevant: equality is based on Arc identity (see
        // PartialEq impl), and local counters never enter the DashMap.
        let inner = CounterInner::new(0, &INFO, KEY.clone(), metrics::Counter::noop());
        Counter(Arc::new(inner))
    }
}

/// Bridges `Counter` into the `metrics` recorder trait so it can be
/// used wherever a `CounterFn` is expected.
impl CounterFn for Counter {
    fn increment(&self, value: u64) {
        Self::inc_by(self, value);
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
/// c.inc();
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
/// child.inc();
/// ```
#[macro_export]
macro_rules! counter {
    // Base declaration with explicit system and subsystem prefix - zero allocations.
    (
        name: $name:literal,
        description: $description:literal,
        system: $system:expr,
        subsystem: $subsystem:expr
        $(, $label:literal => $value:literal)* $(,)?
    ) => {{
        $crate::__key_info_metadata!(
            kind: $crate::MetricKind::Counter,
            name: $name,
            description: $description,
            system: $system,
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

    // Base declaration with subsystem only — system defaults to SYSTEM.
    (
        name: $name:literal,
        description: $description:literal,
        subsystem: $subsystem:expr
        $(, $label:literal => $value:literal)* $(,)?
    ) => {{
        $crate::counter!(
            name: $name,
            description: $description,
            system: $crate::SYSTEM,
            subsystem: $subsystem
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
            hash: $crate::__key_hash!($parent.__hash(), $(($label, $value)),+),
            label_count: $crate::__count!($($label)*),
            labels_iter: [$($crate::__metrics::Label::new($label, $value)),+].into_iter()
        )
    };

    // Parent extension via one or more pre-built Labels<N> bundles.
    (
        parent: $parent:expr,
        labels: [$($labels:expr),+ $(,)?] $(,)?
    ) => {
        $crate::__metric_extension!(
            metric_type: $crate::Counter,
            register_fn: $crate::__counter_get_or_register,
            parent: $parent,
            metric_info: $parent.__info(),
            hash: $crate::__key_hash(
                $parent.__hash(),
                std::iter::empty()$(.chain($labels.iter()))+,
            ),
            label_count: 0usize $(+ $labels.len())+,
            labels_iter: std::iter::empty()$(.chain($labels.__to_labels()))+
        )
    };
}

/// A lazily-initialized [`Counter`].
///
/// The counter is registered with the recorder on first access.
/// See [`lazy_counter!`][macro@crate::lazy_counter] for the recommended way to construct this type.
pub type LazyCounter = LazyLock<Counter>;

/// Wraps a [`counter!`] invocation in a [`LazyCounter`].
///
/// Accepts exactly the same arguments as [`counter!`] and produces a
/// `LazyCounter` (i.e. `LazyLock<Counter>`).
///
/// # Example
///
/// ```ignore
/// static REQUESTS: LazyCounter = lazy_counter!(
///     name: "requests_total",
///     description: "Total HTTP requests",
///     subsystem: "http",
/// );
///
/// // Equivalent to:
/// static REQUESTS: LazyCounter = LazyLock::new(|| {
///     counter!(
///         name: "requests_total",
///         description: "Total HTTP requests",
///         subsystem: "http",
///     )
/// });
/// ```
#[macro_export]
macro_rules! lazy_counter {
    ($($arg:tt)*) => {
        $crate::LazyCounter::new(|| {
            $crate::counter!($($arg)*)
        })
    };
}
