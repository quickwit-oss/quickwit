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

use std::sync::atomic::Ordering;
use std::sync::{Arc, LazyLock};

use atomic_float::AtomicF64;
use dashmap::DashMap;
use metrics::GaugeFn;

use crate::MetricInfo;

/// Global deduplication map: cache-key hash → shared gauge handle.
///
/// All threads that declare the same metric (same name + labels) share
/// a single `GaugeInner` behind an `Arc`. The `DashMap` entry is
/// created once; subsequent lookups just `Arc::clone` the value.
static GAUGES: LazyLock<DashMap<u64, Arc<GaugeInner>>> = LazyLock::new(DashMap::new);

/// Looks up or creates a `Gauge` in the global `GAUGES` map.
///
/// `build` is called at most once per unique `hash` — it constructs
/// the static metadata, key, and recorder handle that live for the
/// lifetime of the process.
///
/// # Recorder-binding invariant
///
/// The `metrics::Gauge` handle returned by `register_gauge` is
/// permanently bound to whichever [`metrics::Recorder`] is active at
/// first-access time. If this function is called before the production
/// recorder is installed (e.g. while the noop default is still active),
/// the cached handle will silently discard all subsequent mutations.
///
/// **Callers must ensure that the global recorder is installed before
/// any metric is first accessed.** In Quickwit this is guaranteed by
/// `init_telemetry()` running before any `LazyLock<Gauge>` is forced.
///
/// Prefer the `gauge!` macro over calling this directly.
#[doc(hidden)]
pub fn __gauge_get_or_register(
    hash: u64,
    build: impl FnOnce() -> (
        &'static MetricInfo,
        metrics::Key,
        metrics::Metadata<'static>,
    ),
) -> Gauge {
    let gauge_arc = GAUGES
        .entry(hash)
        .or_insert_with(|| {
            let (info, key, metadata) = build();
            let recorder_gauge =
                metrics::with_recorder(|recorder| recorder.register_gauge(&key, &metadata));
            Arc::new(GaugeInner::new(hash, info, key, recorder_gauge))
        })
        .value()
        .clone(); // Arc::clone — cheap reference count bump.
    Gauge(gauge_arc)
}

/// Internal storage for a single gauge metric.
///
/// Held behind an `Arc` so that all handles (`Gauge` clones, thread-local
/// caches, parent extensions with matching labels) point to the same data.
struct GaugeInner {
    /// Static metadata (name, subsystem, description).
    info: &'static MetricInfo,
    /// Full metric key: qualified name + all labels.
    key: metrics::Key,
    /// Recorder-provided gauge handle for the actual recording backend.
    inner: metrics::Gauge,
    /// Shadow atomic that mirrors every mutation so `get()` can read
    /// the current value without querying the recorder.
    shadow: AtomicF64,
    /// Pre-computed cache key used for DashMap lookups, thread-local
    /// comparisons, and the `Hash` / `Eq` impls on `Gauge`.
    hash: u64,
}

impl GaugeInner {
    fn new(hash: u64, info: &'static MetricInfo, key: metrics::Key, inner: metrics::Gauge) -> Self {
        Self {
            info,
            key,
            inner,
            shadow: AtomicF64::new(0.0),
            hash,
        }
    }
}

/// A registered gauge metric backed by [`metrics::Gauge`].
///
/// Created via the `gauge!` macro, either as a base declaration with
/// static labels or as a child that extends a parent's labels at runtime.
///
/// Unlike counters, gauges can go **up and down** — they represent a
/// point-in-time value such as active connections or queue depth.
///
/// Every gauge maintains an `AtomicF64` shadow so that [`get()`](Self::get)
/// can read the current value without querying the recorder. All clones
/// share the same shadow via `Arc<GaugeInner>`.
#[derive(Clone)]
#[repr(transparent)]
pub struct Gauge(Arc<GaugeInner>);

impl std::fmt::Debug for Gauge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Gauge").field("key", &self.0.key).finish()
    }
}

/// Two gauges are equal when they point to the same `Arc` allocation,
/// i.e. they were produced by cloning the same handle. The global
/// `DashMap` guarantees that all call sites with identical name + labels
/// share one `Arc`, so identity equality implies semantic equality.
impl PartialEq for Gauge {
    fn eq(&self, other: &Self) -> bool {
        Arc::as_ptr(&self.0) == Arc::as_ptr(&other.0)
    }
}

impl Eq for Gauge {}

impl std::hash::Hash for Gauge {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.0).hash(state);
    }
}

impl Gauge {
    // NOTE: never call it directly, use the macro instead because it ensures the hash is
    // pre-computed correctly.
    #[doc(hidden)]
    pub fn __new(
        hash: u64,
        info: &'static MetricInfo,
        key: metrics::Key,
        inner: metrics::Gauge,
    ) -> Self {
        Self(Arc::new(GaugeInner::new(hash, info, key, inner)))
    }

    #[doc(hidden)]
    pub fn __info(&self) -> &'static MetricInfo {
        self.0.info
    }

    #[doc(hidden)]
    pub fn __hash(&self) -> u64 {
        self.0.hash
    }

    /// Returns the [`metrics::Key`] (name + labels) this gauge was
    /// registered with.
    pub fn key(&self) -> &metrics::Key {
        &self.0.key
    }

    /// Adds `value` to the current gauge reading and its shadow atomic.
    pub fn inc_by(&self, value: f64) {
        self.0.shadow.fetch_add(value, Ordering::Relaxed);
        self.0.inner.increment(value);
    }

    /// Increments the gauge by 1 and its shadow atomic.
    pub fn inc(&self) {
        self.inc_by(1.0);
    }

    /// Subtracts `value` from the current gauge reading and its shadow atomic.
    pub fn dec_by(&self, value: f64) {
        self.0.shadow.fetch_sub(value, Ordering::Relaxed);
        self.0.inner.decrement(value);
    }

    /// Decrements the gauge by 1 and its shadow atomic.
    pub fn dec(&self) {
        self.dec_by(1.0);
    }

    /// Replaces the current gauge reading and its shadow atomic with `value`.
    pub fn set(&self, value: f64) {
        self.0.shadow.store(value, Ordering::Relaxed);
        self.0.inner.set(value);
    }

    /// Returns the current gauge value from the shadow atomic.
    pub fn get(&self) -> f64 {
        self.0.shadow.load(Ordering::Relaxed)
    }
}

/// Bridges `Gauge` into the `metrics` recorder trait so it can be
/// used wherever a `GaugeFn` is expected.
impl GaugeFn for Gauge {
    fn increment(&self, value: f64) {
        Self::inc_by(self, value);
    }

    fn decrement(&self, value: f64) {
        Self::dec_by(self, value);
    }

    fn set(&self, value: f64) {
        Self::set(self, value);
    }
}

impl Gauge {
    /// Returns a detached gauge with a noop recorder and its own shadow atomic.
    pub fn local() -> Gauge {
        // Static key from empty parts — clone is free (Cow::Borrowed).
        static KEY: metrics::Key = metrics::Key::from_static_parts("", &[]);
        static METADATA: metrics::Metadata<'static> =
            metrics::Metadata::new("local", metrics::Level::DEBUG, None);
        static INFO: MetricInfo = MetricInfo {
            key_name: "",
            description: "local gauge",
            kind: crate::MetricKind::Gauge,
            metadata: &METADATA,
            static_labels: &[],
        };
        // Hash is irrelevant: equality is based on Arc identity (see
        // PartialEq impl), and local gauges never enter the DashMap.
        let inner = GaugeInner::new(0, &INFO, KEY.clone(), metrics::Gauge::noop());
        Gauge(Arc::new(inner))
    }
}

/// RAII guard that tracks increments to a [`Gauge`] and decrements the
/// tracked amount when dropped.
///
/// Useful for tracking in-flight work (connections, requests, etc.)
/// with automatic cleanup on scope exit — even via `?`, `return`, or
/// a panic.
///
/// ```ignore
/// let guard = GaugeGuard::new(&gauge, 1.0);
/// // gauge is incremented by 1.0
/// // ... do work ...
/// // gauge is decremented by 1.0 when guard drops
/// ```
#[derive(Debug)]
pub struct GaugeGuard {
    gauge: Gauge,
    delta: AtomicF64,
}

impl GaugeGuard {
    /// Creates a guard that adds `delta` to `gauge` and tracks it until drop.
    pub fn new(gauge: &Gauge, delta: f64) -> Self {
        if delta != 0.0 {
            gauge.inc_by(delta);
        }
        Self {
            gauge: gauge.clone(),
            delta: AtomicF64::new(delta),
        }
    }

    /// Adds `delta` to the gauge and to the value this guard tracks.
    ///
    /// NOTE: if the cumulative effect of `increment` and `decrement` calls makes the
    /// tracked delta negative, the guard's drop will effectively *increment*
    /// the gauge (subtracting a negative value).
    pub fn increment(&self, delta: f64) {
        self.delta.fetch_add(delta, Ordering::Relaxed);
        self.gauge.inc_by(delta);
    }

    /// Subtracts `delta` from the gauge and from the value this guard tracks.
    ///
    /// NOTE: if the cumulative effect of `increment` and `decrement` calls makes the
    /// tracked delta negative, the guard's drop will effectively *increment*
    /// the gauge (subtracting a negative value).
    pub fn decrement(&self, delta: f64) {
        self.delta.fetch_sub(delta, Ordering::Relaxed);
        self.gauge.dec_by(delta);
    }

    /// Returns the value this guard is tracking.
    pub fn delta(&self) -> f64 {
        self.delta.load(Ordering::Relaxed)
    }
}

impl Drop for GaugeGuard {
    fn drop(&mut self) {
        self.gauge.dec_by(self.delta.load(Ordering::Relaxed));
    }
}

/// Declares or extends a gauge metric.
///
/// # Base declaration
///
/// Creates a new gauge with a static name, description, subsystem, and
/// optional static labels. Every gauge maintains a shadow `AtomicF64`
/// so [`get()`](Gauge::get) always returns the current value.
///
/// ```ignore
/// let g = gauge!(
///     name: "active_connections",
///     description: "Number of currently active HTTP connections",
///     subsystem: "http",
///     "region" => "us-east-1",
/// );
/// g.set(42.0);
/// ```
///
/// # Parent extension
///
/// Derives a child gauge from an existing one, inheriting its name and
/// labels while appending additional (possibly dynamic) labels.
///
/// ```ignore
/// let child = gauge!(parent: base, "method" => method);
/// let guard = GaugeGuard::new(&child, 1.0);
/// ```
#[macro_export]
macro_rules! gauge {
    // Base declaration: all-static name, labels, and key — zero allocations.
    (
        name: $name:literal,
        description: $description:literal,
        subsystem: $subsystem:tt
        $(, $label:literal => $value:literal)* $(,)?
    ) => {{
        $crate::__key_info_metadata!(
            kind: $crate::MetricKind::Gauge,
            name: $name,
            description: $description,
            subsystem: $subsystem
            $(, $label => $value)*
        );
        $crate::__metric_declaration!(
            metric_type: $crate::Gauge,
            register_fn: $crate::__gauge_get_or_register,
            metric_info: &INFO
            $(, $label => $value)*
        )
    }};

    // Parent extension with inline dynamic labels.
    // Derives a child gauge by inheriting the parent's name and labels,
    // appending new (possibly dynamic) key => value pairs.
    (
        parent: $parent:expr,
        $($label:literal => $value:expr),+ $(,)?
    ) => {
        $crate::__metric_extension!(
            metric_type: $crate::Gauge,
            register_fn: $crate::__gauge_get_or_register,
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
            metric_type: $crate::Gauge,
            register_fn: $crate::__gauge_get_or_register,
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

/// A lazily-initialized [`Gauge`].
///
/// The gauge is registered with the recorder on first access.
/// See [`lazy_gauge!`] for the recommended way to construct this type.
pub type LazyGauge = LazyLock<Gauge>;

/// Wraps a [`gauge!`] invocation in a [`LazyGauge`].
///
/// Accepts exactly the same arguments as [`gauge!`] and produces a
/// `LazyGauge` (i.e. `LazyLock<Gauge>`).
///
/// # Example
///
/// ```ignore
/// static ACTIVE_CONNS: LazyGauge = lazy_gauge!(
///     name: "active_connections",
///     description: "Currently active HTTP connections",
///     subsystem: "http",
/// );
///
/// // Equivalent to:
/// static ACTIVE_CONNS: LazyGauge = LazyLock::new(|| {
///     gauge!(
///         name: "active_connections",
///         description: "Currently active HTTP connections",
///         subsystem: "http",
///     )
/// });
/// ```
#[macro_export]
macro_rules! lazy_gauge {
    ($($arg:tt)*) => {
        $crate::LazyGauge::new(|| {
            $crate::gauge!($($arg)*)
        })
    };
}
