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

use std::sync::{Arc, LazyLock};

use dashmap::DashMap;
use metrics::HistogramFn;
use quanta::Instant;

use crate::MetricInfo;

/// Extended configuration for a histogram metric, adding bucket boundaries
/// on top of the base [`MetricInfo`].
///
/// Collected at link time via [`inventory`] alongside [`MetricInfo`].
#[doc(hidden)]
#[derive(Clone, Copy)]
pub struct HistogramConfig {
    /// Shared name / subsystem / description metadata.
    pub info: &'static MetricInfo,
    /// Returns the bucket boundaries for this histogram.
    pub buckets_fn: fn() -> Vec<f64>,
}

inventory::collect!(HistogramConfig);

/// Global deduplication map: cache-key hash → shared histogram handle.
///
/// All threads that declare the same metric (same name + labels) share
/// a single `HistogramInner` behind an `Arc`. The `DashMap` entry is
/// created once; subsequent lookups just `Arc::clone` the value.
static HISTOGRAMS: LazyLock<DashMap<u64, Arc<HistogramInner>>> = LazyLock::new(DashMap::new);

/// Looks up or creates a `Histogram` in the global `HISTOGRAMS` map.
///
/// `build` is called at most once per unique `hash` — it constructs
/// the static metadata, key, and recorder handle that live for the
/// lifetime of the process.
///
/// # Recorder-binding invariant
///
/// The `metrics::Histogram` handle returned by `register_histogram` is
/// permanently bound to whichever [`metrics::Recorder`] is active at
/// first-access time. If this function is called before the production
/// recorder is installed (e.g. while the noop default is still active),
/// the cached handle will silently discard all subsequent recordings.
///
/// **Callers must ensure that the global recorder is installed before
/// any metric is first accessed.** In Quickwit this is guaranteed by
/// `init_telemetry()` running before any `LazyLock<Histogram>` is forced.
///
/// Prefer the `histogram!` macro over calling this directly.
#[doc(hidden)]
pub fn __histogram_get_or_register(
    hash: u64,
    build: impl FnOnce() -> (
        &'static HistogramConfig,
        metrics::Key,
        metrics::Metadata<'static>,
    ),
) -> Histogram {
    let histogram_arc = HISTOGRAMS
        .entry(hash)
        .or_insert_with(|| {
            let (config, key, metadata) = build();
            let recorder_histogram =
                metrics::with_recorder(|recorder| recorder.register_histogram(&key, &metadata));
            Arc::new(HistogramInner::new(hash, config, key, recorder_histogram))
        })
        .value()
        .clone(); // Arc::clone — cheap reference count bump.
    Histogram(histogram_arc)
}

/// Internal storage for a single histogram metric.
///
/// Held behind an `Arc` so that all handles (`Histogram` clones, thread-local
/// caches, parent extensions with matching labels) point to the same data.
struct HistogramInner {
    /// Static configuration (name, subsystem, description, buckets).
    info: &'static HistogramConfig,
    /// Full metric key: qualified name + all labels.
    key: metrics::Key,
    /// Recorder-provided histogram handle for the actual recording backend.
    inner: metrics::Histogram,
    /// Pre-computed cache key used for DashMap lookups, thread-local
    /// comparisons, and the `Hash` / `Eq` impls on `Histogram`.
    hash: u64,
}

impl HistogramInner {
    fn new(
        hash: u64,
        info: &'static HistogramConfig,
        key: metrics::Key,
        inner: metrics::Histogram,
    ) -> Self {
        Self {
            info,
            key,
            inner,
            hash,
        }
    }
}

/// A registered histogram metric backed by [`metrics::Histogram`].
///
/// Created via the `histogram!` macro, either as a base declaration with
/// static labels or as a child that extends a parent's labels at runtime.
///
/// Histograms do not support the `observable` flag — they have no shadow
/// atomic and no `get()` method.
#[derive(Clone)]
#[repr(transparent)]
pub struct Histogram(Arc<HistogramInner>);

impl std::fmt::Debug for Histogram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Histogram")
            .field("key", &self.0.key)
            .finish()
    }
}

/// Two histograms are equal when they point to the same `Arc` allocation,
/// i.e. they were produced by cloning the same handle. The global
/// `DashMap` guarantees that all call sites with identical name + labels
/// share one `Arc`, so identity equality implies semantic equality.
impl PartialEq for Histogram {
    fn eq(&self, other: &Self) -> bool {
        Arc::as_ptr(&self.0) == Arc::as_ptr(&other.0)
    }
}

impl Eq for Histogram {}

impl std::hash::Hash for Histogram {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.0).hash(state);
    }
}

impl Histogram {
    // NOTE: never call it directly, use the macro instead because it ensures the hash is
    // pre-computed correctly.
    #[doc(hidden)]
    pub fn __new(
        hash: u64,
        info: &'static HistogramConfig,
        key: metrics::Key,
        inner: metrics::Histogram,
    ) -> Self {
        Self(Arc::new(HistogramInner::new(hash, info, key, inner)))
    }

    #[doc(hidden)]
    pub fn __info(&self) -> &'static HistogramConfig {
        self.0.info
    }

    #[doc(hidden)]
    pub fn __hash(&self) -> u64 {
        self.0.hash
    }

    /// Returns the [`metrics::Key`] (name + labels) this histogram was
    /// registered with.
    pub fn key(&self) -> &metrics::Key {
        &self.0.key
    }

    /// Records a single observation.
    pub fn observe(&self, value: f64) {
        self.0.inner.record(value);
    }
}

/// Bridges `Histogram` into the `metrics` recorder trait so it can be
/// used wherever a `HistogramFn` is expected.
impl HistogramFn for Histogram {
    fn record(&self, value: f64) {
        Self::observe(self, value);
    }
}

/// RAII timer that records elapsed wall-clock time into a [`Histogram`].
#[derive(Debug)]
pub struct HistogramTimer {
    histogram: Histogram,
    start: Instant,
    observed: bool,
}

impl HistogramTimer {
    /// Starts a timer that records the elapsed time in seconds when dropped.
    pub fn new(histogram: &Histogram) -> Self {
        Self {
            histogram: histogram.clone(),
            start: Instant::now(),
            observed: false,
        }
    }

    /// Records the elapsed duration immediately.
    ///
    /// The timer is consumed so the duration is not recorded again on drop.
    pub fn observe_duration(self) {
        let mut timer = self;
        timer.observed = true;
        timer.histogram.observe(timer.start.elapsed().as_secs_f64());
    }
}

impl Drop for HistogramTimer {
    fn drop(&mut self) {
        if !self.observed {
            self.histogram.observe(self.start.elapsed().as_secs_f64());
        }
    }
}

/// Declares or extends a histogram metric.
///
/// # Base declaration
///
/// Creates a new histogram with a static name, description, subsystem,
/// bucket boundaries, and optional static labels. All parts are `'static`,
/// so the base form performs **zero heap allocations**.
///
/// ```ignore
/// let h = histogram!(
///     name: "request_duration_seconds",
///     description: "Time spent processing HTTP requests",
///     subsystem: "http",
///     buckets: vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
///     "env" => "prod",
/// );
/// h.record(0.042);
/// ```
///
/// # Parent extension
///
/// Derives a child histogram from an existing one, inheriting its name and
/// labels while appending additional (possibly dynamic) labels.
///
/// ```ignore
/// let child = histogram!(parent: base, "method" => method, "path" => path);
/// child.record(duration);
/// ```
#[macro_export]
macro_rules! histogram {
    // Base declaration with explicit separator, system, and subsystem prefix - zero allocations.
    (
        name: $name:literal,
        description: $description:literal,
        system: $system:expr,
        subsystem: $subsystem:expr,
        separator: $separator:expr,
        buckets: $buckets:expr
        $(, $label:literal => $value:literal)* $(,)?
    ) => {{
        $crate::__key_info_metadata!(
            kind: $crate::MetricKind::Histogram,
            name: $name,
            description: $description,
            system: $system,
            subsystem: $subsystem,
            separator: $separator
            $(, $label => $value)*
        );
        static HISTOGRAM_CONFIG: $crate::HistogramConfig = $crate::HistogramConfig {
            info: &INFO,
            buckets_fn: || $buckets,
        };
        $crate::__inventory::submit!(HISTOGRAM_CONFIG);
        $crate::__metric_declaration!(
            metric_type: $crate::Histogram,
            register_fn: $crate::__histogram_get_or_register,
            metric_info: &HISTOGRAM_CONFIG
            $(, $label => $value)*
        )
    }};

    // Base declaration with explicit system and subsystem prefix - zero allocations.
    (
        name: $name:literal,
        description: $description:literal,
        system: $system:expr,
        subsystem: $subsystem:expr,
        buckets: $buckets:expr
        $(, $label:literal => $value:literal)* $(,)?
    ) => {{
        $crate::histogram!(
            name: $name,
            description: $description,
            system: $system,
            subsystem: $subsystem,
            separator: $crate::SEPARATOR,
            buckets: $buckets
            $(, $label => $value)*
        )
    }};

    // Base declaration with subsystem only — system defaults to SYSTEM.
    (
        name: $name:literal,
        description: $description:literal,
        subsystem: $subsystem:expr,
        buckets: $buckets:expr
        $(, $label:literal => $value:literal)* $(,)?
    ) => {{
        $crate::histogram!(
            name: $name,
            description: $description,
            system: $crate::SYSTEM,
            subsystem: $subsystem,
            buckets: $buckets
            $(, $label => $value)*
        )
    }};

    // Parent extension with inline dynamic labels.
    // Derives a child histogram by inheriting the parent's name and labels,
    // appending new (possibly dynamic) key => value pairs.
    (
        parent: $parent:expr,
        $($label:literal => $value:expr),+ $(,)?
    ) => {
        $crate::__metric_extension!(
            metric_type: $crate::Histogram,
            register_fn: $crate::__histogram_get_or_register,
            parent: $parent,
            // Unwrap HistogramConfig -> MetricInfo for the extension.
            metric_info: $parent.__info().info,
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
            metric_type: $crate::Histogram,
            register_fn: $crate::__histogram_get_or_register,
            parent: $parent,
            metric_info: $parent.__info().info,
            hash: $crate::__key_hash(
                $parent.__hash(),
                std::iter::empty()$(.chain($labels.iter()))+,
            ),
            label_count: 0usize $(+ $labels.len())+,
            labels_iter: std::iter::empty()$(.chain($labels.__to_labels()))+
        )
    };
}

/// A lazily-initialized [`Histogram`].
///
/// The histogram is registered with the recorder on first access.
/// See [`lazy_histogram!`][macro@crate::lazy_histogram] for the recommended way to construct this
/// type.
pub type LazyHistogram = LazyLock<Histogram>;

/// Wraps a [`histogram!`] invocation in a [`LazyHistogram`].
///
/// Accepts exactly the same arguments as [`histogram!`] and produces a
/// `LazyHistogram` (i.e. `LazyLock<Histogram>`).
///
/// # Example
///
/// ```ignore
/// static REQUEST_DURATION: LazyHistogram = lazy_histogram!(
///     name: "request_duration_seconds",
///     description: "Time spent processing HTTP requests",
///     subsystem: "http",
///     buckets: vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
/// );
///
/// // Equivalent to:
/// static REQUEST_DURATION: LazyHistogram = LazyLock::new(|| {
///     histogram!(
///         name: "request_duration_seconds",
///         description: "Time spent processing HTTP requests",
///         subsystem: "http",
///         buckets: vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
///     )
/// });
/// ```
#[macro_export]
macro_rules! lazy_histogram {
    ($($arg:tt)*) => {
        $crate::LazyHistogram::new(|| {
            $crate::histogram!($($arg)*)
        })
    };
}
