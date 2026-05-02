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

//! # quickwit-metrics
//!
//! Type-safe, zero-allocation metric declarations built on top of the
//! [`metrics`](https://docs.rs/metrics) crate.
//!
//! This crate provides [`counter!`], [`gauge!`], and [`histogram!`] macros that
//! produce typed handles ([`Counter`], [`Gauge`], [`Histogram`]) instead of
//! raw strings. All metric names, descriptions, and labels declared at the
//! call site are compiled into statics — the base form performs **zero heap
//! allocations** at runtime.
//!
//! Metric names are composed at compile time as
//! `{SYSTEM}_{subsystem}_{name}`, where [`SYSTEM`] is a library-level
//! constant (currently `"quickwit"`). For example, a counter with
//! `subsystem: "http"` and `name: "requests_total"` produces the final
//! metric name `"quickwit_http_requests_total"`.
//!
//! ## Recommended pattern
//!
//! ### 1. Declare metrics as lazy statics
//!
//! Each metric is defined once, typically at module level, wrapped in a
//! [`LazyLock`](std::sync::LazyLock). The macro registers the metric with
//! the current recorder on first access and returns a typed handle.
//!
//! ```rust,ignore
//! use quickwit_metrics::*;
//! use std::sync::LazyLock;
//!
//! static HTTP_REQUESTS: LazyLock<Counter> = LazyLock::new(|| {
//!     counter!(
//!         name: "requests_total",
//!         description: "Total HTTP requests",
//!         subsystem: "http"
//!     )
//! });
//!
//! static REQUEST_DURATION: LazyLock<Histogram> = LazyLock::new(|| {
//!     histogram!(
//!         name: "request_duration_seconds",
//!         description: "Time spent processing HTTP requests",
//!         subsystem: "http",
//!         buckets: vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
//!     )
//! });
//!
//! static ACTIVE_CONNS: LazyLock<Gauge> = LazyLock::new(|| {
//!     gauge!(
//!         name: "active_connections",
//!         description: "Currently active HTTP connections",
//!         subsystem: "http"
//!     )
//! });
//! ```
//!
//! ### 2. Derive child metrics from a parent
//!
//! Use the `parent:` form to extend an existing metric with additional labels.
//! The child inherits the parent's name and labels, appending new ones.
//! This is the only form that allocates (a single, exactly-sized `Vec`).
//!
//! ```rust,ignore
//! let by_method = counter!(parent: HTTP_REQUESTS, "method" => "GET", "path" => path);
//! by_method.increment(1);
//! ```
//!
//! ### 3. Reusable label templates with `Labels<N>`
//!
//! When several metrics share the same dynamic label names, use [`Labels<N>`]
//! to define the template once and pair it with values at each call site.
//! The resulting [`LabelValues<N>`] is passed via the `labels:` arm of
//! any metric macro.
//!
//! ```rust,ignore
//! use quickwit_metrics::*;
//!
//! const ROUTE: Labels<2> = Labels::new(["method", "path"]);
//!
//! fn on_request(method: &'static str, path: &'static str, duration: f64) {
//!     let route = ROUTE.with_values([method, path]);
//!     histogram!(parent: REQUEST_DURATION, labels: &route).record(duration);
//!     counter!(parent: HTTP_REQUESTS, labels: &route).increment(1);
//! }
//! ```
//!
//! ### 4. Install a recorder and initialize descriptions
//!
//! At the start of `main`, install a recorder (e.g. Prometheus) **before**
//! any metric is accessed. Then call [`describe_metrics()`] to push all descriptions
//! to the recorder. For histograms, iterate over
//! histogram entries collected by [`inventory`] to configure
//! per-metric bucket boundaries on the exporter.
//!
//! ```rust,ignore
//! use metrics_exporter_prometheus::{Matcher, PrometheusBuilder};
//!
//! fn main() {
//!     let mut builder = PrometheusBuilder::new()
//!         .with_http_listener(([127, 0, 0, 1], 9000));
//!
//!     for (name, buckets) in quickwit_metrics::histogram_buckets() {
//!         builder = builder
//!             .set_buckets_for_metric(
//!                 Matcher::Full(name.to_owned()),
//!                 &buckets,
//!             )
//!             .expect("valid buckets");
//!     }
//!
//!     builder.install().expect("failed to install recorder");
//!
//!     quickwit_metrics::describe_metrics();
//!
//!     // ... application code ...
//! }
//! ```
//!
//! ### 5. Use `GaugeGuard` for RAII-based gauge balancing
//!
//! [`GaugeGuard`] increments a gauge on creation and decrements it on drop,
//! which is useful for tracking in-flight resources like active connections.
//!
//! ```rust,ignore
//! {
//!     let mut _guard = GaugeGuard::from_gauge(&ACTIVE_CONNS);
//!     _guard.increment(1.0);
//!     // ... connection is alive here ...
//! }
//! // gauge decremented automatically on drop
//! ```
//!
//! ### 6. Observable metrics
//!
//! All counters and gauges expose a `get()` method. By default
//! (non-observable), `get()` returns a sentinel (`u64::MAX` for counters,
//! `f64::NAN` for gauges). When you need to read the actual metric value
//! in production — for back-pressure, health checks, or conditional
//! logging — add `observable: true`:
//!
//! ```rust,ignore
//! static PENDING: LazyLock<Gauge> = LazyLock::new(|| {
//!     gauge!(
//!         name: "pending_bytes",
//!         description: "Bytes waiting to be flushed",
//!         subsystem: "indexer",
//!         observable: true,
//!     )
//! });
//!
//! PENDING.set(1024.0);
//! assert_eq!(PENDING.get(), 1024.0);
//! ```
//!
//! `get()` always returns a value directly (`u64` for counters, `f64` for
//! gauges). Non-observable metrics return `u64::MAX` / `f64::NAN` and
//! skip the shadow entirely.
//!
//! Under the hood, observable state lives inside the shared
//! `Arc<…Inner>` (e.g. `Arc<CounterInner>`, `Arc<GaugeInner>`). All
//! handles to the same metric (including `parent:` extensions with
//! identical labels) share the same `Arc`, so the shadow atomic is
//! always consistent.
//!
//! Cost per observable mutation: one extra `Relaxed` atomic operation
//! (~0.4 ns). `get()` is a single `Relaxed` load (~0.75 ns).
//!
//! ## Architecture: two-level caching model
//!
//! Every macro invocation resolves the metric handle through a two-level
//! cache, avoiding the cost of key construction and map lookups on the
//! hot path.
//!
//! The diagram below shows three consecutive invocations of the same
//! `counter!(parent: P, "method" => "GET")` call site — first from
//! Thread A (cold start), then from Thread B (cold on B, warm in L2),
//! then from Thread A again (fully warm in L1).
//!
//! ```text
//!   Thread A              │ L2: DashMap (global)  │  Thread B
//!   ──────────────────────│───────────────────────│──────────────────────
//!                         │                       │
//!   ┌─ call 1 ───────────┐│                       │
//!   │ counter!(parent: P,││                       │
//!   │  "method" => "GET")││                       │
//!   └───────┬────────────┘│                       │
//!           │             │                       │
//!           ▼             │                       │
//!   L1 check: empty       │                       │
//!   (first call on A)     │                       │
//!           │             │                       │
//!           │  hash ──────>  L2 lookup: miss      │
//!           │             │       │               │
//!           │             │       ▼               │
//!           │             │  build metrics::Key   │
//!           │             │  register w/ recorder │
//!           │             │  create Arc<Inner>    │
//!           │             │  insert into DashMap  │
//!           │             │       │               │
//!           │  <─ Arc::clone ─────┘               │
//!           │             │                       │
//!           ▼             │                       │
//!   store in L1           │                       │
//!   return clone (~30 ns) │                       │
//!           │             │                       │
//!           │             │                       │
//!           │             │                       │  ┌─ call 2 ───────────┐
//!           │             │                       │  │ counter!(parent: P,│
//!           │             │                       │  │  "method" => "GET")│
//!           │             │                       │  └───────┬────────────┘
//!           │             │                       │          │
//!           │             │                       │          ▼
//!           │             │                       │  L1 check: empty
//!           │             │                       │  (first call on B)
//!           │             │                       │          │
//!           │             │  L2 lookup: hit <──── hash ──────┘
//!           │             │       │               │
//!           │             │  Arc::clone ──────────>
//!           │             │                       │          │
//!           │             │                       │          ▼
//!           │             │                       │  store in L1
//!           │             │                       │  return clone (~8 ns)
//!           │             │                       │          │
//!           │             │                       │          │
//!   ┌─ call 3 ───────────┐│                       │          │
//!   │ counter!(parent: P,││                       │          │
//!   │  "method" => "GET")││                       │          │
//!   └───────┬────────────┘│                       │          │
//!           │             │                       │          │
//!           ▼             │                       │          │
//!   L1 check: hit!        │                       │          │
//!   (hash matches)        │                       │          │
//!           │             │                       │          │
//!           ▼             │                       │          │
//!   return cached clone   │                       │          │
//!   (~3.7 ns, no L2)      │                       │          │
//! ```
//!
//! **L1 — per-call-site `thread_local`:** Each macro expansion gets its own
//! `thread_local!` slot holding the last-seen `(hash, metric)` pair. On the
//! hot path (same labels as the previous call), this is a single `RefCell`
//! borrow + `u64` comparison + `Arc::clone` (~3.7 ns). L2 is never touched.
//!
//! **L2 — global `DashMap`:** On L1 miss (first call on a thread, or labels
//! changed since last call), the macro computes a cheap `u64` hash from the
//! label name/value pairs and probes the shared `DashMap`. If the entry
//! exists, it `Arc::clone`s and populates L1. Only on a full L2 miss does
//! it construct the `metrics::Key`, register with the recorder, and insert.
//!
//! The hash is **order-independent** and **composable** via XOR-folding of
//! per-label `FxHasher` outputs. This means
//! `hash(parent, [A,B]) == hash(hash(parent, [A]), [B])`, which is what
//! makes the `parent:` extension pattern work correctly without rehashing
//! all labels.

#![deny(clippy::disallowed_methods)]

/// System-level prefix prepended to every metric name.
///
/// Every metric declared via [`counter!`], [`gauge!`], or [`histogram!`]
/// has its name composed at compile time as `{SYSTEM}_{subsystem}_{name}`.
///
/// Hardcoded for now — making this configurable is tracked separately.
pub const SYSTEM: &str = "quickwit";

// ─── Metric modules ───
mod counter;
mod gauge;
mod histogram;
#[doc(hidden)]
mod inner;
mod labels;

// ─── Internal helpers (re-exported for macro expansion) ───
//
// These re-exports exist so that downstream crates only need
// `quickwit-metrics` in their Cargo.toml — the macros reference
// them via `$crate::__metrics_*` / `$crate::__inventory_*` paths.
#[doc(hidden)]
pub use counter::__counter_get_or_register;
#[doc(hidden)]
pub use gauge::__gauge_get_or_register;
#[doc(hidden)]
pub use histogram::__histogram_get_or_register;
#[doc(hidden)]
pub use inner::{__concatcp, __key_hash};

// Re-exports of `metrics` and `inventory` used inside macro expansions.
#[doc(hidden)]
pub mod __metrics {
    pub use metrics::*;
}
#[doc(hidden)]
pub mod __inventory {
    pub use inventory::*;
}

// ─── Public types ───
pub use counter::Counter;
pub use gauge::{Gauge, GaugeGuard};
pub use histogram::{Histogram, HistogramConfig, HistogramTimer};
pub use labels::{LabelValues, Labels};
// ─── metrics-rs re-exports ───
pub use metrics::{CounterFn, GaugeFn, HistogramFn};
pub use metrics_util::MetricKind;

// ─── MetricInfo ───
/// Static metadata shared by every metric type (counter, gauge, histogram).
///
/// Collected at link time via [`inventory`] so that all registered metrics can
/// be enumerated without a global registry at runtime.
#[derive(Clone, Copy)]
pub struct MetricInfo {
    /// Fully-qualified metric name composed at compile time as
    /// `{SYSTEM}_{subsystem}_{name}` (e.g. `"quickwit_http_requests_total"`).
    pub key_name: &'static str,
    /// Human-readable description passed to the recorder via `describe_*`.
    pub description: &'static str,
    /// Which `describe_*` method to call for this metric.
    pub kind: MetricKind,
    /// Whether this metric was declared with `observable: true`.
    pub observable: bool,
    /// Recorder metadata capturing the subsystem (target), verbosity level,
    /// and module path where the metric was declared.
    pub metadata: &'static metrics::Metadata<'static>,
    /// Label name/value pairs from the base declaration (compile-time constants).
    /// Parent-extension labels are dynamic and not captured here.
    pub static_labels: &'static [(&'static str, &'static str)],
}

inventory::collect!(MetricInfo);

// ─── Public API ───
/// Describes all registered metrics to the current recorder.
///
/// Call this once after installing your recorder. It iterates every
/// metric registered via [`counter!`], [`gauge!`], or [`histogram!`] and
/// invokes the corresponding `describe_*` method so that exporters
/// (e.g. Prometheus) can expose description text.
pub fn describe_metrics() {
    metrics::with_recorder(|recorder| {
        for info in inventory::iter::<MetricInfo> {
            let key_name = metrics::KeyName::from_const_str(info.key_name);
            let description: metrics::SharedString = info.description.into();
            match info.kind {
                MetricKind::Counter => {
                    recorder.describe_counter(key_name, None, description);
                }
                MetricKind::Gauge => {
                    recorder.describe_gauge(key_name, None, description);
                }
                MetricKind::Histogram => {
                    recorder.describe_histogram(key_name, None, description);
                }
            }
        }
    });
}

/// Returns an iterator over all [`MetricInfo`] entries registered via
/// the [`counter!`], [`gauge!`], or [`histogram!`] macros.
pub fn metrics_info() -> impl Iterator<Item = &'static MetricInfo> {
    inventory::iter::<MetricInfo>.into_iter()
}

/// Returns an iterator of `(name, buckets)` for every histogram registered
/// via the [`histogram!`] macro.
///
/// Use this to configure per-metric bucket boundaries on your exporter
/// before any histogram is accessed.
pub fn histogram_buckets() -> impl Iterator<Item = (&'static str, Vec<f64>)> {
    inventory::iter::<HistogramConfig>
        .into_iter()
        .map(|c| (c.info.key_name, (c.buckets_fn)()))
}
