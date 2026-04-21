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

use std::collections::BTreeMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, OnceLock};

use dashmap::DashMap;
use fnv::FnvHasher;
use metrics_exporter_prometheus::PrometheusHandle;
pub use prometheus::{exponential_buckets, linear_buckets};

/// Cache keys come from `hash_label_values` (FNV), so they're already
/// well-distributed u64 hashes. Routing them through `DashMap`'s default
/// `SipHasher` would just rehash them on every lookup; this hasher returns
/// the key verbatim to skip that redundant work.
#[derive(Default)]
struct NoHashHasher(u64);

impl Hasher for NoHashHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, _bytes: &[u8]) {
        unreachable!("NoHashHasher only supports write_u64");
    }

    fn write_u64(&mut self, i: u64) {
        self.0 = i;
    }
}

type BuildNoHashHasher = BuildHasherDefault<NoHashHasher>;

fn hash_label_values(values: &[&str]) -> u64 {
    let mut h = FnvHasher::default();
    for v in values {
        // Length prefix prevents collisions when label boundaries shift,
        // e.g. ["ab", "c"] vs ["a", "bc"].
        Hasher::write_u64(&mut h, v.len() as u64);
        Hasher::write(&mut h, v.as_bytes());
    }
    Hasher::finish(&h)
}

fn get_or_insert_cached<T: Clone>(
    cache: &DashMap<u64, T, BuildNoHashHasher>,
    label_values: &[&str],
    build: impl FnOnce() -> T,
) -> T {
    let hash = hash_label_values(label_values);
    cache.entry(hash).or_insert_with(build).value().clone()
}

fn atomic_f64_set(atomic: &AtomicU64, val: f64) {
    atomic.store(val.to_bits(), Ordering::Relaxed);
}

fn atomic_f64_get(atomic: &AtomicU64) -> f64 {
    f64::from_bits(atomic.load(Ordering::Relaxed))
}

fn atomic_f64_add(atomic: &AtomicU64, delta: f64) {
    let _ = atomic.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
        Some((f64::from_bits(old) + delta).to_bits())
    });
}

fn make_metric_name(name: &str, subsystem: &str) -> String {
    if subsystem.is_empty() {
        format!("quickwit_{name}")
    } else {
        format!("quickwit_{subsystem}_{name}")
    }
}

fn make_const_labels(const_labels: &[(&str, &str)]) -> Vec<metrics::Label> {
    const_labels
        .iter()
        .map(|(k, v)| metrics::Label::new(k.to_string(), v.to_string()))
        .collect()
}

fn compose_labels<const N: usize>(
    const_labels: &[metrics::Label],
    label_names: &[String; N],
    label_values: &[&str; N],
) -> Vec<metrics::Label> {
    let mut labels = Vec::with_capacity(const_labels.len() + N);
    labels.extend_from_slice(const_labels);
    for (name, value) in label_names.iter().zip(label_values.iter()) {
        labels.push(metrics::Label::new(name.clone(), value.to_string()));
    }
    labels
}

#[derive(Clone, Debug)]
pub struct IntCounter {
    inner: metrics::Counter,
    value: Arc<AtomicU64>,
}

impl IntCounter {
    /// Constructs a standalone counter that is NOT registered with the global recorder.
    pub fn new(_name: &str, _help: &str) -> Result<Self, String> {
        Ok(Self {
            inner: metrics::Counter::noop(),
            value: Arc::new(AtomicU64::new(0)),
        })
    }

    pub fn inc(&self) {
        self.inc_by(1);
    }

    pub fn inc_by(&self, v: u64) {
        self.inner.increment(v);
        self.value.fetch_add(v, Ordering::Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}

#[derive(Clone, Debug)]
pub struct IntGauge {
    inner: metrics::Gauge,
    value: Arc<AtomicI64>,
}

impl IntGauge {
    pub fn set(&self, v: i64) {
        self.inner.set(v as f64);
        self.value.store(v, Ordering::Relaxed);
    }

    pub fn inc(&self) {
        self.add(1);
    }

    pub fn dec(&self) {
        self.sub(1);
    }

    pub fn add(&self, v: i64) {
        self.inner.increment(v as f64);
        self.value.fetch_add(v, Ordering::Relaxed);
    }

    pub fn sub(&self, v: i64) {
        self.inner.decrement(v as f64);
        self.value.fetch_sub(v, Ordering::Relaxed);
    }

    pub fn get(&self) -> i64 {
        self.value.load(Ordering::Relaxed)
    }
}

#[derive(Clone, Debug)]
pub struct Gauge {
    inner: metrics::Gauge,
    value: Arc<AtomicU64>,
}

impl Gauge {
    pub fn set(&self, v: f64) {
        self.inner.set(v);
        atomic_f64_set(&self.value, v);
    }

    pub fn inc(&self) {
        self.add(1.0);
    }

    pub fn dec(&self) {
        self.sub(1.0);
    }

    pub fn add(&self, v: f64) {
        self.inner.increment(v);
        atomic_f64_add(&self.value, v);
    }

    pub fn sub(&self, v: f64) {
        self.inner.decrement(v);
        atomic_f64_add(&self.value, -v);
    }

    pub fn get(&self) -> f64 {
        atomic_f64_get(&self.value)
    }
}

#[derive(Clone, Debug)]
pub struct Histogram {
    inner: metrics::Histogram,
}

impl Histogram {
    pub fn observe(&self, v: f64) {
        self.inner.record(v);
    }

    pub fn start_timer(&self) -> HistogramTimer {
        HistogramTimer {
            histogram: Some(self.clone()),
            start: std::time::Instant::now(),
        }
    }
}

pub struct HistogramTimer {
    histogram: Option<Histogram>,
    start: std::time::Instant,
}

impl HistogramTimer {
    pub fn observe_duration(mut self) {
        if let Some(histogram) = self.histogram.take() {
            histogram.observe(self.start.elapsed().as_secs_f64());
        }
    }
}

impl Drop for HistogramTimer {
    fn drop(&mut self) {
        if let Some(histogram) = self.histogram.take() {
            histogram.observe(self.start.elapsed().as_secs_f64());
        }
    }
}

#[derive(Clone)]
pub struct IntCounterVec<const N: usize> {
    name: String,
    label_names: [String; N],
    const_labels: Vec<metrics::Label>,
    cache: Arc<DashMap<u64, IntCounter, BuildNoHashHasher>>,
}

impl<const N: usize> IntCounterVec<N> {
    pub fn new(
        name: &str,
        help: &str,
        subsystem: &str,
        const_labels: &[(&str, &str)],
        label_names: [&str; N],
    ) -> IntCounterVec<N> {
        let full_name = make_metric_name(name, subsystem);
        metrics::describe_counter!(full_name.clone(), help.to_string());
        IntCounterVec {
            name: full_name,
            label_names: label_names.map(|s| s.to_string()),
            const_labels: make_const_labels(const_labels),
            cache: Arc::new(DashMap::with_hasher(BuildNoHashHasher::default())),
        }
    }

    pub fn with_label_values(&self, label_values: [&str; N]) -> IntCounter {
        get_or_insert_cached(&self.cache, &label_values, || {
            let labels = compose_labels(&self.const_labels, &self.label_names, &label_values);
            IntCounter {
                inner: metrics::counter!(self.name.clone(), labels),
                value: Arc::new(AtomicU64::new(0)),
            }
        })
    }
}

#[derive(Clone)]
pub struct IntGaugeVec<const N: usize> {
    name: String,
    label_names: [String; N],
    const_labels: Vec<metrics::Label>,
    cache: Arc<DashMap<u64, IntGauge, BuildNoHashHasher>>,
}

impl<const N: usize> IntGaugeVec<N> {
    pub fn with_label_values(&self, label_values: [&str; N]) -> IntGauge {
        get_or_insert_cached(&self.cache, &label_values, || {
            let labels = compose_labels(&self.const_labels, &self.label_names, &label_values);
            IntGauge {
                inner: metrics::gauge!(self.name.clone(), labels),
                value: Arc::new(AtomicI64::new(0)),
            }
        })
    }
}

#[derive(Clone)]
pub struct HistogramVec<const N: usize> {
    name: String,
    label_names: [String; N],
    const_labels: Vec<metrics::Label>,
    cache: Arc<DashMap<u64, Histogram, BuildNoHashHasher>>,
}

impl<const N: usize> HistogramVec<N> {
    pub fn with_label_values(&self, label_values: [&str; N]) -> Histogram {
        get_or_insert_cached(&self.cache, &label_values, || {
            let labels = compose_labels(&self.const_labels, &self.label_names, &label_values);
            Histogram {
                inner: metrics::histogram!(self.name.clone(), labels),
            }
        })
    }
}

pub struct GaugeGuard<'a> {
    gauge: &'a IntGauge,
    delta: i64,
}

impl std::fmt::Debug for GaugeGuard<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.delta.fmt(f)
    }
}

impl<'a> GaugeGuard<'a> {
    pub fn from_gauge(gauge: &'a IntGauge) -> Self {
        Self { gauge, delta: 0i64 }
    }

    pub fn get(&self) -> i64 {
        self.delta
    }

    pub fn add(&mut self, delta: i64) {
        self.gauge.add(delta);
        self.delta += delta;
    }

    pub fn sub(&mut self, delta: i64) {
        self.gauge.sub(delta);
        self.delta -= delta;
    }
}

impl Drop for GaugeGuard<'_> {
    fn drop(&mut self) {
        self.gauge.sub(self.delta)
    }
}

pub struct OwnedGaugeGuard {
    gauge: IntGauge,
    delta: i64,
}

impl std::fmt::Debug for OwnedGaugeGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.delta.fmt(f)
    }
}

impl OwnedGaugeGuard {
    pub fn from_gauge(gauge: IntGauge) -> Self {
        Self { gauge, delta: 0i64 }
    }

    pub fn get(&self) -> i64 {
        self.delta
    }

    pub fn add(&mut self, delta: i64) {
        self.gauge.add(delta);
        self.delta += delta;
    }

    pub fn sub(&mut self, delta: i64) {
        self.gauge.sub(delta);
        self.delta -= delta;
    }
}

impl Drop for OwnedGaugeGuard {
    fn drop(&mut self) {
        self.gauge.sub(self.delta)
    }
}

/// Static registration entry for a histogram. One `HistogramConfig` is submitted per histogram
/// declaration via the `define_histogram!` / `define_histogram_vec!` macros; at recorder install
/// time the full set is read via `inventory::iter::<HistogramConfig>` and applied to the
/// Prometheus / OTLP exporters.
pub struct HistogramConfig {
    pub name: &'static str,
    pub subsystem: &'static str,
    pub help: &'static str,
    pub buckets_fn: fn() -> Vec<f64>,
}

impl HistogramConfig {
    pub fn full_name(&self) -> String {
        make_metric_name(self.name, self.subsystem)
    }
}

inventory::collect!(HistogramConfig);

static PROM_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

pub fn set_prom_handle(handle: PrometheusHandle) {
    let _ = PROM_HANDLE.set(handle);
}

pub fn render_prometheus_text() -> String {
    match PROM_HANDLE.get() {
        Some(handle) => handle.render(),
        None => String::new(),
    }
}

pub fn register_info(name: &'static str, help: &'static str, kvs: BTreeMap<&'static str, String>) {
    let full_name = format!("quickwit_{name}");
    metrics::describe_counter!(full_name.clone(), help.to_string());
    let labels: Vec<metrics::Label> = kvs
        .into_iter()
        .map(|(k, v)| metrics::Label::new(k.to_string(), v))
        .collect();
    metrics::counter!(full_name, labels).increment(1);
}

pub fn new_counter(
    name: &str,
    help: &str,
    subsystem: &str,
    const_labels: &[(&str, &str)],
) -> IntCounter {
    let full_name = make_metric_name(name, subsystem);
    metrics::describe_counter!(full_name.clone(), help.to_string());
    let labels = make_const_labels(const_labels);
    IntCounter {
        inner: metrics::counter!(full_name, labels),
        value: Arc::new(AtomicU64::new(0)),
    }
}

pub fn new_counter_vec<const N: usize>(
    name: &str,
    help: &str,
    subsystem: &str,
    const_labels: &[(&str, &str)],
    label_names: [&str; N],
) -> IntCounterVec<N> {
    IntCounterVec::new(name, help, subsystem, const_labels, label_names)
}

pub fn new_float_gauge(
    name: &str,
    help: &str,
    subsystem: &str,
    const_labels: &[(&str, &str)],
) -> Gauge {
    let full_name = make_metric_name(name, subsystem);
    metrics::describe_gauge!(full_name.clone(), help.to_string());
    let labels = make_const_labels(const_labels);
    Gauge {
        inner: metrics::gauge!(full_name, labels),
        value: Arc::new(AtomicU64::new(0f64.to_bits())),
    }
}

pub fn new_gauge(
    name: &str,
    help: &str,
    subsystem: &str,
    const_labels: &[(&str, &str)],
) -> IntGauge {
    let full_name = make_metric_name(name, subsystem);
    metrics::describe_gauge!(full_name.clone(), help.to_string());
    let labels = make_const_labels(const_labels);
    IntGauge {
        inner: metrics::gauge!(full_name, labels),
        value: Arc::new(AtomicI64::new(0)),
    }
}

pub fn new_gauge_vec<const N: usize>(
    name: &str,
    help: &str,
    subsystem: &str,
    const_labels: &[(&str, &str)],
    label_names: [&str; N],
) -> IntGaugeVec<N> {
    let full_name = make_metric_name(name, subsystem);
    metrics::describe_gauge!(full_name.clone(), help.to_string());
    IntGaugeVec {
        name: full_name,
        label_names: label_names.map(|s| s.to_string()),
        const_labels: make_const_labels(const_labels),
        cache: Arc::new(DashMap::with_hasher(BuildNoHashHasher::default())),
    }
}

/// Implementation details exposed only for the `define_histogram!` / `define_histogram_vec!`
/// macros. Do not call these items directly.
#[doc(hidden)]
pub mod __private {
    use std::sync::Arc;

    use dashmap::DashMap;

    use super::{BuildNoHashHasher, Histogram, HistogramVec, make_const_labels, make_metric_name};

    pub fn new_histogram(name: &str, subsystem: &str) -> Histogram {
        let full_name = make_metric_name(name, subsystem);
        Histogram {
            inner: metrics::histogram!(full_name),
        }
    }

    pub fn new_histogram_vec<const N: usize>(
        name: &str,
        subsystem: &str,
        const_labels: &[(&str, &str)],
        label_names: [&str; N],
    ) -> HistogramVec<N> {
        HistogramVec {
            name: make_metric_name(name, subsystem),
            label_names: label_names.map(|s| s.to_string()),
            const_labels: make_const_labels(const_labels),
            cache: Arc::new(DashMap::with_hasher(BuildNoHashHasher::default())),
        }
    }
}

#[derive(Clone)]
pub struct MemoryMetrics {
    pub active_bytes: IntGauge,
    pub allocated_bytes: IntGauge,
    pub resident_bytes: IntGauge,
    pub in_flight: InFlightDataGauges,
}

impl Default for MemoryMetrics {
    fn default() -> Self {
        Self {
            active_bytes: new_gauge(
                "active_bytes",
                "Total number of bytes in active pages allocated by the application, as reported \
                 by jemalloc `stats.active`.",
                "memory",
                &[],
            ),
            allocated_bytes: new_gauge(
                "allocated_bytes",
                "Total number of bytes allocated by the application, as reported by jemalloc \
                 `stats.allocated`.",
                "memory",
                &[],
            ),
            resident_bytes: new_gauge(
                "resident_bytes",
                " Total number of bytes in physically resident data pages mapped by the \
                 allocator, as reported by jemalloc `stats.resident`.",
                "memory",
                &[],
            ),
            in_flight: InFlightDataGauges::default(),
        }
    }
}

#[derive(Clone)]
pub struct InFlightDataGauges {
    pub rest_server: IntGauge,
    pub ingest_router: IntGauge,
    pub ingester_persist: IntGauge,
    pub ingester_replicate: IntGauge,
    pub wal: IntGauge,
    pub fetch_stream: IntGauge,
    pub multi_fetch_stream: IntGauge,
    pub doc_processor_mailbox: IntGauge,
    pub indexer_mailbox: IntGauge,
    pub index_writer: IntGauge,
    in_flight_gauge_vec: IntGaugeVec<1>,
}

impl Default for InFlightDataGauges {
    fn default() -> Self {
        let in_flight_gauge_vec = new_gauge_vec(
            "in_flight_data_bytes",
            "Amount of data in-flight in various buffers in bytes.",
            "memory",
            &[],
            ["component"],
        );
        Self {
            rest_server: in_flight_gauge_vec.with_label_values(["rest_server"]),
            ingest_router: in_flight_gauge_vec.with_label_values(["ingest_router"]),
            ingester_persist: in_flight_gauge_vec.with_label_values(["ingester_persist"]),
            ingester_replicate: in_flight_gauge_vec.with_label_values(["ingester_replicate"]),
            wal: in_flight_gauge_vec.with_label_values(["wal"]),
            fetch_stream: in_flight_gauge_vec.with_label_values(["fetch_stream"]),
            multi_fetch_stream: in_flight_gauge_vec.with_label_values(["multi_fetch_stream"]),
            doc_processor_mailbox: in_flight_gauge_vec.with_label_values(["doc_processor_mailbox"]),
            indexer_mailbox: in_flight_gauge_vec.with_label_values(["indexer_mailbox"]),
            index_writer: in_flight_gauge_vec.with_label_values(["index_writer"]),
            in_flight_gauge_vec: in_flight_gauge_vec.clone(),
        }
    }
}

impl InFlightDataGauges {
    #[inline]
    pub fn file(&self) -> &IntGauge {
        static GAUGE: OnceLock<IntGauge> = OnceLock::new();
        GAUGE.get_or_init(|| self.in_flight_gauge_vec.with_label_values(["file_source"]))
    }

    #[inline]
    pub fn ingest(&self) -> &IntGauge {
        static GAUGE: OnceLock<IntGauge> = OnceLock::new();
        GAUGE.get_or_init(|| {
            self.in_flight_gauge_vec
                .with_label_values(["ingest_source"])
        })
    }

    #[inline]
    pub fn kafka(&self) -> &IntGauge {
        static GAUGE: OnceLock<IntGauge> = OnceLock::new();
        GAUGE.get_or_init(|| self.in_flight_gauge_vec.with_label_values(["kafka_source"]))
    }

    #[inline]
    pub fn kinesis(&self) -> &IntGauge {
        static GAUGE: OnceLock<IntGauge> = OnceLock::new();
        GAUGE.get_or_init(|| {
            self.in_flight_gauge_vec
                .with_label_values(["kinesis_source"])
        })
    }

    #[inline]
    pub fn pubsub(&self) -> &IntGauge {
        static GAUGE: OnceLock<IntGauge> = OnceLock::new();
        GAUGE.get_or_init(|| {
            self.in_flight_gauge_vec
                .with_label_values(["pubsub_source"])
        })
    }

    #[inline]
    pub fn pulsar(&self) -> &IntGauge {
        static GAUGE: OnceLock<IntGauge> = OnceLock::new();
        GAUGE.get_or_init(|| {
            self.in_flight_gauge_vec
                .with_label_values(["pulsar_source"])
        })
    }

    #[inline]
    pub fn other(&self) -> &IntGauge {
        static GAUGE: OnceLock<IntGauge> = OnceLock::new();
        GAUGE.get_or_init(|| {
            self.in_flight_gauge_vec
                .with_label_values(["pulsar_source"])
        })
    }
}

/// This function returns `index_id` as is if per-index metrics are enabled, or projects it to
/// `"__any__"` otherwise.
pub fn index_label(index_id: &str) -> &str {
    static PER_INDEX_METRICS_ENABLED: LazyLock<bool> =
        LazyLock::new(|| !crate::get_bool_from_env("QW_DISABLE_PER_INDEX_METRICS", false));

    if *PER_INDEX_METRICS_ENABLED {
        index_id
    } else {
        "__any__"
    }
}

pub static MEMORY_METRICS: LazyLock<MemoryMetrics> = LazyLock::new(MemoryMetrics::default);

/// Declares a histogram with compile-time static registration.
#[macro_export]
macro_rules! define_histogram {
    (
        $static_name:ident,
        name: $name:literal,
        help: $help:literal,
        subsystem: $subsystem:literal,
        buckets: $buckets:expr $(,)?
    ) => {
        $crate::inventory::submit! {
            $crate::metrics::HistogramConfig {
                name: $name,
                subsystem: $subsystem,
                help: $help,
                buckets_fn: || $buckets,
            }
        }

        pub static $static_name: ::std::sync::LazyLock<$crate::metrics::Histogram> =
            ::std::sync::LazyLock::new(|| {
                $crate::metrics::__private::new_histogram($name, $subsystem)
            });
    };
}

/// Declares a labeled histogram (`HistogramVec<N>`) with compile-time static registration.
#[macro_export]
macro_rules! define_histogram_vec {
    (
        $static_name:ident,
        name: $name:literal,
        help: $help:literal,
        subsystem: $subsystem:literal,
        const_labels: [$(($ck:literal, $cv:literal)),* $(,)?],
        labels: [$($label:literal),+ $(,)?],
        buckets: $buckets:expr $(,)?
    ) => {
        $crate::inventory::submit! {
            $crate::metrics::HistogramConfig {
                name: $name,
                subsystem: $subsystem,
                help: $help,
                buckets_fn: || $buckets,
            }
        }

        pub static $static_name: ::std::sync::LazyLock<
            $crate::metrics::HistogramVec<{ [$($label),+].len() }>,
        > = ::std::sync::LazyLock::new(|| {
            $crate::metrics::__private::new_histogram_vec(
                $name,
                $subsystem,
                &[$(($ck, $cv)),*],
                [$($label),+],
            )
        });
    };
}

#[cfg(test)]
mod tests {
    use metrics_util::MetricKind;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshot};

    use super::*;

    type LabelPair = (String, String);

    fn with_test_recorder<F, R>(f: F) -> (R, Snapshot)
    where F: FnOnce() -> R {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let result = metrics::with_local_recorder(&recorder, f);
        (result, snapshotter.snapshot())
    }

    fn entries_for(
        snapshot: Snapshot,
        name: &str,
    ) -> Vec<(MetricKind, Vec<LabelPair>, DebugValue)> {
        snapshot
            .into_vec()
            .into_iter()
            .filter(|(ck, _, _, _)| ck.key().name() == name)
            .map(|(ck, _, _, value)| {
                let labels: Vec<LabelPair> = ck
                    .key()
                    .labels()
                    .map(|l| (l.key().to_string(), l.value().to_string()))
                    .collect();
                (ck.kind(), labels, value)
            })
            .collect()
    }

    fn single_counter(snapshot: Snapshot, name: &str) -> u64 {
        let entries = entries_for(snapshot, name);
        assert_eq!(entries.len(), 1, "expected one counter entry for {name}");
        let (kind, _, value) = entries.into_iter().next().unwrap();
        assert_eq!(kind, MetricKind::Counter);
        match value {
            DebugValue::Counter(v) => v,
            other => panic!("expected counter for {name}, got {other:?}"),
        }
    }

    fn single_gauge(snapshot: Snapshot, name: &str) -> f64 {
        let entries = entries_for(snapshot, name);
        assert_eq!(entries.len(), 1, "expected one gauge entry for {name}");
        let (kind, _, value) = entries.into_iter().next().unwrap();
        assert_eq!(kind, MetricKind::Gauge);
        match value {
            DebugValue::Gauge(v) => v.into_inner(),
            other => panic!("expected gauge for {name}, got {other:?}"),
        }
    }

    fn single_histogram(snapshot: Snapshot, name: &str) -> Vec<f64> {
        let entries = entries_for(snapshot, name);
        assert_eq!(entries.len(), 1, "expected one histogram entry for {name}");
        let (kind, _, value) = entries.into_iter().next().unwrap();
        assert_eq!(kind, MetricKind::Histogram);
        match value {
            DebugValue::Histogram(v) => v.into_iter().map(|f| f.into_inner()).collect(),
            other => panic!("expected histogram for {name}, got {other:?}"),
        }
    }

    fn counter_by_labels(snapshot: Snapshot, name: &str) -> Vec<(Vec<LabelPair>, u64)> {
        entries_for(snapshot, name)
            .into_iter()
            .map(|(kind, labels, value)| {
                assert_eq!(kind, MetricKind::Counter);
                let v = match value {
                    DebugValue::Counter(v) => v,
                    other => panic!("expected counter, got {other:?}"),
                };
                (labels, v)
            })
            .collect()
    }

    fn gauge_by_labels(snapshot: Snapshot, name: &str) -> Vec<(Vec<LabelPair>, f64)> {
        entries_for(snapshot, name)
            .into_iter()
            .map(|(kind, labels, value)| {
                assert_eq!(kind, MetricKind::Gauge);
                let v = match value {
                    DebugValue::Gauge(v) => v.into_inner(),
                    other => panic!("expected gauge, got {other:?}"),
                };
                (labels, v)
            })
            .collect()
    }

    fn histogram_by_labels(snapshot: Snapshot, name: &str) -> Vec<(Vec<LabelPair>, Vec<f64>)> {
        entries_for(snapshot, name)
            .into_iter()
            .map(|(kind, labels, value)| {
                assert_eq!(kind, MetricKind::Histogram);
                let v = match value {
                    DebugValue::Histogram(v) => v.into_iter().map(|f| f.into_inner()).collect(),
                    other => panic!("expected histogram, got {other:?}"),
                };
                (labels, v)
            })
            .collect()
    }

    fn has_label(labels: &[LabelPair], key: &str, value: &str) -> bool {
        labels.iter().any(|(k, v)| k == key && v == value)
    }

    #[test]
    fn int_counter() {
        let (counter, snapshot) = with_test_recorder(|| {
            let counter = new_counter("requests_total", "help", "test", &[]);
            counter.inc();
            counter.inc_by(5);
            counter
        });
        assert_eq!(counter.get(), 6);
        assert_eq!(single_counter(snapshot, "quickwit_test_requests_total"), 6);
    }

    #[test]
    fn int_gauge() {
        let (gauge, snapshot) = with_test_recorder(|| {
            let gauge = new_gauge("connections", "help", "test", &[]);
            gauge.set(10);
            gauge.add(5);
            gauge.sub(3);
            gauge.inc();
            gauge.dec();
            gauge
        });
        assert_eq!(gauge.get(), 12);
        assert_eq!(single_gauge(snapshot, "quickwit_test_connections"), 12f64);
    }

    #[test]
    fn float_gauge() {
        let (gauge, snapshot) = with_test_recorder(|| {
            let gauge = new_float_gauge("load_factor", "help", "test", &[]);
            gauge.set(2.5);
            gauge.add(1.25);
            gauge.sub(0.5);
            gauge.inc();
            gauge.dec();
            gauge
        });
        assert_eq!(gauge.get(), 3.25);
        assert_eq!(single_gauge(snapshot, "quickwit_test_load_factor"), 3.25);
    }

    #[test]
    fn histogram_observe() {
        let (_, snapshot) = with_test_recorder(|| {
            let h = __private::new_histogram("latency_seconds", "test");
            h.observe(0.1);
            h.observe(0.25);
            h.observe(1.5);
        });
        let values = single_histogram(snapshot, "quickwit_test_latency_seconds");
        assert_eq!(values.len(), 3);
        assert_eq!(values[0], 0.1);
        assert_eq!(values[1], 0.25);
        assert_eq!(values[2], 1.5);
    }

    #[test]
    fn histogram_timer() {
        let (_, snapshot) = with_test_recorder(|| {
            // start_timer() + implicit drop records one observation.
            let drop_h = __private::new_histogram("drop_timer_seconds", "test");
            {
                let _timer = drop_h.start_timer();
            }
            // observe_duration() consumes the timer; the subsequent drop must
            // NOT record a second observation.
            let explicit_h = __private::new_histogram("explicit_timer_seconds", "test");
            let timer = explicit_h.start_timer();
            timer.observe_duration();
        });
        let entries = snapshot.into_vec();
        let histogram_len = |name: &str| -> usize {
            let entry = entries
                .iter()
                .find(|(ck, _, _, _)| ck.key().name() == name)
                .unwrap_or_else(|| panic!("histogram {name} not found"));
            match &entry.3 {
                DebugValue::Histogram(v) => v.len(),
                other => panic!("expected histogram for {name}, got {other:?}"),
            }
        };
        assert_eq!(
            histogram_len("quickwit_test_drop_timer_seconds"),
            1,
            "HistogramTimer drop must record exactly one observation"
        );
        assert_eq!(
            histogram_len("quickwit_test_explicit_timer_seconds"),
            1,
            "observe_duration + subsequent drop must record exactly one observation"
        );
    }

    #[test]
    fn int_counter_vec_caching_and_labels() {
        let (handles, snapshot) = with_test_recorder(|| {
            let vec = new_counter_vec(
                "errors_total",
                "help",
                "test",
                &[("env", "unit")],
                ["status"],
            );
            let c500_first = vec.with_label_values(["500"]);
            c500_first.inc_by(3);
            let c500_second = vec.with_label_values(["500"]);
            c500_second.inc_by(7);
            let c404 = vec.with_label_values(["404"]);
            c404.inc_by(2);
            (c500_first, c500_second, c404)
        });
        let (c500_first, c500_second, c404) = handles;
        assert_eq!(c500_first.get(), 10);
        assert_eq!(c500_second.get(), 10);
        assert_eq!(c404.get(), 2);

        let entries = counter_by_labels(snapshot, "quickwit_test_errors_total");
        assert_eq!(entries.len(), 2);
        for (labels, _) in &entries {
            assert!(
                has_label(labels, "env", "unit"),
                "const label missing: {labels:?}"
            );
        }
        let lookup = |status: &str| -> u64 {
            entries
                .iter()
                .find(|(labels, _)| has_label(labels, "status", status))
                .map(|(_, v)| *v)
                .unwrap_or_else(|| panic!("no entry for status={status}"))
        };
        assert_eq!(lookup("500"), 10);
        assert_eq!(lookup("404"), 2);
    }

    #[test]
    fn int_gauge_vec_caching_and_labels() {
        let (handles, snapshot) = with_test_recorder(|| {
            let vec = new_gauge_vec(
                "queue_depth",
                "help",
                "test",
                &[("region", "eu")],
                ["queue"],
            );
            let primary_first = vec.with_label_values(["primary"]);
            primary_first.set(5);
            let primary_second = vec.with_label_values(["primary"]);
            primary_second.add(3);
            let secondary = vec.with_label_values(["secondary"]);
            secondary.set(2);
            (primary_first, primary_second, secondary)
        });
        let (primary_first, primary_second, secondary) = handles;
        assert_eq!(primary_first.get(), 8);
        assert_eq!(primary_second.get(), 8);
        assert_eq!(secondary.get(), 2);

        let entries = gauge_by_labels(snapshot, "quickwit_test_queue_depth");
        assert_eq!(entries.len(), 2);
        for (labels, _) in &entries {
            assert!(has_label(labels, "region", "eu"));
        }
        let lookup = |queue: &str| -> f64 {
            entries
                .iter()
                .find(|(labels, _)| has_label(labels, "queue", queue))
                .map(|(_, v)| *v)
                .unwrap_or_else(|| panic!("no entry for queue={queue}"))
        };
        assert_eq!(lookup("primary"), 8.0);
        assert_eq!(lookup("secondary"), 2.0);
    }

    #[test]
    fn histogram_vec_caching_and_labels() {
        let (_, snapshot) = with_test_recorder(|| {
            let vec = __private::new_histogram_vec(
                "request_duration_seconds",
                "test",
                &[("service", "search")],
                ["route"],
            );
            let h_search_first = vec.with_label_values(["/search"]);
            h_search_first.observe(0.1);
            let h_search_second = vec.with_label_values(["/search"]);
            h_search_second.observe(0.3);
            let h_health = vec.with_label_values(["/health"]);
            h_health.observe(0.01);
        });
        let entries = histogram_by_labels(snapshot, "quickwit_test_request_duration_seconds");
        assert_eq!(entries.len(), 2);
        for (labels, _) in &entries {
            assert!(has_label(labels, "service", "search"));
        }
        let lookup = |route: &str| -> Vec<f64> {
            entries
                .iter()
                .find(|(labels, _)| has_label(labels, "route", route))
                .map(|(_, v)| v.clone())
                .unwrap_or_else(|| panic!("no entry for route={route}"))
        };
        let search = lookup("/search");
        assert_eq!(
            search.len(),
            2,
            "cached handle must share the underlying histogram"
        );
        assert_eq!(search[0], 0.1);
        assert_eq!(search[1], 0.3);
        let health = lookup("/health");
        assert_eq!(health.len(), 1);
        assert_eq!(health[0], 0.01);
    }

    #[test]
    fn gauge_guard_lifecycle() {
        let (_, snapshot) = with_test_recorder(|| {
            let gauge = new_gauge("pending_bytes", "help", "test", &[]);
            {
                let mut guard = GaugeGuard::from_gauge(&gauge);
                guard.add(3);
                guard.add(2);
                guard.sub(1);
                assert_eq!(guard.get(), 4);
                assert_eq!(gauge.get(), 4);
            }
            assert_eq!(gauge.get(), 0, "guard drop must restore gauge");
        });
        assert_eq!(single_gauge(snapshot, "quickwit_test_pending_bytes"), 0.0);
    }

    #[test]
    fn owned_gauge_guard_lifecycle() {
        let (_, snapshot) = with_test_recorder(|| {
            let gauge = new_gauge("pending_items", "help", "test", &[]);
            let gauge_view = gauge.clone();
            {
                let mut guard = OwnedGaugeGuard::from_gauge(gauge);
                guard.add(5);
                guard.sub(2);
                assert_eq!(guard.get(), 3);
                assert_eq!(gauge_view.get(), 3);
            }
            assert_eq!(gauge_view.get(), 0, "guard drop must restore gauge");
        });
        assert_eq!(single_gauge(snapshot, "quickwit_test_pending_items"), 0.0);
    }
}
