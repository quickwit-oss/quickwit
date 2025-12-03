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

use std::collections::{BTreeMap, HashMap};
use std::sync::{LazyLock, OnceLock};

use prometheus::{Gauge, HistogramOpts, Opts, TextEncoder};
pub use prometheus::{
    Histogram, HistogramTimer, HistogramVec as PrometheusHistogramVec, IntCounter,
    IntCounterVec as PrometheusIntCounterVec, IntGauge, IntGaugeVec as PrometheusIntGaugeVec,
    exponential_buckets, linear_buckets,
};

#[derive(Clone)]
pub struct HistogramVec<const N: usize> {
    underlying: PrometheusHistogramVec,
}

impl<const N: usize> HistogramVec<N> {
    pub fn with_label_values(&self, label_values: [&str; N]) -> Histogram {
        self.underlying.with_label_values(&label_values)
    }
}

#[derive(Clone)]
pub struct IntCounterVec<const N: usize> {
    underlying: PrometheusIntCounterVec,
}

impl<const N: usize> IntCounterVec<N> {
    pub fn new(
        name: &str,
        help: &str,
        subsystem: &str,
        const_labels: &[(&str, &str)],
        label_names: [&str; N],
    ) -> IntCounterVec<N> {
        let owned_const_labels: HashMap<String, String> = const_labels
            .iter()
            .map(|(label_name, label_value)| (label_name.to_string(), label_value.to_string()))
            .collect();
        let counter_opts = Opts::new(name, help)
            .namespace("quickwit")
            .subsystem(subsystem)
            .const_labels(owned_const_labels);
        let underlying = PrometheusIntCounterVec::new(counter_opts, &label_names)
            .expect("failed to create counter vec");
        IntCounterVec { underlying }
    }

    pub fn with_label_values(&self, label_values: [&str; N]) -> IntCounter {
        self.underlying.with_label_values(&label_values)
    }
}

#[derive(Clone)]
pub struct IntGaugeVec<const N: usize> {
    underlying: PrometheusIntGaugeVec,
}

impl<const N: usize> IntGaugeVec<N> {
    pub fn with_label_values(&self, label_values: [&str; N]) -> IntGauge {
        self.underlying.with_label_values(&label_values)
    }
}

pub fn register_info(name: &'static str, help: &'static str, kvs: BTreeMap<&'static str, String>) {
    let mut counter_opts = Opts::new(name, help).namespace("quickwit");
    for (k, v) in kvs {
        counter_opts = counter_opts.const_label(k, v);
    }
    let counter = IntCounter::with_opts(counter_opts).expect("failed to create counter");
    counter.inc();
    prometheus::register(Box::new(counter)).expect("failed to register counter");
}

pub fn new_counter(
    name: &str,
    help: &str,
    subsystem: &str,
    const_labels: &[(&str, &str)],
) -> IntCounter {
    let owned_const_labels: HashMap<String, String> = const_labels
        .iter()
        .map(|(label_name, label_value)| (label_name.to_string(), label_value.to_string()))
        .collect();
    let counter_opts = Opts::new(name, help)
        .namespace("quickwit")
        .subsystem(subsystem)
        .const_labels(owned_const_labels);
    let counter = IntCounter::with_opts(counter_opts).expect("failed to create counter");
    prometheus::register(Box::new(counter.clone())).expect("failed to register counter");
    counter
}

pub fn new_counter_vec<const N: usize>(
    name: &str,
    help: &str,
    subsystem: &str,
    const_labels: &[(&str, &str)],
    label_names: [&str; N],
) -> IntCounterVec<N> {
    let int_counter_vec = IntCounterVec::new(name, help, subsystem, const_labels, label_names);
    let collector = Box::new(int_counter_vec.underlying.clone());
    prometheus::register(collector).expect("failed to register counter vec");
    int_counter_vec
}

pub fn new_float_gauge(
    name: &str,
    help: &str,
    subsystem: &str,
    const_labels: &[(&str, &str)],
) -> Gauge {
    let owned_const_labels: HashMap<String, String> = const_labels
        .iter()
        .map(|(label_name, label_value)| (label_name.to_string(), label_value.to_string()))
        .collect();
    let gauge_opts = Opts::new(name, help)
        .namespace("quickwit")
        .subsystem(subsystem)
        .const_labels(owned_const_labels);
    let gauge = Gauge::with_opts(gauge_opts).expect("failed to create float gauge");
    prometheus::register(Box::new(gauge.clone())).expect("failed to register float gauge");
    gauge
}

pub fn new_gauge(
    name: &str,
    help: &str,
    subsystem: &str,
    const_labels: &[(&str, &str)],
) -> IntGauge {
    let owned_const_labels: HashMap<String, String> = const_labels
        .iter()
        .map(|(label_name, label_value)| (label_name.to_string(), label_value.to_string()))
        .collect();
    let gauge_opts = Opts::new(name, help)
        .namespace("quickwit")
        .subsystem(subsystem)
        .const_labels(owned_const_labels);
    let gauge = IntGauge::with_opts(gauge_opts).expect("failed to create gauge");
    prometheus::register(Box::new(gauge.clone())).expect("failed to register gauge");
    gauge
}

pub fn new_gauge_vec<const N: usize>(
    name: &str,
    help: &str,
    subsystem: &str,
    const_labels: &[(&str, &str)],
    label_names: [&str; N],
) -> IntGaugeVec<N> {
    let owned_const_labels: HashMap<String, String> = const_labels
        .iter()
        .map(|(label_name, label_value)| (label_name.to_string(), label_value.to_string()))
        .collect();
    let gauge_opts = Opts::new(name, help)
        .namespace("quickwit")
        .subsystem(subsystem)
        .const_labels(owned_const_labels);
    let underlying =
        PrometheusIntGaugeVec::new(gauge_opts, &label_names).expect("failed to create gauge vec");

    let collector = Box::new(underlying.clone());
    prometheus::register(collector).expect("failed to register counter vec");

    IntGaugeVec { underlying }
}

pub fn new_histogram(name: &str, help: &str, subsystem: &str, buckets: Vec<f64>) -> Histogram {
    let histogram_opts = HistogramOpts::new(name, help)
        .namespace("quickwit")
        .subsystem(subsystem)
        .buckets(buckets);
    let histogram = Histogram::with_opts(histogram_opts).expect("failed to create histogram");
    prometheus::register(Box::new(histogram.clone())).expect("failed to register histogram");
    histogram
}

pub fn new_histogram_vec<const N: usize>(
    name: &str,
    help: &str,
    subsystem: &str,
    const_labels: &[(&str, &str)],
    label_names: [&str; N],
    buckets: Vec<f64>,
) -> HistogramVec<N> {
    let owned_const_labels: HashMap<String, String> = const_labels
        .iter()
        .map(|(label_name, label_value)| (label_name.to_string(), label_value.to_string()))
        .collect();
    let histogram_opts = HistogramOpts::new(name, help)
        .namespace("quickwit")
        .subsystem(subsystem)
        .const_labels(owned_const_labels)
        .buckets(buckets);
    let underlying = PrometheusHistogramVec::new(histogram_opts, &label_names)
        .expect("failed to create histogram vec");

    let collector = Box::new(underlying.clone());
    prometheus::register(collector).expect("failed to register histogram vec");

    HistogramVec { underlying }
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

pub fn metrics_text_payload() -> Result<String, String> {
    let metric_families = prometheus::gather();
    // Arbitrary non-zero size in order to skip a bunch of
    // buffer growth-reallocations when encoding metrics.
    let mut buffer = String::with_capacity(1024);
    let encoder = TextEncoder::new();
    match encoder.encode_utf8(&metric_families, &mut buffer) {
        Ok(()) => Ok(buffer),
        Err(e) => Err(e.to_string()),
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
