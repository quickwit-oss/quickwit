// Copyright (C) 2024 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::collections::HashMap;

use once_cell::sync::Lazy;
use prometheus::{Encoder, HistogramOpts, Opts, TextEncoder};
pub use prometheus::{
    Histogram, HistogramTimer, HistogramVec as PrometheusHistogramVec, IntCounter,
    IntCounterVec as PrometheusIntCounterVec, IntGauge, IntGaugeVec as PrometheusIntGaugeVec,
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

pub fn new_counter(name: &str, help: &str, subsystem: &str) -> IntCounter {
    let counter_opts = Opts::new(name, help)
        .namespace("quickwit")
        .subsystem(subsystem);
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

    let collector = Box::new(underlying.clone());
    prometheus::register(collector).expect("failed to register counter vec");

    IntCounterVec { underlying }
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

pub fn new_histogram(name: &str, help: &str, subsystem: &str) -> Histogram {
    let histogram_opts = HistogramOpts::new(name, help)
        .namespace("quickwit")
        .subsystem(subsystem);
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
) -> HistogramVec<N> {
    let owned_const_labels: HashMap<String, String> = const_labels
        .iter()
        .map(|(label_name, label_value)| (label_name.to_string(), label_value.to_string()))
        .collect();
    let histogram_opts = HistogramOpts::new(name, help)
        .namespace("quickwit")
        .subsystem(subsystem)
        .const_labels(owned_const_labels);
    let underlying = PrometheusHistogramVec::new(histogram_opts, &label_names)
        .expect("failed to create histogram vec");

    let collector = Box::new(underlying.clone());
    prometheus::register(collector).expect("failed to register histogram vec");

    HistogramVec { underlying }
}

pub struct GaugeGuard {
    gauge: &'static IntGauge,
    delta: i64,
}

impl std::fmt::Debug for GaugeGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.delta.fmt(f)
    }
}

impl GaugeGuard {
    pub fn from_gauge(gauge: &'static IntGauge) -> Self {
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

impl Drop for GaugeGuard {
    fn drop(&mut self) {
        self.gauge.sub(self.delta)
    }
}

pub fn metrics_text_payload() -> String {
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    let encoder = TextEncoder::new();
    let _ = encoder.encode(&metric_families, &mut buffer); // TODO avoid ignoring the error.
    String::from_utf8_lossy(&buffer).to_string()
}

#[derive(Clone)]
pub struct MemoryMetrics {
    pub active_bytes: IntGauge,
    pub allocated_bytes: IntGauge,
    pub resident_bytes: IntGauge,
    pub in_flight_data: InFlightDataGauges,
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
            in_flight_data: InFlightDataGauges::default(),
        }
    }
}

#[derive(Clone)]
pub struct InFlightDataGauges {
    pub doc_processor_mailbox: IntGauge,
    pub indexer_mailbox: IntGauge,
    pub ingest_router: IntGauge,
    pub index_writer: IntGauge,
    pub rest_server: IntGauge,
    pub sources: InFlightDataSourceGauges,
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
            doc_processor_mailbox: in_flight_gauge_vec.with_label_values(["doc_processor_mailbox"]),
            indexer_mailbox: in_flight_gauge_vec.with_label_values(["indexer_mailbox"]),
            ingest_router: in_flight_gauge_vec.with_label_values(["ingest_router"]),
            index_writer: in_flight_gauge_vec.with_label_values(["index_writer"]),
            rest_server: in_flight_gauge_vec.with_label_values(["rest_server"]),
            sources: InFlightDataSourceGauges::new(&in_flight_gauge_vec),
        }
    }
}

/// TODO make those lazy.
#[derive(Clone)]
pub struct InFlightDataSourceGauges {
    pub file: IntGauge,
    pub ingest: IntGauge,
    pub kafka: IntGauge,
    pub kinesis: IntGauge,
    pub pubsub: IntGauge,
    pub pulsar: IntGauge,
    pub other: IntGauge,
}

impl InFlightDataSourceGauges {
    pub fn new(in_flight_gauge_vec: &IntGaugeVec<1>) -> Self {
        Self {
            file: in_flight_gauge_vec.with_label_values(["file_source"]),
            ingest: in_flight_gauge_vec.with_label_values(["ingest_source"]),
            kafka: in_flight_gauge_vec.with_label_values(["kafka_source"]),
            kinesis: in_flight_gauge_vec.with_label_values(["kinesis_source"]),
            pubsub: in_flight_gauge_vec.with_label_values(["pubsub_source"]),
            pulsar: in_flight_gauge_vec.with_label_values(["pulsar_source"]),
            other: in_flight_gauge_vec.with_label_values(["other"]),
        }
    }
}

pub static MEMORY_METRICS: Lazy<MemoryMetrics> = Lazy::new(MemoryMetrics::default);
