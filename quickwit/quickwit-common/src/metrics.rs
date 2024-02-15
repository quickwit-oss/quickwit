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

pub fn new_gauge(name: &str, help: &str, subsystem: &str) -> IntGauge {
    let gauge_opts = Opts::new(name, help)
        .namespace("quickwit")
        .subsystem(subsystem);
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

pub struct GaugeGuard(&'static IntGauge);

impl GaugeGuard {
    pub fn from_gauge(gauge: &'static IntGauge) -> Self {
        gauge.inc();
        Self(gauge)
    }
}

impl Drop for GaugeGuard {
    fn drop(&mut self) {
        self.0.dec();
    }
}

pub fn metrics_text_payload() -> String {
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    let encoder = TextEncoder::new();
    let _ = encoder.encode(&metric_families, &mut buffer); // TODO avoid ignoring the error.
    String::from_utf8_lossy(&buffer).to_string()
}
