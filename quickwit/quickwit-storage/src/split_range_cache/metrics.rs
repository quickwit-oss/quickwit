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

use std::borrow::Cow;
use std::fmt;

use mixtrics::metrics::{
    BoxedCounter, BoxedCounterVec, BoxedGauge, BoxedGaugeVec, BoxedHistogram, BoxedHistogramVec,
    Buckets, CounterOps, CounterVecOps, GaugeOps, GaugeVecOps, HistogramOps, HistogramVecOps,
    RegistryOps,
};
use mixtrics::registry::noop::NoopMetricsRegistry;
use quickwit_metrics::{
    LazyCounter, LazyHistogram, label_names, label_values, lazy_counter, lazy_histogram,
};

const CACHE_RESULT: quickwit_metrics::LabelNames<1> = label_names!("result");

static REQUESTS: LazyCounter = lazy_counter!(
    name: "split_range_disk_cache_requests_total",
    description: "Split range disk cache requests by result",
    subsystem: "storage",
);
static REQUESTED_BYTES: LazyCounter = lazy_counter!(
    name: "split_range_disk_cache_requested_bytes_total",
    description: "Split range disk cache requested bytes by result",
    subsystem: "storage",
);

fn foyer_histogram_buckets(name: &str) -> Option<Vec<f64>> {
    match name {
        "foyer_storage_op_duration" | "foyer_storage_disk_io_duration" => {
            Some(Buckets::exponential(0.000_001, 2.0, 23))
        }
        "foyer_storage_block_engine_buffer_efficiency" => Some(Buckets::linear(0.1, 0.1, 10)),
        _ => None,
    }
}

// The Prometheus recorder reads histogram bucket definitions before Foyer
// registers its metrics at runtime. Mirror the definitions from Foyer 0.22.3
// here so the adapter retains Foyer's intended bucket boundaries.
static _FOYER_STORAGE_OP_DURATION: LazyHistogram = lazy_histogram!(
    name: "foyer_storage_op_duration",
    description: "foyer disk cache op durations",
    system: "",
    subsystem: "",
    buckets: foyer_histogram_buckets("foyer_storage_op_duration").unwrap(),
);
static _FOYER_STORAGE_DISK_IO_DURATION: LazyHistogram = lazy_histogram!(
    name: "foyer_storage_disk_io_duration",
    description: "foyer disk cache disk io duration",
    system: "",
    subsystem: "",
    buckets: foyer_histogram_buckets("foyer_storage_disk_io_duration").unwrap(),
);
static _FOYER_STORAGE_BLOCK_ENGINE_BUFFER_EFFICIENCY: LazyHistogram = lazy_histogram!(
    name: "foyer_storage_block_engine_buffer_efficiency",
    description: "foyer large object disk cache buffer efficiency",
    system: "",
    subsystem: "",
    buckets: foyer_histogram_buckets("foyer_storage_block_engine_buffer_efficiency").unwrap(),
);

pub(crate) static REQUESTS_MEMORY: LazyCounter = lazy_counter!(
    parent: REQUESTS,
    labels: [label_values!(CACHE_RESULT => "memory")]
);
pub(crate) static REQUESTS_DISK: LazyCounter = lazy_counter!(
    parent: REQUESTS,
    labels: [label_values!(CACHE_RESULT => "disk")]
);
pub(crate) static REQUESTS_MISS: LazyCounter = lazy_counter!(
    parent: REQUESTS,
    labels: [label_values!(CACHE_RESULT => "miss")]
);
pub(crate) static REQUESTS_ERROR: LazyCounter = lazy_counter!(
    parent: REQUESTS,
    labels: [label_values!(CACHE_RESULT => "error")]
);
static REQUESTED_BYTES_MEMORY: LazyCounter = lazy_counter!(
    parent: REQUESTED_BYTES,
    labels: [label_values!(CACHE_RESULT => "memory")]
);
static REQUESTED_BYTES_DISK: LazyCounter = lazy_counter!(
    parent: REQUESTED_BYTES,
    labels: [label_values!(CACHE_RESULT => "disk")]
);
static REQUESTED_BYTES_MISS: LazyCounter = lazy_counter!(
    parent: REQUESTED_BYTES,
    labels: [label_values!(CACHE_RESULT => "miss")]
);
static REQUESTED_BYTES_ERROR: LazyCounter = lazy_counter!(
    parent: REQUESTED_BYTES,
    labels: [label_values!(CACHE_RESULT => "error")]
);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FetchOutcome {
    MemoryHit,
    DiskHit,
    RemoteMiss,
    Error,
}

pub(crate) fn record_request(outcome: FetchOutcome, num_bytes: u64) {
    match outcome {
        FetchOutcome::MemoryHit => {
            REQUESTS_MEMORY.inc();
            REQUESTED_BYTES_MEMORY.inc_by(num_bytes);
        }
        FetchOutcome::DiskHit => {
            REQUESTS_DISK.inc();
            REQUESTED_BYTES_DISK.inc_by(num_bytes);
        }
        FetchOutcome::RemoteMiss => {
            REQUESTS_MISS.inc();
            REQUESTED_BYTES_MISS.inc_by(num_bytes);
        }
        FetchOutcome::Error => {
            REQUESTS_ERROR.inc();
            REQUESTED_BYTES_ERROR.inc_by(num_bytes);
        }
    }
}

fn is_dashboard_counter(name: &str) -> bool {
    matches!(
        name,
        "foyer_memory_op_total"
            | "foyer_storage_op_total"
            | "foyer_storage_inner_op_total"
            | "foyer_storage_disk_io_total"
            | "foyer_storage_disk_io_bytes_total"
            | "foyer_storage_block_engine_op_total"
            | "foyer_hybrid_op_total"
    )
}

fn is_dashboard_gauge(name: &str) -> bool {
    matches!(
        name,
        "foyer_memory_usage"
            | "foyer_memory_entries"
            | "foyer_storage_block_engine_block"
            | "foyer_storage_block_engine_block_size_bytes"
    )
}

fn is_dashboard_histogram(name: &str) -> bool {
    matches!(
        name,
        "foyer_storage_op_duration"
            | "foyer_storage_disk_io_duration"
            | "foyer_storage_block_engine_buffer_efficiency"
            | "foyer_hybrid_op_duration"
    )
}

/// Mixtrics registry that forwards dashboarded Foyer metrics to the process
/// `metrics` recorder and discards all other Foyer metrics.
#[derive(Debug)]
pub(crate) struct QuickwitMetricsRegistry;

impl RegistryOps for QuickwitMetricsRegistry {
    fn register_counter_vec(
        &self,
        name: Cow<'static, str>,
        desc: Cow<'static, str>,
        label_names: &'static [&'static str],
    ) -> BoxedCounterVec {
        if !is_dashboard_counter(&name) {
            return Box::new(NoopMetricsRegistry);
        }
        ::metrics::describe_counter!(name.clone(), desc.clone());
        Box::new(MetricsCounterVec { name, label_names })
    }

    fn register_gauge_vec(
        &self,
        name: Cow<'static, str>,
        desc: Cow<'static, str>,
        label_names: &'static [&'static str],
    ) -> BoxedGaugeVec {
        if !is_dashboard_gauge(&name) {
            return Box::new(NoopMetricsRegistry);
        }
        ::metrics::describe_gauge!(name.clone(), desc.clone());
        Box::new(MetricsGaugeVec { name, label_names })
    }

    fn register_histogram_vec(
        &self,
        name: Cow<'static, str>,
        desc: Cow<'static, str>,
        label_names: &'static [&'static str],
    ) -> BoxedHistogramVec {
        if !is_dashboard_histogram(&name) {
            return Box::new(NoopMetricsRegistry);
        }
        ::metrics::describe_histogram!(name.clone(), desc.clone());
        Box::new(MetricsHistogramVec { name, label_names })
    }

    fn register_histogram_vec_with_buckets(
        &self,
        name: Cow<'static, str>,
        desc: Cow<'static, str>,
        label_names: &'static [&'static str],
        buckets: Vec<f64>,
    ) -> BoxedHistogramVec {
        if !is_dashboard_histogram(&name) {
            return Box::new(NoopMetricsRegistry);
        }
        debug_assert_eq!(
            foyer_histogram_buckets(&name),
            Some(buckets),
            "Foyer histogram bucket definitions have changed"
        );
        self.register_histogram_vec(name, desc, label_names)
    }
}

#[derive(Debug)]
struct MetricsCounterVec {
    name: Cow<'static, str>,
    label_names: &'static [&'static str],
}

impl CounterVecOps for MetricsCounterVec {
    fn counter(&self, labels: &[Cow<'static, str>]) -> BoxedCounter {
        Box::new(MetricsCounter(::metrics::counter!(
            self.name.clone(),
            labeled(self.label_names, labels)
        )))
    }
}

#[derive(Debug)]
struct MetricsGaugeVec {
    name: Cow<'static, str>,
    label_names: &'static [&'static str],
}

impl GaugeVecOps for MetricsGaugeVec {
    fn gauge(&self, labels: &[Cow<'static, str>]) -> BoxedGauge {
        Box::new(MetricsGauge(::metrics::gauge!(
            self.name.clone(),
            labeled(self.label_names, labels)
        )))
    }
}

#[derive(Debug)]
struct MetricsHistogramVec {
    name: Cow<'static, str>,
    label_names: &'static [&'static str],
}

impl HistogramVecOps for MetricsHistogramVec {
    fn histogram(&self, labels: &[Cow<'static, str>]) -> BoxedHistogram {
        Box::new(MetricsHistogram(::metrics::histogram!(
            self.name.clone(),
            labeled(self.label_names, labels)
        )))
    }
}

struct MetricsCounter(::metrics::Counter);
struct MetricsGauge(::metrics::Gauge);
struct MetricsHistogram(::metrics::Histogram);

impl fmt::Debug for MetricsCounter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetricsCounter").finish()
    }
}

impl fmt::Debug for MetricsGauge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetricsGauge").finish()
    }
}

impl fmt::Debug for MetricsHistogram {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetricsHistogram").finish()
    }
}

impl CounterOps for MetricsCounter {
    fn increase(&self, val: u64) {
        self.0.increment(val);
    }
}

impl GaugeOps for MetricsGauge {
    fn increase(&self, val: u64) {
        self.0.increment(val as f64);
    }

    fn decrease(&self, val: u64) {
        self.0.decrement(val as f64);
    }

    fn absolute(&self, val: u64) {
        self.0.set(val as f64);
    }
}

impl HistogramOps for MetricsHistogram {
    fn record(&self, val: f64) {
        self.0.record(val);
    }
}

fn labeled(
    label_names: &'static [&'static str],
    labels: &[Cow<'static, str>],
) -> Vec<::metrics::Label> {
    debug_assert_eq!(
        label_names.len(),
        labels.len(),
        "Foyer mixtrics label names and values must have the same length"
    );
    let mut metric_labels = Vec::with_capacity(label_names.len());
    for (name, value) in label_names.iter().zip(labels.iter()) {
        metric_labels.push(::metrics::Label::new(*name, value.clone().into_owned()));
    }
    metric_labels
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use ::metrics::with_local_recorder;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use mixtrics::metrics::{Buckets, RegistryOps};

    use super::*;

    #[test]
    fn test_foyer_dashboard_metric_allowlist() {
        for counter in [
            "foyer_memory_op_total",
            "foyer_storage_op_total",
            "foyer_storage_inner_op_total",
            "foyer_storage_disk_io_total",
            "foyer_storage_disk_io_bytes_total",
            "foyer_storage_block_engine_op_total",
            "foyer_hybrid_op_total",
        ] {
            assert!(is_dashboard_counter(counter));
        }
        for gauge in [
            "foyer_memory_usage",
            "foyer_memory_entries",
            "foyer_storage_block_engine_block",
            "foyer_storage_block_engine_block_size_bytes",
        ] {
            assert!(is_dashboard_gauge(gauge));
        }
        for histogram in [
            "foyer_storage_op_duration",
            "foyer_storage_disk_io_duration",
            "foyer_storage_block_engine_buffer_efficiency",
            "foyer_hybrid_op_duration",
        ] {
            assert!(is_dashboard_histogram(histogram));
        }
    }

    #[test]
    fn test_foyer_histogram_buckets_are_registered_with_exporter() {
        let configured_buckets: HashMap<_, _> = quickwit_metrics::histogram_buckets().collect();
        assert_eq!(
            configured_buckets["foyer_storage_op_duration"],
            Buckets::exponential(0.000_001, 2.0, 23)
        );
        assert_eq!(
            configured_buckets["foyer_storage_disk_io_duration"],
            Buckets::exponential(0.000_001, 2.0, 23)
        );
        assert_eq!(
            configured_buckets["foyer_storage_block_engine_buffer_efficiency"],
            Buckets::linear(0.1, 0.1, 10)
        );
        assert!(!configured_buckets.contains_key("foyer_storage_inner_op_duration"));
        assert!(!configured_buckets.contains_key("foyer_storage_entry_serde_duration"));
        assert!(!configured_buckets.contains_key("foyer_storage_block_engine_recover_duration"));
    }

    #[test]
    fn test_quickwit_metrics_registry_only_records_dashboard_metrics() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        with_local_recorder(&recorder, || {
            let registry = QuickwitMetricsRegistry;
            let counters = registry.register_counter_vec(
                "foyer_memory_op_total".into(),
                "foyer in-memory cache operations".into(),
                &["name", "op"],
            );
            counters
                .counter(&["split-range-v1".into(), "hit".into()])
                .increase(1);
            let gauges = registry.register_gauge_vec(
                "foyer_memory_usage".into(),
                "foyer in-memory cache usage".into(),
                &["name"],
            );
            gauges.gauge(&["split-range-v1".into()]).absolute(7);
            let histograms = registry.register_histogram_vec_with_buckets(
                "foyer_storage_op_duration".into(),
                "foyer storage op duration".into(),
                &["name", "op"],
                foyer_histogram_buckets("foyer_storage_op_duration").unwrap(),
            );
            histograms
                .histogram(&["split-range-v1".into(), "hit".into()])
                .record(0.5);

            registry
                .register_counter_vec(
                    "foyer_future_counter".into(),
                    "not used by the disk cache dashboard".into(),
                    &[],
                )
                .counter(&[])
                .increase(1);
            registry
                .register_gauge_vec(
                    "foyer_future_gauge".into(),
                    "not used by the disk cache dashboard".into(),
                    &[],
                )
                .gauge(&[])
                .absolute(1);
            registry
                .register_histogram_vec_with_buckets(
                    "foyer_storage_entry_serde_duration".into(),
                    "not used by the disk cache dashboard".into(),
                    &[],
                    Buckets::exponential(0.000_000_01, 2.0, 23),
                )
                .histogram(&[])
                .record(0.5);
        });
        let snapshot = snapshotter.snapshot().into_vec();
        let has_counter = snapshot.iter().any(|(key, _, _, value)| {
            key.key().name() == "foyer_memory_op_total" && *value == DebugValue::Counter(1)
        });
        let has_gauge = snapshot
            .iter()
            .any(|(key, _, _, _)| key.key().name() == "foyer_memory_usage");
        let has_histogram = snapshot
            .iter()
            .any(|(key, _, _, _)| key.key().name() == "foyer_storage_op_duration");
        assert!(
            has_counter,
            "Foyer counter must register through the metrics recorder"
        );
        assert!(
            has_gauge,
            "Foyer gauge must register through the metrics recorder"
        );
        assert!(
            has_histogram,
            "Foyer histogram must register through the metrics recorder"
        );
        for ignored_name in [
            "foyer_future_counter",
            "foyer_future_gauge",
            "foyer_storage_entry_serde_duration",
        ] {
            assert!(
                !snapshot
                    .iter()
                    .any(|(key, _, _, _)| key.key().name() == ignored_name),
                "{ignored_name} must not register through the metrics recorder"
            );
        }
    }
}
