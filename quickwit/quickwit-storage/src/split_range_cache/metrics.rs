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
    CounterOps, CounterVecOps, GaugeOps, GaugeVecOps, HistogramOps, HistogramVecOps, RegistryOps,
};
use quickwit_metrics::{LazyCounter, label_names, label_values, lazy_counter};

use super::storage::AdmissionBypass;

const CACHE_RESULT: quickwit_metrics::LabelNames<1> = label_names!("result");
const ADMISSION_REASON: quickwit_metrics::LabelNames<1> = label_names!("reason");

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
static ADMISSION_BYPASSES: LazyCounter = lazy_counter!(
    name: "split_range_disk_cache_admission_bypasses_total",
    description: "Entries kept memory-only by admission checks",
    subsystem: "storage",
);
static FAIL_OPEN_TOTAL: LazyCounter = lazy_counter!(
    name: "split_range_disk_cache_fail_open_total",
    description: "Foyer failures bypassed through lower storage",
    subsystem: "storage",
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
pub(crate) static ADMISSION_MAX_ENTRY_SIZE: LazyCounter = lazy_counter!(
    parent: ADMISSION_BYPASSES,
    labels: [label_values!(ADMISSION_REASON => "max_entry_size")]
);
pub(crate) static ADMISSION_ENCODED_TOO_LARGE: LazyCounter = lazy_counter!(
    parent: ADMISSION_BYPASSES,
    labels: [label_values!(ADMISSION_REASON => "encoded_too_large")]
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

pub(crate) fn record_admission_bypass(reason: AdmissionBypass) {
    match reason {
        AdmissionBypass::MaxEntrySize => ADMISSION_MAX_ENTRY_SIZE.inc(),
        AdmissionBypass::EncodedTooLarge => ADMISSION_ENCODED_TOO_LARGE.inc(),
    }
}

pub(crate) fn record_fail_open() {
    FAIL_OPEN_TOTAL.inc();
}

/// Mixtrics registry that forwards Foyer metrics to the process `metrics` recorder.
#[derive(Debug)]
pub(crate) struct QuickwitMetricsRegistry;

impl RegistryOps for QuickwitMetricsRegistry {
    fn register_counter_vec(
        &self,
        name: Cow<'static, str>,
        desc: Cow<'static, str>,
        label_names: &'static [&'static str],
    ) -> BoxedCounterVec {
        ::metrics::describe_counter!(name.clone(), desc.clone());
        Box::new(MetricsCounterVec { name, label_names })
    }

    fn register_gauge_vec(
        &self,
        name: Cow<'static, str>,
        desc: Cow<'static, str>,
        label_names: &'static [&'static str],
    ) -> BoxedGaugeVec {
        ::metrics::describe_gauge!(name.clone(), desc.clone());
        Box::new(MetricsGaugeVec { name, label_names })
    }

    fn register_histogram_vec(
        &self,
        name: Cow<'static, str>,
        desc: Cow<'static, str>,
        label_names: &'static [&'static str],
    ) -> BoxedHistogramVec {
        ::metrics::describe_histogram!(name.clone(), desc.clone());
        Box::new(MetricsHistogramVec { name, label_names })
    }

    fn register_histogram_vec_with_buckets(
        &self,
        name: Cow<'static, str>,
        desc: Cow<'static, str>,
        label_names: &'static [&'static str],
        _buckets: Vec<f64>,
    ) -> BoxedHistogramVec {
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
    use ::metrics::with_local_recorder;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use mixtrics::metrics::RegistryOps;

    use super::*;

    #[test]
    fn test_quickwit_metrics_registry_records_counter_gauge_histogram() {
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
                vec![0.1, 1.0],
            );
            histograms
                .histogram(&["split-range-v1".into(), "hit".into()])
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
    }
}
