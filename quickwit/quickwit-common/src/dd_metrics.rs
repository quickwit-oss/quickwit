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

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use metrics::{Counter, Histogram as MetricsHistogram, Label, counter, histogram};

pub const DD_STATUS_CODES: &[&str] = &[
    "200", "400", "401", "403", "404", "408", "429", "500", "501", "503",
];

#[derive(Clone)]
pub struct DDCounters {
    inner: Arc<DDCounterInner>,
}

struct DDCounterInner {
    counters: HashMap<&'static str, Counter>,
    other: Counter,
}

impl DDCounters {
    pub fn new(
        name: &'static str,
        label_key: &'static str,
        label_values: &[&'static str],
        extra_labels: &[Label],
    ) -> Self {
        let mut counters = HashMap::with_capacity(label_values.len());
        for &label_value in label_values {
            let mut labels = extra_labels.to_vec();
            labels.push(Label::new(label_key, label_value));
            counters.insert(label_value, counter!(name, labels));
        }
        let mut other_labels = extra_labels.to_vec();
        other_labels.push(Label::new(label_key, "other"));
        let other = counter!(name, other_labels);
        let inner = DDCounterInner { counters, other };
        Self {
            inner: Arc::new(inner),
        }
    }

    pub fn get(&self, label_value: &str) -> &Counter {
        if let Some(counter) = self.inner.counters.get(label_value) {
            counter
        } else {
            &self.inner.other
        }
    }
}

impl std::fmt::Debug for DDCounters {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.inner.counters.fmt(f)
    }
}

#[derive(Clone)]
pub struct DDHistograms {
    inner: Arc<DDHistogramInner>,
}

struct DDHistogramInner {
    histograms: HashMap<&'static str, MetricsHistogram>,
    other: MetricsHistogram,
}

impl DDHistograms {
    pub fn new(
        name: &'static str,
        label_key: &'static str,
        label_values: &[&'static str],
        extra_labels: &[Label],
    ) -> Self {
        let mut histograms = HashMap::with_capacity(label_values.len());
        for &label_value in label_values {
            let mut labels = extra_labels.to_vec();
            labels.push(Label::new(label_key, label_value));
            histograms.insert(label_value, histogram!(name, labels));
        }
        let mut other_labels = extra_labels.to_vec();
        other_labels.push(Label::new(label_key, "other"));
        let other = histogram!(name, other_labels);
        let inner = DDHistogramInner { histograms, other };
        Self {
            inner: Arc::new(inner),
        }
    }

    pub fn get(&self, label_value: &str) -> &MetricsHistogram {
        if let Some(histogram) = self.inner.histograms.get(label_value) {
            histogram
        } else {
            &self.inner.other
        }
    }
}

impl std::fmt::Debug for DDHistograms {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.inner.histograms.fmt(f)
    }
}

pub struct DDIngestMetrics {
    pub ingest_requests_total: DDCounters,
    pub ingest_request_duration_seconds: DDHistograms,
    pub ingest_unrouted_docs_total: Counter,
}

impl Default for DDIngestMetrics {
    fn default() -> Self {
        Self {
            ingest_requests_total: DDCounters::new(
                "ingest_requests.count",
                "status_code",
                DD_STATUS_CODES,
                &[],
            ),
            ingest_request_duration_seconds: DDHistograms::new(
                "ingest_requests.duration_seconds",
                "status_code",
                DD_STATUS_CODES,
                &[],
            ),
            ingest_unrouted_docs_total: counter!("ingest_unrouted_docs.count"),
        }
    }
}

pub static DD_INGEST_METRICS: LazyLock<DDIngestMetrics> = LazyLock::new(DDIngestMetrics::default);

#[cfg(test)]
mod tests {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshot};
    use ordered_float::OrderedFloat;

    use super::*;

    fn snapshot_as_map_for_test(snapshot: Snapshot) -> HashMap<String, DebugValue> {
        snapshot
            .into_vec()
            .into_iter()
            .map(|(composite_key, _, _, value)| {
                (
                    format!("{:?}:{}", composite_key.kind(), composite_key.key()),
                    value,
                )
            })
            .collect()
    }

    #[test]
    fn test_dd_counters() {
        let recorder = DebuggingRecorder::default();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, move || {
            let counters = DDCounters::new("test.counter", "label", &["value1", "value2"], &[]);
            counters.get("value1").increment(1);
            counters.get("value2").increment(2);
            counters.get("value3").increment(3);

            let snapshot = snapshot_as_map_for_test(snapshotter.snapshot());
            assert_eq!(snapshot.len(), 3);
            assert_eq!(
                snapshot
                    .get("Counter:Key(test.counter, [label = value1])")
                    .unwrap(),
                &DebugValue::Counter(1)
            );
            assert_eq!(
                snapshot
                    .get("Counter:Key(test.counter, [label = value2])")
                    .unwrap(),
                &DebugValue::Counter(2)
            );
            assert_eq!(
                snapshot
                    .get("Counter:Key(test.counter, [label = other])")
                    .unwrap(),
                &DebugValue::Counter(3)
            );
        });
    }

    #[test]
    fn test_dd_histograms() {
        let recorder = DebuggingRecorder::default();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, move || {
            let histograms =
                DDHistograms::new("test.histogram", "label", &["value1", "value2"], &[]);
            histograms.get("value1").record(1.0f64);
            histograms.get("value2").record(2.0f64);
            histograms.get("value3").record(3.0f64);

            let snapshot = snapshot_as_map_for_test(snapshotter.snapshot());
            assert_eq!(snapshot.len(), 3);
            assert_eq!(
                snapshot
                    .get("Histogram:Key(test.histogram, [label = value1])")
                    .unwrap(),
                &DebugValue::Histogram(vec![OrderedFloat::from(1.0f64)])
            );
            assert_eq!(
                snapshot
                    .get("Histogram:Key(test.histogram, [label = value2])")
                    .unwrap(),
                &DebugValue::Histogram(vec![OrderedFloat::from(2.0f64)])
            );
            assert_eq!(
                snapshot
                    .get("Histogram:Key(test.histogram, [label = other])")
                    .unwrap(),
                &DebugValue::Histogram(vec![OrderedFloat::from(3.0f64)])
            );
        });
    }
}
