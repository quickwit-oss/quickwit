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
use std::sync::Arc;

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
    pub fn new(name: &'static str, label_key: &'static str, label_values: &[&'static str]) -> Self {
        let mut counters = HashMap::with_capacity(label_values.len());
        for &label_value in label_values {
            counters.insert(
                label_value,
                counter!(name, vec![Label::new(label_key, label_value)]),
            );
        }
        let other = counter!(name, vec![Label::new(label_key, "other")]);
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
    pub fn new(name: &'static str, label_key: &'static str, label_values: &[&'static str]) -> Self {
        let mut histograms = HashMap::with_capacity(label_values.len());
        for &label_value in label_values {
            histograms.insert(
                label_value,
                histogram!(name, vec![Label::new(label_key, label_value)]),
            );
        }
        let other = histogram!(name, vec![Label::new(label_key, "other")]);
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

#[cfg(test)]
mod tests {
    use metrics::Key;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use metrics_util::{CompositeKey, MetricKind};
    use ordered_float::OrderedFloat;

    use super::*;

    #[test]
    fn test_dd_counters() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::set_global_recorder(Box::leak(Box::new(recorder))).unwrap();

        let counters = DDCounters::new("test.counter", "label", &["value1", "value2"]);
        counters.get("value1").increment(1);
        counters.get("value2").increment(2);
        counters.get("value3").increment(3);

        #[allow(clippy::mutable_key_type)]
        let metrics = snapshotter.snapshot().into_hashmap();

        let key = CompositeKey::new(
            MetricKind::Counter,
            Key::from_parts("test.counter", vec![Label::new("label", "value1")]),
        );
        let (_, _, counter) = metrics.get(&key).unwrap();
        assert_eq!(*counter, DebugValue::Counter(1));

        let key = CompositeKey::new(
            MetricKind::Counter,
            Key::from_parts("test.counter", vec![Label::new("label", "value2")]),
        );
        let (_, _, counter) = metrics.get(&key).unwrap();
        assert_eq!(*counter, DebugValue::Counter(2));

        let key = CompositeKey::new(
            MetricKind::Counter,
            Key::from_parts("test.counter", vec![Label::new("label", "other")]),
        );
        let (_, _, counter) = metrics.get(&key).unwrap();
        assert_eq!(*counter, DebugValue::Counter(3));
    }

    #[test]
    fn test_dd_histograms() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::set_global_recorder(Box::leak(Box::new(recorder))).unwrap();

        let histograms = DDHistograms::new("test.histogram", "label", &["value1", "value2"]);
        histograms.get("value1").record(1.0);
        histograms.get("value2").record(2.0);
        histograms.get("value3").record(3.0);

        #[allow(clippy::mutable_key_type)]
        let metrics = snapshotter.snapshot().into_hashmap();

        let key = CompositeKey::new(
            MetricKind::Histogram,
            Key::from_parts("test.histogram", vec![Label::new("label", "value1")]),
        );
        let (_, _, histogram) = metrics.get(&key).unwrap();
        assert_eq!(
            *histogram,
            DebugValue::Histogram(vec![OrderedFloat::from(1.0)])
        );

        let key = CompositeKey::new(
            MetricKind::Histogram,
            Key::from_parts("test.histogram", vec![Label::new("label", "value2")]),
        );
        let (_, _, histogram) = metrics.get(&key).unwrap();
        assert_eq!(
            *histogram,
            DebugValue::Histogram(vec![OrderedFloat::from(2.0)])
        );

        let key = CompositeKey::new(
            MetricKind::Histogram,
            Key::from_parts("test.histogram", vec![Label::new("label", "other")]),
        );
        let (_, _, histogram) = metrics.get(&key).unwrap();
        assert_eq!(
            *histogram,
            DebugValue::Histogram(vec![OrderedFloat::from(3.0)])
        );
    }
}
