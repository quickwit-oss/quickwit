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

use std::sync::Arc;

use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
use metrics_util::MetricKind;

use super::state::OtlpMetricsState;

#[derive(Clone)]
pub(crate) struct OtlpMetricsRecorder {
    state: Arc<OtlpMetricsState>,
}

impl OtlpMetricsRecorder {
    pub(crate) fn new(meter: opentelemetry::metrics::Meter) -> Self {
        Self {
            state: Arc::new(OtlpMetricsState::new(meter)),
        }
    }

    pub(crate) fn set_histogram_bounds(&self, key_name: &KeyName, bounds: Vec<f64>) {
        self.state.set_histogram_bounds(key_name, bounds);
    }
}

impl Recorder for OtlpMetricsRecorder {
    fn describe_counter(&self, key_name: KeyName, unit: Option<Unit>, description: SharedString) {
        self.state
            .set_description(key_name, MetricKind::Counter, unit, description);
    }

    fn describe_gauge(&self, key_name: KeyName, unit: Option<Unit>, description: SharedString) {
        self.state
            .set_description(key_name, MetricKind::Gauge, unit, description);
    }

    fn describe_histogram(&self, key_name: KeyName, unit: Option<Unit>, description: SharedString) {
        self.state
            .set_description(key_name, MetricKind::Histogram, unit, description);
    }

    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
        self.state
            .registry()
            .get_or_create_counter(key, |counter| Counter::from_arc(counter.clone()))
    }

    fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        self.state
            .registry()
            .get_or_create_gauge(key, |gauge| Gauge::from_arc(gauge.clone()))
    }

    fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        self.state
            .registry()
            .get_or_create_histogram(key, |histogram| Histogram::from_arc(histogram.clone()))
    }
}
