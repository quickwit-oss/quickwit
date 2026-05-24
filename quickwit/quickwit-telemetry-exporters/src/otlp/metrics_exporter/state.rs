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
use std::sync::{Arc, RwLock};

use metrics::{Key, KeyName, SharedString, Unit};
use metrics_util::MetricKind;
use metrics_util::registry::Registry;

use super::storage::OtlpMetricStorage;

pub(super) struct OtlpMetricsState {
    registry: Registry<Key, OtlpMetricStorage>,
    metadata: OtlpMetricMetadata,
}

impl OtlpMetricsState {
    pub(super) fn new(meter: opentelemetry::metrics::Meter) -> Self {
        let metadata = OtlpMetricMetadata::default();
        let registry = Registry::new(OtlpMetricStorage::new(meter, metadata.clone()));
        Self { registry, metadata }
    }

    pub(super) fn registry(&self) -> &Registry<Key, OtlpMetricStorage> {
        &self.registry
    }

    pub(super) fn set_description(
        &self,
        key_name: KeyName,
        metric_kind: MetricKind,
        unit: Option<Unit>,
        description: SharedString,
    ) {
        self.metadata
            .set_description(key_name, metric_kind, unit, description);
    }

    pub(super) fn set_histogram_bounds(&self, key_name: &KeyName, bounds: Vec<f64>) {
        self.metadata
            .set_histogram_bounds(key_name.to_retained(), bounds);
    }
}

#[derive(Clone, Default)]
pub(super) struct OtlpMetricMetadata {
    inner: Arc<OtlpMetricMetadataInner>,
}

#[derive(Default)]
struct OtlpMetricMetadataInner {
    descriptions: RwLock<HashMap<(KeyName, MetricKind), OtlpMetricDescription>>,
    histogram_bounds: RwLock<HashMap<KeyName, Vec<f64>>>,
}

impl OtlpMetricMetadata {
    pub(super) fn set_description(
        &self,
        key_name: KeyName,
        metric_kind: MetricKind,
        unit: Option<Unit>,
        description: SharedString,
    ) {
        self.inner
            .descriptions
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                (key_name.to_retained(), metric_kind),
                OtlpMetricDescription { unit, description },
            );
    }

    pub(super) fn get_description(
        &self,
        key_name: &KeyName,
        metric_kind: MetricKind,
    ) -> Option<OtlpMetricDescription> {
        self.inner
            .descriptions
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&(key_name.to_retained(), metric_kind))
            .cloned()
    }

    fn set_histogram_bounds(&self, key_name: KeyName, bounds: Vec<f64>) {
        self.inner
            .histogram_bounds
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(key_name, bounds);
    }

    pub(super) fn get_histogram_bounds(&self, key_name: &KeyName) -> Option<Vec<f64>> {
        self.inner
            .histogram_bounds
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&key_name.to_retained())
            .cloned()
    }
}

#[derive(Clone)]
pub(super) struct OtlpMetricDescription {
    pub(super) unit: Option<Unit>,
    pub(super) description: SharedString,
}
