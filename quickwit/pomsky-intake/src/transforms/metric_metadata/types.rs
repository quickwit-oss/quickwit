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

use serde::{Deserialize, Serialize};
use vector::event::{Metric, MetricValue};

// ---------------------------------------------------------------------------
// Metric type mapping (per D-03, D-04, D-09, D-10)
// ---------------------------------------------------------------------------

/// SaaS-side representation of a metric type, serialized to the exact API
/// string values expected by byoc-ingest-metadata-svc (D-10).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MetadataMetricType {
    Count,
    Rate,
    Gauge,
    // Explicit rename overrides rename_all; documents intent (D-10).
    #[serde(rename = "ddsketch")]
    DdSketch,
}

/// Pair of (metric_type, interval_seconds) sent to byoc-ingest-metadata-svc.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricTypeInfo {
    pub metric_type: MetadataMetricType,
    /// Reporting interval in whole seconds. 0 for point-in-time types.
    pub interval: u32,
}

/// Maps a Vector `Metric` to the SaaS type representation (D-03, D-04).
///
/// Mapping rules:
/// - `Counter` with no `interval_ms`  -> `count`   with `interval = 10`
/// - `Counter` with `interval_ms = N` -> `rate`    with `interval = N / 1000`
/// - `Gauge`                          -> `gauge`   with `interval = 0`
/// - `Sketch`                         -> `ddsketch` with `interval = 0`
/// - Any other variant (Set, Distribution, ...) -> `gauge` with `interval = 0`
///   (conservative fallback; these are not expected from DD Agent / OTel sources)
///
/// Note: `interval_ms / 1000` is integer division -- sub-second intervals
/// (< 1000 ms) round down to 0. This is intentional per D-04.
pub fn map_metric_type(metric: &Metric) -> MetricTypeInfo {
    match metric.value() {
        MetricValue::Counter { .. } => match metric.interval_ms() {
            None => MetricTypeInfo {
                metric_type: MetadataMetricType::Count,
                interval: 10,
            },
            Some(ms) => MetricTypeInfo {
                metric_type: MetadataMetricType::Rate,
                // Integer division is intentional per D-04: interval field is u32 seconds.
                interval: ms.get() / 1000,
            },
        },
        MetricValue::Gauge { .. } => MetricTypeInfo {
            metric_type: MetadataMetricType::Gauge,
            interval: 0,
        },
        MetricValue::Sketch { .. } => MetricTypeInfo {
            metric_type: MetadataMetricType::DdSketch,
            interval: 0,
        },
        // Other MetricValue variants (Set, Distribution, AggregatedHistogram,
        // AggregatedSummary) are not expected from Datadog Agent or OTel sources.
        // Default to Gauge with interval=0 as a conservative fallback.
        _ => MetricTypeInfo {
            metric_type: MetadataMetricType::Gauge,
            interval: 0,
        },
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use vector::event::{Metric, MetricKind, MetricValue};
    use vector_lib::metrics::AgentDDSketch;

    use super::*;

    #[test]
    fn test_counter_without_interval_maps_to_count() {
        let metric = Metric::new(
            "req",
            MetricKind::Incremental,
            MetricValue::Counter { value: 1.0 },
        );
        let info = map_metric_type(&metric);
        assert_eq!(info.metric_type, MetadataMetricType::Count);
        assert_eq!(info.interval, 10);
    }

    #[test]
    fn test_counter_with_interval_ms_maps_to_rate() {
        let metric = Metric::new(
            "req",
            MetricKind::Incremental,
            MetricValue::Counter { value: 1.0 },
        )
        .with_interval_ms(NonZeroU32::new(10_000)); // 10_000 ms = 10 s
        let info = map_metric_type(&metric);
        assert_eq!(info.metric_type, MetadataMetricType::Rate);
        assert_eq!(info.interval, 10);
    }

    #[test]
    fn test_gauge_maps_to_gauge() {
        let metric = Metric::new(
            "cpu",
            MetricKind::Absolute,
            MetricValue::Gauge { value: 0.5 },
        );
        let info = map_metric_type(&metric);
        assert_eq!(info.metric_type, MetadataMetricType::Gauge);
        assert_eq!(info.interval, 0);
    }

    #[test]
    fn test_sketch_maps_to_ddsketch() {
        let sketch = AgentDDSketch::with_agent_defaults();
        let metric = Metric::new(
            "latency",
            MetricKind::Incremental,
            MetricValue::Sketch {
                sketch: vector::event::metric::MetricSketch::AgentDDSketch(sketch),
            },
        );
        let info = map_metric_type(&metric);
        assert_eq!(info.metric_type, MetadataMetricType::DdSketch);
        assert_eq!(info.interval, 0);
    }

    #[test]
    fn test_metric_type_serialization() {
        // Verify serde round-trip produces the exact API strings (D-10).
        let count: MetadataMetricType = serde_yaml::from_str("\"count\"").unwrap();
        assert_eq!(count, MetadataMetricType::Count);
        let serialized = serde_yaml::to_string(&MetadataMetricType::Count).unwrap();
        assert!(
            serialized.contains("count"),
            "expected 'count', got: {serialized}"
        );

        let rate: MetadataMetricType = serde_yaml::from_str("\"rate\"").unwrap();
        assert_eq!(rate, MetadataMetricType::Rate);
        let serialized = serde_yaml::to_string(&MetadataMetricType::Rate).unwrap();
        assert!(
            serialized.contains("rate"),
            "expected 'rate', got: {serialized}"
        );

        let gauge: MetadataMetricType = serde_yaml::from_str("\"gauge\"").unwrap();
        assert_eq!(gauge, MetadataMetricType::Gauge);
        let serialized = serde_yaml::to_string(&MetadataMetricType::Gauge).unwrap();
        assert!(
            serialized.contains("gauge"),
            "expected 'gauge', got: {serialized}"
        );

        let ddsketch: MetadataMetricType = serde_yaml::from_str("\"ddsketch\"").unwrap();
        assert_eq!(ddsketch, MetadataMetricType::DdSketch);
        let serialized = serde_yaml::to_string(&MetadataMetricType::DdSketch).unwrap();
        assert!(
            serialized.contains("ddsketch"),
            "expected 'ddsketch', got: {serialized}"
        );
    }

    #[test]
    fn test_counter_with_sub_second_interval() {
        // 500 ms / 1000 = 0 (integer division). Documents intentional behavior per D-04.
        let metric = Metric::new(
            "req",
            MetricKind::Incremental,
            MetricValue::Counter { value: 1.0 },
        )
        .with_interval_ms(NonZeroU32::new(500));
        let info = map_metric_type(&metric);
        assert_eq!(info.metric_type, MetadataMetricType::Rate);
        assert_eq!(info.interval, 0, "sub-second interval rounds to 0 per D-04");
    }
}
