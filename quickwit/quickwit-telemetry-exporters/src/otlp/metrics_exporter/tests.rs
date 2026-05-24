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

use metrics::{KeyName, Unit};
use opentelemetry::KeyValue;
use opentelemetry::metrics::MeterProvider;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, Metric, MetricData, ResourceMetrics};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};

use super::*;

fn setup_recorder() -> (
    OtlpMetricsRecorder,
    SdkMeterProvider,
    InMemoryMetricExporter,
) {
    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter.clone())
        .build();
    let recorder = OtlpMetricsRecorder::new(provider.meter("quickwit-test"));
    (recorder, provider, exporter)
}

fn exported_metrics(
    provider: &SdkMeterProvider,
    exporter: &InMemoryMetricExporter,
) -> Vec<ResourceMetrics> {
    provider.force_flush().expect("metrics should flush");
    exporter
        .get_finished_metrics()
        .expect("metrics should be exported")
}

fn find_metric<'a>(resource_metrics: &'a [ResourceMetrics], name: &str) -> &'a Metric {
    resource_metrics
        .iter()
        .rev()
        .flat_map(|resource_metric| resource_metric.scope_metrics())
        .flat_map(|scope_metrics| scope_metrics.metrics())
        .find(|metric| metric.name() == name)
        .unwrap_or_else(|| panic!("metric `{name}` should be exported"))
}

fn has_attributes<'a>(
    attributes: impl Iterator<Item = &'a KeyValue>,
    expected_attributes: &[(&str, &str)],
) -> bool {
    let attributes: Vec<_> = attributes.collect();
    expected_attributes
        .iter()
        .all(|(expected_key, expected_value)| {
            attributes.iter().any(|attribute| {
                attribute.key.as_str() == *expected_key
                    && attribute.value.to_string() == *expected_value
            })
        })
}

#[test]
fn counter_exports_increment_and_absolute_delta() {
    let (recorder, provider, exporter) = setup_recorder();

    metrics::with_local_recorder(&recorder, || {
        let counter = metrics::counter!(
            description: "Total test requests",
            unit: Unit::Count,
            "otlp_test_requests_total",
            "route" => "search",
        );
        counter.increment(2);
        counter.absolute(10);
        counter.absolute(4);
        counter.increment(3);
    });

    let resource_metrics = exported_metrics(&provider, &exporter);
    let metric = find_metric(&resource_metrics, "otlp_test_requests_total");
    assert_eq!(metric.description(), "Total test requests");
    assert_eq!(metric.unit(), "1");

    let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() else {
        panic!("counter should export a u64 sum");
    };
    assert!(sum.is_monotonic());
    let data_points: Vec<_> = sum.data_points().collect();
    assert_eq!(data_points.len(), 1);
    assert_eq!(data_points[0].value(), 13);
    assert!(has_attributes(
        data_points[0].attributes(),
        &[("route", "search")]
    ));
}

#[test]
fn counter_exports_distinct_data_points_for_distinct_label_sets() {
    let (recorder, provider, exporter) = setup_recorder();

    metrics::with_local_recorder(&recorder, || {
        metrics::describe_counter!(
            "otlp_test_labeled_requests_total",
            Unit::Count,
            "Total labeled requests"
        );
        metrics::counter!(
            "otlp_test_labeled_requests_total",
            "method" => "GET",
            "status" => "200",
        )
        .increment(1);
        metrics::counter!(
            "otlp_test_labeled_requests_total",
            "method" => "POST",
            "status" => "201",
        )
        .increment(2);
    });

    let resource_metrics = exported_metrics(&provider, &exporter);
    let metric = find_metric(&resource_metrics, "otlp_test_labeled_requests_total");
    assert_eq!(metric.description(), "Total labeled requests");
    assert_eq!(metric.unit(), "1");

    let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() else {
        panic!("counter should export a u64 sum");
    };
    let data_points: Vec<_> = sum.data_points().collect();
    assert_eq!(data_points.len(), 2);

    let get_point = data_points
        .iter()
        .find(|data_point| {
            has_attributes(
                data_point.attributes(),
                &[("method", "GET"), ("status", "200")],
            )
        })
        .expect("GET counter data point should exist");
    assert_eq!(get_point.value(), 1);

    let post_point = data_points
        .iter()
        .find(|data_point| {
            has_attributes(
                data_point.attributes(),
                &[("method", "POST"), ("status", "201")],
            )
        })
        .expect("POST counter data point should exist");
    assert_eq!(post_point.value(), 2);
}

#[test]
fn counter_accumulates_increments_across_flushes() {
    let (recorder, provider, exporter) = setup_recorder();

    metrics::with_local_recorder(&recorder, || {
        metrics::counter!("otlp_test_events_total").increment(5);
    });

    let resource_metrics = exported_metrics(&provider, &exporter);
    let metric = find_metric(&resource_metrics, "otlp_test_events_total");
    let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() else {
        panic!("counter should export a u64 sum");
    };
    let first_point = sum
        .data_points()
        .next()
        .expect("counter data point should exist");
    assert_eq!(first_point.value(), 5);

    metrics::with_local_recorder(&recorder, || {
        metrics::counter!("otlp_test_events_total").increment(3);
    });

    let resource_metrics = exported_metrics(&provider, &exporter);
    let metric = find_metric(&resource_metrics, "otlp_test_events_total");
    let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() else {
        panic!("counter should export a u64 sum");
    };
    let latest_point = sum
        .data_points()
        .next()
        .expect("counter data point should exist");
    assert_eq!(latest_point.value(), 8);
}

#[test]
fn gauge_exports_latest_observed_value() {
    let (recorder, provider, exporter) = setup_recorder();

    metrics::with_local_recorder(&recorder, || {
        let gauge = metrics::gauge!(
            description: "Active test shards",
            unit: Unit::Count,
            "otlp_test_active_shards",
            "node" => "node-a",
        );
        gauge.set(5.0);
        gauge.increment(2.0);
        gauge.decrement(1.5);
    });

    let resource_metrics = exported_metrics(&provider, &exporter);
    let metric = find_metric(&resource_metrics, "otlp_test_active_shards");
    assert_eq!(metric.description(), "Active test shards");
    assert_eq!(metric.unit(), "1");

    let AggregatedMetrics::F64(MetricData::Gauge(gauge)) = metric.data() else {
        panic!("gauge should export an f64 gauge");
    };
    let data_points: Vec<_> = gauge.data_points().collect();
    assert_eq!(data_points.len(), 1);
    assert_eq!(data_points[0].value(), 5.5);
    assert!(has_attributes(
        data_points[0].attributes(),
        &[("node", "node-a")]
    ));
}

#[test]
fn gauge_exports_distinct_data_points_for_distinct_label_sets() {
    let (recorder, provider, exporter) = setup_recorder();

    metrics::with_local_recorder(&recorder, || {
        metrics::describe_gauge!("otlp_test_cpu_usage", Unit::Percent, "Current CPU usage");
        metrics::gauge!("otlp_test_cpu_usage", "core" => "0").set(45.5);
        metrics::gauge!("otlp_test_cpu_usage", "core" => "1").set(62.3);
    });

    let resource_metrics = exported_metrics(&provider, &exporter);
    let metric = find_metric(&resource_metrics, "otlp_test_cpu_usage");
    assert_eq!(metric.description(), "Current CPU usage");
    assert_eq!(metric.unit(), "%");

    let AggregatedMetrics::F64(MetricData::Gauge(gauge)) = metric.data() else {
        panic!("gauge should export an f64 gauge");
    };
    let data_points: Vec<_> = gauge.data_points().collect();
    assert_eq!(data_points.len(), 2);

    let core_0_point = data_points
        .iter()
        .find(|data_point| has_attributes(data_point.attributes(), &[("core", "0")]))
        .expect("core 0 gauge data point should exist");
    assert_eq!(core_0_point.value(), 45.5);

    let core_1_point = data_points
        .iter()
        .find(|data_point| has_attributes(data_point.attributes(), &[("core", "1")]))
        .expect("core 1 gauge data point should exist");
    assert_eq!(core_1_point.value(), 62.3);
}

#[test]
fn gauge_updates_value_across_flushes() {
    let (recorder, provider, exporter) = setup_recorder();

    metrics::with_local_recorder(&recorder, || {
        metrics::gauge!("otlp_test_memory_usage").set(1024.0);
    });
    exported_metrics(&provider, &exporter);

    metrics::with_local_recorder(&recorder, || {
        metrics::gauge!("otlp_test_memory_usage").set(2048.0);
    });

    let resource_metrics = exported_metrics(&provider, &exporter);
    let metric = find_metric(&resource_metrics, "otlp_test_memory_usage");
    let AggregatedMetrics::F64(MetricData::Gauge(gauge)) = metric.data() else {
        panic!("gauge should export an f64 gauge");
    };
    let data_point = gauge
        .data_points()
        .next()
        .expect("gauge data point should exist");
    assert_eq!(data_point.value(), 2048.0);
}

#[test]
fn histogram_exports_distinct_data_points_for_distinct_label_sets() {
    let (recorder, provider, exporter) = setup_recorder();

    metrics::with_local_recorder(&recorder, || {
        metrics::describe_histogram!(
            "otlp_test_response_time_seconds",
            Unit::Seconds,
            "Response time distribution"
        );
        metrics::histogram!("otlp_test_response_time_seconds", "endpoint" => "/api/users")
            .record(0.123);
        metrics::histogram!("otlp_test_response_time_seconds", "endpoint" => "/api/users")
            .record(0.456);
        metrics::histogram!("otlp_test_response_time_seconds", "endpoint" => "/api/posts")
            .record(0.789);
    });

    let resource_metrics = exported_metrics(&provider, &exporter);
    let metric = find_metric(&resource_metrics, "otlp_test_response_time_seconds");
    assert_eq!(metric.description(), "Response time distribution");
    assert_eq!(metric.unit(), "s");

    let AggregatedMetrics::F64(MetricData::Histogram(histogram)) = metric.data() else {
        panic!("histogram should export an f64 histogram");
    };
    let data_points: Vec<_> = histogram.data_points().collect();
    assert_eq!(data_points.len(), 2);

    let users_point = data_points
        .iter()
        .find(|data_point| has_attributes(data_point.attributes(), &[("endpoint", "/api/users")]))
        .expect("users histogram data point should exist");
    assert_eq!(users_point.count(), 2);
    assert!((users_point.sum() - (0.123 + 0.456)).abs() < f64::EPSILON);

    let posts_point = data_points
        .iter()
        .find(|data_point| has_attributes(data_point.attributes(), &[("endpoint", "/api/posts")]))
        .expect("posts histogram data point should exist");
    assert_eq!(posts_point.count(), 1);
    assert!((posts_point.sum() - 0.789).abs() < f64::EPSILON);
}

#[test]
fn histogram_exports_configured_boundaries() {
    let (recorder, provider, exporter) = setup_recorder();
    recorder.set_histogram_bounds(
        &KeyName::from("otlp_test_request_duration_seconds"),
        vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
    );

    metrics::with_local_recorder(&recorder, || {
        let histogram = metrics::histogram!(
            description: "Test request duration",
            unit: Unit::Seconds,
            "otlp_test_request_duration_seconds",
            "route" => "ingest",
        );
        histogram.record(0.03);
        histogram.record(0.12);
        histogram.record(0.75);
        histogram.record(3.5);
    });

    let resource_metrics = exported_metrics(&provider, &exporter);
    let metric = find_metric(&resource_metrics, "otlp_test_request_duration_seconds");
    assert_eq!(metric.description(), "Test request duration");
    assert_eq!(metric.unit(), "s");

    let AggregatedMetrics::F64(MetricData::Histogram(histogram)) = metric.data() else {
        panic!("histogram should export an f64 histogram");
    };
    let data_points: Vec<_> = histogram.data_points().collect();
    assert_eq!(data_points.len(), 1);
    assert!(has_attributes(
        data_points[0].attributes(),
        &[("route", "ingest")]
    ));
    assert_eq!(
        data_points[0].bounds().collect::<Vec<_>>(),
        vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
    );
    assert_eq!(data_points[0].count(), 4);
    assert!((data_points[0].sum() - (0.03 + 0.12 + 0.75 + 3.5)).abs() < f64::EPSILON);
    assert_eq!(
        data_points[0].bucket_counts().collect::<Vec<_>>(),
        vec![0, 1, 0, 1, 0, 1, 0, 1, 0, 0]
    );
}

#[test]
fn metric_descriptions_and_units_are_exported() {
    let (recorder, provider, exporter) = setup_recorder();

    metrics::with_local_recorder(&recorder, || {
        metrics::describe_counter!(
            "otlp_test_described_counter",
            Unit::Count,
            "Counter description"
        );
        metrics::describe_gauge!(
            "otlp_test_described_gauge",
            Unit::Bytes,
            "Gauge description"
        );
        metrics::describe_histogram!(
            "otlp_test_described_histogram",
            Unit::Milliseconds,
            "Histogram description"
        );

        metrics::counter!("otlp_test_described_counter").increment(1);
        metrics::gauge!("otlp_test_described_gauge").set(42.0);
        metrics::histogram!("otlp_test_described_histogram").record(0.5);
    });

    let resource_metrics = exported_metrics(&provider, &exporter);

    let counter = find_metric(&resource_metrics, "otlp_test_described_counter");
    assert_eq!(counter.description(), "Counter description");
    assert_eq!(counter.unit(), "1");

    let gauge = find_metric(&resource_metrics, "otlp_test_described_gauge");
    assert_eq!(gauge.description(), "Gauge description");
    assert_eq!(gauge.unit(), "By");

    let histogram = find_metric(&resource_metrics, "otlp_test_described_histogram");
    assert_eq!(histogram.description(), "Histogram description");
    assert_eq!(histogram.unit(), "ms");
}
