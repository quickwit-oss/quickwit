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

use metrics::with_local_recorder;
use metrics_exporter_prometheus::PrometheusBuilder;
use metrics_util::debugging::{DebugValue, DebuggingRecorder};

use super::*;

type MetricEntry = (String, Vec<(String, String)>, DebugValue);

fn with_recorder(f: impl FnOnce()) -> Vec<MetricEntry> {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    with_local_recorder(&recorder, f);
    snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .map(|(composite_key, _unit, _description, value)| {
            let (_, key) = composite_key.into_parts();
            let labels = key
                .labels()
                .map(|label| (label.key().to_string(), label.value().to_string()))
                .collect();
            (key.name().to_string(), labels, value)
        })
        .collect()
}

#[test]
fn counter_increment_and_absolute_values() {
    let entries = with_recorder(|| {
        let counter = counter!(
            name: "test_counter_increment",
            description: "test counter",
            subsystem: "metrics_tests",
        );
        counter.increment(5);

        let absolute = counter!(
            name: "test_counter_absolute",
            description: "absolute counter",
            subsystem: "metrics_tests",
        );
        absolute.absolute(42);
    });

    assert!(entries.contains(&(
        "quickwit_metrics_tests_test_counter_increment".to_string(),
        Vec::new(),
        DebugValue::Counter(5),
    )));
    assert!(entries.contains(&(
        "quickwit_metrics_tests_test_counter_absolute".to_string(),
        Vec::new(),
        DebugValue::Counter(42),
    )));
}

#[test]
fn gauge_set_increment_and_decrement() {
    let entries = with_recorder(|| {
        let gauge = gauge!(
            name: "test_gauge",
            description: "test gauge",
            subsystem: "metrics_tests",
        );
        gauge.set(10.0);
        gauge.increment(5.0);
        gauge.decrement(3.0);
    });

    assert_eq!(
        entries[0],
        (
            "quickwit_metrics_tests_test_gauge".to_string(),
            Vec::new(),
            DebugValue::Gauge(12.0.into()),
        )
    );
}

#[test]
fn histogram_records_value() {
    let entries = with_recorder(|| {
        let histogram = histogram!(
            name: "test_histogram",
            description: "test histogram",
            subsystem: "metrics_tests",
            buckets: vec![1.0, 5.0, 10.0],
        );
        histogram.record(3.5);
    });

    let (name, labels, value) = &entries[0];
    assert_eq!(name, "quickwit_metrics_tests_test_histogram");
    assert!(labels.is_empty());
    match value {
        DebugValue::Histogram(values) => {
            assert_eq!(values.len(), 1);
            assert_eq!(values[0].into_inner(), 3.5);
        }
        other => panic!("expected histogram, got {other:?}"),
    }
}

#[test]
fn histogram_timer_records_value_on_drop() {
    let entries = with_recorder(|| {
        let histogram = histogram!(
            name: "test_histogram_timer_drop",
            description: "test histogram timer drop",
            subsystem: "metrics_tests",
            buckets: vec![1.0, 5.0, 10.0],
        );
        let _timer = histogram.start_timer();
    });

    let (name, labels, value) = &entries[0];
    assert_eq!(name, "quickwit_metrics_tests_test_histogram_timer_drop");
    assert!(labels.is_empty());
    match value {
        DebugValue::Histogram(values) => {
            assert_eq!(values.len(), 1);
            assert!(values[0].into_inner() >= 0.0);
        }
        other => panic!("expected histogram, got {other:?}"),
    }
}

#[test]
fn histogram_timer_observe_duration_records_once() {
    let entries = with_recorder(|| {
        let histogram = histogram!(
            name: "test_histogram_timer_observe_duration",
            description: "test histogram timer observe duration",
            subsystem: "metrics_tests",
            buckets: vec![1.0, 5.0, 10.0],
        );
        histogram.start_timer().observe_duration();
    });

    let (name, labels, value) = &entries[0];
    assert_eq!(
        name,
        "quickwit_metrics_tests_test_histogram_timer_observe_duration"
    );
    assert!(labels.is_empty());
    match value {
        DebugValue::Histogram(values) => {
            assert_eq!(values.len(), 1);
            assert!(values[0].into_inner() >= 0.0);
        }
        other => panic!("expected histogram, got {other:?}"),
    }
}

#[test]
fn empty_subsystem_omits_double_underscore() {
    let entries = with_recorder(|| {
        let counter = counter!(
            name: "empty_subsystem_counter",
            description: "empty subsystem counter",
            subsystem: "",
        );
        counter.increment(1);
    });

    assert_eq!(entries[0].0, "quickwit_empty_subsystem_counter");
}

#[test]
fn static_labels_are_preserved() {
    let entries = with_recorder(|| {
        let counter = counter!(
            name: "static_labels_counter",
            description: "static labels counter",
            subsystem: "metrics_tests",
            "env" => "prod",
            "region" => "eu",
        );
        counter.increment(1);
    });

    assert_eq!(
        entries[0].1,
        vec![
            ("env".to_string(), "prod".to_string()),
            ("region".to_string(), "eu".to_string()),
        ]
    );
}

#[test]
fn parent_labels_dynamic_values_and_nested_extension() {
    let entries = with_recorder(|| {
        let base = counter!(
            name: "nested_counter",
            description: "nested counter",
            subsystem: "metrics_tests",
            "env" => "prod",
        );
        let region = String::from("us-east");
        let child = counter!(parent: base, "region" => region);
        let grandchild = counter!(parent: child, "az" => "use1-a");
        grandchild.increment(7);
    });

    let grandchild = entries
        .iter()
        .find(|(name, labels, _)| {
            name == "quickwit_metrics_tests_nested_counter" && labels.len() == 3
        })
        .expect("grandchild metric should be recorded");
    assert_eq!(
        grandchild.1,
        vec![
            ("env".to_string(), "prod".to_string()),
            ("region".to_string(), "us-east".to_string()),
            ("az".to_string(), "use1-a".to_string()),
        ]
    );
    assert_eq!(grandchild.2, DebugValue::Counter(7));
}

#[test]
fn observable_counter_and_gauge_get_values() {
    with_recorder(|| {
        let counter = counter!(
            name: "observable_counter",
            description: "observable counter",
            subsystem: "metrics_tests",
            observable: true,
        );
        counter.increment(3);
        counter.absolute(11);
        assert_eq!(counter.get(), 11);

        let gauge = gauge!(
            name: "observable_gauge",
            description: "observable gauge",
            subsystem: "metrics_tests",
            observable: true,
        );
        gauge.set(10.0);
        gauge.increment(2.0);
        gauge.decrement(1.0);
        assert_eq!(gauge.get(), 11.0);
    });
}

#[test]
fn non_observable_metrics_return_sentinel_values() {
    with_recorder(|| {
        let counter = counter!(
            name: "non_observable_counter",
            description: "non observable counter",
            subsystem: "metrics_tests",
        );
        counter.increment(1);
        assert_eq!(counter.get(), u64::MAX);

        let gauge = gauge!(
            name: "non_observable_gauge",
            description: "non observable gauge",
            subsystem: "metrics_tests",
        );
        gauge.set(1.0);
        assert!(gauge.get().is_nan());
    });
}

#[test]
fn gauge_guard_balances_variable_delta_on_drop() {
    let entries = with_recorder(|| {
        let gauge = gauge!(
            name: "guarded_gauge",
            description: "guarded gauge",
            subsystem: "metrics_tests",
            observable: true,
        );
        gauge.set(10.0);
        {
            let mut guard = GaugeGuard::from_gauge(&gauge);
            guard.add(5);
            guard.sub(2);
            assert_eq!(guard.get(), 3);
            assert_eq!(guard.value(), 3.0);
            assert_eq!(gauge.get(), 13.0);
        }
        assert_eq!(gauge.get(), 10.0);
    });

    assert_eq!(entries[0].2, DebugValue::Gauge(10.0.into()));
}

#[test]
fn histogram_bucket_inventory_contains_declared_buckets() {
    with_recorder(|| {
        let _ = histogram!(
            name: "bucketed_histogram",
            description: "bucketed histogram",
            subsystem: "metrics_tests",
            buckets: vec![0.1, 1.0, 10.0],
        );
    });

    assert!(histogram_buckets().any(|(name, buckets)| {
        name == "quickwit_metrics_tests_bucketed_histogram" && buckets == vec![0.1, 1.0, 10.0]
    }));
}

#[test]
fn metrics_info_contains_declared_metadata() {
    with_recorder(|| {
        let _ = counter!(
            name: "metadata_counter",
            description: "metadata counter",
            subsystem: "metrics_tests",
            observable: true,
        );
    });

    let info = metrics_info()
        .find(|info| info.key_name == "quickwit_metrics_tests_metadata_counter")
        .expect("metadata counter info should be registered");
    assert_eq!(info.name, "metadata_counter");
    assert_eq!(info.subsystem, "metrics_tests");
    assert_eq!(info.description, "metadata counter");
    assert_eq!(info.kind, MetricKind::Counter);
    assert!(info.observable);
}

#[test]
fn describe_metrics_sets_debugging_recorder_description() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    with_local_recorder(&recorder, || {
        let counter = counter!(
            name: "described_counter",
            description: "described counter",
            subsystem: "metrics_tests",
        );
        describe_metrics();
        counter.increment(1);
    });

    let snapshot = snapshotter.snapshot().into_vec();
    let (_, _, description, _) = snapshot
        .into_iter()
        .find(|(composite_key, _, _, _)| {
            let (_, key) = composite_key.clone().into_parts();
            key.name() == "quickwit_metrics_tests_described_counter"
        })
        .expect("described counter should be recorded");
    assert_eq!(description.as_deref(), Some("described counter"));
}

#[test]
fn metrics_text_payload_renders_prometheus_handle() {
    let recorder = PrometheusBuilder::new().build_recorder();
    set_prometheus_handle(recorder.handle()).expect("Prometheus handle should be set once");

    with_local_recorder(&recorder, || {
        let counter = counter!(
            name: "prometheus_payload_counter",
            description: "prometheus payload counter",
            subsystem: "metrics_tests",
        );
        describe_metrics();
        counter.increment(1);
    });

    let payload = metrics_text_payload().expect("Prometheus payload should render");
    assert!(payload.contains("# HELP quickwit_metrics_tests_prometheus_payload_counter"));
    assert!(payload.contains("quickwit_metrics_tests_prometheus_payload_counter 1"));
}
