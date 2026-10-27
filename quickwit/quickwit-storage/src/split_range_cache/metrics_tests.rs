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

use ::metrics::with_local_recorder;
use metrics_util::debugging::{DebugValue, DebuggingRecorder};

use super::*;

#[test]
fn test_feuer_histogram_buckets_are_registered_with_exporter() {
    let configured_buckets: HashMap<_, _> = quickwit_metrics::histogram_buckets().collect();
    for name in [
        "feuer_lookup_duration_seconds",
        "feuer_disk_read_size_bytes",
        "feuer_disk_lookup_duration_seconds",
        "feuer_disk_io_duration_seconds",
        "feuer_disk_write_queue_duration_seconds",
        "feuer_disk_read_queue_wait_duration_seconds",
    ] {
        assert_eq!(configured_buckets[name], feuer_histogram_buckets(name));
    }
}

// Exercise the pinned dependency, not just our list of known histogram names.
// Registration checks upstream bucket definitions; the snapshot also detects
// missing exporter declarations and stale declarations for removed metrics.
// Buffer-pool gauges need no static declarations, but must retain their
// upstream type and every bucket/status label combination.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn test_feuer_initialization_registers_histograms_and_buffer_pool_metrics() {
    let configured_buckets: HashMap<_, _> = quickwit_metrics::histogram_buckets()
        .filter(|(name, _)| name.starts_with("feuer_"))
        .map(|(name, buckets)| (name.to_owned(), buckets))
        .collect();
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = ::metrics::set_default_local_recorder(&recorder);
    let temp_dir = tempfile::tempdir().unwrap();
    let _cache =
        super::super::FeuerSplitRangeCache::open(&super::super::config_for_test(temp_dir.path()))
            .await
            .unwrap();
    let mut registered_buckets = HashMap::new();
    let mut buffer_pool_metrics = HashMap::new();
    for (key, _, _, value) in snapshotter.snapshot().into_vec() {
        let name = key.key().name();
        if name.starts_with("feuer_") && matches!(value, DebugValue::Histogram(_)) {
            registered_buckets.insert(name.to_owned(), feuer_histogram_buckets(name));
        }
        if name.starts_with("feuer_") {
            assert!(
                key.key()
                    .labels()
                    .any(|label| { label.key() == "cache" && label.value() == "split_range" })
            );
        }
        if name == "feuer_disk_lookup_duration_seconds" {
            assert_eq!(key.key().labels().count(), 1);
        }
        if name.starts_with("feuer_io_buffer_pool_") {
            buffer_pool_metrics.insert(key.key().clone(), value);
        }
    }
    assert!(!registered_buckets.is_empty());
    assert_eq!(registered_buckets, configured_buckets);
    let buckets = [
        "32 KiB", "256 KiB", "512 KiB", "1 MiB", "2 MiB", "3 MiB", "4 MiB", "8 MiB", "12 MiB",
        "16 MiB", "24 MiB", "32 MiB", "64 MiB", ">64 MiB",
    ];
    assert_eq!(buffer_pool_metrics.len(), buckets.len() * 2);
    for bucket in buckets {
        for status in ["idle", "used"] {
            let labels = vec![
                ::metrics::Label::new("bucket", bucket),
                ::metrics::Label::new("status", status),
                ::metrics::Label::new("cache", "split_range"),
            ];
            let key = ::metrics::Key::from_parts("feuer_io_buffer_pool_bytes", labels);
            assert!(matches!(buffer_pool_metrics[&key], DebugValue::Gauge(_)));
        }
    }
}

#[test]
fn test_quickwit_metrics_registry_separates_caches() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let caches = [("split_range", 1), ("partial_request", 2), ("predicate", 3)];
    with_local_recorder(&recorder, || {
        for (cache_name, value) in caches {
            let registry = QuickwitMetricsRegistry { cache_name };
            registry
                .register_counter_vec("feuer_lookup_total".into(), "Lookups".into(), &["outcome"])
                .counter(&["memory_hit".into()])
                .increase(value);
            registry
                .register_gauge_vec("feuer_memory_used_bytes".into(), "Memory".into(), &[])
                .gauge(&[])
                .absolute(value);
            registry
                .register_histogram_vec_with_buckets(
                    "feuer_lookup_duration_seconds".into(),
                    "Lookup duration".into(),
                    &["outcome"],
                    feuer_histogram_buckets("feuer_lookup_duration_seconds"),
                )
                .histogram(&["disk_hit".into()])
                .record(value as f64);
        }
    });
    let values: HashMap<_, _> = snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .map(|(key, _, _, value)| (key.key().clone(), value))
        .collect();
    assert_eq!(values.len(), caches.len() * 3);
    for (cache_name, value) in caches {
        let lookup = ::metrics::Key::from_parts(
            "feuer_lookup_total",
            labeled(cache_name, &["outcome"], &["memory_hit".into()]),
        );
        assert_eq!(values[&lookup], DebugValue::Counter(value));
        let memory =
            ::metrics::Key::from_parts("feuer_memory_used_bytes", labeled(cache_name, &[], &[]));
        assert_eq!(values[&memory], DebugValue::Gauge((value as f64).into()));
        let duration = ::metrics::Key::from_parts(
            "feuer_lookup_duration_seconds",
            labeled(cache_name, &["outcome"], &["disk_hit".into()]),
        );
        assert_eq!(
            values[&duration],
            DebugValue::Histogram(vec![(value as f64).into()])
        );
    }
}

#[test]
fn test_quickwit_metrics_registry_preserves_values_and_labels() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    with_local_recorder(&recorder, || {
        let registry = QuickwitMetricsRegistry {
            cache_name: "split_range",
        };
        registry
            .register_counter_vec("feuer_lookup_total".into(), "Lookups".into(), &["outcome"])
            .counter(&["memory_hit".into()])
            .increase(3);
        let gauge = registry
            .register_gauge_vec("feuer_memory_used_bytes".into(), "Memory".into(), &[])
            .gauge(&[]);
        gauge.absolute(7);
        gauge.increase(3);
        gauge.decrease(2);
        registry
            .register_histogram_vec_with_buckets(
                "feuer_lookup_duration_seconds".into(),
                "Lookup duration".into(),
                &["outcome"],
                feuer_histogram_buckets("feuer_lookup_duration_seconds"),
            )
            .histogram(&["memory_hit".into()])
            .record(0.5);
        registry
            .register_histogram_vec_with_buckets(
                "feuer_disk_read_size_bytes".into(),
                "Disk read size".into(),
                &[],
                Buckets::exponential(1024.0, 2.0, 21),
            )
            .histogram(&[])
            .record(4096.0);
        registry
            .register_histogram_vec_with_buckets(
                "feuer_disk_write_queue_duration_seconds".into(),
                "Disk-write queue duration".into(),
                &[],
                Buckets::exponential(0.000_001, 2.0, 25),
            )
            .histogram(&[])
            .record(0.25);
        registry
            .register_histogram_vec_with_buckets(
                "feuer_disk_read_queue_wait_duration_seconds".into(),
                "Read-piece channel-capacity wait".into(),
                &[],
                Buckets::exponential(0.000_001, 2.0, 25),
            )
            .histogram(&[])
            .record(0.125);
        let waiters = registry
            .register_gauge_vec(
                "feuer_disk_read_queue_waiters".into(),
                "Read-piece requests waiting for channel capacity".into(),
                &[],
            )
            .gauge(&[]);
        waiters.increase(2);
        waiters.decrease(1);
    });
    let snapshot = snapshotter.snapshot().into_vec();
    assert_eq!(snapshot.len(), 7);
    for (key, _, _, value) in snapshot {
        assert!(
            key.key()
                .labels()
                .any(|label| { label.key() == "cache" && label.value() == "split_range" })
        );
        match key.key().name() {
            "feuer_lookup_total" => {
                assert_eq!(value, DebugValue::Counter(3));
                assert_eq!(
                    key.key().labels().collect::<Vec<_>>(),
                    vec![
                        &::metrics::Label::new("outcome", "memory_hit"),
                        &::metrics::Label::new("cache", "split_range"),
                    ]
                );
            }
            "feuer_memory_used_bytes" => assert_eq!(value, DebugValue::Gauge(8.0.into())),
            "feuer_lookup_duration_seconds" => {
                assert_eq!(value, DebugValue::Histogram(vec![0.5.into()]))
            }
            "feuer_disk_read_size_bytes" => {
                assert_eq!(value, DebugValue::Histogram(vec![4096.0.into()]));
                assert_eq!(key.key().labels().count(), 1);
            }
            "feuer_disk_write_queue_duration_seconds" => {
                assert_eq!(value, DebugValue::Histogram(vec![0.25.into()]));
                assert_eq!(key.key().labels().count(), 1);
            }
            "feuer_disk_read_queue_wait_duration_seconds" => {
                assert_eq!(value, DebugValue::Histogram(vec![0.125.into()]));
                assert_eq!(key.key().labels().count(), 1);
            }
            "feuer_disk_read_queue_waiters" => {
                assert_eq!(value, DebugValue::Gauge(1.0.into()));
                assert_eq!(key.key().labels().count(), 1);
            }
            name => panic!("unexpected metric: {name}"),
        }
    }
}
