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

use std::collections::{HashMap, HashSet};
use std::panic::{AssertUnwindSafe, catch_unwind};

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};

use super::declarations::DECLARATIONS;
use super::*;

#[test]
fn test_complete_inventory_and_buckets() {
    // Inventory is available before any lazy parent or native cache is initialized.
    let inventory: Vec<_> = quickwit_metrics::metrics_info()
        .filter(|info| info.key_name.starts_with("feuer_"))
        .collect();
    assert_eq!(inventory.len(), DECLARATIONS.len());
    let inventory: HashMap<_, _> = inventory
        .into_iter()
        .map(|info| (info.key_name, info))
        .collect();
    assert_eq!(
        inventory.len(),
        DECLARATIONS.len(),
        "duplicate inventory entries"
    );
    let buckets: HashMap<_, _> = quickwit_metrics::histogram_buckets()
        .filter(|(name, _)| name.starts_with("feuer_"))
        .collect();
    let mut counts = [0; 3];
    for declaration in DECLARATIONS {
        let info = inventory[declaration.name];
        assert_eq!(info.kind, declaration.kind);
        assert_eq!(info.description, declaration.description);
        assert!(info.static_labels.is_empty());
        match declaration.kind {
            MetricKind::Counter => counts[0] += 1,
            MetricKind::Gauge => counts[1] += 1,
            MetricKind::Histogram => counts[2] += 1,
        }
        if let Some(expected) = declaration.buckets {
            assert_eq!(buckets[declaration.name], expected());
        } else {
            assert!(!buckets.contains_key(declaration.name));
        }
    }
    assert_eq!(counts, [12, 11, 6]);
    assert_eq!(buckets.len(), counts[2]);
}

// Static parent handles are shared between tests, so a parent may belong to another test's
// recorder. Check that parents observed here have no observations. Callers check the returned
// metrics for each cache.
fn cache_metrics(
    snapshotter: &Snapshotter,
    caches: &[&str],
) -> HashMap<::metrics::Key, DebugValue> {
    let mut cache_metrics = HashMap::new();
    for (key, _, _, value) in snapshotter.snapshot().into_vec() {
        let key = key.key();
        let declaration = Declaration::lookup(key.name());
        let labels: HashMap<_, _> = key
            .labels()
            .map(|label| (label.key(), label.value()))
            .collect();
        let Some(cache) = labels.get("cache") else {
            assert!(labels.is_empty(), "unexpected parent labels: {key:?}");
            let zero = match declaration.kind {
                MetricKind::Counter => DebugValue::Counter(0),
                MetricKind::Gauge => DebugValue::Gauge(0.0.into()),
                MetricKind::Histogram => DebugValue::Histogram(Vec::new()),
            };
            assert_eq!(value, zero, "observations must go to children");
            continue;
        };
        assert!(caches.contains(cache), "unexpected cache label: {key:?}");
        let actual: HashSet<_> = labels.keys().copied().collect();
        let expected: HashSet<_> = declaration
            .label_names
            .iter()
            .copied()
            .chain(["cache"])
            .collect();
        assert_eq!(actual, expected, "incorrect child labels: {key:?}");
        assert_eq!(key.labels().len(), expected.len(), "duplicate labels");
        cache_metrics.insert(key.clone(), value);
    }
    cache_metrics
}

#[test]
fn test_all_typed_metrics_preserve_values_and_cache_isolation() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let caches = [
        "metrics_values_split",
        "metrics_values_partial",
        "metrics_values_predicate",
    ];
    ::metrics::with_local_recorder(&recorder, || {
        quickwit_metrics::describe_metrics();
        for (index, cache_name) in caches.iter().copied().enumerate() {
            let registry = QuickwitMetricsRegistry { cache_name };
            let value = index as u64 + 1;
            for declaration in DECLARATIONS {
                let label_values: Vec<_> = declaration
                    .label_names
                    .iter()
                    .map(|_| "native_value".into())
                    .collect();
                let name = declaration.name.into();
                let desc = declaration.description.into();
                match declaration.kind {
                    MetricKind::Counter => {
                        registry
                            .register_counter_vec(name, desc, declaration.label_names)
                            .counter(&label_values)
                            .increase(value);
                    }
                    MetricKind::Gauge => {
                        let gauge = registry
                            .register_gauge_vec(name, desc, declaration.label_names)
                            .gauge(&label_values);
                        gauge.absolute(value);
                        gauge.increase(3);
                        gauge.decrease(2);
                    }
                    MetricKind::Histogram => {
                        registry
                            .register_histogram_vec_with_buckets(
                                name,
                                desc,
                                declaration.label_names,
                                declaration.buckets.unwrap()(),
                            )
                            .histogram(&label_values)
                            .record(value as f64);
                    }
                }
            }
        }
    });
    let values = cache_metrics(&snapshotter, &caches);
    assert_eq!(values.len(), DECLARATIONS.len() * caches.len());
    for (index, cache) in caches.into_iter().enumerate() {
        let value = index as u64 + 1;
        for declaration in DECLARATIONS {
            let labels: Vec<_> = declaration
                .label_names
                .iter()
                .map(|name| ::metrics::Label::new(*name, "native_value"))
                .chain([::metrics::Label::new("cache", cache)])
                .collect();
            let expected = match declaration.kind {
                MetricKind::Counter => DebugValue::Counter(value),
                MetricKind::Gauge => DebugValue::Gauge(((value + 1) as f64).into()),
                MetricKind::Histogram => DebugValue::Histogram(vec![(value as f64).into()]),
            };
            assert_eq!(
                values[&::metrics::Key::from_parts(declaration.name, labels)],
                expected
            );
        }
    }
}

#[test]
fn test_rejects_native_schema_changes() {
    let registry = QuickwitMetricsRegistry {
        cache_name: "metrics_rejection",
    };
    assert!(
        catch_unwind(|| registry.register_counter_vec("feuer_unknown".into(), "".into(), &[]))
            .is_err()
    );
    for declaration in DECLARATIONS {
        let wrong_kind = match declaration.kind {
            MetricKind::Counter => MetricKind::Gauge,
            _ => MetricKind::Counter,
        };
        for (kind, description, labels) in [
            (wrong_kind, declaration.description, declaration.label_names),
            (
                declaration.kind,
                "changed description",
                declaration.label_names,
            ),
            (
                declaration.kind,
                declaration.description,
                &["unexpected"][..],
            ),
        ] {
            assert!(
                catch_unwind(|| registry.register(declaration.name, description, labels, kind))
                    .is_err()
            );
        }
        if declaration.label_names.len() > 1 {
            let mut reversed = declaration.label_names.to_vec();
            reversed.reverse();
            assert!(
                catch_unwind(|| declaration.validate(
                    declaration.kind,
                    declaration.description,
                    &reversed
                ))
                .is_err()
            );
        }
        let vector = registry.register(
            declaration.name,
            declaration.description,
            declaration.label_names,
            declaration.kind,
        );
        for count in [declaration.label_names.len() + 1, 0] {
            if count == declaration.label_names.len() {
                continue;
            }
            let invalid_values = vec![Cow::Borrowed("invalid"); count];
            assert!(
                catch_unwind(AssertUnwindSafe(|| match declaration.kind {
                    MetricKind::Counter => {
                        vector.counter(&invalid_values);
                    }
                    MetricKind::Gauge => {
                        vector.gauge(&invalid_values);
                    }
                    MetricKind::Histogram => {
                        vector.histogram(&invalid_values);
                    }
                }))
                .is_err()
            );
        }
        if let Some(buckets) = declaration.buckets {
            assert!(
                catch_unwind(|| registry.register_histogram_vec(
                    declaration.name.into(),
                    declaration.description.into(),
                    declaration.label_names,
                ))
                .is_err()
            );
            let mut changed = buckets();
            changed[0] *= 2.0;
            assert!(
                catch_unwind(|| registry.register_histogram_vec_with_buckets(
                    declaration.name.into(),
                    declaration.description.into(),
                    declaration.label_names,
                    changed,
                ))
                .is_err()
            );
        }
    }
}

#[test]
fn test_global_labels_are_inherited() {
    const CHILD: &str = "QW_FEUER_METRICS_LABEL_TEST";
    if std::env::var_os(CHILD).is_none() {
        // Global labels and typed handles initialize once per process. Exercise startup in a
        // separate process so this test neither mutates nor depends on other tests' globals.
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "split_range_cache::metrics::tests::test_global_labels_are_inherited",
            ])
            .env(CHILD, "1")
            .env(
                "QW_METRICS_LABELS",
                "environment=feuer_test,region=test_region",
            )
            .status()
            .unwrap();
        assert!(status.success());
        return;
    }
    quickwit_metrics::init_metrics_labels_env_var().unwrap();
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    ::metrics::with_local_recorder(&recorder, || {
        let declaration = Declaration::lookup("feuer_lookup_total");
        QuickwitMetricsRegistry {
            cache_name: "metrics_global_labels",
        }
        .register_counter_vec(
            declaration.name.into(),
            declaration.description.into(),
            declaration.label_names,
        )
        .counter(&["memory_hit".into()])
        .increase(7);
    });
    let series = snapshotter.snapshot().into_vec();
    assert_eq!(
        series.len(),
        2,
        "one typed parent and one cache-labeled child"
    );
    for (key, _, _, value) in series {
        let labels: HashMap<_, _> = key
            .key()
            .labels()
            .map(|label| (label.key(), label.value()))
            .collect();
        assert_eq!(labels["environment"], "feuer_test");
        assert_eq!(labels["region"], "test_region");
        if let Some(cache) = labels.get("cache") {
            assert_eq!(*cache, "metrics_global_labels");
            assert_eq!(labels["outcome"], "memory_hit");
            assert_eq!(labels.len(), 4);
            assert_eq!(value, DebugValue::Counter(7));
        } else {
            assert_eq!(labels.len(), 2);
            assert_eq!(value, DebugValue::Counter(0));
        }
    }
}

#[test]
fn test_native_memory_registration_and_values_without_runtime() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = ::metrics::set_default_local_recorder(&recorder);
    let cache_name = "metrics_native_memory";
    // Feuer registration validates names, descriptions, types, ordered labels and buckets.
    let cache = crate::FeuerCache::memory(1 << 20, cache_name).unwrap();
    futures::executor::block_on(async {
        assert!(
            cache
                .put("key".into(), crate::OwnedBytes::new(&b"value"[..]))
                .await
        );
        assert_eq!(cache.get("key".into()).await.unwrap().as_slice(), b"value");
    });
    let values = cache_metrics(&snapshotter, &[cache_name]);
    let names: HashSet<_> = values.keys().map(|key| key.name()).collect();
    let expected: HashSet<_> = DECLARATIONS
        .iter()
        .filter(|declaration| {
            declaration.name.starts_with("feuer_memory_")
                || declaration.name.starts_with("feuer_lookup_")
                || declaration.name == "feuer_io_buffer_pool_bytes"
                || (cfg!(target_os = "linux") && declaration.name == "feuer_disk_write_queue_total")
        })
        .map(|declaration| declaration.name)
        .collect();
    assert_eq!(names, expected);
    let hit = ::metrics::Key::from_parts(
        "feuer_lookup_total",
        vec![
            ::metrics::Label::new("cache", cache_name),
            ::metrics::Label::new("outcome", "memory_hit"),
        ],
    );
    assert_eq!(values[&hit], DebugValue::Counter(1));
    let capacity = ::metrics::Key::from_parts(
        "feuer_memory_capacity_bytes",
        vec![::metrics::Label::new("cache", cache_name)],
    );
    assert_eq!(
        values[&capacity],
        DebugValue::Gauge(((1 << 20) as f64).into())
    );
    let statuses: HashSet<_> = values
        .keys()
        .filter(|key| key.name() == "feuer_io_buffer_pool_bytes")
        .map(|key| {
            assert!(
                key.labels()
                    .any(|label| label.key() == "bucket" && !label.value().is_empty())
            );
            key.labels()
                .find(|label| label.key() == "status")
                .unwrap()
                .value()
        })
        .collect();
    assert_eq!(statuses, ["idle", "used"].into());
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn test_native_disk_registration_matches_complete_inventory() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = ::metrics::set_default_local_recorder(&recorder);
    let directory = tempfile::tempdir().unwrap();
    let cache_name = "metrics_native_disk";
    let _cache = crate::FeuerIoQueues::default()
        .open(
            &super::super::config_for_test(directory.path()),
            "metrics_cache",
            cache_name,
            0,
        )
        .await
        .unwrap();
    let values = cache_metrics(&snapshotter, &[cache_name]);
    let names: HashSet<_> = values.keys().map(|key| key.name()).collect();
    assert_eq!(
        names,
        DECLARATIONS
            .iter()
            .map(|declaration| declaration.name)
            .collect()
    );
}
